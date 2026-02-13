import pandas as pd
import requests
import time
from io import StringIO

COUNTRIES = ['AT', 'BE', 'BG', 'CY', 'CZ', 'DE', 'DK', 'EE', 'EL', 'ES', 'FI',
             'FR', 'HR', 'HU', 'IE', 'IT', 'LT', 'LU', 'LV', 'MT', 'NL', 'PL',
             'PT', 'RO', 'SE', 'SI', 'SK']

MAPPING_FILE = "input_data/mapping_cpa_hs.tsv"

YEAR_START = 2013
YEAR_END = 2024
DATASET = 'DS-059358'
INDICATORS = ['PRODVAL', 'PRODQNT']


def extract_prodcom():
    """
    Scarica PRODVAL (valore) e PRODQNT (quantità) per tutti i codici PRODCOM disponibili per ciascun paese, anni 2013-2024.
    Usa '..' (wildcard) per i codici prodotto: nessuna lista esterna necessaria.
    """
    risultati_raw = []

    print(f"Esecuzione: {len(COUNTRIES)} paesi...")

    for idx, paese in enumerate(COUNTRIES):
        print(f"  [{idx+1}/{len(COUNTRIES)}] {paese}...")

        indicators_str = '+'.join(INDICATORS)

        url = (
            f"https://ec.europa.eu/eurostat/api/comext/dissemination/sdmx/2.1/data/"
            f"{DATASET}/A.{paese}..{indicators_str}"
            f"?startPeriod={YEAR_START}&endPeriod={YEAR_END}&format=SDMX-CSV"
        )

        try:
            r = requests.get(url, timeout=120)  # timeout alto: risposta grande

            if r.status_code != 200:
                print(f"    HTTP {r.status_code} — saltato")
                continue

            if 'OBS_VALUE' not in r.text:
                print(f"    Risposta vuota o non valida — saltato")
                continue

            df_temp = pd.read_csv(StringIO(r.text))
            risultati_raw.append(df_temp)
            print(f"    OK — {len(df_temp)} righe")

        except Exception as e:
            print(f"    Errore: {e}")
            continue

        time.sleep(0.5)

    if not risultati_raw:
        print("Errore: nessun dato scaricato.")
        return

    # AGGREGAZIONE
    df_all = pd.concat(risultati_raw, ignore_index=True)
    print(f"\nTotale righe scaricate: {len(df_all)}")

    # Colonne API: DATAFLOW, LAST UPDATE, freq, reporter, product, indicators, TIME_PERIOD, OBS_VALUE
    print(df_all['indicators'].value_counts())

    df_all = df_all.drop_duplicates(
        subset=['TIME_PERIOD', 'reporter', 'product', 'indicators'], keep='last'
    )

    print(f"Dopo drop_duplicates: {len(df_all)}")

    # Pivot: una riga per (anno, paese, prodotto) con colonne PRODVAL e PRODQNT
    df_pivot = df_all.pivot_table(
        index=['TIME_PERIOD', 'reporter', 'product'],
        columns='indicators',
        values='OBS_VALUE',
        aggfunc='last'
    ).reset_index()
    df_pivot.columns.name = None  # rimuove il nome dell'asse colonne dopo pivot

    print(f"Dopo pivot_table: {len(df_pivot)}")

    df_pivot = df_pivot.rename(columns={
        'reporter': 'COUNTRY',
        'product':  'PRCCODE',
        'PRODVAL':  'sold_value_eur',
        'PRODQNT':  'sold_quantity',
    })

    # Converti a numerico (i flag tipo ':C', ':' diventano NaN)
    df_pivot['sold_value_eur'] = pd.to_numeric(df_pivot['sold_value_eur'], errors='coerce')
    df_pivot['sold_quantity']  = pd.to_numeric(df_pivot['sold_quantity'],  errors='coerce')

    n_flag_val = df_pivot['sold_value_eur'].isna().sum()
    n_flag_qnt = df_pivot['sold_quantity'].isna().sum()
    print(f"Righe con flag/confidenziale: valore={n_flag_val}, quantità={n_flag_qnt}")

    # MAPPING PRODCOM to HS8 through CPA6 (ie first 6 digits of PRODCOM code)
    try:
        mapping = pd.read_csv(MAPPING_FILE, sep='\t', dtype=str)
        mapping['HS8_clean'] = mapping['HS_2022_CODE'].str.replace(r'\s+', '', regex=True)

        cpa6_of_interest = set(
            mapping.loc[
                mapping['HS8_clean'].str.startswith(('29', '30'), na=False),
                'CPA_Ver_22_ID'
            ]
        )
        print(f"CPA6 che mappano a HS cap. 29/30: {len(cpa6_of_interest)}")
        
        df_pivot['cpa6'] = df_pivot['PRCCODE'].astype(str).str[:6]
        df_pivot_filtered = df_pivot[df_pivot['cpa6'].isin(cpa6_of_interest)].copy()
        print(f"Righe PRODCOM di interesse: {len(df_pivot_filtered)} / {len(df_pivot)}")
        
        df_final = pd.merge(
            df_pivot_filtered,
            mapping[['CPA_Ver_22_ID', 'HS8_clean']],
            left_on='cpa6', right_on='CPA_Ver_22_ID',
            how='left'
        )

        n_unmatched = df_final['HS8_clean'].isna().sum()
        print(f"Righe senza match HS8: {n_unmatched} ({n_unmatched/len(df_final)*100:.1f}%)")

        # --- TRADE WEIGHTING ---
        # Carica import Comext e aggrega per (anno, paese, hs8) ignorando i partner
        imports = pd.read_csv("output_comext/comext_imports_hs8_2013_2024.csv", dtype={'product': str})
        imports_agg = (
            imports.groupby(['TIME_PERIOD', 'reporter', 'product'])['VALUE_IN_EUROS']
            .sum()
            .reset_index()
            .rename(columns={'reporter': 'COUNTRY', 'product': 'HS8_clean', 'VALUE_IN_EUROS': 'import_value'})
        )

        # Calcola peso di ciascun HS8 sul totale import del suo CPA6, per (anno, paese)
        df_final = df_final.merge(imports_agg, on=['TIME_PERIOD', 'COUNTRY', 'HS8_clean'], how='left')
        df_final['import_value'] = df_final['import_value'].fillna(0)

        cpa6_total = df_final.groupby(['TIME_PERIOD', 'COUNTRY', 'cpa6'])['import_value'].transform('sum')
        df_final['weight'] = df_final['import_value'] / cpa6_total.replace(0, float('nan'))

        # Fallback: se tutti gli HS8 di un CPA6 hanno import=0, distribuisci uniformemente
        n_hs8_per_cpa6 = df_final.groupby(['TIME_PERIOD', 'COUNTRY', 'cpa6'])['HS8_clean'].transform('count')
        df_final['weight'] = df_final['weight'].fillna(1 / n_hs8_per_cpa6)

        # Applica il peso
        df_final['sold_value_eur'] = df_final['sold_value_eur'] * df_final['weight']
        df_final['sold_quantity']  = df_final['sold_quantity']  * df_final['weight']

        # Somma per anno, paese, HS8
        output = (
            df_final.dropna(subset=['HS8_clean'])
            .groupby(['TIME_PERIOD', 'COUNTRY', 'HS8_clean'])
            .agg(
                sold_value_eur=('sold_value_eur', lambda x: x.sum(min_count=1)), # if all hs8 under that combination are NaNs, NaN is returned instead of 0
                sold_quantity=('sold_quantity',   lambda x: x.sum(min_count=1))
            )
            .reset_index()
        )

        zero_filter = ((output['sold_value_eur'] == 0) & (output['sold_quantity'] == 0 )
                       ) | ((output['sold_value_eur'] == 0) & (output['sold_quantity'].isna())
                            ) | ((output['sold_value_eur'].isna()) & (output['sold_quantity'] == 0 )
                                 )
        output_clear = output[~zero_filter]

        print(f'Obs removed due to 0 values: {len(output) - len(output_clear)}')

        output_clear.to_csv("output_comext/EUProd_val&qnty_2013to2024_hs8.csv", index=False)
        print(f"\nFatto. 'EUProd_val&qnty_2013to2024_hs8.csv.csv' — {len(output_clear)} righe.")

    except Exception as e:
        print(f"\nDati scaricati, errore nel mapping: {e}")
        df_pivot.to_csv("backup_dati_scaricati.csv", index=False)
        print("Salvato backup in 'backup_dati_scaricati.csv'.")


if __name__ == "__main__":
    extract_prodcom()