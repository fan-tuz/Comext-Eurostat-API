import pandas as pd
import requests
import time
from io import StringIO

COUNTRIES = ['AT', 'BE', 'BG', 'CY', 'CZ', 'DE', 'DK', 'EE', 'GR', 'ES', 'FI',
             'FR', 'HR', 'HU', 'IE', 'IT', 'LT', 'LU', 'LV', 'MT', 'NL', 'PL',
             'PT', 'RO', 'SE', 'SI', 'SK']

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

    df_all = df_all.drop_duplicates(
        subset=['TIME_PERIOD', 'reporter', 'product', 'indicators'], keep='last'
    )

    # Pivot: una riga per (anno, paese, prodotto) con colonne PRODVAL e PRODQNT
    df_pivot = df_all.pivot_table(
        index=['TIME_PERIOD', 'reporter', 'product'],
        columns='indicators',
        values='OBS_VALUE',
        aggfunc='last'
    ).reset_index()
    df_pivot.columns.name = None  # rimuove il nome dell'asse colonne dopo pivot

    print(f"Dopo pivot_table: {len(df_pivot)}")

    df_pivot.to_csv(f'output_comext/prodcom_{YEAR_START}to{YEAR_END}_raw.csv')

    return df_pivot 


if __name__ == "__main__":
    extract_prodcom()