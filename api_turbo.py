import requests
import pandas as pd
import time
import sys
from pathlib import Path
from datetime import datetime
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CONFIGURAZIONE ---
OUTPUT_DIR = Path("./output_comext")
OUTPUT_DIR.mkdir(exist_ok=True)
START_YEAR = 2013
END_YEAR = 2024

EU27_COUNTRIES = {
    "AT":'Austria', "BE":'Belgio', "BG":'Bulgaria', "HR":'Croazia',
    "CY":'Cipro', "CZ":'Repubblica Ceca', "DK":'Danimarca', "EE":'Estonia',
    "FI":'Finlandia', "FR":'Francia', "DE":'Germania', "EL":'Grecia',
    "HU":'Ungheria', "IE":'Irlanda', "IT":'Italia', "LV":'Lettonia',
    "LT":'Lituania', "LU":'Lussemburgo', "MT":'Malta', "NL":'Paesi Bassi',
    "PL":'Polonia', "PT":'Portogallo', "RO":'Romania', "SK":'Slovacchia',
    "SI":'Slovenia', "ES":'Spagna', "SE":'Svezia'
}

HS4_CHAPTER_29 = {
    "2901": "Hydrocarbons acyclic", "2902": "Hydrocarbons cyclic", "2903": "Halogenated derivatives",
    "2904": "Sulphonated derivatives", "2905": "Acyclic alcohols", "2906": "Cyclic alcohols",
    "2907": "Phenols", "2908": "Phenol derivatives", "2909": "Ethers", "2910": "Epoxides",
    "2911": "Acetals", "2912": "Aldehydes", "2913": "Halogenated aldehydes", "2914": "Ketones/quinones",
    "2915": "Saturated acyclic monocarboxylic acids", "2916": "Unsaturated acyclic monocarboxylic acids",
    "2917": "Polycarboxylic acids", "2918": "Carboxylic acids (additional)", "2919": "Phosphoric esters",
    "2920": "Esters of other inorganic acids", "2921": "Amine-function compounds", "2922": "Oxygen-function amino-compounds",
    "2923": "Quaternary ammonium salts", "2924": "Carboxyamide-function compounds", "2925": "Carboxyimide/imine-function compounds",
    "2926": "Nitrile-function compounds", "2927": "Diazo/azo/azoxy-compounds", "2928": "Organic derivatives of hydrazine",
    "2929": "Compounds with other nitrogen function", "2930": "Organo-sulphur compounds", "2931": "Other organo-inorganic compounds",
    "2932": "Heterocyclic compounds (O only)", "2933": "Heterocyclic compounds (N only)", "2934": "Nucleic acids and their salts",
    "2935": "Sulphonamides", "2936": "Provitamins and vitamins", "2937": "Hormones/polypeptides",
    "2938": "Glycosides", "2939": "Alkaloids", "2940": "Sugars (chemically pure)", "2941": "Antibiotics", "2942": "Other organic compounds",
}

HS4_CHAPTER_30 = {
    "3001": "Glands/organs dried", "3002": "Blood/antisera/vaccines/toxins", "3003": "Medicaments (not in dosage)",
    "3004": "Medicaments (in dosage)", "3005": "Wadding/gauze/bandages", "3006": "Pharmaceutical preparations",
}

ALL_HS4_PRODUCTS = {**HS4_CHAPTER_29, **HS4_CHAPTER_30}

KEY_PARTNERS = {
    "CN": "Cina", "IN": "India", "US": "USA", "CH": "Svizzera",
    "KR": "Corea del Sud", "JP": "Giappone", "GB": "Regno Unito",
    "IL": "Israele", "SG": "Singapore",
}

class ComextDownloader:
    API_BASE_URL = "https://ec.europa.eu/eurostat/api/comext/dissemination/sdmx/2.1/data"
    DATASET = "DS-045409"
    
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0'})
        self.INDICATORS = ["VALUE_IN_EUROS", "QUANTITY_IN_100KG"]
        self.failed_queries = []

    def download_product_batch(self, reporter, partner_str, product_code, indicator, start_year, end_year):
        query_filter = f"A.{reporter}.{partner_str}.{product_code}.1.{indicator}"
        url = f"{self.API_BASE_URL}/{self.DATASET}/{query_filter}"
        params = {"startPeriod": str(start_year), "endPeriod": str(end_year), "format": "SDMX-CSV"}
        
        try:
            response = self.session.get(url, params=params, timeout=180)
            if response.status_code == 200:
                df = pd.read_csv(StringIO(response.text))
                return df
            return None
        except Exception as e:
            self.failed_queries.append((reporter, product_code, str(e)))
            return None

    def download_all_data(self, countries, exporters, products, start_year, end_year):
        all_dfs = []
        partner_str = "+".join(exporters.keys())
        total_steps = len(countries) * len(products) * len(self.INDICATORS)
        current_step = 0
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            for reporter_code in countries:
                reporter_name = EU27_COUNTRIES[reporter_code]
                print(f"\n--- In corso: {reporter_name} ---")
                
                futures = [
                    executor.submit(self.download_product_batch, reporter_code, partner_str, hs4, ind, start_year, end_year)
                    for hs4 in products.keys() for ind in self.INDICATORS
                ]
                
                country_results = []
                for future in as_completed(futures):
                    current_step += 1
                    res = future.result()
                    if res is not None: country_results.append(res)
                    if current_step % 20 == 0: print(f"Query: {current_step}/{total_steps}")

                if country_results:
                    df_c = pd.concat(country_results, ignore_index=True)
                    df_c["reporter_name"] = reporter_name
                    df_c["partner_name"] = df_c["partner"].map(KEY_PARTNERS)
                    df_c["product_description"] = df_c["product"].astype(str).map(products)
                    all_dfs.append(df_c)
                
                time.sleep(1)

        return pd.concat(all_dfs, ignore_index=True) if all_dfs else None

def main():
    downloader = ComextDownloader(OUTPUT_DIR)
    raw_df = downloader.download_all_data(EU27_COUNTRIES, KEY_PARTNERS, ALL_HS4_PRODUCTS, START_YEAR, END_YEAR)
    
    if raw_df is not None:
        print("\n--- Trasformazione dati (Pivot Valore + Quantità) ---")
        
        # LOGICA PIVOT: Sposta gli indicatori da righe a colonne
        # Identifichiamo le colonne che devono restare fisse
        index_cols = ['TIME_PERIOD', 'reporter', 'reporter_name', 'partner', 'partner_name', 'product', 'product_description']
        
        # Creazione colonne separate per Valore e Quantità
        df_pivot = raw_df.pivot_table(
            index=index_cols, 
            columns='indicators', 
            values='OBS_VALUE', 
            aggfunc='first'
        ).reset_index()

        # Pulizia nomi colonne pivot
        df_pivot.columns.name = None 
        
        final_path = OUTPUT_DIR / f"comext_FINALE_WIDE_{START_YEAR}_{END_YEAR}.csv"
        df_pivot.to_csv(final_path, index=False)
        print(f"✔ File salvato con successo: {final_path}")
        print(f"Esempio colonne: {df_pivot.columns.tolist()}")
    else:
        print("Nessun dato trovato.")

if __name__ == "__main__":
    main()