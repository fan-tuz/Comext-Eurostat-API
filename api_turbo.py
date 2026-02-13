import requests
import pandas as pd
import time
from pathlib import Path
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys 
from hs8_all import HS8_CHAPTER_29, HS8_CHAPTER_30

# --- CONFIGURAZIONE ---
OUTPUT_DIR = Path("./output_comext")
OUTPUT_DIR.mkdir(exist_ok=True)
START_YEAR = 2013
END_YEAR = 2024

# Example of final query structure updated February 2026
# https://ec.europa.eu/eurostat/api/comext/dissemination/sdmx/2.1/data/DS-045409/A..CN+IN+US.29011000.1.VALUE_IN_EUROS+QUANTITY_IN_100KG?startPeriod=2022&endPeriod=2022&format=SDMX-CSV


EU27_COUNTRIES = {
    "AT":'Austria', "BE":'Belgio', "BG":'Bulgaria', "HR":'Croazia',
    "CY":'Cipro', "CZ":'Repubblica Ceca', "DK":'Danimarca', "EE":'Estonia',
    "FI":'Finlandia', "FR":'Francia', "DE":'Germania', 'GR':'Grecia',
    "HU":'Ungheria', "IE":'Irlanda', "IT":'Italia', "LV":'Lettonia',
    "LT":'Lituania', "LU":'Lussemburgo', "MT":'Malta', "NL":'Paesi Bassi',
    "PL":'Polonia', "PT":'Portogallo', "RO":'Romania', "SK":'Slovacchia',
    "SI":'Slovenia', "ES":'Spagna', "SE":'Svezia'
}

ALL_HS8_PRODUCTS = {**HS8_CHAPTER_29, **HS8_CHAPTER_30}

KEY_PARTNERS = {
    "CN": "Cina","IN": "India","US": "USA","CH": "Svizzera","KR": "Corea del Sud","JP": "Giappone","GB": "Regno Unito",
    "IL": "Israele","SG": "Singapore","VN": "Vietnam","TH": "Thailandia","ID": "Indonesia","BD": "Bangladesh","MY": "Malaysia",
    "PH": "Filippine","SA": "Arabia Saudita","AE": "Emirati Arabi Uniti","NO": "Norvegia","CA": "Canada","AU": "Australia",
    "NZ": "Nuova Zelanda","TR": "Turchia","TW": "Taiwan","RU": "Russia","BY": "Bielorussia","BR": "Brasile",
    "MX": "Messico","AR": "Argentina","ZA": "Sudafrica","EG": "Egitto",
    "EXT_EU27_2020":'World',
}

class ComextDownloader:
    API_BASE_URL = "https://ec.europa.eu/eurostat/api/comext/dissemination/sdmx/2.1/data"
    DATASET = "DS-045409"
    INDICATORS = "VALUE_IN_EUROS+QUANTITY_IN_100KG"  # entrambi in una chiamata sola
    
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0'})
        self.failed_queries = []

    def download_product_batch(self, partner_str, product_code, flow_direction, start_year, end_year):
        # reporter = '..' (wildcard): tutti i paesi EU in una chiamata sola
        query_filter = f"A..{partner_str}.{product_code}.{flow_direction}.{self.INDICATORS}"
        url = f"{self.API_BASE_URL}/{self.DATASET}/{query_filter}"
        params = {"startPeriod": str(start_year), "endPeriod": str(end_year), "format": "SDMX-CSV"}
        
        try:
            response = self.session.get(url, params=params, timeout=180)
            if response.status_code == 200:
                df = pd.read_csv(StringIO(response.text))
                return df
            return None
        except Exception as e:
            self.failed_queries.append((product_code, str(e)))
            return None

    def download_all_data(self, countries, exporters, products, flow_direction, start_year, end_year):
        all_dfs = []
        partner_str = "+".join(exporters.keys())
        total_steps = len(products)
        current_step = 0

        print(f"Esecuzione: {total_steps} prodotti")
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                executor.submit(self.download_product_batch, partner_str, hs8, flow_direction, start_year, end_year): hs8
                for hs8 in products.keys()
            }
            
            for future in as_completed(futures):
                current_step += 1
                res = future.result()
                if res is not None:
                    all_dfs.append(res)
                if current_step % 20 == 0:
                    print(f"  Query: {current_step}/{total_steps}")

        if not all_dfs:
            return None

        df_all = pd.concat(all_dfs, ignore_index=True)

        # Filtra solo i paesi EU27 di interesse (il wildcard restituisce tutti i reporter)
        df_all = df_all[df_all['reporter'].isin(countries.keys())]

        df_all["reporter_name"] = df_all["reporter"].map(EU27_COUNTRIES)
        df_all["partner_name"] = df_all["partner"].map(KEY_PARTNERS)
        df_all["product_description"] = df_all["product"].astype(str).map(products)

        return df_all

def main():

    flow_direction = sys.argv[1] if len(sys.argv) > 1 else print("Usa: python script.py [1|2] (1=import, 2=export)")
    if flow_direction not in ["1", "2"]:
        print("Usa: python script.py [1|2] (1=import, 2=export)")
        sys.exit(1)

    start_time = time.time()
    downloader = ComextDownloader(OUTPUT_DIR)
    raw_df = downloader.download_all_data(EU27_COUNTRIES, KEY_PARTNERS, ALL_HS8_PRODUCTS, flow_direction, START_YEAR, END_YEAR)
    
    if raw_df is not None:
        
        index_cols = ['TIME_PERIOD', 'reporter', 'reporter_name', 'partner', 'partner_name', 'product', 'product_description']
        
        df_pivot = raw_df.pivot_table(
            index=index_cols, 
            columns='indicators', 
            values='OBS_VALUE', 
            aggfunc='first'
        ).reset_index()

        df_pivot.columns.name = None 

        if flow_direction == '1':
            final_path = OUTPUT_DIR / f"comext_imports_hs8_{START_YEAR}_{END_YEAR}.csv"
        else:
            final_path = OUTPUT_DIR / f"comext_exports_hs8_{START_YEAR}_{END_YEAR}.csv"

        print(f"Number of rows of final dataset: {len(df_pivot)}")
        df_pivot.to_csv(final_path, index=False)
        print(f"File salvato con successo: {final_path}")
        print(f"Esempio colonne: {df_pivot.columns.tolist()}")
    else:
        print("Nessun dato trovato.")
    
    print(f"\nTempo totale: {(time.time() - start_time)/60:.1f} minuti")

if __name__ == "__main__":
    main()