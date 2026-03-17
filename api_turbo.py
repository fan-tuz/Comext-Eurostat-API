import requests
import pandas as pd
import time
from pathlib import Path
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys

# CONFIGURAZIONE
OUTPUT_DIR = Path("./output_comext")
OUTPUT_DIR.mkdir(exist_ok=True)
START_YEAR = 2013
END_YEAR = 2025

# Example of final query structure updated February 2026
# https://ec.europa.eu/eurostat/api/comext/dissemination/sdmx/2.1/data/DS-045409/A.IT.CN+IN+US..1.QUANTITY_IN_100KG?startPeriod=2022&endPeriod=2022&format=SDMX-CSV


EU27_COUNTRIES = {
    "AT":'Austria', "BE":'Belgio', "BG":'Bulgaria', "HR":'Croazia',
    "CY":'Cipro', "CZ":'Repubblica Ceca', "DK":'Danimarca', "EE":'Estonia',
    "FI":'Finlandia', "FR":'Francia', "DE":'Germania', 'GR':'Grecia',
    "HU":'Ungheria', "IE":'Irlanda', "IT":'Italia', "LV":'Lettonia',
    "LT":'Lituania', "LU":'Lussemburgo', "MT":'Malta', "NL":'Paesi Bassi',
    "PL":'Polonia', "PT":'Portogallo', "RO":'Romania', "SK":'Slovacchia',
    "SI":'Slovenia', "ES":'Spagna', "SE":'Svezia'
}

KEY_PARTNERS = {
    "CN": "Cina","IN": "India","US": "USA","CH": "Svizzera","KR": "Corea del Sud","JP": "Giappone","GB": "Regno Unito",
    "IL": "Israele","SG": "Singapore","VN": "Vietnam","TH": "Thailandia","ID": "Indonesia","BD": "Bangladesh","MY": "Malaysia",
    "PH": "Filippine","SA": "Arabia Saudita","AE": "Emirati Arabi Uniti","NO": "Norvegia","CA": "Canada","AU": "Australia",
    "NZ": "Nuova Zelanda","TR": "Turchia","TW": "Taiwan","RU": "Russia","BY": "Bielorussia","BR": "Brasile",
    "MX": "Messico","AR": "Argentina","ZA": "Sudafrica","EG": "Egitto","MA": 'Marocco',
    "EXT_EU27_2020":'World',
}

class ComextDownloader:
    API_BASE_URL = "https://ec.europa.eu/eurostat/api/comext/dissemination/sdmx/2.1/data"
    DATASET = "DS-045409"
    # Riuniti in una stringa unica: un reporter per chiamata gestisce la dimensione
    INDICATORS = ["VALUE_IN_EUROS", "QUANTITY_IN_100KG"]
    
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0'})
        self.failed_queries = []

    def download_product_batch(self, reporter, partner_str, flow_direction, indicator, year):

        query_filter = f"A.{reporter}.{partner_str}..{flow_direction}.{indicator}"
        url = f"{self.API_BASE_URL}/{self.DATASET}/{query_filter}"
        # Anno singolo per contenere la dimensione della risposta
        params = {"startPeriod": str(year), "endPeriod": str(year), "format": "SDMX-CSV"}
        
        try:
            response = self.session.get(url, params=params, timeout=180)
            if response.status_code == 200:

                df = pd.read_csv(StringIO(response.text))

                # Filtri necessari
                df = df[df['product'].str.len() == 8] # '+' wildcard downloads HS4 and HS6 as well.
                df = df[df['product'].astype(str).str[:2].isin(['29', '30'])]
                df = df[df['partner'].isin(KEY_PARTNERS.keys())]
                return df
            
            print(f"  HTTP {response.status_code} — {reporter} {year}")
            return None
        except Exception as e:
            self.failed_queries.append((reporter, year, str(e)))
            return None

    def download_all_data(self, countries, exporters, flow_direction, start_year, end_year):
        all_dfs = []
        partner_str = "+".join(exporters.keys())
        # Loop su anni x reporter x indicator
        total_steps = (end_year - start_year + 1) * len(countries) * len(self.INDICATORS)
        current_step = 0

        print(f"Esecuzione: {total_steps} chiamate ({end_year - start_year + 1} anni x {len(countries)} reporter x {len(self.INDICATORS)} indicatori)")

        # futures su (reporter, indicator, year)
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                executor.submit(self.download_product_batch, reporter, partner_str, flow_direction, indicator, year): (reporter, indicator, year)
                for year in range(start_year, end_year + 1)
                for reporter in countries.keys()
                for indicator in self.INDICATORS
            }

            for future in as_completed(futures):
                reporter, indicator, year = futures[future]
                current_step += 1
                res = future.result()
                if res is not None:
                    all_dfs.append(res)
                if current_step % 20 == 0:
                    print(f"  [{current_step}/{total_steps}] — {'OK' if res is not None else 'FALLITO'}")

        if not all_dfs:
            return None

        df_all = pd.concat(all_dfs, ignore_index=True)

        df_all["reporter_name"] = df_all["reporter"].map(EU27_COUNTRIES)
        df_all["partner_name"] = df_all["partner"].map(KEY_PARTNERS)

        return df_all

def main():

    flow_direction = sys.argv[1] if len(sys.argv) > 1 else print("Usa: python script.py [1|2] (1=import, 2=export)")
    if flow_direction not in ["1", "2"]:
        print("Usa: python script.py [1|2] (1=import, 2=export)")
        sys.exit(1)

    start_time = time.time()
    downloader = ComextDownloader(OUTPUT_DIR)
    raw_df = downloader.download_all_data(EU27_COUNTRIES, KEY_PARTNERS, flow_direction, START_YEAR, END_YEAR)
    
    if raw_df is not None:

        index_cols = ['TIME_PERIOD', 'reporter', 'reporter_name', 'partner', 'partner_name', 'product']
        
        df_pivot = raw_df.pivot_table(
            index=index_cols, 
            columns='indicators', 
            values='OBS_VALUE', 
            aggfunc='first'
        ).reset_index()

        df_pivot.columns.name = None 

        if flow_direction == '1':
            final_path = OUTPUT_DIR / f"comext_imports_hs8_{START_YEAR}_{END_YEAR}_raw.csv"
        else:
            final_path = OUTPUT_DIR / f"comext_exports_hs8_{START_YEAR}_{END_YEAR}_raw.csv"

        print(f"Number of rows of final dataset: {len(df_pivot)}")
        df_pivot.to_csv(final_path, index=False)
        print(f"File salvato con successo: {final_path}")
        print(f"Esempio colonne: {df_pivot.columns.tolist()}")
    else:
        print("Nessun dato trovato.")
    
    print(f"\nTempo totale: {(time.time() - start_time)/60:.1f} minuti")

if __name__ == "__main__":
    main()