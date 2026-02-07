#Python3.13.11 - Starting from the resulting dataset from api_turbo.py, let's compute the incidence of key partners on the total import volume.
# key_partners_incidence.py

import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt

KEY_PARTNERS = {
    "CN": "Cina",
    "IN": "India", 
    "US": "USA",
    "CH": "Svizzera",
    "KR": "Corea del Sud",
    "JP": "Giappone",
    "GB": "Regno Unito", # Extra-EU only after 2020!
    "IL": "Israele",
    "SG": "Singapore",
}

def compute_incidence(path=Path("output_comext/comext_FINALE_WIDE_2013_2024.csv")):
    
    df=pd.read_csv(path)
    years = ['2013','2014','2015','2016','2017','2018','2019','2020','2021','2022','2023','2024']

    incidence_list_key = []
    incidence_list_all = []

    nonEUcode = 'EXT_EU27_2020'

    for year in years:

        year_filter = df['TIME_PERIOD'].astype(str).str.contains(year)
        dfByYear = df.loc[year_filter]

        # 9 key partners 

        key_filter = dfByYear['partner'].isin(KEY_PARTNERS.keys())
        key_partners_volume = dfByYear.loc[key_filter]['VALUE_IN_EUROS'].sum()

        # All 30 downloaded partners

        all_30_volume = dfByYear.loc[dfByYear['partner'] != nonEUcode]['VALUE_IN_EUROS'].sum()

        world_volume = dfByYear[dfByYear['partner'] == nonEUcode]['VALUE_IN_EUROS'].sum()

        try:
            incidence_year_key = key_partners_volume / world_volume
            incidence_list_key.append(incidence_year_key)

            incidence_year_all = all_30_volume / world_volume
            incidence_list_all.append(incidence_year_all)

        except ZeroDivisionError:
            print(f'ZeroDivisionError in {year}')
            continue
    
    plt.figure(figsize=(10, 6))
    plt.plot(years, incidence_list_key, marker='o', linewidth=2.5, 
             color='#2E86AB', label='TOP 9 partner')
    plt.plot(years, incidence_list_all, marker='s', linewidth=2.5, 
             color='#A23B72', label='Tutti i 30 partner')
    
    plt.xlabel('Anno', fontsize=12)
    plt.ylabel('Incidenza su import totale UE27', fontsize=12)
    plt.title('Copertura import UE27 - Prodotti chimici e farmaceutici', fontsize=14, fontweight='bold')
    plt.legend(loc='best', fontsize=11)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('graphs/key_partners_incidence.png', dpi=300)
    plt.close()

if __name__ == '__main__':
    compute_incidence()