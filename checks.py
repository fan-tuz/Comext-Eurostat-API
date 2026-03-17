import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from pathlib import Path

# configurazione

GRAPHS_DIR = Path("./graphs")
GRAPHS_DIR.mkdir(exist_ok=True)
REPORT_PATH = Path("./quality_check_report.txt")

EU27 = ['AT','BE','BG','CY','CZ','DE','DK','EE','GR','ES','FI',
        'FR','HR','HU','IE','IT','LT','LU','LV','MT','NL','PL',
        'PT','RO','SE','SI','SK']

KEY_PARTNERS = ['CN','IN','US','CH','KR','JP','GB','IL','SG','VN',
                'TH','ID','BD','MY','PH','SA','AE','NO','CA','AU',
                'NZ','TR','TW','RU','BY','BR','MX','AR','ZA','EG',
                'MA','EXT_EU27_2020']

imp_ = pd.read_csv('output_comext/R_imp_mapped_cn8topc8_2013to2025.csv', dtype={'PC8': str})
exp_ = pd.read_csv('output_comext/R_exp_mapped_cn8topc8_2013to2025.csv', dtype={'PC8': str})
prodcom_ = pd.read_csv('output_comext/prodcom_2013to2025_raw.csv')
exp_hs4_ = pd.read_csv('output_comext/comext_exports_2013_2024_hs4.csv')
imp_hs4_ = pd.read_csv('output_comext/comext_imports_2013_2024_hs4.csv')



def slice_prodcom(imp, exp, prodcom):
    # Find all PC8 appearing in IM/EX datasets in order to slice the PRODCOM dataset downloaded in prodcom_api.py
    pc8_imp = imp['PC8'].unique().tolist()
    print(f'{len(pc8_imp)} unique PC8 in import dataset')
    pc8_exp = exp['PC8'].unique().tolist()
    print(f'{len(pc8_exp)} unique PC8 in export dataset')

    for pc8 in pc8_imp:
        if pc8 in pc8_exp:
            continue
        else:
            pc8_exp.append(pc8)
    pc8_list = pc8_exp

    print(f"Total relevant PC8 mapped in IM/EX datasets: {len(pc8_list)}")

    relevant_prodcom = prodcom[prodcom['product'].isin(pc8_list)]
    relevant_prodcom.to_csv('output_comext/prodcom_2013to2025_29+30.csv')

    print(f'Exported prodcom dataset with following shapes: {relevant_prodcom.shape}')

    hs8_imp = set(imp['CN8'].unique())
    hs8_exp = set(exp['CN8'].unique())

    only_in_imp = sorted(hs8_imp - hs8_exp)
    only_in_exp = sorted(hs8_exp - hs8_imp)

    print(f"Codici presenti solo in Import:  {len(only_in_imp)}")
    print(f"Codici presenti solo in Export:  {len(only_in_exp)}")
    print("Solo in Import:")
    print(str(only_in_imp))
    print("Solo in Export:")
    print(str(only_in_exp))

    return relevant_prodcom

#   COPERTURA REPORTER E PARTNER PER ANNO

def coverage_check(imp, exp, relevant_prodcom):
    datasets = {
        "Import HS8":   (imp,     'reporter', 'partner'),
        "Export HS8":   (exp,     'reporter', 'partner'),
        "PRODCOM raw":  (relevant_prodcom, 'reporter', None),
    }

    for ds_label, (df, rep_col, par_col) in datasets.items():
        print(f"\n{ds_label}:")

        # Reporter mancanti per anno
        years = sorted(df['TIME_PERIOD'].unique())
        missing_rep = {}
        for y in years:
            present = set(df[df['TIME_PERIOD'] == y][rep_col].unique())
            missing = set(EU27) - present
            if missing:
                missing_rep[y] = sorted(missing)

        if missing_rep:
            print("  Reporter mancanti per anno:")
            for y, m in missing_rep.items():
                print(f"    {y}: {m}")
        else:
            print(f"  Tutti i {len(EU27)} reporter presenti in ogni anno.")

        # Partner mancanti per anno (solo per import/export)
        if par_col:
            missing_par = {}
            for y in years:
                present = set(df[df['TIME_PERIOD'] == y][par_col].unique())
                missing = set(KEY_PARTNERS) - present
                if missing:
                    missing_par[y] = sorted(missing)

            if missing_par:
                print("  Partner mancanti per anno:")
                for y, m in missing_par.items():
                    print(f"    {y}: {m}")
            else:
                print(f"  Tutti i {len(KEY_PARTNERS)} partner presenti in ogni anno.")

# GRAFICO: Confronto totali CN8 vs HS4 per gruppo HS4

def plot(exp_hs4, imp_hs4, imp, exp):

    # Estrai codice HS4 dai dataset HS4 (primi 4 caratteri della colonna product)
    exp_hs4['HS4'] = exp_hs4['product'].astype(str).str[:4]
    imp_hs4['HS4'] = imp_hs4['product'].astype(str).str[:4]

    # Totali per HS4 dai dataset HS4
    exp_hs4_totals = exp_hs4.groupby('HS4')['VALUE_IN_EUROS'].sum()
    imp_hs4_totals = imp_hs4.groupby('HS4')['VALUE_IN_EUROS'].sum()

    # Estrai HS4 dai dataset CN8 (primi 4 caratteri di CN8)
    exp_cn8 = exp.copy()
    imp_cn8 = imp.copy()

    # ignore new data in the comparison
    filt_exp = (exp_cn8['TIME_PERIOD'] != 2025) & (exp_cn8['reporter'] != 'MA')
    filt_imp = (imp_cn8['TIME_PERIOD'] != 2025) & (imp_cn8['reporter'] != 'MA')
    exp_cn8 = exp_cn8[filt_exp]
    imp_cn8 = imp_cn8[filt_imp]

    exp_cn8['HS4'] = exp_cn8['CN8'].astype(str).str[:4]
    imp_cn8['HS4'] = imp_cn8['CN8'].astype(str).str[:4]

    exp_cn8_totals = exp_cn8.groupby('HS4')['VALUE_IN_EUROS'].sum()
    imp_cn8_totals = imp_cn8.groupby('HS4')['VALUE_IN_EUROS'].sum()

    # Unisci tutto in un unico DataFrame per HS4 codes presenti in almeno uno dei dataset
    all_hs4 = sorted(set(exp_hs4_totals.index) | set(imp_hs4_totals.index) |
                    set(exp_cn8_totals.index) | set(imp_cn8_totals.index))

    compare = pd.DataFrame({
        'EXP_CN8': [exp_cn8_totals.get(h, 0) for h in all_hs4],
        'EXP_HS4': [exp_hs4_totals.get(h, 0) for h in all_hs4],
        'IMP_CN8': [imp_cn8_totals.get(h, 0) for h in all_hs4],
        'IMP_HS4': [imp_hs4_totals.get(h, 0) for h in all_hs4],
    }, index=all_hs4)

    # Grafico Export
    fig, axes = plt.subplots(2, 1, figsize=(22, 14))

    x = range(len(all_hs4))
    w = 0.4

    for ax, (col_cn8, col_hs4, label) in zip(axes, [
        ('EXP_CN8', 'EXP_HS4', 'Export'),
        ('IMP_CN8', 'IMP_HS4', 'Import'),
    ]):
        bars1 = ax.bar([i - w/2 for i in x], compare[col_cn8] / 1e9, width=w,
                    label='CN8 mapped', color='steelblue', alpha=0.85)
        bars2 = ax.bar([i + w/2 for i in x], compare[col_hs4] / 1e9, width=w,
                    label='HS4 raw', color='tomato', alpha=0.85)
        ax.set_title(f'{label}: totali CN8 vs HS4 per gruppo HS4', fontsize=13)
        ax.set_xticks(list(x))
        ax.set_xticklabels(all_hs4, rotation=90, fontsize=7)
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f'{v:.1f}B'))
        ax.set_ylabel('Valore (miliardi €)')
        ax.legend()
        ax.grid(axis='y', alpha=0.3)

    plt.tight_layout()
    out_path = GRAPHS_DIR / 'cn8_vs_hs4_totals_by_hs4group_.png'
    plt.savefig(out_path, dpi=150)
    plt.close()
    print(f"\nGrafico salvato in: {out_path}")


if __name__ == '__main__':

    rel_prodcom = slice_prodcom(imp_, exp_, prodcom_)
    coverage_check(imp_, exp_, rel_prodcom)
    plot(exp_hs4_, imp_hs4_, imp_, exp_)