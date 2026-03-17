# EU Trade & Production Data Pipeline — Comext / Prodcom (2013–2025)

Pipeline per il download, la concordanza e l'analisi dei dati di commercio estero UE27 (Comext) e produzione industriale (Prodcom) per i settori chimico (HS/CN capitolo 29) e farmaceutico (HS/CN capitolo 30). Copertura: tutti i 27 stati membri UE come reporter, 31 partner commerciali chiave + totali extra UE (`EXT_EU27_2020`), anni 2013–2025.

---

## Struttura del progetto

```
eurostat/files/
├── api_turbo.py               # Download Comext HS8 import/export via API Eurostat
├── prodcom_api.py             # Download Prodcom (PRODVAL + PRODQNT) via API Eurostat
├── concordance_PC8_CN8.R      # Concordanza CN8 → PC8 (R, pacchetto harmonizer + CBS 2025)
├── checks.py                  # Quality check: copertura, totali CN8 vs HS4, pixel X
├── key_partners_incidence.py  # Calcolo incidenza partner chiave su import totale UE27
├── graphs/                    # Grafici di output
├── input_data/                # File di input statici (es. CBS_2025_concordance.csv)
└── output_comext/             # Dataset CSV prodotti dalla pipeline
    ├── comext_imports_hs8_2013_2025_raw.csv
    ├── comext_exports_hs8_2013_2025_raw.csv
    ├── R_imp_mapped_cn8topc8_2013to2025.csv # dataset finale, escluso perché >100MB
    ├── R_exp_mapped_cn8topc8_2013to2025.csv # dataset finale, escluso perché >100MB
    ├── prodcom_2013to2025_raw.csv
    ├── prodcom_2013to2025_29+30.csv # dataset finale
    ├── comext_imports_2013_2024_hs4.csv
    ├── comext_exports_2013_2024_hs4.csv
    └── debug/
```

---

## Workflow

### 1. Download dati Comext HS8
```bash
python api_turbo.py 1   # import
python api_turbo.py 2   # export
```

Scarica da `DS-045409` (Comext) tutti i codici CN8 appartenenti ai capitoli **29** (chimica) e **30** (farmaceutica) per tutti i 27 reporter UE e 31 partner chiave (+ totali extra UE), anni 2013–2025. Le chiamate API sono parallelizzate su 2 thread (`ThreadPoolExecutor`), con un loop su `reporter × anno × indicatore`. Il risultato è ruotato in long format.

Piccole modifiche al codice sorgente permettono di scaricare i dati di altri capitoli.

Output: `output_comext/comext_{imports|exports}_hs8_2013_2025_raw.csv`

### 2. Download dati Prodcom
```bash
python prodcom_api.py
```

Scarica da `DS-059358` i dati di produzione industriale (`PRODVAL`, `PRODQNT`) per tutti i 27 paesi UE, anni 2013–2025, usando il wildcard `..` sul codice prodotto (nessuna lista esterna necessaria). Pivota in long format per riga `(anno, paese, prodotto)`.

Output: `output_comext/prodcom_2013to2025_raw.csv`

### 3. Concordanza CN8 → PC8
```r
Rscript concordance_PC8_CN8.R   # modificare INPUT_FILE / OUT_* per import o export
```

Script R che mappa i codici CN8 del Comext verso i codici PC8 Prodcom. Strategia a cascata:

1. **Pacchetto `harmonizer`** — tavole di concordanza ufficiali PC8↔CN8 per anni 2013–2021
2. **CBS 2025** — tavola di concordanza aggiornata (`input_data/CBS_2025_concordance.csv`) per colmare le lacune
3. **Fallback via successori** — per i CN8 non mappati, `harmonize_cn8()` cerca i successori nel periodo e tenta la concordanza sul codice successore
4. **Filtro CN8 "XX"** — i codici aggregati che terminano in `XX` vengono rimossi prima del mapping

Produce anche un report diagnostico sui 109 codici CN8 noti come assenti nel CBS 2025 (`cbs_unmapped_109`).

Output: `R_{imp|exp}_mapped_cn8topc8_2013to2025.csv` + file di debug in `output_comext/debug/`

### 4. Quality checks
```bash
python checks.py
```

Esegue tre verifiche sui dataset mappati:

- **`slice_prodcom`** — estrae dal dataset Prodcom grezzo solo i PC8 presenti nell'import/export mappato per ottenere il sottoinsieme di produzione relativo ai capitoli 29/30; identifica codici CN8 presenti solo in import o solo in export
- **`coverage_check`** — verifica per ogni anno se tutti i 27 reporter e i 32 partner sono presenti in ciascun dataset
- **`plot`** — confronta i totali per gruppo HS4 tra dataset CN8 mappato e dataset HS4 grezzo (grafico Export + Import, valori in miliardi €)

Output: `graphs/cn8_vs_hs4_totals_by_hs4group_.png`

### 5. Incidenza partner chiave
```bash
python key_partners_incidence.py
```

A partire dal dataset finale long, calcola per ogni anno l'incidenza dell'import dai **TOP 9 partner** (CN, IN, US, CH, KR, JP, GB, IL, SG) e da tutti i **31 partner scaricati** sul totale import UE27 (proxy: codice `EXT_EU27_2020`).

Output: `graphs/key_partners_incidence.png`

---

## Copertura partner

| Gruppo | Paesi |
|---|---|
| Reporter | 27 stati membri UE |
| Partner chiave (TOP 9) | CN, IN, US, CH, KR, JP, GB, IL, SG |
| Partner completi (31) | + VN, TH, ID, BD, MY, PH, SA, AE, NO, CA, AU, NZ, TR, TW, RU, BY, BR, MX, AR, ZA, EG, MA |
| Aggregato mondo | EXT_EU27_2020 |

---

## Risultati principali

- **Copertura partner**: i 30 partner coprono stabilmente il 96–98% dell'import totale UE27; i soli TOP 9 coprono l'88–93% nel periodo 2013–2024.
- **Coerenza CN8 vs HS4**: i totali aggregati per gruppo HS4 derivati dal mapping CN8 sono sostanzialmente allineati con i dati HS4 grezzi Comext, confermando la qualità della concordanza.
- **Codici "X"**: il peso dei codici CN8 aggregati (che terminano in `XX`, rimossi prima del mapping) è sceso sotto il 2% per l'import e sotto il 2% per l'export nel 2024, in calo rispetto a valori >4% nel 2013–2016.

---

## Dipendenze Python

```
pandas
requests
matplotlib
pathlib
concurrent.futures  # stdlib
```

```bash
pip install pandas requests matplotlib
```

## Dipendenze R

```r
install.packages(c("harmonizer", "dplyr", "readr", "tidyr", "stringr"))
```

---

## Note

- L'API Comext è `https://ec.europa.eu/eurostat/api/comext/dissemination/sdmx/2.1/data/DS-045409/`. Il formato della query è `A.{reporter}.{partners}..{flow}.{indicator}` (struttura aggiornata a marzo 2026).
- Il wildcard `..` sul codice prodotto restituisce anche HS4 e HS6; il filtro `df[df['product'].str.len() == 8]` in `api_turbo.py` mantiene solo gli 8 digit.
- Il dataset Prodcom grezzo contiene tutti i PC8 disponibili per paese; `checks.py` lo restringe ai soli PC8 rilevanti (capitoli 29–30) tramite `slice_prodcom`.
- GB è incluso come partner extra-UE; i dati pre-2021 includono flussi intra-UE con il UK, da tenere presente nelle analisi temporali.
