library(harmonizer)
library(dplyr)
library(readr)
library(tidyr)
library(stringr)

# CONFIG: paths in brackets to use this file for the import dataset

#INPUT_FILE  <- "/home/mf/eurostat/files/output_comext/comext_imports_hs8_2013_2025_raw.csv"
INPUT_FILE  <- "/home/mf/eurostat/files/output_comext/comext_exports_hs8_2013_2025_raw.csv"
INPUT_SEP   <- ","
YEAR_BEGIN  <- 2013
YEAR_END    <- 2025

CBS_2025    <- "/home/mf/eurostat/files/input_data/CBS_2025_concordance.csv"
#OUT_MAPPED   <- "/home/mf/eurostat/files/output_comext/R_imp_mapped_cn8topc8_2013to2025.csv"
#OUT_UNMAPPED <- "/home/mf/eurostat/files/output_comext/debug/R_imp_unmapped_cn8_.csv"
#OUT_FLAGS    <- "/home/mf/eurostat/files/output_comext/debug/R_imp_csb25_flags_.csv"
OUT_MAPPED   <- "/home/mf/eurostat/files/output_comext/R_exp_mapped_cn8topc8_2013to2025.csv"
OUT_UNMAPPED <- "/home/mf/eurostat/files/output_comext/debug/R_exp_unmapped_cn8_.csv"
OUT_FLAGS    <- "/home/mf/eurostat/files/output_comext/debug/R_exp_csb25_flags_.csv"

cbs_unmapped_109 <- c( # missing codes in CBS 2025 conversion table
  "30021091","30029050","29299000","29399900","30021010","30021098","30029090",
  "29269095","29031980","29242998","29309099","29319090","29350090","30022000",
  "29039990","29049095","29146990","29171990","29209050","29209085","29221390",
  "29221930","29221985","30023000","30044080","30062000","29038990","29096000",
  "29147000","29173995","29319010","29033919","29033990","29037740","29037990",
  "29053925","29181950","29209040","29211960","29221310","29309020","29319040",
  "30034080","30069200","29309060","29399100","29033915","29049040","29031910",
  "29033100","29033911","29037929","29209020","29209030","29221920","29319030",
  "29350030","30044020","30044030","29037919","30034030","29305000","29037720",
  "29037710","29037911","29037921","29221910","30044040","30034020","29319020",
  "29037730","29319060","29319080","29319050","29037750","29033980","29033924",
  "29033926","29033925","29033929","29033939","29033928","29033921","29033923",
  "29033927","29033931","29033935","30021900","29309098","29313990","29397100",
  "29397900","29313950","29313960","29038980","30021100","29313100","29313300",
  "29313200","29313400","29313800","29313600","29313930","29313920","29313500",
  "29313700","30022090","30022010","29314990"
)

# CONCORDANZA HARMONIZER (2013–2021)
data_dir <- get_data_directory()
pc8_dir  <- file.path(data_dir, "PC8")

concordance_all <- bind_rows(lapply(seq(YEAR_BEGIN, YEAR_END), function(yr) {
  fname <- file.path(pc8_dir, paste0("PC8_CN8_", yr, ".rds"))
  if (!file.exists(fname)) {
    message("Concordanza MANCANTE per anno ", yr, " - saltato.")
    return(NULL)
  }
  readRDS(fname) %>%
    rename_with(toupper) %>%
    transmute(PC8 = as.character(PRCCODE), CN8 = as.character(CNCODE), year = yr) %>%
    filter(!is.na(CN8), !is.na(PC8))
}))

cat("Anni caricati da harmonizer:", paste(unique(concordance_all$year), collapse = ", "), "\n\n")

concordance_harmonizer <- concordance_all %>%
  group_by(CN8) %>%
  summarise(
    PC8         = first(PC8[order(-year)]),
    PC8_all     = paste(sort(unique(PC8)), collapse = "|"),
    years_found = paste(sort(unique(year)), collapse = ","),
    n_pc8       = n_distinct(PC8),
    .groups = "drop"
  ) %>%
  mutate(mapping_source = "harmonizer")

# CONCORDANZA CBS 2025
cat("Caricamento CBS 2025...\n")

cbs_2025 <- read_delim(CBS_2025, delim = ",", col_types = cols(.default = col_character()),
                       show_col_types = FALSE) %>%
  rename_with(trimws) %>%
  mutate(
    PC8 = str_replace_all(`Prodcom code`, "\\.", ""),
    CN8 = as.character(trimws(`Commodity code`))
  ) %>%
  filter(!is.na(PC8), !is.na(CN8), PC8 != "", CN8 != "") %>%
  select(CN8, PC8) %>%
  distinct() %>%
  group_by(CN8) %>%
  summarise(
    PC8         = first(PC8),
    PC8_all     = paste(sort(unique(PC8)), collapse = "|"),
    years_found = "2025",
    n_pc8       = n_distinct(PC8),
    .groups = "drop"
  ) %>%
  mutate(mapping_source = "CBS_2025")

cat("CN8 unici in CBS 2025:", nrow(cbs_2025), "\n\n")
# CONCORDANZA HARMONIZER (2013–2021)

# MERGE: harmonizer priorità, CBS 2025 colma lacune
concordance_summary <- bind_rows(
  concordance_harmonizer,
  cbs_2025 %>% filter(!CN8 %in% concordance_harmonizer$CN8)
)

cat("CN8 totali in concordanza combinata:", nrow(concordance_summary), "\n\n")

# DATI INPUT + FILTRO CN8 CHE FINISCONO CON 'XX'
raw_data <- read_delim(INPUT_FILE, delim = INPUT_SEP,
                       col_types = cols(.default = col_character()),
                       show_col_types = FALSE) %>%
  rename(CN8 = product)

cat("Righe totali nel file input:", nrow(raw_data), "\n")
cat("CN8 unici (pre-filtro XX):", n_distinct(raw_data$CN8), "\n")

# Conta e rimuovi CN8 che finiscono con 'XX'
cn8_with_xx <- raw_data %>% filter(grepl("XX$", CN8)) %>% pull(CN8) %>% unique()
cat("CN8 che finiscono con XX (da rimuovere):", length(cn8_with_xx), "\n")
if (length(cn8_with_xx) > 0) {
  cat("Esempi:", paste(head(cn8_with_xx, 5), collapse = ", "), "\n")
}

raw_data <- raw_data %>% filter(!grepl("XX$", CN8))

cat("Righe dopo filtro XX:", nrow(raw_data), "\n")
cat("CN8 unici dopo filtro XX:", n_distinct(raw_data$CN8), "\n\n")

unique_cn8 <- distinct(raw_data, CN8)

# MAPPING DIRETTO
mapped <- unique_cn8 %>%
  left_join(concordance_summary, by = "CN8") %>%
  mutate(
    CN8plus  = NA_character_,
    flag     = NA_integer_,
    flagyear = NA_integer_
  )

unmapped_codes <- mapped %>% filter(is.na(PC8)) %>% pull(CN8)
cat("Mappati direttamente:", sum(!is.na(mapped$PC8)), "\n")
cat("Non mappati:         ", length(unmapped_codes), "\n\n")

# FALLBACK: successori
if (length(unmapped_codes) > 0) {
  cat("Esecuzione harmonize_cn8()...\n")
  harm <- tryCatch(
    harmonize_cn8(b = YEAR_BEGIN, e = min(YEAR_END, 2021), progress = FALSE),
    error = function(e) { warning(conditionMessage(e)); NULL }
  )
  
  if (!is.null(harm)) {
    cn8_year_cols <- paste0("CN8_", seq(YEAR_BEGIN, min(YEAR_END, 2021)))
    
    harm_long <- harm %>%
      select(CN8plus, all_of(cn8_year_cols), flag, flagyear) %>%
      pivot_longer(cols = all_of(cn8_year_cols), names_to = "year_col", values_to = "CN8") %>%
      filter(!is.na(CN8), CN8 != CN8plus, CN8 %in% unmapped_codes) %>%
      distinct(CN8, CN8plus, flag, flagyear)
    
    fallback <- harm_long %>%
      left_join(concordance_summary %>%
                  select(CN8plus = CN8, PC8_fb = PC8, PC8_all_fb = PC8_all,
                         yf_fb = years_found, n_pc8_fb = n_pc8),
                by = "CN8plus") %>%
      filter(!is.na(PC8_fb)) %>%
      transmute(CN8, CN8plus, flag, flagyear,
                PC8 = PC8_fb, PC8_all = PC8_all_fb,
                years_found = yf_fb, n_pc8 = n_pc8_fb,
                mapping_source = "harmonized_successor")
    
    cat("Aggiuntivi via successore:", nrow(fallback), "\n\n")
    
    mapped <- mapped %>%
      left_join(fallback %>% rename_with(~ paste0(.x, "_fb"), .cols = -CN8), by = "CN8") %>%
      mutate(
        PC8            = coalesce(PC8,            PC8_fb),
        PC8_all        = coalesce(PC8_all,        PC8_all_fb),
        years_found    = coalesce(years_found,    years_found_fb),
        n_pc8          = coalesce(n_pc8,          n_pc8_fb),
        mapping_source = coalesce(mapping_source, mapping_source_fb),
        CN8plus        = coalesce(CN8plus,        CN8plus_fb),
        flag           = coalesce(flag,           flag_fb),
        flagyear       = coalesce(flagyear,       flagyear_fb)
      ) %>%
      select(-ends_with("_fb"))
  }
}

# RISULTATO FINALE
result <- raw_data %>%
  left_join(mapped, by = "CN8") %>%
  mutate(
    mapped       = !is.na(PC8),
    mapping_type = case_when(
      is.na(PC8)  ~ "UNMAPPED",
      n_pc8 == 1  ~ "one-to-one",
      n_pc8 >  1  ~ "one-to-many"
    )
  )

total <- nrow(unique_cn8)
n_map <- sum(!is.na(mapped$PC8))
cat("=== SUMMARY (post-filtro XX) ===\n")
cat(sprintf("Mappati  : %d / %d (%.1f%%)\n", n_map, total, 100*n_map/total))
cat(sprintf("  di cui harmonizer          : %d\n", sum(mapped$mapping_source == "harmonizer",            na.rm = TRUE)))
cat(sprintf("  di cui CBS 2025            : %d\n", sum(mapped$mapping_source == "CBS_2025",              na.rm = TRUE)))
cat(sprintf("  di cui harmonized_successor: %d\n", sum(mapped$mapping_source == "harmonized_successor",  na.rm = TRUE)))
cat(sprintf("Unmapped : %d / %d (%.1f%%)\n", total-n_map, total, 100*(total-n_map)/total))

# FLAGS 109
flags_109 <- tibble(CN8 = cbs_unmapped_109) %>%
  left_join(mapped %>%
              select(CN8, PC8, PC8_all, n_pc8, years_found,
                     mapping_source, CN8plus, flag, flagyear),
            by = "CN8") %>%
  mutate(
    in_your_data           = CN8 %in% unique_cn8$CN8,
    resolved_by_harmonizer = !is.na(PC8),
    status = case_when(
      !is.na(PC8) & mapping_source == "harmonizer"            ~ "RESOLVED – harmonizer",
      !is.na(PC8) & mapping_source == "CBS_2025"              ~ "RESOLVED – CBS 2025",
      !is.na(PC8) & mapping_source == "harmonized_successor"  ~ "RESOLVED – successor",
      TRUE ~ "STILL UNMAPPED"
    )
  ) %>%
  arrange(status, CN8)

cat("\n=== FLAGS 109 ===\n")
print(table(flags_109$status))
cat("\n")

# Dettaglio: quanti dei CBS-109 sono nel tuo dataset dopo filtro XX?
cat("Dei 109 CBS-unmapped:\n")
cat("  Nel tuo dataset (post-filtro XX):", sum(flags_109$in_your_data), "\n")
cat("  Risolti da harmonizer/CBS      :", sum(flags_109$resolved_by_harmonizer), "\n")
cat("  Ancora unmapped                :", sum(!flags_109$resolved_by_harmonizer), "\n")

# EXPORT
write_csv(result,                      OUT_MAPPED)
write_csv(result %>% filter(!mapped),  OUT_UNMAPPED)
write_csv(flags_109,                   OUT_FLAGS)
cat("\nFile salvati.\n")