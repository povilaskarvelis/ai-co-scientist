# BigQuery Execution Playbook

- Before using BigQuery, check whether the current step is already covered by a dedicated source tool such as Open Targets association or L2G, GWAS study-variant lookup, JASPAR motif lookup, TCGA availability, CELLxGENE marker genes, Human Protein Atlas single-cell lookup, Ensembl canonical transcript or TSS lookup, RefSeq record lookup, ENCODE metadata lookup, or UniProt protein profile lookup.
- Use `list_bigquery_tables` to inspect dataset or table schema when exact column names are uncertain.
- Use `run_bigquery_select_query` only for read-only SQL.
- Keep queries narrow and tied to the current step objective; avoid broad exploratory scans when the step is identifier-ready.
- For BigQuery-backed evidence, preserve dataset names and returned identifiers in the summary.
- If a structured result is useful but not directly citable, follow it with PubMed, OpenAlex, or ClinicalTrials.gov corroboration before concluding.

## Dataset Coverage

- `open_targets_platform`: targets, diseases, drugs, evidence, and association tables.
- `ebi_chembl`: bioactivity tables. Prefer `get_chembl_bioactivities` for named drug bioactivity and selectivity questions.
- `gnomad`: population variant frequencies.
- `human_genome_variants`: 1000 Genomes and related human variation tables.
- `human_variant_annotation`: ClinVar and related clinical variant annotation tables.
- `nlm_rxnorm`: drug nomenclature and clinical drug relationships.
- `fda_drug`: labels, NDC, and enforcement tables. Prefer `search_fda_adverse_events` for FAERS adverse-event reports and `get_dailymed_drug_label` for current label warning language.
- `umiami_lincs`: L1000 perturbation metadata and signatures.
- `ebi_surechembl`: patent-derived chemistry.

## SQL Mechanics

- Always wrap table references in backticks. Short names are auto-expanded, so `open_targets_platform.target` resolves to `bigquery-public-data.open_targets_platform.target`.
- Example: ``SELECT id, approvedSymbol FROM `open_targets_platform.target` WHERE approvedSymbol = 'BRCA1'``.
- If a filter value contains an apostrophe, escape it as two single quotes, such as `WHERE name = 'Alzheimer''s disease'`.
- For unfamiliar or nested-field queries, run the same SQL first with `dryRun=true` to catch syntax and bytes-billed issues before execution.
- Before writing unfamiliar queries, inspect tables with `list_bigquery_tables(dataset="<dataset_name>")`, then inspect columns with `list_bigquery_tables(dataset="<dataset_name>", table="<table_name>")`.
- Never guess column names. BigQuery column names are often singular, such as `target` rather than `targets`, and many joins require IDs rather than human-readable names. For example, `targetId` may be an Ensembl ID and `diseaseId` may be an EFO ID.
- Look up IDs from reference tables before filtering evidence or association tables.

## LINCS Limits

- For `umiami_lincs`, prefer metadata-sized tables: `signature`, `perturbagen`, `small_molecule`, `model_system`, and `cell_line`.
- Use the `readout` table only when you already have exact signature IDs or the user has explicitly raised the bytes-billed cap. Broad gene-list filters usually still scan roughly the full table.

## Dedicated-Source Fallbacks

- Use literature tools for literature search and paper grounding: `search_pubmed`, `get_pubmed_abstract`, `get_paper_fulltext`, `search_openalex_works`, and `search_europe_pmc_literature`.
- Use biomedical source tools instead of BigQuery when they directly match the requested evidence type: ClinicalTrials.gov, UniProt, Reactome, STRING, IntAct, BioGRID, IEDB, MyGene.info, EBI OxO, QuickGO, VEP, MyVariant.info, CIViC, AlphaFold, GWAS Catalog, DGIdb, GTEx, Human Protein Atlas, DepMap, BioGRID ORCS, GDSC, PRISM, PharmacoDB, CELLxGENE, ClinGen, Alliance Genome Resources, Pathway Commons, Guide to Pharmacology, DailyMed, RCSB PDB, cBioPortal, ChEMBL, PubChem, and openFDA FAERS.
