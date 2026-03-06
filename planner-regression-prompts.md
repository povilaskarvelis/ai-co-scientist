# Planner Regression Prompt Pack

Use this file to regression-test planner routing for the newest data sources:

- `search_europe_pmc_literature`
- `search_pathway_commons_top_pathways`
- `get_guidetopharmacology_target`
- `get_dailymed_drug_label`
- `get_clingen_gene_curation`
- `get_gdsc_drug_sensitivity`
- `get_intact_interactions`
- `get_biogrid_interactions`
- `get_biogrid_orcs_gene_summary`
- `search_hpo_terms`
- `get_orphanet_disease_profile`
- `query_monarch_associations`
- `get_alliance_genome_gene_profile`
- `get_prism_repurposing_response`
- `get_pharmacodb_compound_response`

## How To Use

For each prompt:

1. Start a fresh research run.
2. Inspect the proposed plan before approval.
3. Confirm the expected tool appears in the step plan or in the step executor trace.
4. Approve and verify the tool is actually called in `tools_called`.

Pass criteria:

- The expected new tool is used.
- The planner does not substitute a clearly weaker source when the prompt explicitly asks for the new source's evidence type.

Acceptable behavior:

- The planner may add supporting tools, but it should still include the expected primary tool.

## Single-Source Prompts

### Europe PMC

Prompt:
`Search Europe PMC for recent TP53 cancer papers and preprints, and summarize the top results with source type, year, and citation counts.`

Must include:

- `search_europe_pmc_literature`

Should not rely only on:

- `search_pubmed`
- `search_openalex_works`

Why:

- The prompt explicitly asks for Europe PMC plus preprints and citation metadata.

### Pathway Commons

Prompt:
`Use Pathway Commons to find the top human pathways associated with EGFR and summarize the highest-ranking pathways and their source databases.`

Must include:

- `search_pathway_commons_top_pathways`

Should not rely only on:

- `search_reactome_pathways`
- `get_string_interactions`

Why:

- The request is for integrated pathway context across providers, not Reactome-only pathways.

### Guide to Pharmacology

Prompt:
`Use Guide to Pharmacology to summarize curated target-ligand interactions for EGFR in human, including representative ligands, action types, and affinity evidence.`

Must include:

- `get_guidetopharmacology_target`

Should not rely only on:

- `get_chembl_bioactivities`
- `search_drug_gene_interactions`

Why:

- The prompt asks for curated target-ligand pharmacology rather than broad chemistry or druggability.

### DailyMed

Prompt:
`Pull the current DailyMed label for metformin and summarize the boxed warning, indications, contraindications, and warnings/precautions.`

Must include:

- `get_dailymed_drug_label`

Should not rely only on:

- `search_fda_adverse_events`
- BigQuery `fda_drug`

Why:

- This is a label question, not a post-marketing safety question.

### ClinGen

Prompt:
`Summarize ClinGen gene-disease validity and dosage sensitivity curation for TP53, including classification strength and dosage conclusions.`

Must include:

- `get_clingen_gene_curation`

Should not rely only on:

- `get_variant_annotations`
- `search_civic_genes`
- `search_gwas_associations`

Why:

- The prompt is about expert-curated gene validity and dosage sensitivity, which those other tools do not provide.

### GDSC / CancerRxGene

Prompt:
`Use GDSC / CancerRxGene to summarize sorafenib sensitivity across cancer cell lines, including the most sensitive tissues and top sensitive cell lines.`

Must include:

- `get_gdsc_drug_sensitivity`

Should not rely only on:

- `get_chembl_bioactivities`
- `get_guidetopharmacology_target`
- `search_drug_gene_interactions`

Why:

- The prompt asks for pharmacogenomic response data, not target pharmacology or chemistry.

### PRISM Repurposing

Prompt:
`Use PRISM Repurposing to summarize erlotinib single-dose response across cancer cell lines, including the most sensitive tissues, top sensitive cell lines, and representative log2-fold-change values.`

Must include:

- `get_prism_repurposing_response`

Should not rely only on:

- `get_gdsc_drug_sensitivity`
- `get_pharmacodb_compound_response`
- `get_guidetopharmacology_target`

Why:

- The prompt explicitly asks for Broad PRISM repurposing single-dose response rather than GDSC curves or a cross-dataset portal.

### PharmacoDB

Prompt:
`Use PharmacoDB to summarize paclitaxel response across public pharmacogenomic datasets, including dataset coverage, the most sensitive tissues, and top sensitive cell lines.`

Must include:

- `get_pharmacodb_compound_response`

Should not rely only on:

- `get_gdsc_drug_sensitivity`
- `get_prism_repurposing_response`

Why:

- The request is for harmonized cross-dataset drug-response evidence, which PharmacoDB is designed to provide.

### IntAct

Prompt:
`Use IntAct to summarize curated experimental interaction partners for TP53 in human, including top partners, interaction types, detection methods, and publication support.`

Must include:

- `get_intact_interactions`

Should not rely only on:

- `get_string_interactions`
- `search_pathway_commons_top_pathways`

Why:

- The prompt explicitly asks for curated experimental interaction evidence rather than integrated network predictions or pathway rollups.

### BioGRID

Prompt:
`Use BioGRID to summarize broader experimental interaction evidence for TP53 in human, including top partners, physical versus genetic interaction classes, throughput tags, and supporting PMIDs.`

Must include:

- `get_biogrid_interactions`

Should not rely only on:

- `get_intact_interactions`
- `get_string_interactions`
- `search_pathway_commons_top_pathways`

Why:

- The request is for broader BioGRID interaction coverage with physical/genetic classes and throughput context, not just IntAct curation or network predictions.

### BioGRID ORCS

Prompt:
`Use BioGRID ORCS to summarize published CRISPR screen evidence for EGFR, including hit status, top phenotypes, top cell lines, and representative screens.`

Must include:

- `get_biogrid_orcs_gene_summary`

Should not rely only on:

- `get_depmap_gene_dependency`
- `get_gdsc_drug_sensitivity`

Why:

- The prompt explicitly asks for published screen-level ORCS evidence with phenotype and cell-line context rather than release-level dependency summaries or drug-response screens.

### HPO

Prompt:
`Use HPO to find the best-matching phenotype term for cerebellar ataxia and summarize the canonical term, CURIE, synonyms, and definition.`

Must include:

- `search_hpo_terms`

Should not rely only on:

- `map_ontology_terms_oxo`
- `get_orphanet_disease_profile`

Why:

- This is phenotype-term normalization, not disease profiling or ontology crosswalks.

### Orphanet / ORDO

Prompt:
`Use Orphanet to summarize Rett syndrome, including its OrphaCode, cross-references, inheritance, onset, top phenotype associations, and curated disease-gene links.`

Must include:

- `get_orphanet_disease_profile`

Should not rely only on:

- `search_hpo_terms`
- `query_monarch_associations`

Why:

- The request is for a rare-disease profile with curated phenotype and gene sections, which Orphanet is best suited for.

### Monarch

Prompt:
`Use Monarch to find phenotype-to-gene associations for ataxia and summarize the top associated human genes with source context.`

Must include:

- `query_monarch_associations`

Should not rely only on:

- `search_hpo_terms`
- `get_orphanet_disease_profile`

Why:

- The task is phenotype-driven graph-style gene association reasoning rather than term lookup or rare-disease profiling.

### Alliance Genome Resources

Prompt:
`Use Alliance Genome Resources to summarize translational evidence for TP53, including key model-species orthologs, disease/phenotype evidence counts, and representative disease models.`

Must include:

- `get_alliance_genome_gene_profile`

Should not rely only on:

- `get_clingen_gene_curation`
- `query_monarch_associations`
- `get_orphanet_disease_profile`

Why:

- The request is specifically about orthologs, model-organism context, and translational evidence, which AGR is best suited for.

## Two-Tool Prompts

### Europe PMC + ClinGen

Prompt:
`Assess TP53 in Li-Fraumeni syndrome using ClinGen for curated validity and Europe PMC for recent literature context.`

Must include:

- `get_clingen_gene_curation`
- `search_europe_pmc_literature`

Optional supporting tools:

- `resolve_gene_identifiers`
- `search_pubmed`

### Guide to Pharmacology + DailyMed

Prompt:
`For EGFR inhibitors, use Guide to Pharmacology to summarize curated EGFR ligands and DailyMed to retrieve the current label warning profile for osimertinib.`

Must include:

- `get_guidetopharmacology_target`
- `get_dailymed_drug_label`

Optional supporting tools:

- `search_drug_gene_interactions`
- `get_chembl_bioactivities`

### GDSC + DailyMed

Prompt:
`For sorafenib, summarize GDSC cell-line sensitivity patterns and then retrieve the current DailyMed label warnings and indications.`

Must include:

- `get_gdsc_drug_sensitivity`
- `get_dailymed_drug_label`

### Pathway Commons + Europe PMC

Prompt:
`Find the top EGFR-related pathways in Pathway Commons and then pull Europe PMC literature that gives recent context for those pathways in cancer.`

Must include:

- `search_pathway_commons_top_pathways`
- `search_europe_pmc_literature`

### IntAct + Pathway Commons

Prompt:
`For TP53, use IntAct for experimental interaction evidence and Pathway Commons for broader pathway context.`

Must include:

- `get_intact_interactions`
- `search_pathway_commons_top_pathways`

### BioGRID + Pathway Commons

Prompt:
`For TP53, use BioGRID for broader experimental interaction evidence and Pathway Commons for integrated pathway context.`

Must include:

- `get_biogrid_interactions`
- `search_pathway_commons_top_pathways`

### ORCS + DepMap

Prompt:
`For EGFR, use BioGRID ORCS for published CRISPR screen context and DepMap for release-level dependency metrics.`

Must include:

- `get_biogrid_orcs_gene_summary`
- `get_depmap_gene_dependency`

### HPO + Monarch

Prompt:
`First normalize the phenotype term ataxia with HPO, then use Monarch to identify top associated human genes for that phenotype.`

Must include:

- `search_hpo_terms`
- `query_monarch_associations`

### Orphanet + Monarch

Prompt:
`For Rett syndrome, use Orphanet for the curated rare-disease profile and Monarch for disease-to-gene causal associations.`

Must include:

- `get_orphanet_disease_profile`
- `query_monarch_associations`

### Alliance Genome + ClinGen

Prompt:
`For TP53, use Alliance Genome Resources for model-organism translational evidence and ClinGen for human expert gene-disease validity.`

Must include:

- `get_alliance_genome_gene_profile`
- `get_clingen_gene_curation`

## Planner Preference Prompts

These are useful when you want to confirm the planner chooses the best new source without naming it directly.

### Preprints / Citation Metadata

Prompt:
`Find recent preprints and citation-rich literature on LRRK2 in Parkinson disease.`

Preferred tool:

- `search_europe_pmc_literature`

### Curated Pharmacology

Prompt:
`What are the best-characterized curated ligands for PPARG, and what action types are reported?`

Preferred tool:

- `get_guidetopharmacology_target`

### Rare-Disease Profile

Prompt:
`Give me the best rare-disease profile for Rett syndrome with inheritance, onset, phenotypes, and disease genes.`

Preferred tool:

- `get_orphanet_disease_profile`

### Phenotype Normalization

Prompt:
`What is the canonical phenotype term for cerebellar ataxia?`

Preferred tool:

- `search_hpo_terms`

### Phenotype-To-Gene Reasoning

Prompt:
`Which human genes are most associated with ataxia from a phenotype-first perspective?`

Preferred tool:

- `query_monarch_associations`

### Model-Organism Translational Evidence

Prompt:
`What model-organism and ortholog evidence helps translate TP53 biology beyond human-only sources?`

Preferred tool:

- `get_alliance_genome_gene_profile`

### Label Language

Prompt:
`What does the current US label say about boxed warnings and contraindications for metformin?`

Preferred tool:

- `get_dailymed_drug_label`

### Expert Gene Validity

Prompt:
`What is the curated strength of evidence linking TP53 to Li-Fraumeni syndrome, and what does dosage sensitivity say?`

Preferred tool:

- `get_clingen_gene_curation`

### Integrated Pathway Context

Prompt:
`What are the main pathway contexts around EGFR across integrated pathway databases?`

Preferred tool:

- `search_pathway_commons_top_pathways`

### Experimental Interaction Evidence

Prompt:
`What experimentally curated molecular interaction partners are reported for TP53 in human, and how were they measured?`

Preferred tool:

- `get_intact_interactions`

### Broader Experimental Interaction Coverage

Prompt:
`What broader experimental physical and genetic interaction evidence exists for TP53, including throughput context and PMIDs?`

Preferred tool:

- `get_biogrid_interactions`

### Published CRISPR Screen Context

Prompt:
`Which published CRISPR screens report phenotype and cell-line context for EGFR hits?`

Preferred tool:

- `get_biogrid_orcs_gene_summary`

## Source Precedence Checks

These are overlap cases where the planner should prefer one source over nearby alternatives.

### PubMed vs Europe PMC vs OpenAlex

Prompt:
`Find recent peer-reviewed biomedical papers on LRRK2 in Parkinson disease and record PMIDs.`

Preferred tool:

- `search_pubmed`

Should not default to:

- `search_openalex_works`

Why:

- The task is standard biomedical literature retrieval with PMIDs, not broader citation graph exploration.

### DailyMed vs FAERS

Prompt:
`What does the current US label say about boxed warnings and contraindications for sorafenib?`

Preferred tool:

- `get_dailymed_drug_label`

Should not default to:

- `search_fda_adverse_events`

Why:

- This asks for current label language, not post-marketing safety signals.

### IntAct vs STRING

Prompt:
`What curated experimental interaction partners are reported for TP53, with detection methods and PMIDs?`

Preferred tool:

- `get_intact_interactions`

Should not default to:

- `get_string_interactions`

Why:

- Detection methods and PMIDs point to curated experimental interaction records.

### BioGRID vs IntAct

Prompt:
`What broader experimental physical and genetic interaction evidence exists for TP53, including throughput classes and PMIDs?`

Preferred tool:

- `get_biogrid_interactions`

Should not default to:

- `get_intact_interactions`

Why:

- This asks for broader BioGRID-style physical/genetic interaction coverage and throughput context, not the narrower IntAct curation lane.

### DepMap vs GDSC

Prompt:
`Is KRAS a dependency in cancer cell lines, and how strong is the vulnerability signal?`

Preferred tool:

- `get_depmap_gene_dependency`

Should not default to:

- `get_gdsc_drug_sensitivity`

Why:

- This is a target dependency question, not a compound-response question.

### GDSC vs PRISM vs PharmacoDB

Prompt:
`Compare large public compound-response resources for paclitaxel and summarize the broadest cross-dataset view.`

Preferred tool:

- `get_pharmacodb_compound_response`

Acceptable supporting tools:

- `get_gdsc_drug_sensitivity`
- `get_prism_repurposing_response`

Should not default to:

- `get_gdsc_drug_sensitivity`
- `get_prism_repurposing_response`

Why:

- The phrase "broadest cross-dataset view" should route to PharmacoDB first rather than a single upstream screen.

### ORCS vs DepMap

Prompt:
`Which published CRISPR screens report EGFR hit status together with phenotype and cell-line context?`

Preferred tool:

- `get_biogrid_orcs_gene_summary`

Should not default to:

- `get_depmap_gene_dependency`

Why:

- The question asks for published screen-level context, not just aggregate dependency metrics.

### Pharmacogenomic Drug Response

Prompt:
`Which tissues and cell lines look most sensitive to sorafenib in large public cancer drug-response screens?`

Preferred tool:

- `get_gdsc_drug_sensitivity`

## Combined Regression Scenarios

### Scenario A

Prompt:
`Evaluate EGFR as a lung cancer target using integrated pathways, curated pharmacology, and current US label warnings for an approved EGFR inhibitor.`

Expected core tools:

- `search_pathway_commons_top_pathways`
- `get_guidetopharmacology_target`
- `get_dailymed_drug_label`

Likely supporting tools:

- `get_depmap_gene_dependency`
- `get_human_protein_atlas_gene`
- `get_chembl_bioactivities`

### Scenario C

Prompt:
`Evaluate sorafenib for liver cancer by combining public pharmacogenomic sensitivity data, curated target pharmacology, and the current US label.`

Expected core tools:

- `get_gdsc_drug_sensitivity`
- `get_guidetopharmacology_target`
- `get_dailymed_drug_label`

### Scenario D

Prompt:
`Evaluate TP53 using experimental interaction evidence, integrated pathway context, and recent literature.`

Expected core tools:

- `get_intact_interactions`
- `search_pathway_commons_top_pathways`
- `search_europe_pmc_literature`

### Scenario E

Prompt:
`Evaluate EGFR by combining published CRISPR screen evidence, release-level dependency metrics, and compound-response pharmacogenomics.`

Expected core tools:

- `get_biogrid_orcs_gene_summary`
- `get_depmap_gene_dependency`
- `get_gdsc_drug_sensitivity`

### Scenario B

Prompt:
`Evaluate TP53 in hereditary cancer using ClinGen curation strength and recent Europe PMC literature.`

Expected core tools:

- `get_clingen_gene_curation`
- `search_europe_pmc_literature`

Likely supporting tools:

- `resolve_gene_identifiers`
- `search_pubmed`

## Failure Signatures

Treat these as regressions:

- A Europe PMC prompt uses only PubMed or OpenAlex.
- A Pathway Commons prompt uses only Reactome.
- A curated pharmacology prompt uses only ChEMBL or DGIdb.
- A label prompt uses only FAERS or BigQuery drug tables.
- A ClinGen curation prompt uses only ClinVar, CIViC, or GWAS.
- A BioGRID interaction prompt uses only IntAct or STRING.
- A BioGRID ORCS prompt uses only DepMap or GDSC.

## Minimal Smoke Set

If you only want five prompts, use these:

1. `Search Europe PMC for recent TP53 cancer papers and preprints, and summarize the top results with source type, year, and citation counts.`
2. `Use Pathway Commons to find the top human pathways associated with EGFR and summarize the highest-ranking pathways and their source databases.`
3. `Use Guide to Pharmacology to summarize curated target-ligand interactions for EGFR in human, including representative ligands, action types, and affinity evidence.`
4. `Pull the current DailyMed label for metformin and summarize the boxed warning, indications, contraindications, and warnings/precautions.`
5. `Summarize ClinGen gene-disease validity and dosage sensitivity curation for TP53, including classification strength and dosage conclusions.`
