Use this reference when the objective is oncology target validation.

- Prefer a sequence such as: gene/target normalization -> dependency evidence -> drug-response or pharmacology context -> literature or trial corroboration.
- Use dependency tools to ask whether the target is selective, not just whether it ever scores negative.
- Pair `get_depmap_gene_dependency` with `get_biogrid_orcs_gene_summary` when you need release-level metrics plus published screen context.
- Treat `get_depmap_gene_dependency` as a release-level named-gene summary tool. Do not use it alone for lineage-, mutation-, or cell-line-subset co-dependency discovery.
- Pair screening evidence with `get_gdsc_drug_sensitivity`, `get_prism_repurposing_response`, or `get_pharmacodb_compound_response` when the question shifts from vulnerability to tractability and a candidate compound is already named.
- Do not plan model-first drug-discovery steps around those compound-response tools; they require a named drug/compound query.

## Coverage Contract

Use archetype `target_validation` and make the `coverage.covered_dimensions` list match the plan steps, not just the user intent.

- `human_disease_association`: Open Targets, GWAS, ClinGen, Monarch, or another human disease relevance source.
- `tumor_context`: cBioPortal/CIViC-style alteration, biomarker, or tumor-genetics context.
- `dependency_selectivity`: DepMap plus ORCS or another screen source, with selectivity caveats.
- `tractability_pharmacology`: DGIdb, Guide to Pharmacology, ChEMBL, PubChem, DailyMed, or named-compound evidence.
- `clinical_translation`: ClinicalTrials.gov, trial landscape, or safety/post-marketing evidence when therapy translation matters.
- `model_organism_context`: Alliance Genome or model-system support when translational biology is material.
- `literature_corroboration`: PubMed/OpenAlex/Europe PMC with PMIDs, DOIs, or other citable identifiers.

If one of these is not useful for the user's objective, put it in `coverage.omitted_dimensions` with a short reason instead of silently skipping it.
