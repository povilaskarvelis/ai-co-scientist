#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="shaquille-oneal-1771992308"
DATASET="hackathon_data"
BUCKET="gs://benchspark-data-1771447466-datasets"

echo "=== Creating BigQuery dataset ==="
bq mk --project_id="${PROJECT_ID}" --location=US "${DATASET}" 2>/dev/null || echo "Dataset already exists"

load_tsv() {
  local table="$1" path="$2"
  echo "Loading ${table} from ${path}..."
  bq load --autodetect --source_format=CSV --field_delimiter='\t' \
    --allow_quoted_newlines --allow_jagged_rows \
    "${PROJECT_ID}:${DATASET}.${table}" "${path}" || echo "FAILED: ${table}"
}

load_csv() {
  local table="$1" path="$2" skip="${3:-0}"
  echo "Loading ${table} from ${path}..."
  bq load --autodetect --source_format=CSV \
    --allow_quoted_newlines --allow_jagged_rows \
    --skip_leading_rows="${skip}" \
    "${PROJECT_ID}:${DATASET}.${table}" "${path}" || echo "FAILED: ${table}"
}

load_space_tsv() {
  local table="$1" path="$2"
  echo "Loading ${table} from ${path} (space-delimited)..."
  bq load --autodetect --source_format=CSV --field_delimiter=' ' \
    "${PROJECT_ID}:${DATASET}.${table}" "${path}" || echo "FAILED: ${table}"
}

# ── CIViC (TSV) ─────────────────────────────────────────
echo ""
echo "=== CIViC ==="
load_tsv civic_assertion_summaries      "${BUCKET}/civic/nightly-AcceptedAssertionSummaries.tsv"
load_tsv civic_clinical_evidence        "${BUCKET}/civic/nightly-AcceptedClinicalEvidenceSummaries.tsv"
load_tsv civic_features                 "${BUCKET}/civic/nightly-FeatureSummaries.tsv"
load_tsv civic_molecular_profiles       "${BUCKET}/civic/nightly-MolecularProfileSummaries.tsv"
load_tsv civic_variant_groups           "${BUCKET}/civic/nightly-VariantGroupSummaries.tsv"
load_tsv civic_variants                 "${BUCKET}/civic/nightly-VariantSummaries.tsv"

# ── ClinGen (CSV with 6-line header) ────────────────────
echo ""
echo "=== ClinGen ==="
load_csv clingen_gene_disease_validity    "${BUCKET}/clingen/gene-disease-validity.csv" 6
load_csv clingen_variant_pathogenicity    "${BUCKET}/clingen/variant-pathogenicity.csv" 6
load_csv clingen_dosage_sensitivity       "${BUCKET}/clingen/dosage-sensitivity-all.csv" 6
load_csv clingen_dosage_genes             "${BUCKET}/clingen/dosage-sensitivity-genes.csv" 6
load_csv clingen_curation_summary         "${BUCKET}/clingen/curation-activity-summary.csv" 6
load_tsv clingen_actionability_adult      "${BUCKET}/clingen/actionability-adult.tsv"
load_tsv clingen_actionability_pediatric  "${BUCKET}/clingen/actionability-pediatric.tsv"

# ── STRING (space-delimited .txt.gz) ────────────────────
echo ""
echo "=== STRING ==="
load_space_tsv string_protein_links          "${BUCKET}/string/9606.protein.links.v12.0.txt.gz"
load_space_tsv string_physical_links         "${BUCKET}/string/9606.protein.physical.links.v12.0.txt.gz"
load_tsv       string_protein_info           "${BUCKET}/string/9606.protein.info.v12.0.txt.gz"
load_tsv       string_protein_aliases        "${BUCKET}/string/9606.protein.aliases.v12.0.txt.gz"
load_tsv       string_enrichment_terms       "${BUCKET}/string/9606.protein.enrichment.terms.v12.0.txt.gz"

# ── Reactome (tab-delimited, no header) ─────────────────
echo ""
echo "=== Reactome ==="
bq load --source_format=CSV --field_delimiter='\t' --skip_leading_rows=0 \
  --noautodetect \
  "${PROJECT_ID}:${DATASET}.reactome_pathways" \
  "${BUCKET}/reactome/ReactomePathways.txt" \
  pathway_id:STRING,pathway_name:STRING,species:STRING \
  || echo "FAILED: reactome_pathways"

bq load --source_format=CSV --field_delimiter='\t' --skip_leading_rows=0 \
  --noautodetect \
  "${PROJECT_ID}:${DATASET}.reactome_pathway_relations" \
  "${BUCKET}/reactome/ReactomePathwaysRelation.txt" \
  parent_pathway_id:STRING,child_pathway_id:STRING \
  || echo "FAILED: reactome_pathway_relations"

bq load --source_format=CSV --field_delimiter='\t' --skip_leading_rows=0 \
  --noautodetect \
  "${PROJECT_ID}:${DATASET}.reactome_uniprot_pathways" \
  "${BUCKET}/reactome/UniProt2Reactome_All_Levels.txt" \
  uniprot_id:STRING,pathway_id:STRING,url:STRING,pathway_name:STRING,evidence_code:STRING,species:STRING \
  || echo "FAILED: reactome_uniprot_pathways"

bq load --source_format=CSV --field_delimiter='\t' --skip_leading_rows=0 \
  --noautodetect \
  "${PROJECT_ID}:${DATASET}.reactome_ncbi_pathways" \
  "${BUCKET}/reactome/NCBI2Reactome_All_Levels.txt" \
  ncbi_id:STRING,pathway_id:STRING,url:STRING,pathway_name:STRING,evidence_code:STRING,species:STRING \
  || echo "FAILED: reactome_ncbi_pathways"

bq load --source_format=CSV --field_delimiter='\t' --skip_leading_rows=0 \
  --noautodetect \
  "${PROJECT_ID}:${DATASET}.reactome_ensembl_pathways" \
  "${BUCKET}/reactome/Ensembl2Reactome_All_Levels.txt" \
  ensembl_id:STRING,pathway_id:STRING,url:STRING,pathway_name:STRING,evidence_code:STRING,species:STRING \
  || echo "FAILED: reactome_ensembl_pathways"

bq load --source_format=CSV --field_delimiter='\t' --skip_leading_rows=0 \
  --noautodetect \
  "${PROJECT_ID}:${DATASET}.reactome_chebi_pathways" \
  "${BUCKET}/reactome/ChEBI2Reactome_All_Levels.txt" \
  chebi_id:STRING,pathway_id:STRING,url:STRING,pathway_name:STRING,evidence_code:STRING,species:STRING \
  || echo "FAILED: reactome_chebi_pathways"

load_tsv reactome_interactions "${BUCKET}/reactome/reactome.homo_sapiens.interactions.psi-mitab.txt"

# ── Human Protein Atlas (TSV inside ZIP — use external table) ──
echo ""
echo "=== Human Protein Atlas ==="
echo "HPA is in .zip format — skipping direct load (use proteinatlas.json.gz or API instead)"

# ── GTEx (GCT format — skip 2-line header, then tab-delimited) ──
echo ""
echo "=== GTEx ==="
load_tsv gtex_sample_attributes "${BUCKET}/gtex/GTEx_Analysis_v8_Annotations_SampleAttributesDS.txt"
load_tsv gtex_subject_phenotypes "${BUCKET}/gtex/GTEx_Analysis_v8_Annotations_SubjectPhenotypesDS.txt"

# ── bioRxiv/medRxiv (JSON — convert to NDJSON) ──────────
echo ""
echo "=== bioRxiv/medRxiv ==="
echo "Converting API JSON to NDJSON for BigQuery..."

TMPDIR=$(mktemp -d)
for prefix in biorxiv medrxiv; do
  NDJSON_FILE="${TMPDIR}/${prefix}_all.ndjson"
  > "${NDJSON_FILE}"
  for offset in 0 100 200 300 400 500 600 700 800 900; do
    SRC="${BUCKET}/biorxiv-medrxiv/${prefix}_metadata_offset_${offset}.json"
    gsutil cat "${SRC}" 2>/dev/null | python3 -c "
import json, sys
data = json.load(sys.stdin)
for rec in data.get('collection', []):
    rec['source_server'] = '${prefix}'
    print(json.dumps(rec))
" >> "${NDJSON_FILE}" || true
  done
  DEST="${BUCKET}/biorxiv-medrxiv/${prefix}_all.ndjson"
  gsutil cp "${NDJSON_FILE}" "${DEST}" 2>/dev/null || true
  bq load --autodetect --source_format=NEWLINE_DELIMITED_JSON \
    "${PROJECT_ID}:${DATASET}.preprints_${prefix}" \
    "${DEST}" || echo "FAILED: preprints_${prefix}"
done
rm -rf "${TMPDIR}"

# ── PubMedQA (JSON — convert to NDJSON) ─────────────────
echo ""
echo "=== PubMedQA ==="
TMPDIR=$(mktemp -d)
gsutil cat "${BUCKET}/pubmedqa/ori_pqal.json" 2>/dev/null | python3 -c "
import json, sys
data = json.load(sys.stdin)
for pmid, rec in data.items():
    rec['pmid'] = pmid
    print(json.dumps(rec))
" > "${TMPDIR}/pubmedqa.ndjson"
gsutil cp "${TMPDIR}/pubmedqa.ndjson" "${BUCKET}/pubmedqa/pubmedqa.ndjson" 2>/dev/null || true
bq load --autodetect --source_format=NEWLINE_DELIMITED_JSON \
  "${PROJECT_ID}:${DATASET}.pubmedqa" \
  "${BUCKET}/pubmedqa/pubmedqa.ndjson" || echo "FAILED: pubmedqa"
rm -rf "${TMPDIR}"

echo ""
echo "=== Done ==="
echo "Loaded tables into ${PROJECT_ID}:${DATASET}"
echo "Verify with: bq ls ${PROJECT_ID}:${DATASET}"
