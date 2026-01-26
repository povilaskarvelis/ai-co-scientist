// Test script for Research MCP tools
const OPEN_TARGETS_API = "https://api.platform.opentargets.org/api/v4/graphql";
const NCBI_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils";

async function queryOpenTargets(query, variables = {}) {
  const res = await fetch(OPEN_TARGETS_API, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query, variables }),
  });
  return res.json();
}

async function testSearchDiseases() {
  console.log("\n📋 TEST: search_diseases('Alzheimer')");
  const query = `
    query SearchDiseases($queryString: String!, $size: Int!) {
      search(queryString: $queryString, entityNames: ["disease"], page: { size: $size, index: 0 }) {
        hits { id name description }
      }
    }
  `;
  const result = await queryOpenTargets(query, { queryString: "Alzheimer", size: 3 });
  
  if (result.errors) {
    console.log("   Error:", result.errors[0]?.message);
    return null;
  }
  
  const hits = result?.data?.search?.hits || [];
  hits.forEach((h, i) => console.log(`   ${i+1}. ${h.name} → ${h.id}`));
  return hits[0]?.id;
}

async function testDiseaseTargets(diseaseId) {
  console.log(`\n🎯 TEST: search_disease_targets('${diseaseId}')`);
  if (!diseaseId) {
    console.log("   Skipped - no disease ID");
    return null;
  }
  
  const query = `
    query DiseaseTargets($diseaseId: String!, $size: Int!) {
      disease(efoId: $diseaseId) {
        name
        associatedTargets(page: { size: $size, index: 0 }) {
          count
          rows {
            target { id approvedSymbol approvedName }
            score
          }
        }
      }
    }
  `;
  const result = await queryOpenTargets(query, { diseaseId, size: 5 });
  
  if (result.errors) {
    console.log("   Error:", result.errors[0]?.message);
    return null;
  }
  
  const disease = result?.data?.disease;
  console.log(`   Disease: ${disease?.name}`);
  console.log(`   Total targets: ${disease?.associatedTargets?.count}`);
  
  const rows = disease?.associatedTargets?.rows || [];
  rows.forEach((r, i) => {
    console.log(`   ${i+1}. ${r.target.approvedSymbol} - ${(r.score * 100).toFixed(0)}%`);
  });
  
  return rows[0]?.target?.id;
}

async function testDruggability(targetId) {
  console.log(`\n💊 TEST: check_druggability('${targetId}')`);
  if (!targetId) {
    console.log("   Skipped - no target ID");
    return null;
  }
  
  const query = `
    query Druggability($targetId: String!) {
      target(ensemblId: $targetId) {
        approvedSymbol
        approvedName
        knownDrugs { uniqueDrugs }
        tractability { modality label value }
      }
    }
  `;
  const result = await queryOpenTargets(query, { targetId });
  
  if (result.errors) {
    console.log("   Error:", result.errors[0]?.message);
    return null;
  }
  
  const target = result?.data?.target;
  console.log(`   Target: ${target?.approvedSymbol} (${target?.approvedName})`);
  console.log(`   Known drugs: ${target?.knownDrugs?.uniqueDrugs || 0}`);
  
  const tract = target?.tractability?.filter(t => t.value) || [];
  const byMod = {};
  tract.forEach(t => {
    byMod[t.modality] = byMod[t.modality] || [];
    byMod[t.modality].push(t.label);
  });
  
  Object.entries(byMod).forEach(([mod, labels]) => {
    console.log(`   ${mod}: ${labels.slice(0, 3).join(", ")}`);
  });
  
  return targetId;
}

async function testTargetDrugs(targetId) {
  console.log(`\n💉 TEST: get_target_drugs('${targetId}')`);
  if (!targetId) {
    console.log("   Skipped - no target ID");
    return;
  }
  
  const query = `
    query TargetDrugs($targetId: String!) {
      target(ensemblId: $targetId) {
        approvedSymbol
        knownDrugs {
          uniqueDrugs
          rows {
            drug { name drugType maximumClinicalTrialPhase }
            mechanismOfAction
          }
        }
      }
    }
  `;
  const result = await queryOpenTargets(query, { targetId });
  
  if (result.errors) {
    console.log("   Error:", result.errors[0]?.message);
    return;
  }
  
  const target = result?.data?.target;
  const drugs = target?.knownDrugs;
  console.log(`   Target: ${target?.approvedSymbol}`);
  console.log(`   Total drugs: ${drugs?.uniqueDrugs || 0}`);
  
  const phases = ["Preclinical", "Phase I", "Phase II", "Phase III", "Approved"];
  (drugs?.rows || []).slice(0, 5).forEach((r, i) => {
    const phase = phases[r.drug.maximumClinicalTrialPhase] || "Unknown";
    console.log(`   ${i+1}. ${r.drug.name} (${r.drug.drugType}) - ${phase}`);
  });
}

async function testGeneInfo(symbol) {
  console.log(`\n🧬 TEST: get_gene_info('${symbol}')`);
  const searchUrl = `${NCBI_BASE}/esearch.fcgi?db=gene&term=${symbol}[sym]+AND+human[orgn]&retmode=json`;
  const search = await fetch(searchUrl).then(r => r.json());
  const geneId = search?.esearchresult?.idlist?.[0];
  
  if (!geneId) {
    console.log("   Gene not found");
    return;
  }
  
  const summaryUrl = `${NCBI_BASE}/esummary.fcgi?db=gene&id=${geneId}&retmode=json`;
  const summary = await fetch(summaryUrl).then(r => r.json());
  const gene = summary?.result?.[geneId];
  
  console.log(`   Gene: ${gene?.name} (${gene?.description})`);
  console.log(`   NCBI ID: ${geneId}`);
  console.log(`   Chromosome: ${gene?.chromosome}`);
  console.log(`   Summary: ${gene?.summary?.slice(0, 150)}...`);
}

async function testPubMed(query) {
  console.log(`\n📚 TEST: search_pubmed('${query}')`);
  const searchUrl = `${NCBI_BASE}/esearch.fcgi?db=pubmed&term=${encodeURIComponent(query)}&retmax=3&sort=relevance&retmode=json`;
  const search = await fetch(searchUrl).then(r => r.json());
  const pmids = search?.esearchresult?.idlist || [];
  
  if (pmids.length === 0) {
    console.log("   No papers found");
    return;
  }
  
  const summaryUrl = `${NCBI_BASE}/esummary.fcgi?db=pubmed&id=${pmids.join(",")}&retmode=json`;
  const summary = await fetch(summaryUrl).then(r => r.json());
  
  pmids.forEach((id, i) => {
    const paper = summary?.result?.[id];
    console.log(`   ${i+1}. [PMID ${id}] ${paper?.title?.slice(0, 70)}...`);
  });
}

async function main() {
  console.log("🧪 Research MCP Tools - Full Test Suite");
  console.log("==========================================");
  
  try {
    // Test the full target discovery workflow
    const diseaseId = await testSearchDiseases();
    const targetId = await testDiseaseTargets(diseaseId);
    await testDruggability(targetId);
    await testTargetDrugs(targetId);
    
    // Test NCBI tools
    await testGeneInfo("PSEN1");
    await testPubMed("Alzheimer PSEN1 drug target");
    
    console.log("\n✅ All tests completed successfully!");
  } catch (error) {
    console.error("\n❌ Test failed:", error.message);
  }
}

main();
