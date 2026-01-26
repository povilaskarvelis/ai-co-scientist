import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const server = new McpServer({
  name: "research-mcp",
  version: "0.1.0",
});

const NCBI_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils";
const OPEN_TARGETS_API = "https://api.platform.opentargets.org/api/v4/graphql";
const UNIPROT_API = "https://rest.uniprot.org";
const CLINICAL_TRIALS_API = "https://clinicaltrials.gov/api/v2";
const DATA_DIR = path.resolve(__dirname, "data");

function sanitizeXmlText(value) {
  if (!value) return "";
  return value.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
}

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Request failed (${response.status}): ${url}`);
  }
  return response.json();
}

async function fetchText(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Request failed (${response.status}): ${url}`);
  }
  return response.text();
}

async function queryOpenTargets(query, variables = {}) {
  try {
    const response = await fetch(OPEN_TARGETS_API, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query, variables }),
    });
    if (!response.ok) {
      console.error(`Open Targets API error: ${response.status}`);
      return { data: null, error: `API returned ${response.status}` };
    }
    return response.json();
  } catch (error) {
    console.error(`Open Targets network error: ${error.message}`);
    return { data: null, error: `Network error: ${error.message}` };
  }
}

async function listDataFiles() {
  try {
    const entries = await fs.readdir(DATA_DIR, { withFileTypes: true });
    return entries.filter((entry) => entry.isFile()).map((entry) => entry.name);
  } catch (error) {
    if (error.code === "ENOENT") {
      return [];
    }
    throw error;
  }
}

function resolveDataPath(filename) {
  const safeName = path.basename(filename);
  const resolved = path.resolve(DATA_DIR, safeName);
  if (!resolved.startsWith(DATA_DIR)) {
    throw new Error("Invalid filename. Only files in ./data are allowed.");
  }
  return resolved;
}

// ============================================
// TOOL 1: Search PubMed for relevant papers
// ============================================
server.registerTool(
  "search_pubmed",
  {
    description: "Searches PubMed and returns recent papers with IDs and titles",
    inputSchema: {
      query: z.string().describe("PubMed search query (e.g., 'Alzheimer microglia single cell')"),
      retmax: z.number().optional().default(5).describe("Max number of results"),
      sort: z.string().optional().default("relevance").describe("Sort order: 'relevance' or 'date'"),
    },
  },
  async ({ query, retmax = 5, sort = "relevance" }) => {
    try {
      const searchUrl = `${NCBI_BASE}/esearch.fcgi?db=pubmed&term=${encodeURIComponent(
        query
      )}&retmax=${retmax}&sort=${encodeURIComponent(sort)}&retmode=json`;
      const search = await fetchJson(searchUrl);
      const ids = search?.esearchresult?.idlist ?? [];
      if (ids.length === 0) {
        return {
          content: [
            {
              type: "text",
              text: `No PubMed results found for query: "${query}". Try different keywords.`,
            },
          ],
        };
      }

      const summaryUrl = `${NCBI_BASE}/esummary.fcgi?db=pubmed&id=${ids.join(
        ","
      )}&retmode=json`;
      const summary = await fetchJson(summaryUrl);
      const results = ids.map((id) => ({
        pmid: id,
        title: summary?.result?.[id]?.title ?? "Untitled",
        pubdate: summary?.result?.[id]?.pubdate ?? "Unknown date",
        journal: summary?.result?.[id]?.fulljournalname ?? "Unknown journal",
      }));

      return {
        content: [
          {
            type: "text",
            text: `PubMed results for "${query}":\n\n` +
              results
                .map(
                  (item, index) =>
                    `${index + 1}. PMID ${item.pmid} — ${item.title} (${item.journal}, ${item.pubdate})`
                )
                .join("\n"),
          },
        ],
      };
    } catch (error) {
      return {
        content: [{ type: "text", text: `Error searching PubMed: ${error.message}. Try again.` }],
      };
    }
  }
);

// ============================================
// TOOL 2: Fetch PubMed abstract by PMID
// ============================================
server.registerTool(
  "get_pubmed_abstract",
  {
    description: "Fetches a PubMed abstract and title for a given PMID",
    inputSchema: {
      pmid: z.string().describe("PubMed ID (PMID) to fetch"),
    },
  },
  async ({ pmid }) => {
    const fetchUrl = `${NCBI_BASE}/efetch.fcgi?db=pubmed&id=${encodeURIComponent(
      pmid
    )}&retmode=xml`;
    const xml = await fetchText(fetchUrl);
    const titleMatch = xml.match(/<ArticleTitle>([\s\S]*?)<\/ArticleTitle>/);
    const abstractMatches = [...xml.matchAll(/<AbstractText[^>]*>([\s\S]*?)<\/AbstractText>/g)];
    const title = sanitizeXmlText(titleMatch?.[1]);
    const abstract = sanitizeXmlText(
      abstractMatches.map((match) => match[1]).join(" ")
    );

    if (!title && !abstract) {
      return {
        content: [
          {
            type: "text",
            text: `No abstract found for PMID ${pmid}.`,
          },
        ],
      };
    }

    return {
      content: [
        {
          type: "text",
          text: `PMID ${pmid}\nTitle: ${title || "Untitled"}\nAbstract: ${
            abstract || "No abstract available."
          }`,
        },
      ],
    };
  }
);

// ============================================
// TOOL 3: List local datasets (./data)
// ============================================
server.registerTool(
  "list_local_datasets",
  {
    description: "Lists files in the local ./data directory",
  },
  async () => {
    const files = await listDataFiles();
    if (files.length === 0) {
      return {
        content: [
          {
            type: "text",
            text:
              "No local datasets found. Add files to ./data (e.g., CSV, TSV, JSON).",
          },
        ],
      };
    }
    return {
      content: [
        {
          type: "text",
          text: `Local datasets:\n${files.map((file) => `- ${file}`).join("\n")}`,
        },
      ],
    };
  }
);

// ============================================
// TOOL 4: Read a local dataset file (safe path)
// ============================================
server.registerTool(
  "read_local_dataset",
  {
    description: "Reads the first N lines from a local dataset in ./data",
    inputSchema: {
      filename: z.string().describe("Filename inside ./data"),
      maxLines: z.number().optional().default(200).describe("Max number of lines to return"),
    },
  },
  async ({ filename, maxLines = 200 }) => {
    const resolved = resolveDataPath(filename);
    const contents = await fs.readFile(resolved, "utf-8");
    const lines = contents.split(/\r?\n/).slice(0, maxLines);

    return {
      content: [
        {
          type: "text",
          text: lines.join("\n"),
        },
      ],
    };
  }
);

// ============================================
// TOOL 5: Search for diseases in Open Targets
// ============================================
server.registerTool(
  "search_diseases",
  {
    description:
      "Searches Open Targets for diseases matching a query. Returns disease IDs needed for target lookups.",
    inputSchema: {
      query: z.string().describe("Disease name to search (e.g., 'Alzheimer', 'Parkinson', 'breast cancer')"),
      limit: z.number().optional().default(5).describe("Max number of results"),
    },
  },
  async ({ query, limit = 5 }) => {
    const graphqlQuery = `
      query SearchDiseases($queryString: String!, $size: Int!) {
        search(queryString: $queryString, entityNames: ["disease"], page: { size: $size, index: 0 }) {
          hits {
            id
            name
            description
            entity
          }
        }
      }
    `;
    const result = await queryOpenTargets(graphqlQuery, { queryString: query, size: limit });
    const hits = result?.data?.search?.hits ?? [];

    if (hits.length === 0) {
      return {
        content: [{ type: "text", text: `No diseases found for query: "${query}"` }],
      };
    }

    const formatted = hits
      .map(
        (hit, i) =>
          `${i + 1}. ${hit.name}\n   ID: ${hit.id}\n   ${hit.description || "No description"}`
      )
      .join("\n\n");

    return {
      content: [
        {
          type: "text",
          text: `Diseases matching "${query}":\n\n${formatted}\n\nUse the disease ID (e.g., "${hits[0].id}") with search_disease_targets to find associated drug targets.`,
        },
      ],
    };
  }
);

// ============================================
// TOOL 6: Get drug targets for a disease
// ============================================
server.registerTool(
  "search_disease_targets",
  {
    description:
      "Finds drug targets (genes/proteins) associated with a disease. Returns targets ranked by association score with evidence.",
    inputSchema: {
      diseaseId: z
        .string()
        .describe("Open Targets disease ID (e.g., 'EFO_0000249' for Alzheimer's). Use search_diseases first to find IDs."),
      limit: z.number().optional().default(10).describe("Max number of targets to return"),
    },
  },
  async ({ diseaseId, limit = 10 }) => {
    const graphqlQuery = `
      query DiseaseTargets($diseaseId: String!, $size: Int!) {
        disease(efoId: $diseaseId) {
          id
          name
          associatedTargets(page: { size: $size, index: 0 }) {
            count
            rows {
              target {
                id
                approvedSymbol
                approvedName
                biotype
              }
              score
              datatypeScores {
                id
                score
              }
            }
          }
        }
      }
    `;
    const result = await queryOpenTargets(graphqlQuery, { diseaseId, size: limit });
    const disease = result?.data?.disease;

    if (!disease) {
      return {
        content: [
          {
            type: "text",
            text: `Disease not found: "${diseaseId}". Use search_diseases to find valid disease IDs.`,
          },
        ],
      };
    }

    const rows = disease.associatedTargets?.rows ?? [];
    if (rows.length === 0) {
      return {
        content: [{ type: "text", text: `No targets found for disease: ${disease.name}` }],
      };
    }

    const formatted = rows
      .map((row, i) => {
        const t = row.target;
        const evidenceTypes = row.datatypeScores
          .filter((d) => d.score > 0)
          .map((d) => `${d.id}: ${(d.score * 100).toFixed(0)}%`)
          .join(", ");
        return `${i + 1}. ${t.approvedSymbol} (${t.approvedName})
   Target ID: ${t.id}
   Overall Score: ${(row.score * 100).toFixed(1)}%
   Evidence: ${evidenceTypes || "N/A"}
   Type: ${t.biotype}`;
      })
      .join("\n\n");

    return {
      content: [
        {
          type: "text",
          text: `Top ${rows.length} drug targets for ${disease.name} (${disease.id}):\nTotal associated targets: ${disease.associatedTargets.count}\n\n${formatted}\n\nUse get_target_info or check_druggability with the Target ID for more details.`,
        },
      ],
    };
  }
);

// ============================================
// TOOL 7: Get detailed target information
// ============================================
server.registerTool(
  "get_target_info",
  {
    description:
      "Gets detailed information about a drug target (gene/protein) including function, pathways, and tractability.",
    inputSchema: {
      targetId: z
        .string()
        .describe("Ensembl gene ID (e.g., 'ENSG00000142192' for EGFR). Get this from search_disease_targets."),
    },
  },
  async ({ targetId }) => {
    const graphqlQuery = `
      query TargetInfo($targetId: String!) {
        target(ensemblId: $targetId) {
          id
          approvedSymbol
          approvedName
          biotype
          functionDescriptions
          subcellularLocations {
            location
          }
          pathways {
            pathway
            pathwayId
          }
          tractability {
            label
            modality
            value
          }
          synonyms {
            label
          }
        }
      }
    `;
    const result = await queryOpenTargets(graphqlQuery, { targetId });
    const target = result?.data?.target;

    if (!target) {
      return {
        content: [{ type: "text", text: `Target not found: "${targetId}"` }],
      };
    }

    const functions = target.functionDescriptions?.slice(0, 3).join("\n   ") || "No function data";
    const locations =
      target.subcellularLocations?.map((l) => l.location).join(", ") || "Unknown";
    const pathways =
      target.pathways?.slice(0, 5).map((p) => p.pathway).join(", ") || "None listed";
    const tractability =
      target.tractability
        ?.filter((t) => t.value === true)
        .map((t) => `${t.modality}: ${t.label}`)
        .join(", ") || "No tractability data";
    const synonyms = target.synonyms?.slice(0, 5).map((s) => s.label).join(", ") || "None";

    return {
      content: [
        {
          type: "text",
          text: `Target: ${target.approvedSymbol} (${target.approvedName})
ID: ${target.id}
Type: ${target.biotype}
Synonyms: ${synonyms}

Function:
   ${functions}

Subcellular Location: ${locations}

Key Pathways: ${pathways}

Tractability (druggability indicators):
   ${tractability}

Use check_druggability for detailed druggability assessment, or get_target_drugs to see known drugs.`,
        },
      ],
    };
  }
);

// ============================================
// TOOL 8: Check target druggability
// ============================================
server.registerTool(
  "check_druggability",
  {
    description:
      "Assesses whether a target is druggable - can it be modulated by small molecules, antibodies, or other modalities?",
    inputSchema: {
      targetId: z.string().describe("Ensembl gene ID (e.g., 'ENSG00000142192')"),
    },
  },
  async ({ targetId }) => {
    const graphqlQuery = `
      query Druggability($targetId: String!) {
        target(ensemblId: $targetId) {
          id
          approvedSymbol
          approvedName
          tractability {
            label
            modality
            value
          }
          knownDrugs {
            uniqueDrugs
            count
          }
        }
      }
    `;
    const result = await queryOpenTargets(graphqlQuery, { targetId });
    const target = result?.data?.target;

    if (!target) {
      return {
        content: [{ type: "text", text: `Target not found: "${targetId}"` }],
      };
    }

    // Group tractability by modality
    const tractByModality = {};
    for (const t of target.tractability || []) {
      if (!tractByModality[t.modality]) tractByModality[t.modality] = [];
      if (t.value === true) tractByModality[t.modality].push(t.label);
    }

    const modalityLines = Object.entries(tractByModality)
      .map(([modality, labels]) => {
        if (labels.length === 0) return `   ${modality}: No positive indicators`;
        return `   ${modality}: ${labels.join(", ")}`;
      })
      .join("\n");

    const drugInfo = target.knownDrugs;
    const drugSummary =
      drugInfo && drugInfo.uniqueDrugs > 0
        ? `Yes - ${drugInfo.uniqueDrugs} unique drugs (${drugInfo.count} drug-target interactions)`
        : "No known drugs targeting this protein";

    // Calculate overall druggability assessment
    const hasSmallMolecule = tractByModality["SM"]?.length > 0;
    const hasAntibody = tractByModality["AB"]?.length > 0;
    const hasOther = tractByModality["PR"]?.length > 0 || tractByModality["OC"]?.length > 0;
    
    let assessment = "LOW";
    if (hasSmallMolecule || (drugInfo?.uniqueDrugs > 0)) assessment = "HIGH";
    else if (hasAntibody || hasOther) assessment = "MEDIUM";

    return {
      content: [
        {
          type: "text",
          text: `Druggability Assessment for ${target.approvedSymbol} (${target.approvedName})

Overall Druggability: ${assessment}

Known Drugs: ${drugSummary}

Tractability by Modality:
${modalityLines || "   No tractability data available"}

Legend:
- SM = Small Molecule
- AB = Antibody
- PR = PROTAC
- OC = Other Clinical

${assessment === "HIGH" ? "This target has strong evidence of being druggable." : assessment === "MEDIUM" ? "This target may be druggable with certain modalities." : "This target may be challenging to drug with current approaches."}`,
        },
      ],
    };
  }
);

// ============================================
// TOOL 9: Get known drugs for a target
// ============================================
server.registerTool(
  "get_target_drugs",
  {
    description: "Gets known drugs that target a specific gene/protein, including clinical trial status.",
    inputSchema: {
      targetId: z.string().describe("Ensembl gene ID (e.g., 'ENSG00000146648' for EGFR)"),
      limit: z.number().optional().default(10).describe("Max number of drugs to return"),
    },
  },
  async ({ targetId, limit = 10 }) => {
    const graphqlQuery = `
      query TargetDrugs($targetId: String!) {
        target(ensemblId: $targetId) {
          id
          approvedSymbol
          approvedName
          knownDrugs {
            uniqueDrugs
            count
            rows {
              drug {
                id
                name
                drugType
                maximumClinicalTrialPhase
                hasBeenWithdrawn
                description
              }
              mechanismOfAction
              disease {
                id
                name
              }
            }
          }
        }
      }
    `;
    const result = await queryOpenTargets(graphqlQuery, { targetId });
    const target = result?.data?.target;

    if (!target) {
      return {
        content: [{ type: "text", text: `Target not found: "${targetId}"` }],
      };
    }

    const drugs = target.knownDrugs;
    if (!drugs || drugs.uniqueDrugs === 0) {
      return {
        content: [
          {
            type: "text",
            text: `No known drugs for ${target.approvedSymbol} (${target.approvedName}).\n\nThis could be an opportunity for novel drug development, or the target may be challenging to drug.`,
          },
        ],
      };
    }

    const phaseLabels = {
      4: "Approved",
      3: "Phase III",
      2: "Phase II",
      1: "Phase I",
      0.5: "Early Phase I",
      0: "Preclinical",
    };

    const formatted = drugs.rows
      .slice(0, limit)
      .map((row, i) => {
        const d = row.drug;
        const phase = phaseLabels[d.maximumClinicalTrialPhase] || `Phase ${d.maximumClinicalTrialPhase}`;
        const withdrawn = d.hasBeenWithdrawn ? " [WITHDRAWN]" : "";
        return `${i + 1}. ${d.name}${withdrawn}
   Type: ${d.drugType}
   Status: ${phase}
   Mechanism: ${row.mechanismOfAction || "Unknown"}
   Indication: ${row.disease?.name || "Various"}`;
      })
      .join("\n\n");

    return {
      content: [
        {
          type: "text",
          text: `Known drugs for ${target.approvedSymbol} (${target.approvedName}):
Total: ${drugs.uniqueDrugs} unique drugs, ${drugs.count} interactions

${formatted}

Note: A target with approved drugs validates it as druggable. Multiple drugs may indicate competitive landscape.`,
        },
      ],
    };
  }
);

// ============================================
// TOOL 10: Get gene info from NCBI
// ============================================
server.registerTool(
  "get_gene_info",
  {
    description:
      "Gets gene information from NCBI Gene database including summary, aliases, and genomic location.",
    inputSchema: {
      geneSymbol: z.string().describe("Gene symbol (e.g., 'BRCA1', 'EGFR', 'TP53')"),
    },
  },
  async ({ geneSymbol }) => {
    // First search for the gene to get its ID
    const searchUrl = `${NCBI_BASE}/esearch.fcgi?db=gene&term=${encodeURIComponent(
      geneSymbol
    )}[sym]+AND+human[orgn]&retmode=json`;
    const search = await fetchJson(searchUrl);
    const ids = search?.esearchresult?.idlist ?? [];

    if (ids.length === 0) {
      return {
        content: [{ type: "text", text: `No gene found for symbol: "${geneSymbol}"` }],
      };
    }

    // Fetch gene summary
    const summaryUrl = `${NCBI_BASE}/esummary.fcgi?db=gene&id=${ids[0]}&retmode=json`;
    const summary = await fetchJson(summaryUrl);
    const gene = summary?.result?.[ids[0]];

    if (!gene) {
      return {
        content: [{ type: "text", text: `Could not retrieve info for gene: "${geneSymbol}"` }],
      };
    }

    const aliases = gene.otheraliases || "None";
    const otherDesignations = gene.otherdesignations || "None";

    return {
      content: [
        {
          type: "text",
          text: `Gene: ${gene.name} (${gene.description})
NCBI Gene ID: ${ids[0]}
Organism: ${gene.organism?.scientificname || "Homo sapiens"}

Summary:
${gene.summary || "No summary available"}

Aliases: ${aliases}
Other Names: ${otherDesignations}

Genomic Location:
   Chromosome: ${gene.chromosome || "Unknown"}
   Map Location: ${gene.maplocation || "Unknown"}

Links:
   NCBI Gene: https://www.ncbi.nlm.nih.gov/gene/${ids[0]}`,
        },
      ],
    };
  }
);

// ============================================
// TOOL 11: Search targets by gene symbol
// ============================================
server.registerTool(
  "search_targets",
  {
    description:
      "Searches Open Targets for genes/proteins by symbol or name. Returns target IDs for use with other tools.",
    inputSchema: {
      query: z.string().describe("Gene symbol or name (e.g., 'BRCA1', 'epidermal growth factor')"),
      limit: z.number().optional().default(5).describe("Max number of results"),
    },
  },
  async ({ query, limit = 5 }) => {
    const graphqlQuery = `
      query SearchTargets($queryString: String!, $size: Int!) {
        search(queryString: $queryString, entityNames: ["target"], page: { size: $size, index: 0 }) {
          hits {
            id
            name
            description
            entity
          }
        }
      }
    `;
    const result = await queryOpenTargets(graphqlQuery, { queryString: query, size: limit });
    const hits = result?.data?.search?.hits ?? [];

    if (hits.length === 0) {
      return {
        content: [{ type: "text", text: `No targets found for query: "${query}"` }],
      };
    }

    const formatted = hits
      .map(
        (hit, i) =>
          `${i + 1}. ${hit.name}\n   Target ID: ${hit.id}\n   ${hit.description?.slice(0, 150) || "No description"}...`
      )
      .join("\n\n");

    return {
      content: [
        {
          type: "text",
          text: `Targets matching "${query}":\n\n${formatted}\n\nUse the Target ID with get_target_info, check_druggability, or get_target_drugs.`,
        },
      ],
    };
  }
);

// ============================================
// TOOL 12: Search clinical trials
// ============================================
server.registerTool(
  "search_clinical_trials",
  {
    description:
      "Searches ClinicalTrials.gov for clinical trials. Find trials by disease, drug, target/gene, or sponsor. Returns trial status, phase, and key details.",
    inputSchema: {
      query: z
        .string()
        .describe("Search terms (e.g., 'LRRK2 Parkinson', 'pembrolizumab lung cancer', 'Alzheimer Phase 3')"),
      status: z
        .string()
        .optional()
        .describe("Filter by status: 'RECRUITING', 'COMPLETED', 'ACTIVE_NOT_RECRUITING', 'TERMINATED', or leave empty for all"),
      limit: z.number().optional().default(10).describe("Max number of results"),
    },
  },
  async ({ query, status, limit = 10 }) => {
    const params = new URLSearchParams({
      "query.term": query,
      pageSize: String(limit),
      format: "json",
    });

    if (status) {
      params.append("filter.overallStatus", status);
    }

    const url = `${CLINICAL_TRIALS_API}/studies?${params.toString()}`;
    
    let studies = [];
    try {
      const response = await fetch(url);
      
      if (!response.ok) {
        return {
          content: [{ type: "text", text: `ClinicalTrials.gov API error (${response.status}). Try a different search term.` }],
        };
      }
      
      const text = await response.text();
      if (!text || text.trim() === '') {
        return {
          content: [{ type: "text", text: `ClinicalTrials.gov returned empty response for: "${query}". Try broader search terms.` }],
        };
      }
      
      const data = JSON.parse(text);
      studies = data?.studies ?? [];
    } catch (error) {
      return {
        content: [{ type: "text", text: `Error searching clinical trials: ${error.message}. Try again or use different search terms.` }],
      };
    }

    if (studies.length === 0) {
      return {
        content: [
          {
            type: "text",
            text: `No clinical trials found for: "${query}"${status ? ` with status ${status}` : ""}`,
          },
        ],
      };
    }

    const formatted = studies.map((study, i) => {
      const protocol = study.protocolSection;
      const id = protocol?.identificationModule;
      const status = protocol?.statusModule;
      const design = protocol?.designModule;
      const conditions = protocol?.conditionsModule?.conditions?.slice(0, 3).join(", ") || "Not specified";
      const interventions = protocol?.armsInterventionsModule?.interventions
        ?.slice(0, 2)
        .map((int) => `${int.name} (${int.type})`)
        .join(", ") || "Not specified";
      const sponsor = protocol?.sponsorCollaboratorsModule?.leadSponsor?.name || "Unknown";

      const phase = design?.phases?.join(", ") || "Not specified";
      const enrollment = design?.enrollmentInfo?.count || "Unknown";

      return `${i + 1}. ${id?.briefTitle || "Untitled"}
   NCT ID: ${id?.nctId || "Unknown"}
   Status: ${status?.overallStatus || "Unknown"}
   Phase: ${phase}
   Conditions: ${conditions}
   Interventions: ${interventions}
   Enrollment: ${enrollment} participants
   Sponsor: ${sponsor}`;
    }).join("\n\n");

    return {
      content: [
        {
          type: "text",
          text: `Clinical trials for "${query}":\nFound ${data.totalCount || studies.length} trials\n\n${formatted}\n\nUse get_clinical_trial with the NCT ID for full details including results.`,
        },
      ],
    };
  }
);

// ============================================
// TOOL 13: Get clinical trial details
// ============================================
server.registerTool(
  "get_clinical_trial",
  {
    description:
      "Gets detailed information about a specific clinical trial including design, outcomes, and results if available.",
    inputSchema: {
      nctId: z.string().describe("ClinicalTrials.gov ID (e.g., 'NCT04665245')"),
    },
  },
  async ({ nctId }) => {
    const url = `${CLINICAL_TRIALS_API}/studies/${nctId}?format=json`;
    
    let study;
    try {
      const response = await fetch(url);
      
      if (!response.ok) {
        if (response.status === 404) {
          return {
            content: [{ type: "text", text: `Clinical trial not found: ${nctId}. Check the NCT ID format (e.g., NCT04665245).` }],
          };
        }
        return {
          content: [{ type: "text", text: `ClinicalTrials.gov API error (${response.status}) for ${nctId}.` }],
        };
      }
      
      const text = await response.text();
      if (!text || text.trim() === '') {
        return {
          content: [{ type: "text", text: `Empty response for ${nctId}. The trial may not exist.` }],
        };
      }
      
      study = JSON.parse(text);
    } catch (error) {
      return {
        content: [{ type: "text", text: `Error fetching trial ${nctId}: ${error.message}` }],
      };
    }
    const protocol = study.protocolSection;
    const results = study.resultsSection;

    // Basic info
    const id = protocol?.identificationModule;
    const status = protocol?.statusModule;
    const description = protocol?.descriptionModule;
    const design = protocol?.designModule;
    const eligibility = protocol?.eligibilityModule;
    const outcomes = protocol?.outcomesModule;
    const conditions = protocol?.conditionsModule?.conditions?.join(", ") || "Not specified";
    
    // Interventions
    const interventions = protocol?.armsInterventionsModule?.interventions
      ?.map((int) => `- ${int.name} (${int.type}): ${int.description || "No description"}`)
      .join("\n") || "Not specified";

    // Primary outcomes
    const primaryOutcomes = outcomes?.primaryOutcomes
      ?.map((o) => `- ${o.measure} (${o.timeFrame})`)
      .join("\n") || "Not specified";

    // Results summary (if available)
    let resultsText = "No results posted yet.";
    if (results) {
      const participants = results.participantFlowModule;
      const baseline = results.baselineCharacteristicsModule;
      const outcomeResults = results.outcomeMeasuresModule?.outcomeMeasures;
      
      resultsText = "RESULTS AVAILABLE:\n";
      
      if (participants?.preAssignmentDetails) {
        resultsText += `   Pre-assignment: ${participants.preAssignmentDetails}\n`;
      }
      
      if (outcomeResults && outcomeResults.length > 0) {
        resultsText += "   Outcome measures:\n";
        outcomeResults.slice(0, 3).forEach((outcome) => {
          resultsText += `   - ${outcome.title}: ${outcome.description || "See full results"}\n`;
        });
      }
      
      if (results.adverseEventsModule) {
        const ae = results.adverseEventsModule;
        resultsText += `   Serious adverse events: ${ae.seriousNumAffected || "Not reported"} participants\n`;
      }
    }

    // Determine if trial succeeded/failed (heuristic based on status and results)
    let outcomeAssessment = "";
    const overallStatus = status?.overallStatus;
    if (overallStatus === "TERMINATED") {
      const whyStopped = status?.whyStoppedText || "Reason not provided";
      outcomeAssessment = `⚠️ TERMINATED: ${whyStopped}`;
    } else if (overallStatus === "COMPLETED" && results) {
      outcomeAssessment = "✓ COMPLETED with results posted";
    } else if (overallStatus === "COMPLETED") {
      outcomeAssessment = "✓ COMPLETED (results not yet posted)";
    } else if (overallStatus === "RECRUITING") {
      outcomeAssessment = "🔄 Currently RECRUITING";
    } else {
      outcomeAssessment = `Status: ${overallStatus}`;
    }

    return {
      content: [
        {
          type: "text",
          text: `Clinical Trial: ${id?.briefTitle || "Untitled"}

NCT ID: ${nctId}
${outcomeAssessment}

Official Title: ${id?.officialTitle || "Not provided"}

Phase: ${design?.phases?.join(", ") || "Not specified"}
Study Type: ${design?.studyType || "Not specified"}
Enrollment: ${design?.enrollmentInfo?.count || "Unknown"} participants

Conditions: ${conditions}

Brief Summary:
${description?.briefSummary || "No summary available"}

Interventions:
${interventions}

Primary Outcome Measures:
${primaryOutcomes}

Eligibility:
   Age: ${eligibility?.minimumAge || "Not specified"} to ${eligibility?.maximumAge || "Not specified"}
   Sex: ${eligibility?.sex || "All"}
   Healthy Volunteers: ${eligibility?.healthyVolunteers || "No"}

Dates:
   Start: ${status?.startDateStruct?.date || "Unknown"}
   Completion: ${status?.completionDateStruct?.date || "Unknown"}

${resultsText}

Link: https://clinicaltrials.gov/study/${nctId}`,
        },
      ],
    };
  }
);

async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error("Research MCP server running on stdio");
}

main().catch(console.error);
