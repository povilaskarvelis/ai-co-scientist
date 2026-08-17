function text(value) {
  return String(value ?? "").trim();
}

function displayText(value) {
  if (typeof value === "string") return text(value);
  return text(value?.displayText || value?.formatText || value?.name || value?.label || "");
}

export function normalizeAllianceGeneSummaryPayload(payload = {}) {
  const source = payload && typeof payload === "object" ? payload : {};
  const gene = source.gene && typeof source.gene === "object" ? source.gene : source;
  const taxon = gene.taxon && typeof gene.taxon === "object" ? gene.taxon : gene.species || {};
  const synonyms = Array.isArray(gene.geneSynonyms)
    ? gene.geneSynonyms.map(displayText).filter(Boolean)
    : Array.isArray(gene.synonyms)
      ? gene.synonyms.map(displayText).filter(Boolean)
      : [];
  const secondaryIds = Array.isArray(gene.geneSecondaryIds)
    ? gene.geneSecondaryIds.map((item) => text(item?.secondaryId || item)).filter(Boolean)
    : Array.isArray(gene.secondaryIds)
      ? gene.secondaryIds.map((item) => text(item?.secondaryId || item)).filter(Boolean)
      : [];
  const notes = Array.isArray(gene.relatedNotes)
    ? gene.relatedNotes.map((item) => text(item?.freeText || item?.note || item)).filter(Boolean)
    : [];

  return {
    id: text(gene.primaryExternalId || gene.id || gene.primaryKey),
    symbol: displayText(gene.geneSymbol || gene.symbol),
    name: displayText(gene.geneFullName || gene.name),
    species: {
      name: text(taxon.name || taxon.fullName),
      curie: text(taxon.curie),
      dataProviderShortName: text(taxon?.species?.dataProvider?.abbreviation || gene?.dataProvider?.abbreviation),
    },
    geneSynopsis: text(gene.geneSynopsis || gene.automatedGeneSynopsis || notes[0]),
    synonyms,
    secondaryIds,
    dataProvider: text(gene?.dataProvider?.abbreviation || gene.dataProvider),
    modCrossRefCompleteUrl: text(gene.modCrossRefCompleteUrl),
  };
}

