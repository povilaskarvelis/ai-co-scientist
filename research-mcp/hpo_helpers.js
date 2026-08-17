function normalizeText(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9:]+/g, " ")
    .trim();
}

function normalizeCurie(value) {
  return String(value || "").trim().replace(/^HP_/i, "HP:").toUpperCase();
}

function singularizeSimplePhenotypeQuery(query) {
  const normalized = normalizeText(query);
  if (!normalized || normalized.includes(" ") || normalized.includes(":")) return "";
  if (normalized.endsWith("ies") && normalized.length > 4) {
    return `${normalized.slice(0, -3)}y`;
  }
  if (normalized.endsWith("sses") && normalized.length > 5) {
    return normalized.slice(0, -2);
  }
  if (normalized.endsWith("s") && !normalized.endsWith("ss") && normalized.length > 3) {
    return normalized.slice(0, -1);
  }
  return "";
}

export function getHpoSearchQueryVariants(query) {
  const primary = String(query || "").trim();
  const singular = singularizeSimplePhenotypeQuery(primary);
  if (!singular || normalizeText(primary) === singular) return [primary].filter(Boolean);
  return [primary, singular];
}

export function scoreHpoSearchDoc(doc, queryVariants, originalIndex = 0) {
  const variants = (Array.isArray(queryVariants) ? queryVariants : [queryVariants])
    .map(normalizeText)
    .filter(Boolean);
  const curieVariants = (Array.isArray(queryVariants) ? queryVariants : [queryVariants])
    .map(normalizeCurie)
    .filter(Boolean);
  const curie = normalizeCurie(doc?.obo_id || doc?.short_form || "");
  const label = normalizeText(doc?.label || "");
  const synonyms = [
    ...(Array.isArray(doc?.exact_synonyms) ? doc.exact_synonyms : []),
    ...(Array.isArray(doc?.synonym) ? doc.synonym : []),
  ].map(normalizeText).filter(Boolean);

  let score = Math.max(0, 20 - originalIndex);
  if (curie && curieVariants.includes(curie)) score += 400;
  variants.forEach((variant, index) => {
    const variantWeight = index === 0 ? 0 : -10;
    if (label === variant) score += 260 + variantWeight;
    if (synonyms.includes(variant)) score += 190 + variantWeight;
    if (label && (label.includes(variant) || variant.includes(label))) score += 45 + variantWeight;
  });
  if (normalizeText(doc?.ontology_name || "") === "hp") score += 10;
  return score;
}

export function rankHpoSearchDocs(searchResults, query) {
  const variants = getHpoSearchQueryVariants(query);
  const seen = new Set();
  const rows = [];
  let index = 0;
  for (const result of Array.isArray(searchResults) ? searchResults : []) {
    for (const doc of Array.isArray(result?.docs) ? result.docs : []) {
      const key = normalizeCurie(doc?.obo_id || doc?.short_form || "") || `${normalizeText(doc?.label)}:${index}`;
      if (seen.has(key)) continue;
      seen.add(key);
      rows.push({ doc, score: scoreHpoSearchDoc(doc, variants, index), index });
      index += 1;
    }
  }
  return rows
    .sort((a, b) => b.score - a.score || a.index - b.index)
    .map((entry) => entry.doc);
}
