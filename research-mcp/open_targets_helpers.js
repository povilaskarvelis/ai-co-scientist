const LIVE_RELEASE_NAMES = new Set(["", "latest", "current", "live", "most recent"]);

export function shouldUseLiveOpenTargetsApi(release) {
  return LIVE_RELEASE_NAMES.has(String(release || "").trim().toLowerCase());
}

export function selectOpenTargetsDiseaseAssociation(candidates = [], rows = []) {
  const rowsByDiseaseId = new Map();
  for (const row of rows || []) {
    const diseaseId = String(row?.disease?.id || "").trim();
    if (diseaseId && !rowsByDiseaseId.has(diseaseId)) rowsByDiseaseId.set(diseaseId, row);
  }
  for (const candidate of candidates || []) {
    const diseaseId = String(candidate?.id || candidate?.disease_id || "").trim();
    if (diseaseId && rowsByDiseaseId.has(diseaseId)) {
      return { candidate, row: rowsByDiseaseId.get(diseaseId) };
    }
  }
  return null;
}
