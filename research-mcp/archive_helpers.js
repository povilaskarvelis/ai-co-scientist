export function normalizeNEMARDatasetRecord(raw = {}) {
  const item = raw && typeof raw === "object" ? raw : {};
  const datasetId = String(item.dataset_id || item.id || "").trim();
  return {
    ...item,
    id: datasetId,
    description_name: item.description_name || item.name || "",
    modalities: item.modalities || item.primaryModality || "",
    participants: item.participants ?? item.subject_count ?? null,
    publishDate: item.publishDate || item.publish_date || item.updated_at || item.created_at || "",
    latestSnapshot: item.latestSnapshot || item.latest_version || "",
    totalFiles: item.totalFiles ?? item.total_files ?? null,
    sessionsNum: item.sessionsNum ?? item.sessions_count ?? null,
    BIDSVersion: item.BIDSVersion || item.bids_version || "",
    HEDVersion: item.HEDVersion || item.hed_version || "",
    hasHED: item.has_hed === 1 || item.has_hed === true,
  };
}

export function normalizeNEMARSearchPayload(raw = {}) {
  const source = raw && typeof raw === "object" ? raw : {};
  const rawItems = Array.isArray(source)
    ? source
    : Array.isArray(source.results)
      ? source.results
      : Array.isArray(source.datasets)
        ? source.datasets
        : [];
  return {
    items: rawItems.map(normalizeNEMARDatasetRecord).filter((item) => item.id),
    total: Number(source.total_count ?? source.count ?? rawItems.length),
    method: String(source.method || (Array.isArray(source.results) ? "search" : "browse")),
  };
}

export function filterNEMARByModalities(items, modalities = []) {
  const wanted = modalities.map((value) => String(value).trim().toLowerCase()).filter(Boolean);
  if (wanted.length === 0) return [...items];
  return items.filter((item) => {
    const present = String(item.modalities || "")
      .toLowerCase()
      .split(/[\s,;/|]+/)
      .filter(Boolean);
    return wanted.some((modality) => present.includes(modality.toLowerCase()));
  });
}
