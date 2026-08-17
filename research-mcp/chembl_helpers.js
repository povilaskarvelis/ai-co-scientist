export function selectHumanChEMBLTargetIds(targets) {
  const humanTargets = (Array.isArray(targets) ? targets : []).filter(
    (target) => String(target?.organism || "").toLowerCase() === "homo sapiens"
  );
  const singleProteins = humanTargets.filter(
    (target) => String(target?.target_type || "").toUpperCase() === "SINGLE PROTEIN"
  );
  const selected = singleProteins.length > 0 ? singleProteins : humanTargets;
  return [...new Set(selected.map((target) => String(target?.target_chembl_id || "").trim()).filter(Boolean))];
}

export function filterChEMBLTargetEntries(entries, targetFilter, resolvedTargetIds = []) {
  const filter = String(targetFilter || "").trim().toLowerCase();
  if (!filter) return Array.isArray(entries) ? entries : [];
  const targetIds = new Set(
    (Array.isArray(resolvedTargetIds) ? resolvedTargetIds : [])
      .map((targetId) => String(targetId || "").trim().toUpperCase())
      .filter(Boolean)
  );
  return (Array.isArray(entries) ? entries : []).filter((entry) => (
    String(entry?.target || "").toLowerCase().includes(filter)
    || targetIds.has(String(entry?.targetChemblId || "").trim().toUpperCase())
  ));
}

export function formatChEMBLActivitySummary({ activityCount, chemblId, entries, targetFilter = "" }) {
  const groups = Array.isArray(entries) ? entries : [];
  if (groups.length === 0) {
    const scope = targetFilter
      ? `matching target filter "${targetFilter}"`
      : "with numeric standard values";
    return `ChEMBL found ${activityCount} bioactivity records for ${chemblId}, but none ${scope}.`;
  }
  const leading = groups[0];
  const minimum = Math.min(...leading.values);
  const uniqueTargets = new Set(
    groups.map((entry) => entry.targetChemblId || entry.target).filter(Boolean)
  ).size;
  return `ChEMBL: ${activityCount} bioactivity records for ${chemblId} across ${uniqueTargets} targets. `
    + `Most potent: ${leading.target} (${leading.type} min ${minimum.toFixed(1)} ${leading.units || "nM"}).`;
}
