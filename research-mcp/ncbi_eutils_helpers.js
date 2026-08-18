export function isNcbiEutilsUrl(value) {
  try {
    const url = new URL(String(value || ""));
    return url.hostname === "eutils.ncbi.nlm.nih.gov"
      && url.pathname.startsWith("/entrez/eutils/");
  } catch {
    return false;
  }
}

export function getNcbiEutilsMinIntervalMs(apiKey = "") {
  return String(apiKey || "").trim() ? 110 : 350;
}

export function summarizeNcbiEutilsError(value) {
  const message = String(value || "").replace(/\s+/g, " ").trim();
  if (/429|rate limit/i.test(message)) {
    return "NCBI E-utilities rate limit was reached after bounded retries.";
  }
  if (/AbortError|aborted|timed out/i.test(message)) {
    return "NCBI E-utilities request timed out.";
  }
  const status = message.match(/Request failed \((\d{3})\)/i)?.[1];
  if (status) return `NCBI E-utilities request failed (HTTP ${status}).`;
  const withoutUrls = message.replace(/https?:\/\/\S+/gi, "").replace(/\|\s*\{.*$/s, "").trim();
  if (!withoutUrls) return "NCBI E-utilities request failed.";
  return withoutUrls.length > 240 ? `${withoutUrls.slice(0, 237).trim()}...` : withoutUrls;
}

export function createSerializedRequestPacer({
  minIntervalMs,
  now = () => Date.now(),
  sleep = (delayMs) => new Promise((resolve) => setTimeout(resolve, delayMs)),
} = {}) {
  const intervalMs = Math.max(0, Number(minIntervalMs) || 0);
  let nextAllowedAt = 0;
  let queue = Promise.resolve();

  return function waitForRequestSlot() {
    const slot = queue.then(async () => {
      const delayMs = Math.max(0, nextAllowedAt - now());
      if (delayMs > 0) await sleep(delayMs);
      nextAllowedAt = now() + intervalMs;
    });
    queue = slot.catch(() => {});
    return slot;
  };
}
