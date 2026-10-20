export class APIError extends Error {
  constructor(message, status = 0) { super(message); this.status = status; }
}

export function escapeHTML(value) {
  return String(value ?? "").replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

export function localApiUrl(value) {
  const url = new URL(value);
  if (url.protocol !== "http:" || !["127.0.0.1", "localhost"].includes(url.hostname) || url.username || url.password || url.search || url.hash) {
    throw new APIError("API cần chạy trên http://127.0.0.1 hoặc http://localhost.");
  }
  return url.href.replace(/\/$/, "");
}

export class APIClient {
  constructor(baseUrl, fetcher = globalThis.fetch) { this.baseUrl = localApiUrl(baseUrl); this.fetcher = fetcher; }

  async get(endpoint, params = {}, signal) {
    const url = new URL(`${this.baseUrl}/api/v1/${endpoint}`);
    Object.entries(params).forEach(([key, value]) => { if (value !== null && value !== undefined && value !== "") url.searchParams.set(key, value); });
    const controller = new AbortController();
    const abort = () => controller.abort();
    if (signal?.aborted) abort();
    signal?.addEventListener("abort", abort, { once: true });
    const timer = setTimeout(abort, 4000);
    try {
      const response = await this.fetcher(url, { signal: controller.signal, headers: { Accept: "application/json" }, credentials: "omit", cache: "no-store" });
      if (!response.ok) {
        let message = `API trả HTTP ${response.status}.`;
        try { const error = await response.json(); message = error.detail?.message || message; } catch { /* Keep HTTP status. */ }
        throw new APIError(message, response.status);
      }
      return await response.json();
    } catch (error) {
      if (error instanceof APIError) throw error;
      if (signal?.aborted) throw error;
      throw new APIError("Không kết nối được API. Kiểm tra make api và địa chỉ trong web/config.js.");
    } finally { clearTimeout(timer); signal?.removeEventListener("abort", abort); }
  }

  async bundle(runId, page, filters = {}, paging = {}, signal) {
    const pipeline = await this.get("pipeline", { run_id: runId }, signal);
    const batchId = pipeline.batch_id;
    if (pipeline.run_id !== runId) throw new APIError("API trả sai RUN_ID.");
    const result = { pipeline, batchId, pageReset: paging.batchId !== undefined && paging.batchId !== batchId };
    if (batchId < 0) return result;
    const fetchSnapshot = async (endpoint, extra = {}) => {
      const value = await this.get(endpoint, { run_id: runId, batch_id: batchId, ...extra }, signal);
      if (value.run_id !== runId || value.batch_id !== batchId) throw new APIError("Dữ liệu chưa cùng snapshot; chờ lần cập nhật tiếp theo.");
      return value;
    };
    if (page === "overview") {
      const [overview, timeseries, funnel] = await Promise.all([
        fetchSnapshot("overview"), fetchSnapshot("timeseries", { start: filters.start, end: filters.end }), fetchSnapshot("funnel")
      ]);
      Object.assign(result, { overview, timeseries, funnel });
    } else if (page === "predictions") {
      const [segments, predictions, models] = await Promise.all([
        fetchSnapshot("segments"), fetchSnapshot("predictions", { cluster_id: filters.clusterId, predicted_label: filters.label,
          after: result.pageReset ? undefined : paging.after, limit: 50 }), fetchSnapshot("models")
      ]);
      Object.assign(result, { segments, predictions, models });
      if (filters.visitorId) {
        try { result.visitor = await fetchSnapshot(`visitors/${encodeURIComponent(filters.visitorId)}`); }
        catch (error) { if (error.status === 404) result.visitorMissing = true; else throw error; }
      }
    }
    return result;
  }
}

export function number(value, digits = 0) {
  return value === null || value === undefined ? "—" : new Intl.NumberFormat("vi-VN", { maximumFractionDigits: digits }).format(value);
}

export function percent(value) { return value === null || value === undefined ? "—" : `${number(value * 100, 2)}%`; }

export function table(columns, rows, empty = "Chưa có dữ liệu trong snapshot này.") {
  if (!rows?.length) return `<div class="empty compact">${escapeHTML(empty)}</div>`;
  return `<div class="table-scroll"><table><thead><tr>${columns.map(c => `<th>${escapeHTML(c.label)}</th>`).join("")}</tr></thead><tbody>${rows.map(row => `<tr>${columns.map(c => `<td${c.mono ? ' class="mono"' : ""}>${escapeHTML(c.format ? c.format(row[c.key]) : row[c.key] ?? "—")}</td>`).join("")}</tr>`).join("")}</tbody></table></div>`;
}
