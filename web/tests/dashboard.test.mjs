import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { APIClient, APIError, escapeHTML, number, table, localApiUrl } from "../core.mjs";
import { renderOverview, renderPredictions, renderPipeline, lineChart, renderModelEvaluation,
  silhouetteChart, candidateMetricsChart, thresholdChart, confusionMatrix, featureImportanceChart } from "../views.mjs";
import { feedTable, mergeEvents, LivePanel } from "../live.mjs";

function fixture() {
  const overview = { processed_events: 9, visitors: 3, views: 5, add_to_cart: 2, purchase_events: 2, transactions: 2, buyers: 2, buyer_rate: 2 / 3 };
  const prediction = { visitor_id: "000099000000003", cluster_id: 0, purchase_score: .5, predicted_label: 0, threshold: .96,
    view_count_7d: 1, cart_count_7d: 1, transaction_count_7d: 1, as_of: "2015-09-18T00:02:02.001Z" };
  const data = { overview, timeseries: [{ window_start: "2015-09-18T00:00:00.000Z", ...overview }],
    funnel: { overview: { view_sessions: 2, cart_sessions: 1, purchase_sessions: 1 }, products: [{ item_id: "10", view_sessions: 1 }], is_final: true },
    segments: [{ cluster_id: 0, count: 3, share: 1, raw_feature_means: { view_count_7d: 1.66 } }], predictions: [prediction],
    models: { bindings: { selected_k: 2, threshold: .96, original_threshold: .93 },
      segments: { version: "a".repeat(64), report: {} }, purchase: { version: "b".repeat(64), report: {} },
      policy: { version: "c".repeat(64), report: { evaluation: { test: { precision: .74, recall: .14, pr_auc: .18 } } } } },
    pipeline: { producer: { status: "completed" }, consumer: { status: "completed" }, backlog: 0, steady_state_latency_p95_seconds: 28.3,
      counts: { accepted: 9, duplicate: 0, invalid: 0, late: 0, foreign: 0 }, acknowledged_rate_eps: .3,
      worker_hosts: ["spark-worker-1", "spark-worker-2"], batch_history: [{ batch_id: 4, batch_seconds: 12.3 }] } };
  const request = (endpoint, params = {}) => ({ run_id: "demo", batch_id: 4, status: "ready",
    as_of_event_time: "2015-09-18T00:02:02.001Z", observed_at: "2026-10-20T00:00:00.000Z",
    data: endpoint.startsWith("visitors/") ? prediction : data[endpoint], ...(endpoint === "predictions" ? { next_cursor: null } : {}) });
  return { data, request };
}

test("live feed is bounded, deduplicated, escaped and uses committed offsets", () => {
  const session = { completed_segments: 1, active_run_id: "next", processed_offsets: { "0": 3 } };
  const rows = Array.from({ length: 70 }, (_, sequence) => ({ sequence, event_id: String(sequence), segment_index: 2,
    run_id: "next", partition: "0", next_offset: sequence + 1, visitor_id: "0001", item_id: "<img>", event_type: "view" }));
  const merged = mergeEvents(rows, rows, session);
  assert.equal(merged.length, 50);
  assert.equal(merged[0].sequence, 69);
  assert.equal(merged[0].processing_status, "acked");
  const old = mergeEvents([], [{ ...rows[0], segment_index: 1 }], session);
  assert.equal(old[0].processing_status, "processed");
  const committed = mergeEvents([], rows.slice(0, 2), session);
  assert.ok(committed.every(row => row.processing_status === "processed"));
  const advanced = mergeEvents(merged, [], { ...session, processed_offsets: { "0": 70 } });
  assert.ok(advanced.every(row => row.processing_status === "processed"));
  assert.equal(mergeEvents(merged, rows.slice(0, 1), session, true).length, 1);
  const html = feedTable(merged);
  assert.ok(html.includes("0001")); assert.ok(html.includes("&lt;img&gt;")); assert.ok(!html.includes("<img>"));
});

test("live controller follows snapshots while polling without overlapping requests", async () => {
  const previous = globalThis.Option;
  const elements = new Map(["live-select", "follow-live", "live-status", "live-totals", "live-events", "live-gap"].map(id => [id,
    { value: "", checked: true, events: {}, addEventListener(name, fn) { this.events[name] = fn; }, replaceChildren() {} }]));
  const doc = { hidden: false, getElementById: id => elements.get(id), addEventListener() {}, removeEventListener() {} };
  const session = { live_id: "live", status: "running", segment_index: 1, completed_segments: 0, active_run_id: "child",
    snapshot_run_id: "child", snapshot_batch_id: 1, processed_events: 1, acknowledged_events: 2, processed_offsets: { "0": 1 } };
  let release, waiting = false, reads = 0;
  const seen = [];
  const client = { async get(path) {
    if (path === "live/sessions") return { data: [session] };
    reads++;
    if (waiting) await new Promise(resolve => { release = resolve; });
    return { session, data: [], next_after: 1 };
  } };
  globalThis.Option = class { constructor(label, value) { this.value = value; } };
  const panel = new LivePanel(client, value => seen.push(value), doc);
  try {
    await panel.tick(); assert.equal(seen.length, 1);
    waiting = true;
    const pending = panel.tick();
    await panel.tick(); assert.equal(reads, 2);
    release(); await pending;
    panel.stopFollowing(); waiting = false;
    await panel.tick(); assert.equal(seen.length, 2);
    doc.hidden = true; await panel.tick(); assert.equal(reads, 3);
  } finally { panel.stop(); globalThis.Option = previous; }
});

test("every workspace endpoint pins the committed batch", async () => {
  const client = new APIClient("http://127.0.0.1:8000");
  const seen = [];
  client.get = async (endpoint, params) => { seen.push([endpoint, params]); return fixture().request(endpoint, params); };
  const result = await client.bundle("demo", "overview");
  assert.equal(result.batchId, 4);
  seen.slice(1).forEach(([, params]) => assert.equal(params.batch_id, 4));
  assert.equal(seen.length, 4);
});

test("a mixed snapshot or partial failure rejects the complete refresh", async () => {
  const client = new APIClient("http://localhost:8000");
  client.get = async endpoint => ({ ...fixture().request(endpoint), batch_id: endpoint === "funnel" ? 5 : 4 });
  await assert.rejects(client.bundle("demo", "overview"), APIError);
  client.get = async endpoint => { if (endpoint === "funnel") throw new APIError("temporary"); return fixture().request(endpoint); };
  await assert.rejects(client.bundle("demo", "overview"), /temporary/);
});

test("new committed batch discards the old page cursor", async () => {
  const client = new APIClient("http://localhost:8000");
  let cursor;
  client.get = async (endpoint, params) => { if (endpoint === "predictions") cursor = params.after; return fixture().request(endpoint); };
  const result = await client.bundle("demo", "predictions", {}, { batchId: 3, after: "old" });
  assert.equal(result.pageReset, true);
  assert.equal(cursor, undefined);
});

test("leading-zero visitor identity is preserved in the request and response", async () => {
  const client = new APIClient("http://localhost:8000");
  const seen = [];
  client.get = async endpoint => { seen.push(endpoint); return fixture().request(endpoint); };
  const result = await client.bundle("demo", "predictions", { visitorId: "000099000000003" });
  assert.ok(seen.includes("visitors/000099000000003"));
  assert.equal(result.visitor.data.visitor_id, "000099000000003");
});

test("visitor not found is a page state, not an empty prediction snapshot", async () => {
  const client = new APIClient("http://localhost:8000");
  client.get = async endpoint => { if (endpoint.startsWith("visitors/")) throw new APIError("missing", 404); return fixture().request(endpoint); };
  const result = await client.bundle("demo", "predictions", { visitorId: "001" });
  assert.equal(result.visitorMissing, true);
  assert.equal(result.predictions.data.length, 1);
});

test("pending run makes no unpinned business queries", async () => {
  const client = new APIClient("http://localhost:8000");
  let calls = 0;
  client.get = async () => { calls++; return { run_id: "demo", batch_id: -1 }; };
  const result = await client.bundle("demo", "overview");
  assert.equal(result.batchId, -1);
  assert.equal(calls, 1);
});

test("default and injected browser fetch receive the global context", async () => {
  const previous = globalThis.fetch;
  const calls = [];
  // Native Window.fetch rejects an APIClient receiver with Illegal invocation.
  function browserFetch(url, options) {
    if (this !== globalThis) throw new TypeError("Illegal invocation");
    calls.push({ url, options });
    return Promise.resolve({ ok: true, json: async () => ({ status: "ready" }) });
  }
  try {
    globalThis.fetch = browserFetch;
    for (const client of [new APIClient("http://localhost:8000"), new APIClient("http://localhost:8000", browserFetch)]) {
      assert.deepEqual(await client.get("health"), { status: "ready" });
    }
    assert.equal(calls.length, 2);
    for (const { url, options } of calls) {
      assert.equal(url.href, "http://localhost:8000/api/v1/health");
      assert.equal(options.credentials, "omit");
      assert.equal(options.cache, "no-store");
      assert.ok(options.signal instanceof AbortSignal);
    }
  } finally {
    globalThis.fetch = previous;
  }
});

test("network and HTTP errors stay actionable", async () => {
  const client = new APIClient("http://localhost:8000", async () => { throw new TypeError("network"); });
  await assert.rejects(client.get("health"), /make api/);
  client.fetcher = async () => ({ ok: false, status: 503, json: async () => ({ detail: { message: "Snapshot thiếu dữ liệu" } }) });
  await assert.rejects(client.get("overview"), error => error.status === 503 && /Snapshot/.test(error.message));
});

test("configuration accepts only local HTTP origins", () => {
  assert.equal(localApiUrl("http://localhost:8000/"), "http://localhost:8000");
  for (const url of ["https://example.com", "http://example.com", "http://user:password@localhost:8000", "javascript:alert(1)"]) {
    assert.throws(() => localApiUrl(url));
  }
});

test("table rendering escapes remote values and formats missing values correctly", () => {
  const html = table([{ key: "id", label: "ID" }, { key: "count", label: "COUNT", format: number }], [{ id: '<img src=x onerror="alert(1)">', count: 0 }]);
  assert.ok(!html.includes("<img"));
  assert.ok(html.includes("&lt;img"));
  assert.ok(html.includes(">0</td>"));
  assert.equal(number(null), "—");
  assert.equal(escapeHTML("001"), "001");
});

test("all three screens render real numeric values without external chart libraries", async () => {
  const client = new APIClient("http://localhost:8000");
  client.get = async endpoint => fixture().request(endpoint);
  const overview = renderOverview(await client.bundle("demo", "overview"));
  assert.ok(overview.includes("Nhịp tương tác"));
  assert.ok(overview.includes("<svg"));
  const predictions = renderPredictions(await client.bundle("demo", "predictions"));
  assert.ok(predictions.includes("000099000000003"));
  assert.ok(predictions.includes("fresh_holdout=false"));
  assert.ok(!predictions.includes("undefined"));
  const pipeline = renderPipeline(await client.bundle("demo", "pipeline"));
  assert.ok(pipeline.includes("spark-worker-2"));
  assert.ok(pipeline.includes("28,303") || pipeline.includes("28,3"));
});

test("ML evaluation charts use published report fields and keep model caveats visible", () => {
  const models = { bindings: { selected_k: 2, threshold: .96, original_threshold: .93 },
    segments: { report: { selected_k: 2, candidates: [{ k: 2, valid: true, silhouette: .84 }, { k: 3, valid: true, silhouette: .71 }] } },
    purchase: { report: { selection: { threshold: .93, candidates: [
      { parameters: { numTrees: 50, maxDepth: 5 }, metrics: { pr_auc: .18, roc_auc: .91 } },
      { parameters: { numTrees: 100, maxDepth: 8 }, metrics: { pr_auc: .17, roc_auc: .92 } }],
      threshold_grid: [{ threshold: 0, precision: .05, recall: 1, f1: .1 }, { threshold: .93, precision: .44, recall: .21, f1: .28 },
        { threshold: .96, precision: .74, recall: .14, f1: .24 }], feature_importance: { view_count_7d: .42, cart_count_7d: .31 } },
      evaluation: { test: { pr_auc: .18, roc_auc: .91, precision: .44, recall: .21, f1: .28, precision_at_1pct: .3, tp: 154, fp: 199, fn: 572, tn: 2379625 } } } },
    policy: { report: { evaluation: { threshold: .96, eligible: true, fresh_holdout: false,
      test: { pr_auc: .18, roc_auc: .91, precision: .74, recall: .14, f1: .23, precision_at_1pct: .31,
        tp: 99, fp: 35, fn: 627, tn: 2379789 } } } } };
  const html = renderModelEvaluation(models, models.bindings, [{ cluster_id: 0, count: 2 }, { cluster_id: 1, count: 1 }]);
  assert.ok(html.includes("Silhouette theo K"));
  assert.ok(html.includes("So sánh candidate Random Forest"));
  assert.ok(html.includes("Precision, recall và F1 theo threshold"));
  assert.ok(html.includes("Confusion matrix"));
  assert.ok(html.includes("Feature importance"));
  assert.ok(html.includes("Policy eligible"));
  assert.ok(html.includes("0.96") || html.includes("0,96"));
  assert.ok(html.includes("TP") && html.includes("99"));
  assert.ok(html.includes("<svg"));
  assert.ok(!html.includes("undefined") && !html.includes("NaN"));
});

test("ML evaluation chart empty states do not fabricate metrics", () => {
  assert.ok(silhouetteChart([]).includes("Chưa có silhouette"));
  assert.ok(candidateMetricsChart([]).includes("Chưa có candidate RF"));
  assert.ok(thresholdChart([]).includes("Chưa có threshold grid"));
  assert.ok(confusionMatrix({}).includes("Chưa có confusion matrix"));
  assert.ok(featureImportanceChart({}).includes("Chưa có feature importance"));
  const html = renderModelEvaluation({ segments: { report: {} }, purchase: { report: {} }, policy: { report: {} } }, {}, []);
  assert.ok(html.includes("Chưa có silhouette"));
  assert.ok(!html.includes("NaN") && !html.includes("undefined"));
});

test("waiting and empty chart states do not fabricate zero metrics", () => {
  assert.ok(renderOverview({ overview: { data: null } }).includes("Đang chờ"));
  assert.ok(lineChart([], []).includes("Chưa có"));
});

test("page controller changes run, resets filters and retains the last coherent view on API failure", async () => {
  const html = await readFile(new URL("../index.html", import.meta.url), "utf8");
  const elements = new Map();
  class Element {
    constructor(id = "") { this.id = id; this.value = ""; this.checked = false; this.hidden = false; this.dataset = {}; this.events = {}; this.options = []; this.textContent = ""; this.html = "";
      this.classList = { add() {}, remove() {}, toggle() {} }; }
    addEventListener(name, fn) { this.events[name] = fn; }
    setAttribute() {} removeAttribute() {}
    replaceChildren(...values) { this.options = values; this.html = values.map(v => v.html || "").join(""); }
    emit(name) { this.events[name]?.({ target: this, preventDefault() {} }); }
  }
  for (const [, id] of html.matchAll(/id="([^"]+)"/g)) elements.set(id, new Element(id));
  const nav = ["overview", "predictions", "pipeline"].map(page => { const el = new Element(); el.dataset.page = page; return el; });
  const previous = Object.fromEntries(["document", "window", "location", "Option", "requestAnimationFrame", "fetch"].map(key => [key, globalThis[key]]));
  let fail = false;
  let app;
  const requests = [];
  const wait = async predicate => {
    for (let i = 0; i < 100; i++) { if (predicate()) return; await new Promise(resolve => setTimeout(resolve, 5)); }
    assert.fail("Controller did not settle");
  };
  try {
    globalThis.document = { hidden: false, getElementById: id => elements.get(id) || null, querySelectorAll: () => nav,
      addEventListener() {}, createElement: () => ({ set innerHTML(value) { this.content = { html: value }; } }) };
    globalThis.window = { ECOMMERCE_CONFIG: { apiBaseUrl: "http://localhost:8000" } };
    globalThis.location = { protocol: "http:" };
    globalThis.Option = class { constructor(label, value) { this.label = label; this.value = value; } };
    globalThis.requestAnimationFrame = fn => fn();
    globalThis.fetch = async url => {
      if (fail) throw new TypeError("offline");
      const endpoint = url.pathname.split("/api/v1/")[1];
      requests.push([endpoint, Object.fromEntries(url.searchParams)]);
      if (endpoint === "live/sessions") return { ok: true, json: async () => ({ data: [] }) };
      if (endpoint === "runs") return { ok: true, json: async () => ({ data: ["demo", "second"].map(run_id => ({ run_id, demo_source: "simulator",
        start: "2015-09-18T00:00:00.000Z", end: "2015-09-18T03:00:00.000Z" })) }) };
      const data = fixture().request(endpoint);
      data.run_id = url.searchParams.get("run_id");
      if (endpoint === "overview" && data.run_id === "second") data.data.processed_events = 12;
      return { ok: true, json: async () => data };
    };
    app = await import(`../app.js?controller-test`);
    await wait(() => elements.get("content").html.includes("Nhịp tương tác"));
    nav[1].emit("click");
    await wait(() => elements.get("content").html.includes("fresh_holdout=false"));
    elements.get("visitor-input").value = "000099000000003";
    elements.get("prediction-filter").emit("submit");
    await wait(() => requests.some(([path]) => path === "visitors/000099000000003"));
    elements.get("run-select").value = "second";
    elements.get("run-select").emit("change");
    await wait(() => elements.get("content").html.includes("fresh_holdout=false"));
    assert.equal(elements.get("visitor-input").value, "");
    assert.ok(requests.some(([path, params]) => path === "predictions" && params.run_id === "second" && !params.after));
    nav[0].emit("click");
    await wait(() => elements.get("content").html.includes("Nhịp tương tác"));
    assert.ok(elements.get("content").html.includes(">12</div>"));
    const committed = elements.get("content").html;
    fail = true;
    elements.get("refresh-button").emit("click");
    await wait(() => elements.get("error-banner").textContent.includes("snapshot hoàn chỉnh"));
    assert.equal(elements.get("content").html, committed);
  } finally {
    app?.dispose();
    for (const [key, value] of Object.entries(previous)) { if (value === undefined) delete globalThis[key]; else globalThis[key] = value; }
  }
});
