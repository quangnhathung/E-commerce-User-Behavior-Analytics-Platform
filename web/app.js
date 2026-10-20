import { APIClient } from "./core.mjs";
import { empty, renderOverview, renderPredictions, renderPipeline } from "./views.mjs";

const $ = id => document.getElementById(id);
const config = window.ECOMMERCE_CONFIG || {};
let client, configurationError;
try { client = new APIClient(config.apiBaseUrl || "http://127.0.0.1:8000"); }
catch (cause) { configurationError = `Cấu hình API không hợp lệ trong web/config.js: ${cause.message}`; }
const state = { runId: "", page: "overview", runs: [], filters: {}, paging: { after: undefined, stack: [] },
  generation: 0, busy: false, controller: null, timer: null, bundle: null };
const pages = {
  overview: ["Tổng quan", "Bức tranh hành vi", "Hiểu hành trình khách hàng, từ lần xem đầu tiên đến quyết định mua."],
  predictions: ["Phân khúc & dự đoán", "Hiểu rõ từng nhóm khách hàng", "Từ feature hành vi đến phân khúc và điểm mô hình của từng visitor."],
  pipeline: ["Theo dõi pipeline", "Dòng dữ liệu đang vận hành", "Theo dõi tiến độ, chất lượng xử lý và trạng thái scoring của run."]
};

function status(connected, message) {
  $("sidebar-dot").classList.toggle("connected", connected);
  $("sidebar-status").textContent = message;
}

function error(message) {
  $("error-banner").textContent = message;
  $("error-banner").hidden = !message;
}

function install(html) {
  const template = document.createElement("template");
  template.innerHTML = html;
  $("content").replaceChildren(template.content);
  $("content").classList.remove("updated");
  requestAnimationFrame(() => $("content").classList.add("updated"));
}

function resetPaging() { state.paging = { after: undefined, stack: [] }; }

function changedContext() {
  state.generation++;
  state.controller?.abort();
  state.busy = false;
  state.bundle = null;
  error("");
  install(empty("Đang đọc snapshot", "Đợi dữ liệu của lượt và bộ lọc đã chọn."));
  refresh(true);
}

function selectRun(runId) {
  state.runId = runId;
  state.filters = {};
  resetPaging();
  const run = state.runs.find(r => r.run_id === runId);
  if (!run) return;
  $("time-start").value = run.start;
  $("time-end").value = run.end;
  $("cluster-filter").value = "";
  $("label-filter").value = "";
  $("visitor-input").value = "";
  $("source-badge").textContent = run.demo_source;
  $("run-range").textContent = `${run.start} → ${run.end} · UTC`;
}

function render(bundle) {
  $("snapshot-label").textContent = `Batch ${bundle.batchId < 0 ? "—" : bundle.batchId} · Đọc ${bundle.pipeline.observed_at.slice(11, 19)} UTC`;
  $("snapshot-label").title = `Cutoff event-time: ${bundle.pipeline.as_of_event_time || "chưa có"} · Đọc lúc: ${bundle.pipeline.observed_at}`;
  if (bundle.batchId < 0 && state.page !== "pipeline") {
    install(empty("Đang chờ event đầu tiên", `Scoring: ${bundle.pipeline.status}. Bật consumer rồi phát replay hoặc simulator để bắt đầu.`));
    return;
  }
  if (state.page === "overview") install(renderOverview(bundle));
  if (state.page === "pipeline") install(renderPipeline(bundle, config));
  if (state.page === "predictions") {
    if (bundle.models?.data) {
      const k = bundle.models.data.bindings.selected_k;
      if ($("cluster-filter").options.length !== k + 1) {
        const previous = $("cluster-filter").value;
        $("cluster-filter").replaceChildren(new Option("Tất cả cluster", ""), ...Array.from({ length: k }, (_, i) => new Option(`Cụm ${i}`, String(i))));
        $("cluster-filter").value = previous;
      }
    }
    install(renderPredictions(bundle, { hasPrevious: state.paging.stack.length > 0 }));
    $("previous-page")?.addEventListener("click", () => {
      state.paging.after = state.paging.stack.pop();
      changedContext();
    });
    $("next-page")?.addEventListener("click", () => {
      state.paging.stack.push(state.paging.after);
      state.paging.after = bundle.predictions.next_cursor;
      changedContext();
    });
  }
}

async function refresh(force = false) {
  if (!client) { error(configurationError); return; }
  if (state.busy && !force) return;
  if (force) state.controller?.abort();
  const controller = new AbortController();
  state.controller = controller;
  const generation = state.generation;
  state.busy = true;
  $("content").setAttribute("aria-busy", "true");
  $("refresh-button").disabled = true;
  try {
    try {
      const response = await client.get("runs", { limit: 200 }, controller.signal);
      if (generation !== state.generation) return;
      state.runs = response.data;
      const select = $("run-select");
      const signature = state.runs.map(r => r.run_id).join("|");
      if (select.dataset.signature !== signature) {
        select.replaceChildren(...state.runs.map(r => new Option(r.run_id, r.run_id)));
        select.dataset.signature = signature;
      }
      select.disabled = !state.runs.length;
      if (!state.runs.length) {
        state.bundle = null;
        install(empty("Chưa có lượt dữ liệu ML", "Chuẩn bị một replay hoặc simulator bằng make prepare-ml-demo, rồi quay lại đây."));
        status(true, "API đã kết nối");
        return;
      }
      if (!state.runs.some(r => r.run_id === state.runId)) selectRun(state.runs[0].run_id);
      select.value = state.runId;
    } catch (fetchError) {
      if (!state.runId || controller.signal.aborted) throw fetchError;
      // Retain the current run while a transient run-list request fails.
    }
    const bundle = await client.bundle(state.runId, state.page, state.filters, state.paging, controller.signal);
    if (generation !== state.generation || controller.signal.aborted) return;
    if (bundle.pageReset) resetPaging();
    state.paging.batchId = bundle.batchId;
    state.bundle = bundle;
    error("");
    status(true, "Dữ liệu đã kết nối");
    render(bundle);
  } catch (fetchError) {
    if (generation !== state.generation || controller.signal.aborted) return;
    error(`${fetchError.message}${state.bundle ? " Đang giữ snapshot hoàn chỉnh gần nhất." : ""}`);
    status(false, "Chưa kết nối API");
    if (!state.bundle) install(empty("Chưa đọc được dữ liệu", "Chạy make api trong terminal, mở web/index.html bằng Live Server và kiểm tra web/config.js."));
  } finally {
    if (generation === state.generation && state.controller === controller) {
      state.busy = false;
      $("content").setAttribute("aria-busy", "false");
      $("refresh-button").disabled = false;
    }
  }
}

document.querySelectorAll("[data-page]").forEach(button => button.addEventListener("click", () => {
  state.page = button.dataset.page;
  const [breadcrumb, title, description] = pages[state.page];
  $("breadcrumb").textContent = breadcrumb;
  $("page-title").textContent = title;
  $("page-description").textContent = description;
  document.querySelectorAll("[data-page]").forEach(b => {
    b.classList.toggle("active", b === button);
    if (b === button) b.setAttribute("aria-current", "page"); else b.removeAttribute("aria-current");
  });
  state.filters = {};
  resetPaging();
  $("time-filter").hidden = state.page !== "overview";
  $("prediction-filter").hidden = state.page !== "predictions";
  $("cluster-filter").value = ""; $("label-filter").value = ""; $("visitor-input").value = "";
  const current = state.runs.find(r => r.run_id === state.runId);
  if (current) { $("time-start").value = current.start; $("time-end").value = current.end; }
  changedContext();
}));

$("run-select").addEventListener("change", event => { selectRun(event.target.value); changedContext(); });
$("time-filter").addEventListener("submit", event => {
  event.preventDefault();
  state.filters = { start: $("time-start").value.trim(), end: $("time-end").value.trim() };
  changedContext();
});
$("prediction-filter").addEventListener("submit", event => {
  event.preventDefault();
  state.filters = { clusterId: $("cluster-filter").value === "" ? undefined : Number($("cluster-filter").value),
    label: $("label-filter").value === "" ? undefined : Number($("label-filter").value), visitorId: $("visitor-input").value.trim() || undefined };
  resetPaging();
  changedContext();
});
$("refresh-button").addEventListener("click", () => refresh(true));
function schedule() {
  clearInterval(state.timer);
  if ($("auto-refresh").checked) state.timer = setInterval(() => { if (!document.hidden) refresh(); }, Math.max(5000, Number(config.refreshMs) || 5000));
}
$("auto-refresh").addEventListener("change", schedule);
document.addEventListener("visibilitychange", () => { if (!document.hidden && $("auto-refresh").checked) refresh(); });
$("today").textContent = new Intl.DateTimeFormat("vi-VN", { day: "2-digit", month: "short", year: "numeric" }).format(new Date());
$("time-filter").hidden = false;
if (configurationError) {
  error(configurationError);
  install(empty("Kiểm tra địa chỉ API", "Sửa web/config.js theo hướng dẫn rồi tải lại trang."));
} else if (location.protocol === "file:") {
  error("Hãy mở web/index.html bằng Open with Live Server trong VS Code; không mở trực tiếp bằng file://.");
} else {
  schedule();
  refresh();
}
