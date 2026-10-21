import { escapeHTML as e, number as n, percent, table } from "./core.mjs";

const colors = ["#6f8d86", "#b79568", "#71889b", "#887d9d", "#9a8b63"];
const chartPalette = { data: "#6f8d86", model: "#7c8f68", result: "#a48c69", policy: "#8b7894",
  positive: "#6d8f73", negative: "#b27b70", grid: "#e4ebe6", axis: "#7e918d", text: "#4e625e" };
const featureNames = { view_count_7d: "Lượt xem · 7d", cart_count_7d: "Thêm giỏ · 7d", transaction_count_7d: "Giao dịch · 7d",
  unique_items_7d: "Sản phẩm riêng · 7d", active_days_7d: "Ngày hoạt động · 7d", recency_hours: "Cách lần cuối · giờ",
  cart_view_ratio_7d: "Giỏ / xem", transaction_view_ratio_7d: "Giao dịch / xem" };

function metric(label, value, sub, icon, featured = false) {
  return `<article class="metric ${featured ? "featured" : ""}"><div class="metric-top">${e(label)}<span class="metric-icon" aria-hidden="true">${icon}</span></div><div class="metric-value">${e(value)}</div><div class="metric-sub">${e(sub)}</div></article>`;
}

function heading(title, subtitle = "", kicker = "") {
  return `<div class="panel-head"><div><h2>${e(title)}</h2>${subtitle ? `<p>${e(subtitle)}</p>` : ""}</div>${kicker ? `<span class="panel-kicker">${e(kicker)}</span>` : ""}</div>`;
}

export function empty(title, message) {
  return `<div class="empty"><div class="empty-symbol" aria-hidden="true">◈</div><h2>${e(title)}</h2><p>${e(message)}</p></div>`;
}

function finite(value) { return typeof value === "number" && Number.isFinite(value) ? value : null; }
function count(value) { return Number.isInteger(value) && value >= 0 ? value : null; }
function chartEmpty(title, message) {
  return `<div class="chart-empty"><strong>${e(title)}</strong><span>${e(message)}</span></div>`;
}
function svg(className, label, width, height, body) {
  return `<svg class="chart ml-chart ${className}" viewBox="0 0 ${width} ${height}" role="img" aria-label="${e(label)}">${body}</svg>`;
}
function chartLegend(items) {
  return `<div class="chart-legend">${items.map(item => `<span><i style="background:${item.color}"></i>${e(item.label)}</span>`).join("")}</div>`;
}
function yGrid(width, left, right, top, bottom, height, min, max, ticks = 4, digits = 2) {
  const range = max - min || 1;
  return Array.from({ length: ticks + 1 }, (_, index) => {
    const value = min + range * index / ticks;
    const y = top + (max - value) / range * (height - top - bottom);
    return `<line class="grid-line" x1="${left}" y1="${y}" x2="${width - right}" y2="${y}"/><text x="${left - 7}" y="${y + 3}" text-anchor="end">${e(n(value, digits))}</text>`;
  }).join("");
}

export function silhouetteChart(candidates = [], selectedK = null) {
  const rows = candidates.filter(row => row?.valid && Number.isInteger(row.k) && finite(row.silhouette) !== null);
  if (!rows.length) return chartEmpty("Chưa có silhouette", "Report K-Means chưa có candidate hợp lệ.");
  const width = 620, height = 232, left = 48, right = 16, top = 14, bottom = 36, min = -1, max = 1;
  const y = value => top + (max - value) / (max - min) * (height - top - bottom);
  const slot = (width - left - right) / rows.length;
  const barWidth = Math.min(64, slot * .58);
  const bars = rows.map((row, index) => {
    const x = left + slot * index + (slot - barWidth) / 2;
    const valueY = y(row.silhouette), zeroY = y(0);
    const topY = Math.min(valueY, zeroY), barHeight = Math.max(1, Math.abs(zeroY - valueY));
    const fill = row.k === selectedK ? chartPalette.policy : chartPalette.data;
    return `<rect x="${x}" y="${topY}" width="${barWidth}" height="${barHeight}" rx="4" fill="${fill}"><title>K=${e(row.k)} · silhouette ${e(n(row.silhouette, 4))}${row.k === selectedK ? " · K được chọn" : ""}</title></rect><text x="${x + barWidth / 2}" y="${height - 14}" text-anchor="middle">K=${e(row.k)}</text>`;
  }).join("");
  return chartLegend([{ color: chartPalette.data, label: "Candidate" }, { color: chartPalette.policy, label: "K được chọn" }]) +
    svg("silhouette-chart", "Silhouette theo số cụm K", width, height,
      `${yGrid(width, left, right, top, bottom, height, min, max, 4, 1)}<line class="chart-zero" x1="${left}" y1="${y(0)}" x2="${width - right}" y2="${y(0)}"/>${bars}`);
}

export function candidateMetricsChart(candidates = []) {
  const rows = candidates.filter(row => row?.parameters && row.metrics &&
    finite(row.metrics.pr_auc) !== null && finite(row.metrics.roc_auc) !== null);
  if (!rows.length) return chartEmpty("Chưa có candidate RF", "Report Random Forest chưa có metric AUC.");
  const width = 700, height = 242, left = 52, right = 16, top = 14, bottom = 48, max = 1;
  const y = value => top + (1 - value / max) * (height - top - bottom);
  const slot = (width - left - right) / rows.length, barWidth = Math.min(28, slot * .24);
  const bars = rows.map((row, index) => {
    const center = left + slot * index + slot / 2;
    const label = `${row.parameters.numTrees}T / D${row.parameters.maxDepth}`;
    const values = [[row.metrics.pr_auc, chartPalette.model, "PR-AUC"], [row.metrics.roc_auc, chartPalette.policy, "ROC-AUC"]];
    return values.map(([value, fill, name], offset) => {
      const x = center + (offset ? 2 : -barWidth - 2), heightValue = Math.max(1, (value / max) * (height - top - bottom));
      return `<rect x="${x}" y="${height - bottom - heightValue}" width="${barWidth}" height="${heightValue}" rx="3" fill="${fill}"><title>${e(label)} · ${e(name)} ${e(n(value, 4))}</title></rect>`;
    }).join("") + `<text x="${center}" y="${height - 18}" text-anchor="middle">${e(label)}</text>`;
  }).join("");
  return chartLegend([{ color: chartPalette.model, label: "PR-AUC" }, { color: chartPalette.policy, label: "ROC-AUC" }]) +
    svg("candidate-chart", "So sánh PR-AUC và ROC-AUC của các candidate Random Forest", width, height,
      `${yGrid(width, left, right, top, bottom, height, 0, max, 4, 1)}${bars}`);
}

export function thresholdChart(grid = [], selectedThreshold = null, policyThreshold = null) {
  const rows = grid.filter(row => finite(row?.threshold) !== null && finite(row?.precision) !== null &&
    finite(row?.recall) !== null && finite(row?.f1) !== null).sort((a, b) => a.threshold - b.threshold);
  if (!rows.length) return chartEmpty("Chưa có threshold grid", "Report chưa lưu đủ precision, recall và F1 theo ngưỡng.");
  const width = 760, height = 260, left = 46, right = 18, top = 18, bottom = 38;
  const x = value => left + value * (width - left - right);
  const y = value => top + (1 - value) * (height - top - bottom);
  const line = (key, color, label) => `<polyline points="${rows.map(row => `${x(row.threshold)},${y(row[key])}`).join(" ")}" fill="none" stroke="${color}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><title>${e(label)}</title></polyline>`;
  const marker = (value, label, color) => finite(value) === null ? "" : `<line class="threshold-marker" x1="${x(value)}" y1="${top}" x2="${x(value)}" y2="${height - bottom}" stroke="${color}" stroke-dasharray="4 4"/><text class="threshold-label" x="${Math.min(width - right - 4, Math.max(left + 4, x(value)))}" y="${top + 12}" text-anchor="middle">${e(label)}</text>`;
  return chartLegend([{ color: chartPalette.positive, label: "Precision" }, { color: chartPalette.data, label: "Recall" }, { color: chartPalette.policy, label: "F1" }]) +
    svg("threshold-chart", "Precision, recall và F1 theo threshold", width, height,
      `${yGrid(width, left, right, top, bottom, height, 0, 1, 4, 1)}${line("precision", chartPalette.positive, "Precision")}${line("recall", chartPalette.data, "Recall")}${line("f1", chartPalette.policy, "F1")}${marker(selectedThreshold, "RF", chartPalette.data)}${marker(policyThreshold, "Policy", chartPalette.policy)}<text x="${width / 2}" y="${height - 5}" text-anchor="middle">Threshold</text>`);
}

export function confusionMatrix(metrics = {}) {
  const values = [metrics.tp, metrics.fp, metrics.fn, metrics.tn].map(count);
  if (values.some(value => value === null)) return chartEmpty("Chưa có confusion matrix", "Test evaluation chưa có đủ TP, FP, FN và TN.");
  const [tp, fp, fn, tn] = values, width = 450, height = 250, x0 = 122, y0 = 55, cellW = 135, cellH = 66;
  const cells = [["TP", tp, x0, y0, chartPalette.positive], ["FN", fn, x0, y0 + cellH, "#e9eee5"],
    ["FP", fp, x0 + cellW, y0, "#f1e5e1"], ["TN", tn, x0 + cellW, y0 + cellH, "#e6ecee"]];
  return svg("confusion-chart", "Confusion matrix của test evaluation", width, height,
    `<text x="${x0 + cellW}" y="20" text-anchor="middle">Dự đoán</text><text x="20" y="${y0 + cellH}" transform="rotate(-90 20 ${y0 + cellH})" text-anchor="middle">Thực tế</text><text x="${x0 + cellW / 2}" y="43" text-anchor="middle">Mua</text><text x="${x0 + cellW + cellW / 2}" y="43" text-anchor="middle">Không mua</text><text x="${x0 - 12}" y="${y0 + cellH / 2 + 4}" text-anchor="end">Mua</text><text x="${x0 - 12}" y="${y0 + cellH + cellH / 2 + 4}" text-anchor="end">Không mua</text>${cells.map(([label, value, x, y, fill]) => `<rect x="${x}" y="${y}" width="${cellW}" height="${cellH}" rx="5" fill="${fill}" stroke="#d5dfda"/><text x="${x + cellW / 2}" y="${y + 25}" text-anchor="middle">${label}</text><text class="matrix-value" x="${x + cellW / 2}" y="${y + 49}" text-anchor="middle">${e(n(value))}</text>`).join("")}`);
}

export function featureImportanceChart(importance = {}) {
  const rows = Object.entries(importance).map(([key, value]) => ({ key, value: finite(value) }))
    .filter(row => row.value !== null && row.value >= 0).sort((a, b) => b.value - a.value);
  if (!rows.length) return chartEmpty("Chưa có feature importance", "Candidate winner chưa có thông tin đóng góp feature.");
  const width = 700, height = Math.max(210, 38 + rows.length * 26), left = 170, right = 60, top = 14, bottom = 16;
  const max = Math.max(1e-12, ...rows.map(row => row.value)), barH = 14, gap = 12;
  const bars = rows.map((row, index) => { const y = top + index * (barH + gap), widthValue = row.value / max * (width - left - right);
    return `<text x="${left - 8}" y="${y + 11}" text-anchor="end">${e(featureNames[row.key] || row.key)}</text><rect x="${left}" y="${y}" width="${Math.max(1, widthValue)}" height="${barH}" rx="4" fill="${chartPalette.model}"><title>${e(row.key)} · ${e(n(row.value, 4))}</title></rect><text x="${left + widthValue + 7}" y="${y + 11}">${e(n(row.value, 4))}</text>`;
  }).join("");
  return svg("importance-chart", "Feature importance của Random Forest winner", width, height, bars);
}

export function clusterSizeChart(segments = []) {
  const rows = segments.filter(row => Number.isInteger(row?.cluster_id) && count(row.count) !== null);
  if (!rows.length) return chartEmpty("Chưa có cluster profile", "Snapshot ML chưa có phân bố cluster.");
  const width = 560, height = 210, left = 54, right = 18, top = 14, bottom = 34, max = Math.max(1, ...rows.map(row => row.count));
  const slot = (width - left - right) / rows.length, barWidth = Math.min(64, slot * .58);
  const bars = rows.map((row, index) => { const x = left + slot * index + (slot - barWidth) / 2, h = row.count / max * (height - top - bottom);
    return `<rect x="${x}" y="${height - bottom - h}" width="${barWidth}" height="${Math.max(1, h)}" rx="4" fill="${colors[index % colors.length]}"><title>Cụm ${e(row.cluster_id)} · ${e(n(row.count))} visitor</title></rect><text x="${x + barWidth / 2}" y="${height - 14}" text-anchor="middle">Cụm ${e(row.cluster_id)}</text>`;
  }).join("");
  return svg("cluster-size-chart", "Số visitor theo cluster", width, height, `${yGrid(width, left, right, top, bottom, height, 0, max, 2, 0)}${bars}`);
}

function chartCard(title, subtitle, body) {
  return `<section class="evaluation-chart-card"><div class="chart-card-head"><h3>${e(title)}</h3><p>${e(subtitle)}</p></div>${body}</section>`;
}

export function renderModelEvaluation(models = {}, bindings = {}, segments = []) {
  const segmentReport = models.segments?.report || {};
  const purchaseReport = models.purchase?.report || {};
  const selection = purchaseReport.selection || {};
  const purchaseEvaluation = purchaseReport.evaluation || {};
  const policyEvaluation = models.policy?.report?.evaluation || {};
  const test = policyEvaluation.test || purchaseEvaluation.test || {};
  const policyThreshold = finite(bindings.threshold) ?? finite(policyEvaluation.threshold);
  const originalThreshold = finite(bindings.original_threshold) ?? finite(selection.threshold);
  const selectedK = Number.isInteger(bindings.selected_k) ? bindings.selected_k : segmentReport.selected_k;
  const importance = selection.feature_importance || selection.winner?.feature_importance || {};
  const eligible = policyEvaluation.eligible;
  const eligibilityText = eligible === true ? "Policy eligible theo report lịch sử" : eligible === false ? "Policy chưa eligible theo report" : "Chưa có trạng thái policy";
  const metricRows = [
    metric("PR-AUC test", n(test.pr_auc, 4), "Đánh giá xếp hạng", "∿", true),
    metric("ROC-AUC test", n(test.roc_auc, 4), "Đánh giá xếp hạng", "⌁"),
    metric("Precision test", percent(test.precision), "Theo threshold đang dùng", "◎"),
    metric("Recall test", percent(test.recall), "Theo threshold đang dùng", "↗"),
    metric("F1 test", n(test.f1, 4), "Cân bằng precision/recall", "ƒ"),
    metric("Policy threshold", n(policyThreshold, 2), `RF gốc: ${n(originalThreshold, 2)}`, "≋")
  ].join("");
  return `<div class="evaluation-metrics metrics">${metricRows}</div>
    <div class="evaluation-chart-grid"><div>${chartCard("Silhouette theo K", "K-Means chọn K theo silhouette trên toàn cohort", silhouetteChart(segmentReport.candidates || [], selectedK))}${chartCard("Phân bố kích thước cluster", "Số visitor trong snapshot hiện tại", clusterSizeChart(segments))}</div>
      <div>${chartCard("So sánh candidate Random Forest", "PR-AUC và ROC-AUC trên validation", candidateMetricsChart(selection.candidates || []))}${chartCard("Feature importance", "Đóng góp tương đối của tám feature trong winner", featureImportanceChart(importance))}</div></div>
    <div class="evaluation-chart-grid"><div>${chartCard("Precision, recall và F1 theo threshold", "Đường cong lấy từ threshold grid đã lưu; marker cho RF và policy", thresholdChart(selection.threshold_grid || [], originalThreshold, policyThreshold))}</div>
      <div>${chartCard("Confusion matrix", "Test evaluation của policy; fallback về RF gốc khi policy chưa có test", confusionMatrix(test))}</div></div>
    <div class="evaluation-summary"><span class="evaluation-status">${e(eligibilityText)}</span><span>Precision@1%: <strong>${e(percent(test.precision_at_1pct))}</strong></span><span>Fresh holdout: <strong>${policyEvaluation.fresh_holdout === false ? "false" : "—"}</strong></span></div>`;
}

export function lineChart(rows, series) {
  if (!rows?.length) return empty("Chưa có điểm dữ liệu", "Không có event trong khoảng thời gian đang lọc.");
  const w = 660, h = 228, left = 34, top = 12, bottom = 29, right = 15;
  const max = Math.max(1, ...rows.flatMap(r => series.map(s => r[s.key] || 0)));
  const times = rows.map(r => Date.parse(r.window_start));
  const start = Math.min(...times), end = Math.max(...times);
  const x = i => left + (times[i] - start) / (end - start || 1) * (w - left - right);
  const y = value => top + (1 - value / max) * (h - top - bottom);
  const grid = Array.from({ length: 5 }, (_, i) => {
    const value = max * i / 4;
    return `<line class="grid-line" x1="${left}" y1="${y(value)}" x2="${w - right}" y2="${y(value)}"/><text x="${left - 8}" y="${y(value) + 3}" text-anchor="end">${e(n(value))}</text>`;
  }).join("");
  const ticks = [...new Set([0, Math.floor(rows.length / 3), Math.floor(rows.length * 2 / 3), rows.length - 1])];
  const labels = ticks.map(i => `<text x="${x(i)}" y="${h - 6}" text-anchor="middle">${e(rows[i].window_start.slice(11, 16))}</text>`).join("");
  const paths = series.map((s, k) => {
    const points = rows.map((r, i) => `${x(i)},${y(r[s.key] || 0)}`).join(" ");
    const dots = rows.length < 20 ? rows.map((r, i) => `<circle cx="${x(i)}" cy="${y(r[s.key] || 0)}" r="3" fill="${colors[k]}"><title>${e(s.label)}: ${e(n(r[s.key]))} · ${e(r.window_start)}</title></circle>`).join("") : "";
    return `<polyline points="${points}" fill="none" stroke="${colors[k]}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>${dots}`;
  }).join("");
  return `<svg class="chart" viewBox="0 0 ${w} ${h}" role="img" aria-label="Biểu đồ tương tác theo phút UTC">${grid}${labels}${paths}</svg>`;
}

function funnel(summary) {
  const values = [summary.view_sessions, summary.cart_sessions, summary.purchase_sessions];
  const base = values[0];
  return ["Có lượt xem", "Xem → thêm giỏ", "Xem → giỏ → mua"].map((label, i) => {
    const ratio = base ? values[i] / base : 0;
    return `<div class="funnel-row"><span class="funnel-step">0${i + 1}</span><div class="funnel-copy">${label}<div class="funnel-track"><div class="funnel-fill" style="width:${Math.max(0, Math.min(100, ratio * 100))}%;background:${colors[i]}"></div></div></div><div class="funnel-value">${e(n(values[i]))}<small>${e(percent(ratio))}</small></div></div>`;
  }).join("");
}

export function renderOverview(bundle) {
  const overview = bundle.overview?.data;
  if (!overview) return empty("Đang chờ event đầu tiên", "KPI sẽ xuất hiện sau khi một batch hợp lệ được công bố.");
  const behavior = bundle.funnel.data;
  const series = [{ key: "views", label: "Xem" }, { key: "add_to_cart", label: "Thêm giỏ" }, { key: "purchase_events", label: "Mua" }];
  const products = behavior?.products || [];
  return `<div class="metrics">${metric("Event đã xử lý", n(overview.processed_events), "Sự kiện hợp lệ của run", "↗", true)}${metric("Visitor", n(overview.visitors), "Số visitor duy nhất", "◉")}${metric("Giao dịch", n(overview.transactions), `${n(overview.purchase_events)} event mua hàng`, "▣")}${metric("Tỷ lệ visitor mua", percent(overview.buyer_rate), `${n(overview.buyers)} visitor có mua`, "↗")}</div>
    <div class="grid-two"><article class="panel">${heading("Nhịp tương tác", "Diễn biến hành vi trong khoảng thời gian đã chọn", "THEO PHÚT")}
      <div class="legend">${series.map((s, i) => `<span><i style="background:${colors[i]}"></i>${s.label} <b>${e(n(overview[s.key]))}</b></span>`).join("")}</div>
      ${lineChart(bundle.timeseries.data, series)}<div class="chart-caption"><span>Event được xử lý thành công</span><span>Thời gian sự kiện · UTC</span></div></article>
    <article class="panel">${heading("Hành trình chuyển đổi", "Cùng phiên, cùng sản phẩm và đúng thứ tự", behavior?.is_final ? "HOÀN TẤT" : "TẠM TÍNH")}
      ${behavior ? funnel(behavior.overview) : '<div class="empty compact">Funnel chưa sẵn sàng.</div>'}<div class="funnel-footer">Tỷ lệ tính trên lượt phiên / sản phẩm có xem. Không đồng nhất với tỷ lệ visitor mua ở KPI.</div></article></div>
    <article class="panel">${heading("Sản phẩm được quan tâm", "Xếp theo số phiên có lượt xem; dùng để tìm điểm cần điều tra", "TOP 20")}
      ${table([{ key: "item_id", label: "MÃ SẢN PHẨM", mono: true }, { key: "view_sessions", label: "PHIÊN CÓ XEM", format: n },
        { key: "cart_sessions", label: "XEM → GIỎ", format: n }, { key: "purchase_sessions", label: "XEM → GIỎ → MUA", format: n },
        { key: "view_to_cart_rate", label: "TỶ LỆ THÊM GIỎ", format: percent }, { key: "view_to_purchase_rate", label: "TỶ LỆ MUA", format: percent }], products)}</article>`;
}

function donut(segments) {
  const total = segments.reduce((sum, r) => sum + r.count, 0);
  let cursor = 0;
  const stops = segments.map((r, i) => { const start = cursor; cursor += total ? r.count / total * 100 : 0; return `${colors[i % colors.length]} ${start}% ${cursor}%`; });
  return `<div class="donut-layout"><div class="donut" style="background:${total ? `conic-gradient(${stops.join(",")})` : "#edf3e8"}" role="img" aria-label="Phân bố visitor theo cluster"><div class="donut-label">${e(n(total))}<span>VISITOR TRONG RUN</span></div></div><div>${segments.map((r, i) => `<div class="cluster-key"><span class="key-dot" style="background:${colors[i % colors.length]}"></span><div>Cụm ${e(r.cluster_id)} · ${e(n(r.count))}<small>${e(percent(r.share))} visitor</small></div></div>`).join("")}</div></div>`;
}

export function renderPredictions(bundle, paging = {}) {
  const segments = bundle.segments?.data;
  if (!segments) return empty("Scoring chưa sẵn sàng", "Chờ batch có event hợp lệ để xem feature, phân khúc và prediction.");
  const models = bundle.models.data, bindings = models.bindings;
  const segmentRows = segments.map(r => ({ cluster_id: r.cluster_id, count: r.count, ...(r.raw_feature_means || {}) }));
  const scoreColumns = [{ key: "visitor_id", label: "VISITOR ID", mono: true }, { key: "cluster_id", label: "CLUSTER" },
    { key: "purchase_score", label: "ĐIỂM MÔ HÌNH", format: v => n(v, 4) }, { key: "predicted_label", label: "NHÃN POLICY", format: v => v ? "Dự đoán mua" : "Chưa dự đoán mua" },
    { key: "threshold", label: "NGƯỠNG", format: v => n(v, 2) }, { key: "view_count_7d", label: "XEM · 7D", format: n },
    { key: "cart_count_7d", label: "GIỎ · 7D", format: n }, { key: "transaction_count_7d", label: "GIAO DỊCH · 7D", format: n }];
  let visitor = "";
  if (bundle.visitorMissing) visitor = '<div class="notice info">Visitor chưa xuất hiện trong snapshot của run này.</div>';
  else if (bundle.visitor?.data) {
    const row = bundle.visitor.data;
    visitor = `<article class="panel">${heading(`Visitor ${row.visitor_id}`, `Cutoff: ${row.as_of} · Điểm: ${n(row.purchase_score, 4)} · Nhãn policy: ${row.predicted_label}`)}
      ${table(Object.entries(featureNames).map(([key, label]) => ({ key, label, format: v => n(v, 5) })), [row])}</article>`;
  }
  return `<div class="grid-two grid-equal"><article class="panel">${heading("Phân bố khách hàng", "Cluster của visitor đã nhận event trong run")}${donut(segments)}</article>
    <article class="panel">${heading("Đặc trưng phân khúc", "Trung bình feature bảy ngày tại cùng cutoff")}${table([
      { key: "cluster_id", label: "CỤM" }, { key: "count", label: "VISITOR", format: n },
      { key: "view_count_7d", label: "LƯỢT XEM", format: v => n(v, 2) }, { key: "cart_count_7d", label: "THÊM GIỎ", format: v => n(v, 2) },
      { key: "transaction_count_7d", label: "GIAO DỊCH", format: v => n(v, 2) }], segmentRows)}</article></div>
    <article class="panel">${heading("Visitor ưu tiên theo điểm mô hình", "Điểm chưa hiệu chuẩn thành xác suất mua; nhãn sử dụng policy đã khóa", "50 DÒNG / TRANG")}
      ${table(scoreColumns, bundle.predictions.data)}<div class="pager"><button class="button secondary" id="previous-page" ${paging.hasPrevious ? "" : "disabled"}>← Trang trước</button><button class="button secondary" id="next-page" ${bundle.predictions.next_cursor ? "" : "disabled"}>Trang tiếp →</button></div></article>
    ${visitor}<article class="panel">${heading("Đánh giá mô hình", "Metrics lịch sử của policy đang được dùng; không phải precision của event streaming")}
    ${renderModelEvaluation(models, bindings, segments)}
    <div class="model-ids">${[["K-Means", models.segments.version], ["Random Forest", models.purchase.version], ["Policy", models.policy.version]].map(([label, id]) => `<span>${label}: <code title="${e(id)}">${e(id.slice(0, 14))}…</code></span>`).join("")}</div>
    <div class="notice subtle">Policy là đánh giá lại ngưỡng trên test đã quan sát (reassessment); fresh_holdout=false. Score hoặc nhãn không bắt buộc tăng sau mỗi hành động.</div>
    <details><summary>Tham số và báo cáo mô hình đã khóa</summary><pre>${e(JSON.stringify(models, null, 2))}</pre></details></article>`;
}

function barChart(rows) {
  if (!rows.length) return empty("Chưa có batch", "Chờ consumer công bố batch đầu tiên.");
  const max = Math.max(1, ...rows.map(r => r.batch_seconds));
  const width = 900, left = 40, height = 200, spacing = (width - left - 10) / rows.length;
  return `<svg class="chart" viewBox="0 0 ${width} ${height}" role="img" aria-label="Thời gian xử lý từng batch">${rows.map((r, i) => {
    const h = r.batch_seconds / max * 145, x = left + i * spacing;
    return `<rect x="${x}" y="${165 - h}" width="${Math.max(1, spacing * .65)}" height="${h}" rx="3" fill="${i === rows.length - 1 ? chartPalette.model : chartPalette.data}"><title>Batch ${e(r.batch_id)}: ${e(n(r.batch_seconds, 3))} giây</title></rect>${rows.length <= 30 ? `<text x="${x + spacing * .325}" y="185" text-anchor="middle">${e(r.batch_id)}</text>` : ""}`;
  }).join("")}<line class="grid-line" x1="${left}" y1="165" x2="890" y2="165"/><text x="5" y="20">${e(n(max, 1))}s</text></svg>`;
}

export function renderPipeline(bundle, config = {}) {
  const data = bundle.pipeline.data;
  const counts = data.counts || {};
  const state = data.consumer.status;
  const latency = data.steady_state_latency_p95_seconds;
  const uiPort = key => Number.isInteger(Number(config[key])) && Number(config[key]) >= 1024 && Number(config[key]) <= 65535 ? Number(config[key]) : (key === "sparkUiPort" ? 18080 : 9870);
  return `<div class="metrics">${metric("Backlog đã ACK", n(data.backlog), "ACK offsets − processed offsets", "⇄", true)}${metric("Latency p95", latency == null ? "—" : `${n(latency, 3)}s`, "Bỏ batch đầu; chưa đủ mẫu sẽ là —", "◷")}${metric("Batch hiện tại", bundle.batchId < 0 ? "—" : n(bundle.batchId), "Batch đã commit của snapshot", "▤")}${metric("Tốc độ producer", data.acknowledged_rate_eps == null ? "—" : n(data.acknowledged_rate_eps, 2), "Event/giây đã ACK · trung bình run", "↗")}</div>
    <article class="panel">${heading("Trạng thái luồng dữ liệu", "Chỉ hiển thị batch đã được công bố")}
      <div class="pipeline-steps">${[["Producer", data.producer.status, "Replay / simulator"], ["Consumer", state, "Spark Structured Streaming"], ["Scoring", bundle.pipeline.status, "K-Means + Random Forest + policy"]].map(([label, value, sub], i) => `${i ? '<span class="pipeline-arrow">→</span>' : ""}<div class="pipeline-node"><h3>${label}</h3><p>${sub}</p><span class="badge">${e(value)}</span></div>`).join("")}</div>
      <p class="small-note">Backlog bằng 0 trước khi producer phát event không có nghĩa run đã hoàn thành. Worker đã tham gia: ${e(data.worker_hosts.join(", ") || "chưa có")}.</p>
      ${data.consumer.reason ? '<div class="notice error">Consumer báo lỗi. Chạy lệnh status của run để xem log và chẩn đoán.</div>' : ""}</article>
    <article class="panel">${heading("Chất lượng xử lý", "Tổng cộng đến batch đang xem")}${table([
      { key: "accepted", label: "HỢP LỆ", format: n }, { key: "duplicate", label: "TRÙNG", format: n },
      { key: "invalid", label: "KHÔNG HỢP LỆ", format: n }, { key: "late", label: "QUÁ TRỄ", format: n },
      { key: "foreign", label: "KHÁC RUN", format: n }], data.counts ? [counts] : [])}</article>
    <article class="panel">${heading("Thời gian xử lý mỗi batch", "Giây từ bắt đầu xử lý đến bước công bố", "GIÂY / BATCH")}${barChart(data.batch_history)}
      <div class="link-row"><a href="http://127.0.0.1:${uiPort("sparkUiPort")}" target="_blank" rel="noopener">Mở Spark UI ↗</a><a href="http://127.0.0.1:${uiPort("hdfsUiPort")}" target="_blank" rel="noopener">Mở HDFS UI ↗</a></div></article>`;
}
