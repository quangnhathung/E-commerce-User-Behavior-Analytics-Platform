import { escapeHTML as e, number as n, percent, table } from "./core.mjs";

const colors = ["#82a868", "#dfa267", "#739bb1", "#a58cb3", "#bba66b"];
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
  const test = models.policy.report.evaluation?.test || {};
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
    <div class="metrics">${metric("Precision test lịch sử", percent(test.precision), "Theo policy đang áp dụng", "◎", true)}${metric("Recall test lịch sử", percent(test.recall), "Theo policy đang áp dụng", "↗")}${metric("PR-AUC test lịch sử", n(test.pr_auc, 4), "Metrics xếp hạng mô hình", "∿")}${metric("Ngưỡng policy", n(bindings.threshold, 2), `Ngưỡng RF gốc: ${n(bindings.original_threshold, 2)}`, "≋")}</div>
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
    return `<rect x="${x}" y="${165 - h}" width="${Math.max(1, spacing * .65)}" height="${h}" rx="3" fill="${i === rows.length - 1 ? "#b9cf98" : "#82a868"}"><title>Batch ${e(r.batch_id)}: ${e(n(r.batch_seconds, 3))} giây</title></rect>${rows.length <= 30 ? `<text x="${x + spacing * .325}" y="185" text-anchor="middle">${e(r.batch_id)}</text>` : ""}`;
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
