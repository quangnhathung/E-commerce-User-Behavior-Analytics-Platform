import { table, number } from "./core.mjs";

const statuses = { starting: "Đang khởi động", preparing: "Đang chuẩn bị lượt", running: "Đang phát trực tiếp",
  rotating: "Đang chuyển lượt", stopped: "Đã dừng · có thể tiếp tục", completed: "Đã hoàn tất", failed: "Phiên gặp lỗi",
  stale: "Mất heartbeat · trạng thái chưa được xác nhận" };
const kinds = { view: "Xem", add_to_cart: "Thêm giỏ", purchase: "Mua" };

export function processed(event, session) {
  return event.segment_index <= session.completed_segments || (event.run_id === session.active_run_id &&
    event.next_offset <= (session.processed_offsets?.[event.partition] ?? -1));
}

export function mergeEvents(previous, incoming, session, gap = false) {
  const values = new Map((gap ? [] : previous).map(event => [event.event_id, event]));
  for (const event of incoming) values.set(event.event_id, event);
  return [...values.values()].sort((a, b) => b.sequence - a.sequence).slice(0, 50)
    .map(event => ({ ...event, processing_status: processed(event, session) ? "processed" : "acked" }));
}

export function feedTable(events) {
  return table([{ key: "emitted_at", label: "PHÁT LÚC · UTC", format: value => value?.slice(11, 23) },
    { key: "event_type", label: "HÀNH ĐỘNG", format: value => kinds[value] || value },
    { key: "visitor_id", label: "VISITOR", mono: true }, { key: "item_id", label: "SẢN PHẨM", mono: true },
    { key: "processing_status", label: "TRẠNG THÁI", format: value => value === "processed" ? "Spark đã xử lý" : "Kafka đã ACK · chờ Spark" }],
  events, "Chưa có event đã được Kafka xác nhận trong phiên này.");
}

export class LivePanel {
  constructor(client, onSnapshot, doc = document) {
    this.client = client; this.onSnapshot = onSnapshot; this.doc = doc;
    this.$ = id => doc.getElementById(id);
    this.identity = ""; this.after = undefined; this.rows = []; this.sessions = [];
    this.generation = 0; this.busy = false; this.lastListAt = 0; this.pinned = false;
    this.$("live-select").addEventListener("change", () => {
      this.pinned = true; this.identity = this.$("live-select").value;
      this.reset(); this.tick();
    });
    this.$("follow-live").addEventListener("change", () => this.tick());
    this.visibility = () => { if (!doc.hidden) this.tick(); };
    doc.addEventListener("visibilitychange", this.visibility);
  }

  reset() {
    this.generation++; this.controller?.abort(); this.busy = false;
    this.after = undefined; this.rows = [];
    this.$("live-events").innerHTML = feedTable([]);
  }

  stopFollowing() { this.$("follow-live").checked = false; }

  start() { this.tick(); this.timer = setInterval(() => this.tick(), 1000); }

  stop() {
    clearInterval(this.timer); this.generation++; this.controller?.abort();
    this.doc.removeEventListener?.("visibilitychange", this.visibility);
  }

  async tick() {
    if (this.busy || this.doc.hidden) return;
    this.busy = true;
    const generation = this.generation, controller = new AbortController();
    this.controller = controller;
    try {
      if (Date.now() - this.lastListAt >= 5000 || !this.identity) {
        const response = await this.client.get("live/sessions", {}, controller.signal);
        if (generation !== this.generation) return;
        this.sessions = response.data || [];
        this.lastListAt = Date.now();
        const active = this.sessions.find(s => ["starting", "preparing", "running", "rotating"].includes(s.status));
        const selected = this.sessions.find(s => s.live_id === this.identity);
        const candidate = (!this.pinned && this.$("follow-live").checked && active) || selected || this.sessions[0];
        if (candidate?.live_id !== this.identity) {
          this.identity = candidate?.live_id || ""; this.after = undefined; this.rows = [];
        }
        this.$("live-select").replaceChildren(...this.sessions.map(s => new Option(s.live_id, s.live_id)));
        this.$("live-select").value = this.identity;
        this.$("live-select").disabled = !this.identity;
      }
      if (!this.identity) {
        this.$("live-status").textContent = "Chưa có phiên live · chạy make live-demo LIVE_ID=demo-live-001";
        this.$("live-events").innerHTML = feedTable([]);
        return;
      }
      const response = await this.client.get(`live/sessions/${encodeURIComponent(this.identity)}/events`,
        { after: this.after, limit: 100 }, controller.signal);
      if (generation !== this.generation) return;
      const session = response.session;
      this.after = response.next_after ?? this.after;
      this.rows = mergeEvents(this.rows, response.data, session, response.gap);
      this.$("live-status").textContent = `${statuses[session.status] || session.status} · Lượt ${number(session.segment_index)} · ${session.active_run_id || "đang chuẩn bị"}`;
      this.$("live-totals").textContent = `Toàn phiên: ${number(session.acknowledged_events)} event đã ACK · ${number(session.processed_events)} đã xử lý · ${number(session.completed_segments)} lượt hoàn tất`;
      this.$("live-gap").hidden = !response.gap;
      this.$("live-events").innerHTML = feedTable(this.rows);
      if (this.$("follow-live").checked) this.onSnapshot(session);
    } catch (error) {
      if (generation !== this.generation || controller.signal.aborted) return;
      this.$("live-status").textContent = error.status === 404 ? "Khởi động lại make api để nạp API live mới." :
        "Chưa đọc được feed live; giữ dữ liệu lần đọc gần nhất. " + error.message;
    } finally {
      if (generation === this.generation) this.busy = false;
    }
  }
}
