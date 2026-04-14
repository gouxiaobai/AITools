"""
Web 控制台后端 (Flask)
提供 REST API 和 SSE 实时推送
"""
import json
import os
import sys
import queue
import threading
import io
from pathlib import Path
from flask import Flask, request, jsonify, Response, send_from_directory, send_file

from store import PacketStore, Packet
from proxy import TCPProxy
from test_runner import TestRunner, TestRunService
from suite_xlsx import load_suite_from_xlsx_bytes, build_template_xlsx_bytes


def _static_dir() -> str:
    """兼容 PyInstaller 打包后的静态文件路径"""
    if getattr(sys, "frozen", False):
        return os.path.join(sys._MEIPASS, "static")
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")


def _pkt_to_dict(pkt: Packet, full_hex: bool = False) -> dict:
    return {
        "id": pkt.id,
        "direction": pkt.direction,
        "action": pkt.action,
        "decoded": pkt.decoded,
        "timestamp": pkt.timestamp,
        "note": pkt.note,
        "raw_hex": pkt.raw.hex() if full_hex else pkt.raw.hex()[:256],
    }


def create_app(
    store: PacketStore,
    proxy: TCPProxy,
    local_ip: str = "127.0.0.1",
    http_port: int = 8080,
    reports_dir: str = "reports",
    suites_dir: str = "suites",
) -> Flask:
    app = Flask(__name__, static_folder=_static_dir())
    runner = TestRunner(store, proxy, reports_dir=reports_dir)
    run_service = TestRunService(runner)

    # ---------- SSE 广播 ----------
    _queues: list[queue.Queue] = []
    _qlk = threading.Lock()

    def _broadcast(pkt: Packet):
        data = json.dumps(_pkt_to_dict(pkt))
        with _qlk:
            dead = []
            for q in _queues:
                try:
                    q.put_nowait(data)
                except queue.Full:
                    dead.append(q)
            for q in dead:
                _queues.remove(q)

    store.add_listener(_broadcast)

    # ================================================================== #
    #  路由                                                                #
    # ================================================================== #

    @app.route("/")
    def index():
        return send_from_directory("static", "index.html")

    # ---------- 状态 ----------

    @app.route("/api/status")
    def status():
        return jsonify({
            "active_connections": proxy.active_count(),
            "packet_count": len(store.get_all()),
            "token": store.token,
            "next_msg_id": store.peek_next_msg_id(),
            "local_ip": local_ip,
            "http_port": http_port,
        })

    # ---------- 数据包列表 ----------

    @app.route("/api/packets")
    def get_packets():
        after_id = int(request.args.get("after", -1))
        action_filter = request.args.get("action")
        direction_filter = request.args.get("direction")

        packets = store.get_all(after_id)

        if action_filter:
            try:
                af = int(action_filter)
                packets = [p for p in packets if p.action == af]
            except ValueError:
                pass

        if direction_filter in ("C2S", "S2C"):
            packets = [p for p in packets if p.direction == direction_filter]

        return jsonify([_pkt_to_dict(p) for p in packets])

    @app.route("/api/packets/<int:pid>")
    def get_packet(pid):
        pkt = store.get_by_id(pid)
        if not pkt:
            return jsonify({"error": "not found"}), 404
        return jsonify(_pkt_to_dict(pkt, full_hex=True))

    @app.route("/api/packets/<int:pid>/note", methods=["POST"])
    def set_note(pid):
        pkt = store.get_by_id(pid)
        if not pkt:
            return jsonify({"error": "not found"}), 404
        pkt.note = (request.json or {}).get("note", "")
        return jsonify({"ok": True})

    @app.route("/api/clear", methods=["POST"])
    def clear():
        store.clear()
        return jsonify({"ok": True})

    # ---------- 发包 / 重放 ----------

    @app.route("/api/send", methods=["POST"])
    def send_packet():
        body = request.json or {}
        action = body.get("action")
        params = body.get("params", {})
        conn_index = int(body.get("conn", 0))

        if action is None:
            return jsonify({"error": "action 字段必填"}), 400

        msg = dict(params)
        msg["action"] = int(action)

        try:
            msg_id = proxy.inject(msg, conn_index)
            return jsonify({"ok": True, "msg_id": msg_id})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route("/api/replay/<int:pid>", methods=["POST"])
    def replay(pid):
        pkt = store.get_by_id(pid)
        if not pkt:
            return jsonify({"error": "packet not found"}), 404
        if pkt.direction != "C2S":
            return jsonify({"error": "只能重放 C2S 方向的包"}), 400

        body = request.json or {}
        conn_index = int(body.get("conn", 0))
        overrides = body.get("overrides", {})   # 允许覆盖字段后重放

        msg = dict(pkt.decoded)
        msg.update(overrides)
        msg.pop("commReq", None)    # 让 inject 重新生成 commReq

        try:
            msg_id = proxy.inject(msg, conn_index)
            return jsonify({"ok": True, "msg_id": msg_id})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    # ---------- 自动化测试 ----------

    @app.route("/api/test/run", methods=["POST"])
    def test_run():
        body = request.json or {}
        suite = body.get("suite")
        suite_path = body.get("suite_path")
        conn_index = int(body.get("conn", 0))
        if not suite and not suite_path:
            return jsonify({"error": "suite 或 suite_path 必填"}), 400
        try:
            rec = run_service.start_run(suite=suite, suite_path=suite_path, conn_index=conn_index)
            return jsonify({"ok": True, "run": rec})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route("/api/test/run/<run_id>", methods=["GET"])
    def test_run_status(run_id):
        rec = run_service.get_run(run_id)
        if not rec:
            return jsonify({"error": "run not found"}), 404
        return jsonify(rec)

    @app.route("/api/test/stop/<run_id>", methods=["POST"])
    def test_stop(run_id):
        rec = run_service.stop_run(run_id)
        if not rec:
            return jsonify({"error": "run not found"}), 404
        return jsonify({"ok": True, "run": rec})

    @app.route("/api/test/runs", methods=["GET"])
    def test_runs():
        return jsonify(run_service.list_runs())

    @app.route("/api/test/upload-run", methods=["POST"])
    def test_upload_run():
        up = request.files.get("file")
        if not up:
            return jsonify({"error": "xlsx file is required"}), 400
        filename = (up.filename or "").strip()
        if not filename.lower().endswith(".xlsx"):
            return jsonify({"error": "only .xlsx is supported"}), 400
        conn_index = int((request.form or {}).get("conn", 0))
        suite_name = Path(filename).stem or "uploaded_suite"
        try:
            suite = load_suite_from_xlsx_bytes(up.read(), default_suite_name=suite_name)
            rec = run_service.start_run(suite=suite, conn_index=conn_index)
            return jsonify({"ok": True, "run": rec, "suite_name": suite.get("name")})
        except Exception as e:
            return jsonify({"error": str(e)}), 400

    @app.route("/api/test/template.xlsx", methods=["GET"])
    def test_template():
        data = build_template_xlsx_bytes()
        return send_file(
            io.BytesIO(data),
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name="suite_template.xlsx",
        )

    @app.route("/api/test/suites", methods=["GET"])
    def test_suites():
        files = []
        if os.path.isdir(suites_dir):
            for root, _, names in os.walk(suites_dir):
                for name in names:
                    if not name.lower().endswith(".json"):
                        continue
                    full = os.path.join(root, name)
                    rel = os.path.relpath(full, os.getcwd())
                    files.append(rel.replace("\\", "/"))
        files.sort()
        return jsonify(files)

    # ---------- Token ----------

    @app.route("/api/token", methods=["POST"])
    def set_token():
        store.token = (request.json or {}).get("token", "")
        return jsonify({"ok": True})

    # ---------- SSE 实时推送 ----------

    @app.route("/api/stream")
    def stream():
        q: queue.Queue = queue.Queue(maxsize=200)
        with _qlk:
            _queues.append(q)

        def generate():
            try:
                while True:
                    try:
                        data = q.get(timeout=25)
                        yield f"data: {data}\n\n"
                    except queue.Empty:
                        yield ": ping\n\n"   # 保活
            finally:
                with _qlk:
                    if q in _queues:
                        _queues.remove(q)

        return Response(
            generate(),
            mimetype="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    return app
