"""BlackMist TCP protocol debug tool."""
import argparse
import atexit
import io
import signal
import sys
import threading

from http_interceptor import get_local_ip, start as start_http, stop as stop_http
from proxy import TCPProxy
from store import PacketStore
from test_runner import TestRunner
from web import create_app

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace", line_buffering=True)


def main():
    parser = argparse.ArgumentParser(
        description="BlackMist TCP protocol debug tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--server-host", default=None, help="Game TCP server IP. If omitted, discover from SSO")
    parser.add_argument("--server-port", type=int, default=8080, help="Game TCP server port")
    parser.add_argument("--listen-port", type=int, default=18080, help="Local TCP proxy port")
    parser.add_argument("--http-port", type=int, default=8080, help="HTTP intercept proxy port")
    parser.add_argument("--web-port", type=int, default=8888, help="Web console port")
    parser.add_argument("--run-suite", default=None, help="Run JSON suite in CLI mode")
    parser.add_argument("--suite-conn", type=int, default=0, help="Connection index for suite run")
    parser.add_argument("--reports-dir", default="reports", help="Test report output directory")
    parser.add_argument("--suites-dir", default="suites", help="Suite directory")
    args = parser.parse_args()

    local_ip = get_local_ip()

    if not args.server_host:
        print("=" * 60, flush=True)
        print("  GameProxyTool - TCP protocol debug tool", flush=True)
        print("=" * 60, flush=True)
        print("  Game TCP server IP (press Enter to auto-discover from SSO)", flush=True)
        raw = input("  Input: ").strip()
        if raw:
            raw = raw.replace("http://", "").replace("https://", "").rstrip("/")
            host_part = raw.split(":")[0]
            args.server_host = host_part
            if ":" in raw:
                try:
                    args.server_port = int(raw.split(":", 1)[1])
                except ValueError:
                    pass

    store = PacketStore()
    proxy = TCPProxy(args.listen_port, args.server_host, args.server_port if args.server_host else None, store)
    proxy.start()

    on_discovered = None if args.server_host else proxy.set_target
    http_server = start_http(args.http_port, args.listen_port, on_server_discovered=on_discovered)

    shutdown_lock = threading.Lock()
    shutdown_once = {"done": False}

    def shutdown_all():
        with shutdown_lock:
            if shutdown_once["done"]:
                return
            shutdown_once["done"] = True
        print("\n[Lifecycle] shutting down services...", flush=True)
        stop_http(http_server)
        proxy.stop()

    atexit.register(shutdown_all)

    def handle_signal(signum, _frame):
        print(f"\n[Lifecycle] receive signal {signum}", flush=True)
        shutdown_all()
        raise SystemExit(0)

    signal.signal(signal.SIGINT, handle_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, handle_signal)

    if args.run_suite:
        try:
            runner = TestRunner(store, proxy, reports_dir=args.reports_dir)
            print("=" * 60, flush=True)
            print(f"  start suite: {args.run_suite}", flush=True)
            print(f"  conn index: {args.suite_conn}", flush=True)
            print("=" * 60, flush=True)
            result = runner.run_suite_from_path(args.run_suite, conn_index=args.suite_conn)
            print(flush=True)
            print(f"  result: {result.get('suite_status')}", flush=True)
            print(f"  passed cases: {result.get('passed_cases')}/{result.get('total_cases')}", flush=True)
            print(f"  report: {result.get('report_path')}", flush=True)
            raise SystemExit(0 if result.get("suite_status") == "passed" else 1)
        finally:
            shutdown_all()

    app = create_app(
        store,
        proxy,
        local_ip=local_ip,
        http_port=args.http_port,
        reports_dir=args.reports_dir,
        suites_dir=args.suites_dir,
    )

    server_desc = f"{args.server_host}:{args.server_port}" if args.server_host else "auto discover from SSO"
    print("=" * 60, flush=True)
    print(f"  Web console: http://localhost:{args.web_port}", flush=True)
    print(f"  HTTP intercept: 0.0.0.0:{args.http_port}", flush=True)
    print(f"  TCP proxy: 0.0.0.0:{args.listen_port} -> {server_desc}", flush=True)
    print(flush=True)
    print(f"  Mobile WiFi proxy -> {local_ip}:{args.http_port}", flush=True)
    print("=" * 60, flush=True)
    print(flush=True)

    try:
        app.run(host="0.0.0.0", port=args.web_port, debug=False, threaded=True)
    finally:
        shutdown_all()


if __name__ == "__main__":
    main()
