"""
HTTP interceptor for replacing game server address from SSO response.
"""
import json
import socket
import threading
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer


def get_local_ip() -> str:
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(("8.8.8.8", 80))
        return sock.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        sock.close()


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self._proxy()

    def do_POST(self):
        self._proxy()

    def do_HEAD(self):
        self._proxy()

    def _proxy(self):
        if self.path.startswith("http://"):
            url = self.path
        else:
            host = self.headers.get("Host", "")
            url = f"http://{host}{self.path}"

        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length > 0 else None

        try:
            req = urllib.request.Request(url, data=body, method=self.command)
            skip_req = {"host", "content-length", "proxy-connection", "connection"}
            for key, value in self.headers.items():
                if key.lower() not in skip_req:
                    req.add_header(key, value)

            with urllib.request.urlopen(req, timeout=20) as resp:
                resp_body = resp.read()
                status = resp.status
                resp_hdrs = list(resp.headers.items())
        except urllib.error.HTTPError as exc:
            resp_body = exc.read()
            status = exc.code
            resp_hdrs = list(exc.headers.items())
        except Exception as exc:
            self.send_error(502, f"proxy forward failed: {exc}")
            return

        if "ssoGetServerList" in url:
            resp_body = self._inject(resp_body)

        skip_resp = {"transfer-encoding", "content-encoding", "content-length", "connection"}
        self.send_response(status)
        for key, value in resp_hdrs:
            if key.lower() not in skip_resp:
                self.send_header(key, value)
        self.send_header("Content-Length", str(len(resp_body)))
        self.end_headers()
        self.wfile.write(resp_body)

    def _inject(self, body: bytes) -> bytes:
        try:
            data = json.loads(body.decode("utf-8"))
            local_ip = get_local_ip()
            proxy_port = self.server.game_proxy_port
            original = data.get("s_url_list", [])

            if original and self.server.on_server_discovered:
                try:
                    host, port_str = original[0].rsplit(":", 1)
                    self.server.on_server_discovered(host, int(port_str))
                except Exception:
                    pass

            data["s_url_list"] = [f"{local_ip}:{proxy_port}"]
            print(f"[HTTPIntercept] s_url_list {original} -> [{local_ip}:{proxy_port}]", flush=True)
            return json.dumps(data, ensure_ascii=False).encode("utf-8")
        except Exception as exc:
            print(f"[HTTPIntercept] inject response failed: {exc}", flush=True)
            return body

    def log_message(self, fmt, *args):
        lower_path = self.path.lower()
        if any(key in lower_path for key in ("sso", "serverlist", "server")):
            print(f"[HTTPIntercept] {self.command} {self.path[:100]}", flush=True)


class _Server(HTTPServer):
    def __init__(self, addr, handler, game_proxy_port: int, on_server_discovered=None):
        self.game_proxy_port = game_proxy_port
        self.on_server_discovered = on_server_discovered
        super().__init__(addr, handler)


def start(listen_port: int, game_proxy_port: int, on_server_discovered=None) -> _Server:
    server = _Server(("0.0.0.0", listen_port), _Handler, game_proxy_port, on_server_discovered)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    print(f"[HTTPIntercept] listen 0.0.0.0:{listen_port}", flush=True)
    return server


def stop(server: _Server | None):
    if server is None:
        return
    try:
        server.shutdown()
    except Exception:
        pass
    try:
        server.server_close()
    except Exception:
        pass
