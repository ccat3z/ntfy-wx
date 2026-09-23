"""ntfy-wx 单元测试.

约定:
- ntfy 用仓库根目录自部署的二进制 (./ntfy, 不存在时自动下载) 起本地 server, 真实 HTTP, 不 mock
- 企业微信 API 用本地 mock HTTP 服务器 (http.server), 只 mock 协议层
- 其余一律真实逻辑, 不 mock

运行: nix-shell -p python313Packages.pytest python313Packages.pytest-cov \\
        --run "pytest tests/ --cov=ntfy-wx.py --cov-report=term-missing"
"""

import configparser
import datetime
import json
import os
import queue
import shutil
import signal
import subprocess
import tempfile
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

import importlib.util

spec = importlib.util.spec_from_file_location(
    "ntfy_wx", os.path.join(os.path.dirname(__file__), "..", "ntfy-wx.py"))
ntfy_wx = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ntfy_wx)

# ---------------------------------------------------------------------------
# 企业微信 mock HTTP 服务器
# ---------------------------------------------------------------------------


class WecomMockState:
    """记录 mock 服务器收到的请求, 可配置响应与错误注入.

    lock 保护记录列表: handler 线程写, 测试线程读 (多线程 main 测试需要).
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.reset()

    def reset(self):
        self.token_requests = []
        self.send_requests = []      # (access_token, payload dict)
        self.token_responses = []    # 每次 gettoken 的响应队列 (dict), 空则默认成功
        self.send_responses = []     # 每次 message/send 的响应队列
        self.send_status = []        # 每次 message/send 的 HTTP 状态码队列
        self.send_delay = 0          # 响应延迟 (测超时)


def make_wecom_mock(state):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):  # 静默
            pass

        def _respond(self, obj, status=200):
            if state.send_delay:
                time.sleep(state.send_delay)
            body = json.dumps(obj).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            # /gettoken?corpid=..&corpsecret=..
            assert self.path.startswith("/gettoken"), self.path
            from urllib.parse import urlparse, parse_qs
            q = parse_qs(urlparse(self.path).query)
            with state.lock:
                state.token_requests.append((q.get("corpid"), q.get("corpsecret")))
            resp = state.token_responses.pop(0) if state.token_responses else {
                "errcode": 0, "errmsg": "ok",
                "access_token": "mock-token", "expires_in": 7200,
            }
            self._respond(resp)

        def do_POST(self):
            assert self.path.startswith("/message/send"), self.path
            from urllib.parse import urlparse, parse_qs
            token = parse_qs(urlparse(self.path).query).get("access_token", [None])[0]
            length = int(self.headers.get("Content-Length", 0))
            payload = json.loads(self.rfile.read(length))
            with state.lock:
                state.send_requests.append((token, payload))
                if state.send_status:
                    self.send_response(state.send_status.pop(0))
                    self.end_headers()
                    return
                resp = state.send_responses.pop(0) if state.send_responses else {
                "errcode": 0, "errmsg": "ok",
            }
            self._respond(resp)

    return Handler


@pytest.fixture()
def wecom_server():
    """启动本地 mock 企业微信 HTTP 服务器, yield (state, base_url), 结束后关闭.

    只 mock 协议层: 真实 HTTP 请求/响应, 行为 (返回码/错误注入) 通过 state 配置.
    """
    state = WecomMockState()
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_wecom_mock(state))
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    yield state, "http://127.0.0.1:{}".format(server.server_address[1])
    server.shutdown()
    server.server_close()


@pytest.fixture()
def wecom(wecom_server):
    """(state, base_url) 元组的简写, 只需要状态/地址的用例用这个."""
    return wecom_server


@pytest.fixture()
def sender(wecom_server):
    """指向 mock 服务器的真实 WeComSender 实例."""
    _, base = wecom_server
    return ntfy_wx.WeComSender("cid-x", "aid-100", "sec-y", base_url=base)


# ---------------------------------------------------------------------------
# 自部署 ntfy server (仓库 bin/ntfy 二进制, session 级)
# ---------------------------------------------------------------------------

NTFY_BIN = os.path.join(os.path.dirname(__file__), "..", "ntfy")
NTFY_VERSION = "2.28.0"


def ensure_ntfy_binary():
    """仓库/ntfy 不存在时自动从 GitHub releases 下载 (linux amd64)."""
    if os.path.exists(NTFY_BIN):
        return
    import tarfile
    import urllib.request as urlreq
    bin_dir = os.path.dirname(NTFY_BIN)
    os.makedirs(bin_dir, exist_ok=True)
    url = ("https://github.com/binwiederhier/ntfy/releases/download/"
           "v{}/ntfy_{}_linux_amd64.tar.gz".format(NTFY_VERSION, NTFY_VERSION))
    tgz = os.path.join(bin_dir, "ntfy.tar.gz")
    print("下载 {} ...".format(url))
    urlreq.urlretrieve(url, tgz)
    with tarfile.open(tgz) as tf:
        member = next(m for m in tf.getmembers()
                      if m.name.endswith("/ntfy") and m.isfile())
        member.name = "ntfy"
        tf.extract(member, bin_dir)
    os.remove(tgz)
    os.chmod(NTFY_BIN, 0o755)


@pytest.fixture(scope="session")
def ntfy_url():
    """启动仓库内 ntfy 二进制 serve (随机端口, 临时缓存), yield URL, 结束后杀进程.

    ntfy 不存在时先自动下载 (见 ensure_ntfy_binary).
    """
    ensure_ntfy_binary()
    cache_dir = tempfile.mkdtemp(prefix="ntfy-wx-ut-cache-")
    port = 20000 + os.getpid() % 10000  # 随机但稳定
    proc = subprocess.Popen(
        [NTFY_BIN, "serve", "--listen-http", ":{}".format(port),
         "--cache-file", os.path.join(cache_dir, "cache.db")],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        preexec_fn=os.setsid)
    url = "http://127.0.0.1:{}".format(port)
    # 等就绪
    deadline = time.time() + 15
    while time.time() < deadline:
        try:
            with ntfy_wx.urllib.request.urlopen(url + "/v1/health", timeout=1) as r:
                if r.status == 200:
                    break
        except OSError:
            time.sleep(0.2)
    else:
        proc.terminate()
        pytest.fail("ntfy server 启动超时")
    yield url
    os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    proc.wait(timeout=5)
    shutil.rmtree(cache_dir, ignore_errors=True)


@pytest.fixture(scope="session")
def ntfy_auth_url():
    """带访问控制的 ntfy server (deny-all): 任何订阅 (无/坏凭据) 都被拒绝.

    无凭据返回 403, 坏 Basic 凭据返回 401, 均为 HTTPError.
    """
    ensure_ntfy_binary()
    cache_dir = tempfile.mkdtemp(prefix="ntfy-wx-ut-cache-")
    auth_db = os.path.join(cache_dir, "auth.db")
    port = 21000 + os.getpid() % 10000
    proc = subprocess.Popen(
        [NTFY_BIN, "serve", "--listen-http", ":{}".format(port),
         "--cache-file", os.path.join(cache_dir, "cache.db"),
         "--auth-file", auth_db, "--auth-default-access", "deny-all"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        preexec_fn=os.setsid)
    url = "http://127.0.0.1:{}".format(port)
    deadline = time.time() + 15
    while time.time() < deadline:
        try:
            with ntfy_wx.urllib.request.urlopen(url + "/v1/health", timeout=1) as r:
                if r.status == 200:
                    break
        except ntfy_wx.urllib.error.HTTPError:
            break  # auth 开启后 health 也可能要凭据, 能返回 HTTP 就说明已就绪
        except OSError:
            time.sleep(0.2)
    else:
        proc.terminate()
        pytest.fail("ntfy auth server 启动超时")
    yield url
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        proc.wait(timeout=5)
    except OSError:
        pass
    shutil.rmtree(cache_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# ntfy helpers (真实服务器)
# ---------------------------------------------------------------------------


def ut_topic():
    return "ntfy-wx-ut-" + uuid.uuid4().hex[:12]


def publish(base_url, topic, message, title="", token=""):
    """向 ntfy server 真实发布一条消息, 返回服务器的响应 id."""
    req_type = ntfy_wx.urllib.request.Request
    headers = {"Title": title} if title else {}
    if token:
        headers["Authorization"] = "Bearer " + token
    req = req_type("{}/{}/json".format(base_url, topic),
                   data=message.encode(), headers=headers, method="POST")
    with ntfy_wx.urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())["id"]


# ---------------------------------------------------------------------------
# WeComSender
# ---------------------------------------------------------------------------


class TestWeComSender:
    def test_get_token_caches(self, wecom, sender):
        state, _ = wecom
        t1 = sender.get_token()
        t2 = sender.get_token()  # 第二次应命中缓存, 不再请求
        assert t1 == "mock-token" == t2
        assert len(state.token_requests) == 1
        # 请求里带了凭据
        assert state.token_requests[0] == (["cid-x"], ["sec-y"])

    def test_send_text_payload(self, wecom, sender):
        sender.send("hello", "text")
        token, payload = wecom[0].send_requests[0]
        assert token == "mock-token"
        assert payload["touser"] == "@all"
        assert payload["msgtype"] == "text"
        assert payload["agentid"] == "aid-100"
        assert payload["text"]["content"] == "hello"

    def test_send_markdown_payload(self, wecom, sender):
        sender.send("**bold**", "markdown")
        assert wecom[0].send_requests[0][1]["markdown"]["content"] == "**bold**"

    def test_send_invalid_type(self, sender):
        with pytest.raises(ValueError):
            sender.send("x", "image")

    def test_token_error_raises(self, wecom, sender):
        wecom[0].token_responses.append({"errcode": 40001, "errmsg": "bad cred"})
        with pytest.raises(RuntimeError, match="access_token"):
            sender.get_token()

    def test_send_api_error_raises(self, wecom, sender):
        wecom[0].send_responses.append({"errcode": 81013, "errmsg": "no agent"})
        with pytest.raises(RuntimeError, match="81013"):
            sender.send("hi", "text")

    def test_send_token_invalid_retry(self, wecom, sender):
        """40014 应刷新 token 重试一次并成功."""
        state = wecom[0]
        state.send_responses.append({"errcode": 40014, "errmsg": "invalid token"})
        sender.send("hi", "text")
        # 第一次用缓存 token, 第二次强制刷新后用新 token
        assert len(state.send_requests) == 2
        assert len(state.token_requests) == 2
        assert state.send_requests[1][0] == "mock-token"

    def test_send_token_expired_retry_exhausted(self, wecom, sender):
        state = wecom[0]
        state.send_responses.append({"errcode": 42001, "errmsg": "expired"})
        state.send_responses.append({"errcode": 42001, "errmsg": "expired"})
        with pytest.raises(RuntimeError, match="42001"):
            sender.send("hi", "text")
        assert len(state.send_requests) == 2


# ---------------------------------------------------------------------------
# ntfy 流 (真实服务器)
# ---------------------------------------------------------------------------


class TestNtfyStream:
    def test_publish_then_poll_roundtrip(self, ntfy_url):
        """真实发布 -> 订阅流读取, 验证 parse 与回放."""
        topic = ut_topic()
        publish(ntfy_url, topic, "msg-1", title="T1")
        publish(ntfy_url, topic, "msg-2")
        msgs = []
        for m in ntfy_wx.NtfyForwarder._stream_messages(ntfy_url, "", [topic], since="latest"):
            msgs.append(m)
            if m["message"] == "msg-2":
                break  # 拿到目标消息即可, 流后续由 daemon 线程特性保证不阻塞测试
        texts = [m["message"] for m in msgs]
        assert "msg-2" in texts

    def test_since_skips_old(self, ntfy_url):
        """since=<时间戳> 只回放之后的消息."""
        topic = ut_topic()
        publish(ntfy_url, topic, "old-msg")
        time.sleep(1.1)  # 确保时间戳跨秒
        boundary = int(time.time())
        time.sleep(1.1)
        publish(ntfy_url, topic, "new-msg")
        msgs = []
        for m in ntfy_wx.NtfyForwarder._stream_messages(ntfy_url, "", [topic], since=str(boundary)):
            msgs.append(m)
            if m["message"] == "new-msg":
                break
        texts = [m["message"] for m in msgs]
        assert "new-msg" in texts
        assert "old-msg" not in texts

    def test_keepalive_lines_are_ignored(self, ntfy_url):
        """keepalive/open 等事件行不会成为消息 (生成器只在真消息上 yield);
        流本身阻塞不结束, 用守护线程 + 超时验证."""
        topic = ut_topic()  # 空话题: 只有 open/keepalive
        got = queue.Queue()

        def read():
            for m in ntfy_wx.NtfyForwarder._stream_messages(ntfy_url, "", [topic], since="latest"):
                got.put(m)

        t = threading.Thread(target=read, daemon=True)
        t.start()
        # 3 秒内不应产出任何消息 (只有事件行); 线程仍阻塞在读上 (正常)
        with pytest.raises(queue.Empty):
            got.get(timeout=3)

    def test_auth_header_accepted_without_auth_server(self, ntfy_url):
        """自部署 server 未开启访问控制: 带 token 订阅也被接受, 流正常建立
        (能读到消息而不是 HTTP 错误)."""
        topic = ut_topic()
        publish(ntfy_url, topic, "hi")
        msgs = []
        for m in ntfy_wx.NtfyForwarder._stream_messages(ntfy_url, "any-token", [topic], since="latest"):
            msgs.append(m)
            break  # 读到第一条 (回放的 hi) 即成功
        assert msgs and msgs[0]["message"] == "hi"


class TestParseStreamLine:
    def test_plain_json(self):
        m = ntfy_wx.NtfyForwarder._parse_stream_line(
            b'{"id":"a","event":"message","topic":"t","message":"hi"}')
        assert m["message"] == "hi"

    def test_sse_prefix(self):
        m = ntfy_wx.NtfyForwarder._parse_stream_line(
            b'data: {"id":"a","event":"message","topic":"t","message":"hi"}')
        assert m["message"] == "hi"

    def test_keepalive_and_open(self):
        assert ntfy_wx.NtfyForwarder._parse_stream_line(b'{"event":"keepalive"}') is None
        assert ntfy_wx.NtfyForwarder._parse_stream_line(b'{"event":"open","topic":"t"}') is None

    def test_empty_and_comment(self):
        assert ntfy_wx.NtfyForwarder._parse_stream_line(b"") is None
        assert ntfy_wx.NtfyForwarder._parse_stream_line(b": ping") is None
        assert ntfy_wx.NtfyForwarder._parse_stream_line(b"   ") is None

    def test_no_message_field(self):
        assert ntfy_wx.NtfyForwarder._parse_stream_line(b'{"event":"message","topic":"t"}') is None

    def test_str_input(self):
        m = ntfy_wx.NtfyForwarder._parse_stream_line('{"event":"message","message":"x"}')
        assert m["message"] == "x"

    def test_invalid_json_raises(self):
        with pytest.raises(json.JSONDecodeError):
            ntfy_wx.NtfyForwarder._parse_stream_line(b"not json at all")


# ---------------------------------------------------------------------------
# 持久化 / 去重 (db 的 host key 只是标识字符串, 不发请求)
# ---------------------------------------------------------------------------

NTFY_URL = "https://db-key.example"


@pytest.fixture()
def db(tmpdir):
    """在 tmpdir 中打开 ntfy-wx.db, 用例间完全隔离."""
    return ntfy_wx.Storage(str(tmpdir.join("ntfy-wx.db")))


class TestDb:
    def test_forward_roundtrip(self, db):
        assert not db.is_forwarded(NTFY_URL, "t", "m1")
        db.mark(NTFY_URL, "t", "m1", 100)
        assert db.is_forwarded(NTFY_URL, "t", "m1")
        assert not db.is_forwarded(NTFY_URL, "t", "m2")  # 其它 id
        assert not db.is_forwarded(NTFY_URL, "t2", "m1")  # 其它 topic
        assert not db.is_forwarded("https://x.example", "t", "m1")

    def test_dropped_not_counted_as_forwarded(self, db):
        """dropped 记录不参与去重 (失败消息回放可重试), 也不进 since."""
        db.mark(NTFY_URL, "t", "m1", 100, dropped=True)
        assert not db.is_forwarded(NTFY_URL, "t", "m1")
        assert db.since(NTFY_URL) is None

    def test_since_max_global(self, db):
        """since 取全局 (跨 topic) 已成功转发的最大 time."""
        assert db.since(NTFY_URL) is None
        db.mark(NTFY_URL, "t1", "a", 100)
        db.mark(NTFY_URL, "t2", "b", 300)
        db.mark(NTFY_URL, "t1", "c", 200)
        db.mark(NTFY_URL, "t1", "d", 400, dropped=True)  # dropped 不推进 since
        assert db.since(NTFY_URL) == 300

    def test_stats_since(self, db):
        now = int(time.time())
        db.mark(NTFY_URL, "t", "a", now - 100)
        db.mark(NTFY_URL, "t", "b", now - 50)
        db.mark(NTFY_URL, "t", "c", now - 10, dropped=True)
        f, d = db.stats_since(NTFY_URL, now - 60)
        assert (f, d) == (1, 1)  # now-60 之后: b 成功, c 丢弃; a 在窗口外

    def test_persistence_across_reopen(self, db, tmpdir):
        now = int(time.time())
        db.mark(NTFY_URL, "t", "a", now)
        db2 = ntfy_wx.Storage(str(tmpdir.join("ntfy-wx.db")))
        assert db2.is_forwarded(NTFY_URL, "t", "a")
        assert db2.since(NTFY_URL) == now

    def test_retention_cleanup(self, db, tmpdir):
        """超出保留期的记录在重新打开时被清理."""
        db.mark(NTFY_URL, "t", "old", 100)  # 1970 年, 远超保留期
        db2 = ntfy_wx.Storage(str(tmpdir.join("ntfy-wx.db")))
        assert not db2.is_forwarded(NTFY_URL, "t", "old")

    def test_mark_idempotent(self, db):
        db.mark(NTFY_URL, "t", "a", 100)
        db.mark(NTFY_URL, "t", "a", 999)  # OR IGNORE, 不更新
        assert db.since(NTFY_URL) == 100


# ---------------------------------------------------------------------------
# format_message
# ---------------------------------------------------------------------------


class TestFormatMessage:
    def test_plain(self):
        assert ntfy_wx.NtfyForwarder._format_message(
            {"message": "body"}, "tt") == "[tt] body"

    def test_title(self):
        assert ntfy_wx.NtfyForwarder._format_message(
            {"title": "T", "message": "B"}, "tt") == "[tt] T\nB"

    def test_truncate(self):
        m = {"message": "x" * 5000}
        out = ntfy_wx.NtfyForwarder._format_message(m, "tt")
        assert len(out.encode("utf-8")) <= 2001  # 截断 + 省略号

    def test_utf8_safe_truncate(self):
        m = {"message": "好" * 1500}  # 3 字节/字符, 截断点落在多字节序列中间
        out = ntfy_wx.NtfyForwarder._format_message(m, "tt")
        out.encode("utf-8")  # 不应抛 UnicodeEncodeError


# ---------------------------------------------------------------------------
# DailyStats
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# 端到端: 真实 ntfy -> mock 微信
# ---------------------------------------------------------------------------


class TestEndToEnd:
    def test_publish_forward_and_dedup(self, wecom_server, sender, db, ntfy_url):
        """发布到真实 ntfy, 用 reader 的核心逻辑转发到 mock 微信,
        再跑一遍验证按 id 去重不重发."""
        state, _ = wecom_server
        topic = ut_topic()
        publish(ntfy_url, topic, "e2e-msg", title="E2E")

        def run_once():
            """复刻 _reader_loop 的一轮 (单线程版本, 读到目标消息即返回)."""
            since = db.since(ntfy_url)
            since = str(since) if since is not None else "latest"
            for m in ntfy_wx.NtfyForwarder._stream_messages(ntfy_url, "", [topic], since):
                msg_id = m.get("id")
                if msg_id and db.is_forwarded(ntfy_url, topic, msg_id):
                    break  # 回放到已转发的最新一条, 后面不会有新消息了
                sender.send(ntfy_wx.NtfyForwarder._format_message(m, topic), "text")
                db.mark(ntfy_url, topic, msg_id, m.get("time", 0))
                break

        run_once()
        assert len(state.send_requests) == 1
        content = state.send_requests[0][1]["text"]["content"]
        assert "e2e-msg" in content and topic in content

        run_once()  # 重启回放: 同一条消息必须被去重
        assert len(state.send_requests) == 1

    def test_send_failure_not_marked_then_skipped_by_since(self, wecom_server, sender, db, ntfy_url):
        """发送失败的消息不记 id, 重启后靠 since 越过 (与 main 一致: 失败不重试)."""
        state, _ = wecom_server
        topic = ut_topic()
        publish(ntfy_url, topic, "will-fail")
        state.send_responses.append({"errcode": 81013, "errmsg": "fail"})

        for m in ntfy_wx.NtfyForwarder._stream_messages(ntfy_url, "", [topic], "latest"):
            ok = True
            try:
                sender.send(ntfy_wx.NtfyForwarder._format_message(m, topic), "text")
            except Exception:
                ok = False
            if ok and m.get("id"):
                db.mark(ntfy_url, topic, m["id"], m.get("time", 0))
            break  # 回放的最新一条即目标
        assert len(state.send_requests) == 1  # 发过一次且失败
        # 失败的消息没有 id 记录 -> since 无值
        assert db.since(ntfy_url) is None


# ---------------------------------------------------------------------------
# main() 集成: 真实本地 ntfy + mock 微信 + 临时 config, 有限轮数
# ---------------------------------------------------------------------------


class TestForwarder:
    """NtfyForwarder 集成: 真实本地 ntfy + mock 微信 + tmpdir 存储.

    _reader_loop 无限循环且无 stop, 测试用后台 daemon 线程跑, 断言后任其随
    进程退出; 每个 case 用独立 Storage 路径隔离.
    """

    @staticmethod
    def _make_forwarder(cfg_path, base, tmpdir, **kwargs):
        cfg = ntfy_wx.load_config(str(cfg_path))
        sender = ntfy_wx.WeComSender(cfg["wx_cid"], cfg["wx_aid"], cfg["wx_secret"],
                                     base_url=base)
        storage = ntfy_wx.Storage(str(tmpdir.join("ntfy-wx.db")))
        return ntfy_wx.NtfyForwarder(cfg, sender, storage=storage, **kwargs)

    @staticmethod
    def _wait_for(pred, timeout=10):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if pred():
                return
            time.sleep(0.1)
        raise AssertionError("等待条件超时")

    def _write_cfg(self, tmpdir, base, ntfy_url, topic):
        cfg_path = tmpdir.join("config.ini")
        cfg_path.write("""
[wecom]
cid = c
aid = a
secret = s
api_base = {}

[ntfy]
url = {}
topics = {}
""".format(base, ntfy_url, topic), "w")
        return cfg_path

    def test_full_flow_forward_and_dedup(self, wecom_server, ntfy_url, tmpdir):
        """转发真实 ntfy 消息到 mock 微信; 同一 forwarder 内重复回放被去重."""
        state, base = wecom_server
        topic = ut_topic()
        publish(ntfy_url, topic, "main-msg")
        cfg_path = self._write_cfg(tmpdir, base, ntfy_url, topic)

        fwd = self._make_forwarder(cfg_path, base, tmpdir)
        threading.Thread(target=fwd._reader_loop, daemon=True).start()
        self._wait_for(lambda: any(
            "main-msg" in p["text"]["content"] for _, p in state.send_requests))
        time.sleep(1)  # 让回放消息全部处理完
        n_main = sum("main-msg" in p["text"]["content"] for _, p in state.send_requests)
        assert n_main == 1
        # 进度已落库: 重启 (新 forwarder, 同 storage) 不会重发
        fwd2 = self._make_forwarder(cfg_path, base, tmpdir)
        threading.Thread(target=fwd2._reader_loop, daemon=True).start()
        time.sleep(2)  # 回放窗口内无新消息
        assert sum("main-msg" in p["text"]["content"]
                   for _, p in state.send_requests) == 1

    def test_send_failure_skips_and_not_marked(self, wecom_server, ntfy_url, tmpdir):
        """转发失败: 记日志跳过, 不标记已转发."""
        state, base = wecom_server
        topic = ut_topic()
        publish(ntfy_url, topic, "fail-msg")
        cfg_path = self._write_cfg(tmpdir, base, ntfy_url, topic)
        state.send_responses.append({"errcode": 81013, "errmsg": "fail"})

        fwd = self._make_forwarder(cfg_path, base, tmpdir)
        threading.Thread(target=fwd._reader_loop, daemon=True).start()
        self._wait_for(lambda: any(
            "fail-msg" in p["text"]["content"] for _, p in state.send_requests))
        time.sleep(0.5)
        assert sum("fail-msg" in p["text"]["content"]
                   for _, p in state.send_requests) == 1
        # 失败记为 dropped: 不算已转发, 统计可见
        f, d = fwd._storage.stats_since(ntfy_url, 0)
        assert (f, d) == (0, 1)

    def test_connection_error_keeps_retrying(self, wecom_server, tmpdir):
        """ntfy 不可达: 记日志, 不崩溃, 持续重试."""
        _, base = wecom_server
        cfg_path = tmpdir.join("config.ini")
        cfg_path.write("""
[wecom]
cid = c
aid = a
secret = s
api_base = {}

[ntfy]
url = http://127.0.0.1:1   # 不可达端口
topics = t
""".format(base), "w")
        fwd = self._make_forwarder(cfg_path, base, tmpdir)
        t = threading.Thread(target=fwd._reader_loop, daemon=True)
        t.start()
        time.sleep(2)  # 跑几轮连接失败
        assert t.is_alive()  # 不崩溃, 持续重试

    def test_ntfy_403_notifies_once(self, wecom_server, ntfy_auth_url, tmpdir):
        """ntfy 拒绝访问 (403): 通知微信一次, 之后静默按间隔重试."""
        state, base = wecom_server
        cfg_path = tmpdir.join("config.ini")
        cfg_path.write("""
[wecom]
cid = c
aid = a
secret = s
api_base = {}

[ntfy]
url = {}
topics = t
""".format(base, ntfy_auth_url), "w")
        fwd = self._make_forwarder(cfg_path, base, tmpdir,
                                   ntfy_error_retry_interval=0.5)
        threading.Thread(target=fwd._reader_loop, daemon=True).start()
        time.sleep(3)  # 足够经历 2+ 轮 403 重试 (间隔 0.5s)
        contents = [p["text"]["content"] for _, p in state.send_requests]
        assert sum("HTTP 403" in c and "重试" in c for c in contents) == 1


class TestDumpLoop:
    """_dump_loop: 睡到 dump_time 发当日 (自 0 点) 的 db 统计.

    不 mock 时间: 选一个几秒后的 dump_time, 真实睡眠触发.
    """

    def _forwarder(self, wecom_server, tmpdir, dump_time):
        state, base = wecom_server
        cfg = {"wx_cid": "c", "wx_aid": "a", "wx_secret": "s",
               "wx_api_base": base, "ntfy_url": "https://n.example",
               "ntfy_token": "", "ntfy_topics": ["t"]}
        sender = ntfy_wx.WeComSender("c", "a", "s", base_url=base)
        storage = ntfy_wx.Storage(str(tmpdir.join("ntfy-wx.db")))
        return ntfy_wx.NtfyForwarder(cfg, sender, storage=storage,
                                     dump_time=dump_time), state

    @staticmethod
    def _soon(seconds=3):
        """当前时刻 + seconds, 转成带本地时区的 time (dump_time 参数用)."""
        at = datetime.datetime.now().astimezone() + datetime.timedelta(seconds=seconds)
        return datetime.time(at.hour, at.minute, at.second, tzinfo=at.tzinfo)

    def _run_dump_once(self, fwd, state):
        t = threading.Thread(target=fwd._dump_loop, daemon=True)
        t.start()
        deadline = time.time() + 15
        while time.time() < deadline:
            if state.send_requests:
                break
            time.sleep(0.1)
        else:
            raise AssertionError("等待日报请求超时")
        time.sleep(0.2)

    def test_report_from_db_stats(self, wecom_server, tmpdir):
        # 当日 (自 0 点) 的真实时间戳窗口
        day_start = int(datetime.datetime.now().astimezone()
                        .replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        fwd, state = self._forwarder(wecom_server, tmpdir, self._soon())
        url = fwd._cfg["ntfy_url"]
        # 今日: 5 成功 + 1 失败; 昨日: 2 成功 (不应计入)
        for i in range(5):
            fwd._storage.mark(url, "t", "ok{}".format(i), day_start + 3600 + i)
        fwd._storage.mark(url, "t", "bad", day_start + 7200, dropped=True)
        fwd._storage.mark(url, "t", "y1", day_start - 100)
        fwd._storage.mark(url, "t", "y2", day_start - 200)

        self._run_dump_once(fwd, state)
        content = state.send_requests[0][1]["text"]["content"]
        assert "日报" in content and "5 条消息" in content and "失败 1 条" in content

    def test_report_send_failure_logged_not_raised(self, wecom_server, tmpdir):
        fwd, state = self._forwarder(wecom_server, tmpdir, self._soon())
        state.send_responses.append({"errcode": -1, "errmsg": "boom"})
        self._run_dump_once(fwd, state)  # 发送失败不抛出


class TestLoadConfig:
    def _write(self, tmpdir, body):
        p = tmpdir.join("config.ini")
        with open(str(p), "w", encoding="utf-8") as f:
            f.write(body)
        return str(p)

    def test_full(self, tmpdir):
        p = self._write(tmpdir, """
[wecom]
cid = c1
aid = a1
secret = s1

[ntfy]
url = https://n.example
token = tk
topics = t1, t2
""")
        cfg = ntfy_wx.load_config(p)
        assert cfg["wx_cid"] == "c1"
        assert cfg["ntfy_url"] == "https://n.example"
        assert cfg["ntfy_topics"] == ["t1", "t2"]

    def test_defaults(self, tmpdir):
        p = self._write(tmpdir, """
[wecom]
cid = c
aid = a
secret = s

[ntfy]
topics = x
""")
        cfg = ntfy_wx.load_config(p)
        assert cfg["ntfy_url"] == "https://ntfy.sh"
        assert cfg["ntfy_token"] == ""

    def test_multiline_topics(self, tmpdir):
        p = self._write(tmpdir, """
[wecom]
cid = c
aid = a
secret = s

[ntfy]
topics =
    aa
    bb
""")
        assert ntfy_wx.load_config(p)["ntfy_topics"] == ["aa", "bb"]

    def test_missing_file(self):
        with pytest.raises(SystemExit):
            ntfy_wx.load_config("/nonexistent/config.ini")

    def test_missing_option(self, tmpdir):
        p = self._write(tmpdir, """
[wecom]
cid = c

[ntfy]
topics = x
""")
        with pytest.raises(configparser.NoOptionError):
            ntfy_wx.load_config(p)

    def test_empty_topics(self, tmpdir):
        p = self._write(tmpdir, """
[wecom]
cid = c
aid = a
secret = s

[ntfy]
topics =
""")
        with pytest.raises(Exception, match="topics"):
            ntfy_wx.load_config(p)
