#!/usr/bin/env python3
"""ntfy-wx: 转发 ntfy 消息到企业微信应用消息.

仅依赖 Python 标准库. 参考:
  https://github.com/easychen/wecomchan/tree/main/go-wecomchan

用法:
  ntfy-wx.py -c config.ini

转发进度存在工作目录下的 ntfy-wx.db (SQLite).

线程模型 (NtfyForwarder):
  _dump_loop    后台线程: 睡到下一个 23:00 (UTC+8), 发送当日汇总
  _reader_loop  前台: 阻塞读 ntfy 长连接, 按 id 去重后直接转发到企业微信

配置文件示例 (config.ini):
  [wecom]
  cid = 企业微信 corpid
  aid = 企业微信应用 agentid
  secret = 企业微信应用 secret
  api_base = ...           ; 可选, 企业微信 API 地址 (测试用)

  [ntfy]
  url = https://ntfy.sh    ; 可选, 默认 https://ntfy.sh
  token = ntfy 访问令牌
  topics = aa, bb          ; 逗号分隔, 也可写成多行缩进 (见 configparser multiline)
"""

import argparse
import configparser
import datetime
import json
import logging
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

WX_API_BASE = "https://qyapi.weixin.qq.com/cgi-bin"

log = logging.getLogger("ntfy-wx")


# ---------------------------------------------------------------------------
# 企业微信
# ---------------------------------------------------------------------------

class WeComSender:
    """企业微信应用消息发送. 线程安全: 多线程共用一个实例 (内部锁保护 token)."""

    def __init__(self, cid, aid, secret, base_url=WX_API_BASE, token_expiry_margin=300):
        """token_expiry_margin: 提前多少秒刷新 access_token (官方有效期 7200s)."""
        self.cid = cid
        self.aid = aid
        self.secret = secret
        self.base_url = base_url
        self.token_expiry_margin = token_expiry_margin
        self.token = None
        self.token_expire_at = 0
        self._lock = threading.Lock()

    def get_token(self, force_refresh=False):
        now = time.time()
        if not force_refresh and self.token and now < self.token_expire_at:
            return self.token
        url = "{}/gettoken?corpid={}&corpsecret={}".format(
            self.base_url,
            urllib.parse.quote(self.cid),
            urllib.parse.quote(self.secret),
        )
        req = urllib.request.Request(url, headers={"User-Agent": "ntfy-wx/1.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        if data.get("errcode") != 0:
            raise RuntimeError("获取 access_token 失败: {}".format(data))
        self.token = data["access_token"]
        # 官方有效期为 7200 秒, 留出余量
        self.token_expire_at = now + data.get("expires_in", 7200) - self.token_expiry_margin
        return self.token

    def send(self, content, msg_type="text"):
        """发送应用消息, 失败时刷新 token 重试一次 (处理 token 失效)."""
        payload = {
            "touser": "@all",
            "msgtype": msg_type,
            "agentid": self.aid,
        }
        if msg_type == "text":
            payload["text"] = {"content": content}
        elif msg_type == "markdown":
            payload["markdown"] = {"content": content}
        else:
            raise ValueError("不支持的 msg_type: {}".format(msg_type))

        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        with self._lock:  # 多线程 (转发/日报/自检) 并发发送时串行化, 保护 token 缓存
            for attempt in range(2):
                token = self.get_token(force_refresh=attempt > 0)
                url = "{}/message/send?access_token={}".format(self.base_url, token)
                req = urllib.request.Request(
                    url, data=body, headers={"Content-Type": "application/json"}
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
                errcode = data.get("errcode")
                # 40014: invalid access_token, 42001: token expired
                if errcode == 0:
                    return data
                if attempt == 0 and errcode in (40014, 42001):
                    continue
                raise RuntimeError("发送企业微信消息失败: {}".format(data))
            raise RuntimeError("发送企业微信消息失败: token 重试后仍失败")


# ---------------------------------------------------------------------------
# 持久化: 已转发消息记录
# ---------------------------------------------------------------------------

class Storage:
    """ntfy-wx.db: 消息处理记录 (url, topic, msg_id, time, dropped).

    既做重启回放的按 id 精确去重 (秒级时间戳无法区分同秒内的多条消息),
    又通过 max(time) 提供回放起点, dropped 列支撑日报统计.

    check_same_thread=False: 允许构造方线程建连接、工作线程使用
    (实际使用仍是单线程串行, 无并发写).
    """

    def __init__(self, path="ntfy-wx.db", retention_days=30):
        """retention_days: 记录保留天数 (只需覆盖回放窗口与日报统计)."""
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS forwarded ("
            " url TEXT NOT NULL, topic TEXT NOT NULL, msg_id TEXT NOT NULL,"
            " time INTEGER NOT NULL, dropped INTEGER NOT NULL DEFAULT 0,"
            " PRIMARY KEY (url, topic, msg_id))"
        )
        # 清理超出保留期的记录
        cutoff = int(time.time()) - retention_days * 86400
        with self._conn:
            self._conn.execute("DELETE FROM forwarded WHERE time < ?", (cutoff,))

    def is_forwarded(self, url, topic, msg_id):
        return self._conn.execute(
            "SELECT 1 FROM forwarded WHERE url = ? AND topic = ? AND msg_id = ? AND dropped = 0",
            (url, topic, msg_id),
        ).fetchone() is not None

    def since(self, url):
        """回放起点: 已成功转发的最大 time (全局, 跨 topic), 没有记录则 None."""
        row = self._conn.execute(
            "SELECT MAX(time) FROM forwarded WHERE url = ? AND dropped = 0",
            (url,),
        ).fetchone()
        return row[0]

    def mark(self, url, topic, msg_id, ts, dropped=False):
        """记录一条消息处理结果 (成功转发或丢弃), 已存在则不覆盖."""
        with self._conn:
            self._conn.execute(
                "INSERT OR IGNORE INTO forwarded (url, topic, msg_id, time, dropped)"
                " VALUES (?, ?, ?, ?, ?)",
                (url, topic, msg_id, int(ts), int(dropped)),
            )

    def stats_since(self, url, ts):
        """统计 ts 之后的处理条数, 返回 (forwarded, dropped)."""
        rows = self._conn.execute(
            "SELECT dropped, COUNT(*) FROM forwarded WHERE url = ? AND time >= ? GROUP BY dropped",
            (url, int(ts)),
        ).fetchall()
        counts = {bool(d): n for d, n in rows}
        return counts.get(False, 0), counts.get(True, 0)


# ---------------------------------------------------------------------------
# 转发器
# ---------------------------------------------------------------------------

class NtfyForwarder:
    """ntfy -> 企业微信转发器. run() 前台跑 _reader_loop, 后台跑 _dump_loop."""

    def __init__(self, cfg, sender, storage,
                 reconnect_interval=1, ntfy_error_retry_interval=60,
                 dump_time=datetime.time(23, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=8)))):
        """reconnect_interval / ntfy_error_retry_interval: 重连等待 (秒).
        dump_time: 日报发送时刻 (带时区的 datetime.time, 默认 23:00 UTC+8).
        """
        self._cfg = cfg
        self._sender = sender
        self._storage = storage
        self._reconnect_interval = reconnect_interval
        self._ntfy_error_retry_interval = ntfy_error_retry_interval
        self._dump_time = dump_time

    # -- ntfy 流 ------------------------------------------------------------

    @staticmethod
    def _parse_stream_line(line):
        """解析流的一行为消息 dict; 空行/事件行返回 None, 无法解析抛 JSONDecodeError."""
        if isinstance(line, bytes):
            line = line.decode("utf-8", errors="replace")
        line = line.strip()
        # SSE 格式的行带 data: 前缀, 一并兼容
        if line.startswith("data:"):
            line = line[5:].strip()
        if not line or line.startswith(":"):  # 空行或 SSE 注释
            return None
        m = json.loads(line)
        if m.get("event") in ("open", "keepalive"):
            return None
        return m if m.get("message") else None

    @staticmethod
    def _stream_messages(ntfy_url, token, topics, since):
        """长连接订阅多个 topic (逗号合并), 生成器逐条 yield 新消息.

        阻塞读直到连接断开 (由调用方重连), keepalive 等事件行被忽略.
        since 为回放起点 (None 表示不回放).
        """
        query = {}
        if since is not None:
            query["since"] = since
        log.debug("订阅 %s 上的 %d 个 topic: %s, since=%s",
                  ntfy_url, len(topics), ",".join(topics), since)
        path = ",".join(urllib.parse.quote(t, safe="") for t in topics)
        url = "{}/{}/json?{}".format(ntfy_url.rstrip("/"), path, urllib.parse.urlencode(query))
        req = urllib.request.Request(url, headers={"User-Agent": "ntfy-wx/1.0"})
        if token:
            req.add_header("Authorization", "Bearer {}".format(token))
        # timeout 只用于连接建立; 连上后清除读超时, 无限阻塞等推送
        with urllib.request.urlopen(req, timeout=30) as resp:
            try:
                resp.fp.raw._sock.settimeout(None)
            except AttributeError:
                pass  # 非 socket 底层, 断线由 readline 抛错兜底
            for line in resp:
                m = NtfyForwarder._parse_stream_line(line)
                if m:
                    yield m

    @staticmethod
    def _format_message(m, topic):
        title = m.get("title") or ""
        body = m.get("message", "")
        text = ("[{}] {}\n{}".format(topic, title, body)).strip() if title else "[{}] {}".format(topic, body)
        encoded = text.encode("utf-8")
        if len(encoded) > 2000:
            # 截到 1997 字节 + 省略号 (3 字节), 总长不超过企业微信 2048 字节限制
            text = encoded[:1997].decode("utf-8", errors="ignore") + "…"
        return text

    # -- 读循环 (前台) -------------------------------------------------------

    def _reader_loop(self):
        """循环订阅 ntfy 流, 去重后转发. ntfy 拒绝访问的通知只发一次."""
        notified = False
        while True:
            try:
                # 回放起点: 已成功转发的最大 time (全局). 落后 topic 在此之后的
                # 新消息照常推送; 无任何缓存时用 "latest".
                since = self._storage.since(self._cfg["ntfy_url"])
                since = str(since) if since is not None else "latest"

                log.info("已连接, 开始监听 %d 个 topic", len(self._cfg["ntfy_topics"]))
                for m in self._stream_messages(self._cfg["ntfy_url"], self._cfg["ntfy_token"],
                                               self._cfg["ntfy_topics"], since):
                    notified = False  # 流真正建立, 重置故障通知标记
                    self._handle_message(m)
            except Exception as e:
                log.exception("ntfy 连接断开或读取出错")
                if isinstance(e, urllib.error.HTTPError) and not notified:
                    # ntfy 拒绝访问 (如 token 无效的 401): 通知 (仅首次), 稍后重试
                    notified = True
                    self._sender.send(
                        "ntfy-wx: ntfy 连接失败 (HTTP {}), 1 分钟后重试".format(e.code), "text")
                time.sleep(self._ntfy_error_retry_interval if isinstance(e, urllib.error.HTTPError)
                           else self._reconnect_interval)

    def _handle_message(self, m):
        """去重后转发一条消息并落进度; 发送失败记为 dropped, 回放可重试."""
        topic, msg_id = m.get("topic"), m.get("id")
        if topic not in self._cfg["ntfy_topics"]:
            return
        # 按 id 精确去重: 重启/重连回放会重复带出已转发的消息
        if msg_id and self._storage.is_forwarded(self._cfg["ntfy_url"], topic, msg_id):
            log.debug("跳过已转发: topic=%s id=%s", topic, msg_id)
            return
        try:
            self._sender.send(self._format_message(m, topic), "text")
            dropped = False
        except Exception:
            log.exception("转发到企业微信失败, 跳过该消息: topic=%s id=%s", topic, msg_id)
            dropped = True
        if msg_id:
            self._storage.mark(self._cfg["ntfy_url"], topic, msg_id, m.get("time", 0), dropped)

    # -- 日报线程 ------------------------------------------------------------

    def _dump_loop(self):
        """睡到下一个 dump_time, 发送当日 (自 0 点起) 的转发统计."""
        tz = self._dump_time.tzinfo
        while True:
            now = datetime.datetime.now(tz)
            report_at = datetime.datetime.combine(now.date(), self._dump_time)
            if report_at <= now:
                report_at += datetime.timedelta(days=1)
            time.sleep((report_at - now).total_seconds())
            # 汇总的是 report_at 当天 (0 点 ~ dump_time) 的记录
            day_start = report_at.replace(hour=0, minute=0, second=0, microsecond=0)
            day_start_ts = int(day_start.timestamp())
            forwarded, dropped = self._storage.stats_since(self._cfg["ntfy_url"], day_start_ts)
            try:
                self._sender.send("ntfy-wx 日报 {}: 转发 {} 条消息, 失败 {} 条"
                                  .format(report_at.date(), forwarded, dropped), "text")
            except Exception:
                log.exception("日报发送失败")

    # -- 启动 ---------------------------------------------------------------

    def run(self):
        """后台启动日报线程, 前台阻塞跑读循环 (无限)."""
        threading.Thread(target=self._dump_loop, daemon=True).start()
        self._reader_loop()


# ---------------------------------------------------------------------------
# 配置与入口
# ---------------------------------------------------------------------------

def load_config(path):
    """解析 config.ini, 返回带默认值和校验的配置 dict."""
    cp = configparser.ConfigParser()
    if not cp.read(path, encoding="utf-8"):
        raise SystemExit("无法读取配置文件: {}".format(path))
    cfg = {
        "wx_cid": cp.get("wecom", "cid"),
        "wx_aid": cp.get("wecom", "aid"),
        "wx_secret": cp.get("wecom", "secret"),
        # 可选: 企业微信 API 地址 (测试指向 mock server 用)
        "wx_api_base": cp.get("wecom", "api_base", fallback=WX_API_BASE),
        "ntfy_url": cp.get("ntfy", "url", fallback="https://ntfy.sh"),
        "ntfy_token": cp.get("ntfy", "token", fallback=""),
        # 逗号分隔或多行缩进写法都支持
        "ntfy_topics": [t.strip() for t in cp.get("ntfy", "topics").replace(",", "\n").splitlines() if t.strip()],
    }
    if not cfg["ntfy_topics"]:
        raise Exception("配置项 ntfy.topics 不能为空")
    return cfg


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(message)s (%(filename)s:%(lineno)d)",
    )
    parser = argparse.ArgumentParser(description="转发 ntfy 消息到企业微信应用")
    parser.add_argument("-c", "--config", default="config.ini", metavar="FILE",
                        help="配置文件路径 (默认 ./config.ini)")
    args = parser.parse_args()

    cfg = load_config(args.config)
    sender = WeComSender(cfg["wx_cid"], cfg["wx_aid"], cfg["wx_secret"],
                         base_url=cfg.get("wx_api_base", WX_API_BASE))

    # 自检: 启动时立刻发送一条; 失败说明凭据/网络有问题, 直接退出
    sender.send("ntfy-wx 已启动, 监听 topic: {}".format(", ".join(cfg["ntfy_topics"])), "text")

    NtfyForwarder(cfg, sender, Storage()).run()
