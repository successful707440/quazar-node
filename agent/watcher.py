#!/usr/bin/env python3
"""Quazar network watcher — polls nodes and sends Telegram alerts."""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

AGENT_DIR = Path(__file__).resolve().parent
DEFAULT_STATE_PATH = AGENT_DIR / "state.json"
DEFAULT_ENV_PATH = AGENT_DIR / ".env"

# @Quazar_Agent_Bot — verified via getMe
EXPECTED_BOT_ID = 8984617018
EXPECTED_BOT_USERNAME = "Quazar_Agent_Bot"
DEFAULT_SOCKS_GATEWAY = "socks5h://127.0.0.1:1080"

WATCH_EVENT_TYPES = {
    "CitizenAdded",
    "CitizenRemoved",
    "PassportIssued",
    "PassportRevoked",
    "PeerListUpdate",
    "AiyaElected",
    "AppointmentGuardian",
    "AppointmentJudge",
}


@dataclass
class NodeTarget:
    name: str
    url: str.rstrip("/")


@dataclass
class Config:
    telegram_bot_token: str
    telegram_chat_id: str
    nodes: list[NodeTarget]
    master_key: str
    node_secret: str
    block_alert_nodes: set[str]
    interval_secs: int = 30
    pending_alert_threshold: int = 10
    pending_stall_secs: int = 120
    state_path: Path = DEFAULT_STATE_PATH
    request_timeout_secs: int = 10


@dataclass
class NodeSnapshot:
    ok: bool
    node_id: str | None = None
    blocks: int | None = None
    pending: int | None = None
    is_block_producer: bool | None = None
    error: str | None = None


def load_dotenv(path: Path) -> None:
    if not path.is_file():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def parse_nodes(raw: str) -> list[NodeTarget]:
    raw = raw.strip()
    if not raw:
        raise ValueError("QUAZAR_WATCH_NODES is empty")

    if raw.startswith("["):
        items = json.loads(raw)
        nodes: list[NodeTarget] = []
        for item in items:
            if isinstance(item, str):
                if "=" in item:
                    name, url = item.split("=", 1)
                else:
                    name, url = item, item
                nodes.append(NodeTarget(name=name.strip(), url=url.strip().rstrip("/")))
            elif isinstance(item, dict):
                nodes.append(
                    NodeTarget(
                        name=str(item["name"]).strip(),
                        url=str(item["url"]).strip().rstrip("/"),
                    )
                )
            else:
                raise ValueError(f"unsupported node entry: {item!r}")
        return nodes

    nodes = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if "=" in part:
            name, url = part.split("=", 1)
        elif "@" in part:
            name, url = part.split("@", 1)
        else:
            name, url = part, part
        nodes.append(NodeTarget(name=name.strip(), url=url.strip().rstrip("/")))
    if not nodes:
        raise ValueError("QUAZAR_WATCH_NODES has no entries")
    return nodes


def parse_block_alert_nodes(raw: str, nodes: list[NodeTarget]) -> set[str]:
    raw = raw.strip()
    if raw:
        names = {part.strip() for part in raw.split(",") if part.strip()}
        unknown = names - {node.name for node in nodes}
        if unknown:
            raise ValueError(f"Unknown WATCHER_BLOCK_ALERT_NODES: {', '.join(sorted(unknown))}")
        return names
    return {nodes[0].name}


def load_config() -> Config:
    load_dotenv(DEFAULT_ENV_PATH)
    load_dotenv(AGENT_DIR.parent / ".env")

    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        raise ValueError(
            "Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in agent/.env "
            "(see agent/.env.example)"
        )

    nodes_raw = os.environ.get(
        "QUAZAR_WATCH_NODES",
        "local=http://127.0.0.1:8080",
    )
    master_key = os.environ.get("QUAZAR_MASTER_KEY", "").strip()
    node_secret = os.environ.get("QUAZAR_NODE_SECRET", "").strip()
    if not master_key or not node_secret:
        raise ValueError("Set QUAZAR_MASTER_KEY and QUAZAR_NODE_SECRET in agent/.env")

    nodes = parse_nodes(nodes_raw)
    block_alert_nodes = parse_block_alert_nodes(
        os.environ.get("WATCHER_BLOCK_ALERT_NODES", ""),
        nodes,
    )

    return Config(
        telegram_bot_token=token,
        telegram_chat_id=chat_id,
        nodes=nodes,
        master_key=master_key,
        node_secret=node_secret,
        block_alert_nodes=block_alert_nodes,
        interval_secs=int(os.environ.get("WATCHER_INTERVAL_SECS", "30")),
        pending_alert_threshold=int(
            os.environ.get("WATCHER_PENDING_ALERT_THRESHOLD", "10")
        ),
        pending_stall_secs=int(os.environ.get("WATCHER_PENDING_STALL_SECS", "120")),
        state_path=Path(os.environ.get("WATCHER_STATE_PATH", str(DEFAULT_STATE_PATH))),
        request_timeout_secs=int(os.environ.get("WATCHER_REQUEST_TIMEOUT_SECS", "10")),
    )


def http_json(
    url: str,
    *,
    bearer: str | None = None,
    timeout_secs: int = 10,
) -> dict[str, Any]:
    headers = {"Accept": "application/json"}
    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"

    request = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(request, timeout=timeout_secs) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("response is not a JSON object")
    if payload.get("status") != "success":
        raise ValueError(payload.get("error") or "API returned error status")
    return payload


def fetch_status(node: NodeTarget, cfg: Config) -> NodeSnapshot:
    try:
        payload = http_json(
            f"{node.url}/status",
            timeout_secs=cfg.request_timeout_secs,
        )
        data = payload.get("data") or {}
        return NodeSnapshot(
            ok=True,
            node_id=str(data.get("node_id") or node.name),
            blocks=int(data.get("blocks") or 0),
            pending=int(data.get("pending_events_local") or 0),
            is_block_producer=bool(data.get("is_block_producer")),
        )
    except Exception as exc:  # noqa: BLE001 — aggregate for alert text
        return NodeSnapshot(ok=False, error=str(exc))


def fetch_nodes(node: NodeTarget, cfg: Config) -> list[dict[str, Any]]:
    payload = http_json(
        f"{node.url}/nodes",
        bearer=cfg.master_key,
        timeout_secs=cfg.request_timeout_secs,
    )
    data = payload.get("data")
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    return []


def fetch_blocks(node: NodeTarget, cfg: Config) -> list[dict[str, Any]]:
    payload = http_json(
        f"{node.url}/blocks",
        bearer=cfg.node_secret,
        timeout_secs=cfg.request_timeout_secs,
    )
    data = payload.get("data")
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    return []


def load_state(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def detect_socks_gateway(host: str = "127.0.0.1", port: int = 1080) -> str | None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.settimeout(0.3)
        if sock.connect_ex((host, port)) == 0:
            return f"socks5h://{host}:{port}"
    finally:
        sock.close()
    return None


def telegram_proxy_url() -> str | None:
    for key in ("TELEGRAM_PROXY", "HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy"):
        value = os.environ.get(key, "").strip()
        if value:
            return value
    return detect_socks_gateway()


def proxy_uses_socks(proxy: str) -> bool:
    lowered = proxy.lower()
    return lowered.startswith("socks5://") or lowered.startswith("socks5h://")


def telegram_http(
    method: str,
    url: str,
    *,
    form: dict[str, str] | None = None,
    timeout_secs: int = 10,
) -> dict[str, Any]:
    proxy = telegram_proxy_url()
    use_curl = proxy is not None and (proxy_uses_socks(proxy) or proxy.startswith("http"))

    if use_curl:
        cmd = [
            "curl",
            "-sS",
            "--connect-timeout",
            str(timeout_secs),
            "--max-time",
            str(timeout_secs),
            "-X",
            method.upper(),
        ]
        if proxy:
            cmd.extend(["-x", proxy])
        if form is not None:
            cmd.extend(["-H", "Content-Type: application/x-www-form-urlencoded"])
            cmd.extend(["--data", urllib.parse.urlencode(form)])
        cmd.append(url)
        try:
            completed = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False,
            )
        except FileNotFoundError as exc:
            raise RuntimeError("curl не найден — нужен для SOCKS-шлюза Telegram") from exc
        if completed.returncode != 0:
            stderr = (completed.stderr or completed.stdout or "curl failed").strip()
            raise RuntimeError(stderr)
        try:
            payload = json.loads(completed.stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Telegram API returned invalid JSON: {completed.stdout[:200]}") from exc
    else:
        data = None
        headers: dict[str, str] = {}
        if form is not None:
            data = urllib.parse.urlencode(form).encode("utf-8")
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        request = urllib.request.Request(url, data=data, headers=headers, method=method.upper())
        try:
            with urllib.request.urlopen(request, timeout=timeout_secs) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.URLError as exc:
            reason = getattr(exc, "reason", exc)
            hint = (
                "Не удаётся подключиться к api.telegram.org. "
                f"Укажите шлюз: TELEGRAM_PROXY={DEFAULT_SOCKS_GATEWAY}"
            )
            raise RuntimeError(f"{reason}. {hint}") from exc

    if not isinstance(payload, dict):
        raise ValueError("Telegram API returned non-object JSON")
    if not payload.get("ok"):
        raise RuntimeError(payload.get("description") or "Telegram API error")
    return payload


def telegram_api(token: str, method: str, *, params: dict[str, str] | None = None, timeout_secs: int = 10) -> dict[str, Any]:
    query = f"?{urllib.parse.urlencode(params)}" if params else ""
    url = f"https://api.telegram.org/bot{token}/{method}{query}"
    return telegram_http("GET", url, timeout_secs=timeout_secs)


def verify_quazar_bot(token: str) -> dict[str, Any]:
    payload = telegram_api(token, "getMe")
    bot = payload.get("result") or {}
    bot_id = int(bot.get("id") or 0)
    username = str(bot.get("username") or "")
    if bot_id != EXPECTED_BOT_ID or username != EXPECTED_BOT_USERNAME:
        raise ValueError(
            f"Unexpected bot: id={bot_id} username=@{username}, "
            f"expected id={EXPECTED_BOT_ID} username=@{EXPECTED_BOT_USERNAME}"
        )
    return bot


def discover_chat_id(token: str) -> str:
    payload = telegram_api(token, "getUpdates")
    updates = payload.get("result") or []
    if not updates:
        raise ValueError(
            f"Напишите боту @{EXPECTED_BOT_USERNAME} (например /start) "
            "и повторите настройку"
        )

    chat_id: str | None = None
    for update in reversed(updates):
        message = update.get("message") or update.get("edited_message")
        if not isinstance(message, dict):
            continue
        chat = message.get("chat") or {}
        if not isinstance(chat, dict):
            continue
        candidate = chat.get("id")
        if candidate is not None:
            chat_id = str(candidate)
            break

    if not chat_id:
        raise ValueError("getUpdates не содержит chat.id — отправьте боту сообщение")
    return chat_id


def default_quazar_env() -> dict[str, str]:
    load_dotenv(AGENT_DIR.parent / ".env")
    return {
        "QUAZAR_MASTER_KEY": os.environ.get("QUAZAR_MASTER_KEY", "QUAZAR_MASTER_KEY_2026"),
        "QUAZAR_NODE_SECRET": os.environ.get("QUAZAR_NODE_SECRET", "QUAZAR_NODE_SECRET_2026"),
        "QUAZAR_WATCH_NODES": os.environ.get(
            "QUAZAR_WATCH_NODES",
            "local=http://127.0.0.1:8080",
        ),
        "WATCHER_BLOCK_ALERT_NODES": os.environ.get("WATCHER_BLOCK_ALERT_NODES", "local"),
        "WATCHER_INTERVAL_SECS": os.environ.get("WATCHER_INTERVAL_SECS", "30"),
        "WATCHER_PENDING_ALERT_THRESHOLD": os.environ.get("WATCHER_PENDING_ALERT_THRESHOLD", "10"),
        "WATCHER_PENDING_STALL_SECS": os.environ.get("WATCHER_PENDING_STALL_SECS", "120"),
    }


def write_agent_env(token: str, chat_id: str, bot: dict[str, Any]) -> None:
    env = default_quazar_env()
    proxy = telegram_proxy_url() or DEFAULT_SOCKS_GATEWAY
    lines = [
        f"# Bot: @{bot.get('username')} (bot_id={bot.get('id')})",
        f"TELEGRAM_BOT_TOKEN={token}",
        f"TELEGRAM_CHAT_ID={chat_id}",
        f"TELEGRAM_PROXY={proxy}",
        "",
        f"QUAZAR_MASTER_KEY={env['QUAZAR_MASTER_KEY']}",
        f"QUAZAR_NODE_SECRET={env['QUAZAR_NODE_SECRET']}",
        "",
        f"QUAZAR_WATCH_NODES={env['QUAZAR_WATCH_NODES']}",
        f"WATCHER_BLOCK_ALERT_NODES={env['WATCHER_BLOCK_ALERT_NODES']}",
        "",
        f"WATCHER_INTERVAL_SECS={env['WATCHER_INTERVAL_SECS']}",
        f"WATCHER_PENDING_ALERT_THRESHOLD={env['WATCHER_PENDING_ALERT_THRESHOLD']}",
        f"WATCHER_PENDING_STALL_SECS={env['WATCHER_PENDING_STALL_SECS']}",
        "",
    ]
    DEFAULT_ENV_PATH.write_text("\n".join(lines), encoding="utf-8")


def setup_telegram(token: str) -> None:
    token = token.strip()
    if not token:
        raise ValueError("TELEGRAM_BOT_TOKEN is empty")

    bot = verify_quazar_bot(token)
    chat_id = discover_chat_id(token)
    write_agent_env(token, chat_id, bot)

    cfg = load_config()
    send_telegram(
        cfg,
        "✅ Quazar watcher настроен\n"
        f"Бот: @{bot.get('username')}\n"
        f"Chat ID: {chat_id}\n"
        f"Узлы: {', '.join(n.name for n in cfg.nodes)}",
    )
    print(f"Configured agent/.env for @{bot.get('username')}, chat_id={chat_id}")


def send_telegram(cfg: Config, text: str, *, silent: bool = False) -> None:
    url = f"https://api.telegram.org/bot{cfg.telegram_bot_token}/sendMessage"
    telegram_http(
        "POST",
        url,
        form={
            "chat_id": cfg.telegram_chat_id,
            "text": text,
            "disable_web_page_preview": "true",
            "disable_notification": "true" if silent else "false",
        },
        timeout_secs=cfg.request_timeout_secs,
    )


def help_text() -> str:
    return (
        "🛰 Quazar Agent — мониторинг сети Квазар\n\n"
        "Автоматически сообщаю о:\n"
        "• новых блоках и событиях (CitizenAdded, PassportIssued…)\n"
        "• падении / восстановлении узлов\n"
        "• застрявшем pending\n"
        "• смене статуса пиров P2P\n\n"
        "Алерты о блоках — только с primary-узла (local).\n\n"
        "Команды:\n"
        "/status — состояние узлов сейчас\n"
        "/help — это сообщение"
    )


def build_status_report(cfg: Config) -> str:
    lines = ["📊 Статус сети Quazar"]
    for node in cfg.nodes:
        snapshot = fetch_status(node, cfg)
        if not snapshot.ok:
            lines.append(f"\n🔴 {node.name}: недоступен\n{snapshot.error}")
            continue
        producer = "да" if snapshot.is_block_producer else "нет"
        lines.append(
            f"\n🟢 {node.name} ({snapshot.node_id})\n"
            f"блоков: {snapshot.blocks}, pending: {snapshot.pending}\n"
            f"block producer: {producer}"
        )
    return "\n".join(lines)


def command_reply(cfg: Config, text: str) -> str | None:
    normalized = text.strip().lower()
    if normalized in {"/start", "/help", "help", "помощь"}:
        return help_text()
    if "что ты умеешь" in normalized or "что умеешь" in normalized:
        return help_text()
    if normalized in {"/status", "status", "статус"}:
        return build_status_report(cfg)
    return None


def poll_commands(cfg: Config, state: dict[str, Any]) -> list[str]:
    offset = int(state.get("telegram_update_offset") or 0)
    params = {"timeout": "0"}
    if offset:
        params["offset"] = str(offset)

    payload = telegram_api(cfg.telegram_bot_token, "getUpdates", params=params)
    updates = payload.get("result") or []
    replies: list[str] = []

    for update in updates:
        update_id = int(update.get("update_id") or 0)
        if update_id >= offset:
            state["telegram_update_offset"] = update_id + 1

        message = update.get("message") or update.get("edited_message")
        if not isinstance(message, dict):
            continue

        chat = message.get("chat") or {}
        chat_id = str(chat.get("id") or "")
        if chat_id != cfg.telegram_chat_id:
            continue

        text = message.get("text")
        if not isinstance(text, str) or not text.strip():
            continue

        reply = command_reply(cfg, text)
        if reply:
            replies.append(reply)

    return replies


def format_event_line(event: dict[str, Any]) -> str:
    event_type = event.get("event_type", "?")
    title = event.get("title") or ""
    initiator = event.get("initiator") or ""
    parts = [event_type]
    if title:
        parts.append(title)
    if initiator:
        parts.append(f"({initiator})")
    return " • ".join(parts)


def events_from_blocks_after(
    blocks: list[dict[str, Any]],
    after_block_number: int,
) -> list[tuple[int, dict[str, Any]]]:
    """Events from blocks with block_number > after_block_number."""
    found: list[tuple[int, dict[str, Any]]] = []
    for block in blocks:
        block_number = int(block.get("block_number") or 0)
        if block_number <= after_block_number:
            continue
        for event in block.get("events") or []:
            if isinstance(event, dict):
                found.append((block_number, event))
    found.sort(key=lambda item: (item[0], item[1].get("timestamp") or 0))
    return found


def event_id_of(event: dict[str, Any]) -> str:
    return str(event.get("event_id") or "")


def seen_event_ids(node_state: dict[str, Any]) -> set[str]:
    return set(node_state.get("seen_event_ids") or [])


def store_seen_event_ids(node_state: dict[str, Any], seen: set[str]) -> None:
    node_state["seen_event_ids"] = sorted(seen)[-10000:]


def seed_seen_events(
    node_state: dict[str, Any],
    blocks: list[dict[str, Any]],
    up_to_block_number: int,
) -> None:
    """Mark all events already in chain as seen — no Telegram alerts for history."""
    seen = seen_event_ids(node_state)
    for block in blocks:
        block_number = int(block.get("block_number") or 0)
        if block_number > up_to_block_number:
            continue
        for event in block.get("events") or []:
            if not isinstance(event, dict):
                continue
            eid = event_id_of(event)
            if eid:
                seen.add(eid)
    store_seen_event_ids(node_state, seen)


def filter_unseen_events(
    block_events: list[tuple[int, dict[str, Any]]],
    seen: set[str],
) -> list[tuple[int, dict[str, Any]]]:
    unseen: list[tuple[int, dict[str, Any]]] = []
    for block_number, event in block_events:
        eid = event_id_of(event)
        if not eid or eid in seen:
            continue
        seen.add(eid)
        unseen.append((block_number, event))
    return unseen


def format_new_block_messages(
    node_name: str,
    block_events: list[tuple[int, dict[str, Any]]],
) -> list[str]:
    if not block_events:
        return []

    by_block: dict[int, list[dict[str, Any]]] = {}
    for block_number, event in block_events:
        by_block.setdefault(block_number, []).append(event)

    messages: list[str] = []
    for block_number in sorted(by_block):
        events = by_block[block_number]
        interesting = [
            event
            for event in events
            if str(event.get("event_type")) in WATCH_EVENT_TYPES
        ]
        shown = interesting or events
        if not shown:
            continue
        lines = [f"📦 Новый блок на {node_name} (#{block_number})"]
        lines.extend(format_event_line(event) for event in shown[:8])
        if len(shown) > 8:
            lines.append(f"… и ещё {len(shown) - 8} событий")
        messages.append("\n".join(lines))
    return messages


def baseline_chain_state(
    node: NodeTarget,
    cfg: Config,
    node_state: dict[str, Any],
    block_number: int,
) -> None:
    blocks = fetch_blocks(node, cfg)
    seed_seen_events(node_state, blocks, block_number)
    node_state["last_block_number"] = block_number


def evaluate_node(
    node: NodeTarget,
    cfg: Config,
    state: dict[str, Any],
) -> list[str]:
    messages: list[str] = []
    node_state = state.setdefault(node.name, {})
    snapshot = fetch_status(node, cfg)

    if not snapshot.ok:
        if node_state.get("was_up", True):
            messages.append(
                f"🔴 Узел недоступен: {node.name}\n"
                f"URL: {node.url}\n"
                f"Ошибка: {snapshot.error}"
            )
        node_state["was_up"] = False
        return messages

    was_down = not node_state.get("was_up", True)
    if was_down:
        messages.append(
            f"🟢 Узел снова online: {node.name} ({snapshot.node_id})\n"
            f"Блоков: {snapshot.blocks}, pending: {snapshot.pending}"
        )
        if snapshot.blocks is not None and node.name in cfg.block_alert_nodes:
            try:
                baseline_chain_state(node, cfg, node_state, snapshot.blocks)
            except Exception as exc:  # noqa: BLE001
                node_state["last_block_number"] = snapshot.blocks
                print(f"[{now_iso()}] baseline error {node.name}: {exc}", file=sys.stderr)
    node_state["was_up"] = True

    prev_blocks = int(node_state.get("blocks") or 0)
    prev_pending = int(node_state.get("pending") or 0)
    pending_high_since = node_state.get("pending_high_since")

    track_blocks = node.name in cfg.block_alert_nodes
    if track_blocks and snapshot.blocks is not None and not was_down:
        if "last_block_number" not in node_state:
            try:
                baseline_chain_state(node, cfg, node_state, snapshot.blocks)
            except Exception as exc:  # noqa: BLE001
                node_state["last_block_number"] = snapshot.blocks
                print(f"[{now_iso()}] baseline error {node.name}: {exc}", file=sys.stderr)

        last_block_number = int(node_state.get("last_block_number", snapshot.blocks))
        if snapshot.blocks > last_block_number:
            try:
                blocks = fetch_blocks(node, cfg)
                block_events = events_from_blocks_after(blocks, last_block_number)
                seen = seen_event_ids(node_state)
                block_events = filter_unseen_events(block_events, seen)
                store_seen_event_ids(node_state, seen)
                messages.extend(format_new_block_messages(node.name, block_events))
            except Exception as exc:  # noqa: BLE001
                messages.append(
                    f"📦 Новый блок на {node.name} (#{snapshot.blocks}), "
                    f"но не удалось прочитать события: {exc}"
                )
            node_state["last_block_number"] = snapshot.blocks
    elif snapshot.blocks is not None and not track_blocks and "last_block_number" not in node_state:
        node_state["last_block_number"] = snapshot.blocks

    if (
        snapshot.pending is not None
        and snapshot.pending >= cfg.pending_alert_threshold
        and snapshot.blocks == prev_blocks
    ):
        if not pending_high_since:
            node_state["pending_high_since"] = time.time()
        elif time.time() - float(pending_high_since) >= cfg.pending_stall_secs:
            last_alert = float(node_state.get("last_pending_alert_at") or 0)
            if time.time() - last_alert >= cfg.pending_stall_secs:
                messages.append(
                    f"⚠️ Pending застрял на {node.name}\n"
                    f"pending={snapshot.pending}, блоков={snapshot.blocks}\n"
                    f"Порог: {cfg.pending_alert_threshold}, "
                    f"без новых блоков ≥ {cfg.pending_stall_secs}s"
                )
                node_state["last_pending_alert_at"] = time.time()
    else:
        node_state.pop("pending_high_since", None)

    if snapshot.pending is not None and abs(snapshot.pending - prev_pending) >= 5:
        if snapshot.pending > prev_pending:
            messages.append(
                f"📥 Рост pending на {node.name}: {prev_pending} → {snapshot.pending}"
            )

    node_state["blocks"] = snapshot.blocks
    node_state["pending"] = snapshot.pending
    node_state["node_id"] = snapshot.node_id
    node_state["is_block_producer"] = snapshot.is_block_producer

    try:
        peers = fetch_nodes(node, cfg)
        prev_peers: dict[str, str] = node_state.get("peers") or {}
        current_peers = {
            str(peer.get("id") or "?"): str(peer.get("status") or "?")
            for peer in peers
        }
        for peer_id, status in current_peers.items():
            old_status = prev_peers.get(peer_id)
            if old_status and old_status != status:
                messages.append(
                    f"🔗 Пир {peer_id} на {node.name}: {old_status} → {status}"
                )
            elif not old_status and status.lower() != "alive":
                messages.append(f"🔗 Пир {peer_id} на {node.name}: status={status}")
        node_state["peers"] = current_peers
    except Exception as exc:  # noqa: BLE001
        if node_state.get("was_up", True):
            messages.append(
                f"⚠️ Не удалось получить /nodes с {node.name}: {exc}"
            )

    return messages


def run_once(cfg: Config, *, startup: bool = False) -> None:
    state = load_state(cfg.state_path)
    outbound: list[str] = []

    if startup and not state.get("started"):
        nodes_list = ", ".join(f"{n.name} ({n.url})" for n in cfg.nodes)
        outbound.append(
            f"🛰 Quazar watcher запущен\n"
            f"Узлы: {nodes_list}\n"
            f"Интервал: {cfg.interval_secs}s"
        )
        state["started"] = True

    for node in cfg.nodes:
        outbound.extend(evaluate_node(node, cfg, state))

    try:
        outbound.extend(poll_commands(cfg, state))
    except Exception as exc:  # noqa: BLE001
        print(f"[{now_iso()}] command poll error: {exc}", file=sys.stderr)

    save_state(cfg.state_path, state)

    for message in outbound:
        send_telegram(cfg, message)
        print(f"[{now_iso()}] sent:\n{message}\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="Quazar network watcher")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single poll cycle and exit",
    )
    parser.add_argument(
        "--test-telegram",
        action="store_true",
        help="Send a test Telegram message and exit",
    )
    parser.add_argument(
        "--setup-telegram",
        action="store_true",
        help="Verify @Quazar_Agent_Bot, discover chat_id, write agent/.env",
    )
    args = parser.parse_args()

    if args.setup_telegram:
        load_dotenv(DEFAULT_ENV_PATH)
        load_dotenv(AGENT_DIR.parent / ".env")
        token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
        if not token:
            print(
                "Usage: TELEGRAM_BOT_TOKEN=... bash scripts/run_watcher.sh --setup-telegram",
                file=sys.stderr,
            )
            return 1
        try:
            setup_telegram(token)
        except Exception as exc:  # noqa: BLE001
            print(f"Setup error: {exc}", file=sys.stderr)
            return 1
        return 0

    try:
        cfg = load_config()
    except ValueError as exc:
        print(f"Config error: {exc}", file=sys.stderr)
        print(
            "Hint: TELEGRAM_BOT_TOKEN=... bash scripts/run_watcher.sh --setup-telegram",
            file=sys.stderr,
        )
        return 1

    if args.test_telegram:
        send_telegram(
            cfg,
            "✅ Quazar watcher: тестовое сообщение\n"
            f"Узлы: {', '.join(n.name for n in cfg.nodes)}",
        )
        print("Test message sent.")
        return 0

    if args.once:
        run_once(cfg, startup=True)
        return 0

    print(f"Watcher started, interval={cfg.interval_secs}s, nodes={len(cfg.nodes)}")
    run_once(cfg, startup=True)
    while True:
        time.sleep(cfg.interval_secs)
        try:
            run_once(cfg)
        except KeyboardInterrupt:
            print("\nStopped.")
            return 0
        except Exception as exc:  # noqa: BLE001
            print(f"[{now_iso()}] loop error: {exc}", file=sys.stderr)
            try:
                send_telegram(cfg, f"⚠️ Watcher error: {exc}", silent=True)
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
