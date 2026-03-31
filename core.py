import sqlite3
import json
import os
import ssl
import base64
import ipaddress
import socket
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib import parse as urlparse
from urllib import error as urlerror
from urllib import request as urlrequest

import routeros_api


class CoreError(Exception):
    pass


ROUTER_BILLING_DAY = 25
ROUTER_BASE_FEE_PER_USER = 500
ROUTER_REMINDER_DAY = max(1, ROUTER_BILLING_DAY - 3)
ADMIN_WA_NUMBER = os.environ.get("ADMIN_WA_NUMBER", "").strip()
WA_GATEWAY_URL = os.environ.get("WA_GATEWAY_URL", "").strip()
WA_HTTP_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
ROS7_REST_BASE_URL = os.environ.get("ROS7_REST_BASE_URL", "https://server.kipli.net/rest")
ROS7_REST_USERNAME = os.environ.get("ROS7_REST_USERNAME", "").strip()
ROS7_REST_PASSWORD = os.environ.get("ROS7_REST_PASSWORD", "")
ROS7_REST_TIMEOUT_SECONDS = int(os.environ.get("ROS7_REST_TIMEOUT_SECONDS", "20"))
ROS7_REST_VERIFY_SSL = os.environ.get("ROS7_REST_VERIFY_SSL", "1") == "1"
ROS7_L2TP_SECRET_PROFILE = os.environ.get("ROS7_L2TP_SECRET_PROFILE", "default")
ROS7_L2TP_DEFAULT_PASSWORD = os.environ.get("ROS7_L2TP_DEFAULT_PASSWORD", "").strip()
ROS7_L2TP_CONNECT_TO = os.environ.get("ROS7_L2TP_CONNECT_TO", "server.kipli.net")
ROS7_L2TP_LOCAL_ADDRESS = os.environ.get("ROS7_L2TP_LOCAL_ADDRESS", "10.168.0.1")
ROS7_L2TP_REMOTE_NETWORK = os.environ.get("ROS7_L2TP_REMOTE_NETWORK", "10.168.0.0/24")
ROS7_L2TP_USERNAME_PREFIX = os.environ.get("ROS7_L2TP_USERNAME_PREFIX", "billing")
ROS7_WEBFIG_PROXY_HOST = os.environ.get("WEBFIG_PROXY_HOST", "server.kipli.net")
ROS7_WEBFIG_PORT_PREFIX = int(os.environ.get("WEBFIG_PROXY_PREFIX", "6000"))
ROS7_WEBFIG_TO_PORT = int(os.environ.get("ROS7_WEBFIG_TO_PORT", "80"))
ROS7_WEBFIG_DST_ADDRESS = os.environ.get("ROS7_WEBFIG_DST_ADDRESS", "").strip()
ROS7_WINBOX_PROXY_HOST = os.environ.get("WINBOX_PROXY_HOST", ROS7_WEBFIG_PROXY_HOST)
ROS7_WINBOX_PORT_PREFIX = int(os.environ.get("WINBOX_PROXY_PREFIX", "7000"))
ROS7_WINBOX_TO_PORT = int(os.environ.get("ROS7_WINBOX_TO_PORT", "8291"))


@dataclass
class RouterConfig:
    id: int
    label: str
    address: str
    ip: str
    port: int
    username: str
    password: str
    wa_number: str
    created_at: str = ""
    l2tp_username: str = ""
    l2tp_password: str = ""
    l2tp_secret_id: str = ""


class MikroTikCore:
    def __init__(self, db_path: str = "pppoe_manager.db") -> None:
        self.db_path = Path(db_path)
        self._init_db()

    @contextmanager
    def _db(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self._db() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS routers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    label TEXT NOT NULL,
                    address TEXT NOT NULL DEFAULT '',
                    ip TEXT NOT NULL,
                    port INTEGER NOT NULL DEFAULT 8728,
                    username TEXT NOT NULL,
                    password TEXT NOT NULL,
                    wa_number TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            self._ensure_router_billing_columns(conn)
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS payment_state (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    router_id INTEGER NOT NULL,
                    secret_name TEXT NOT NULL,
                    monthly_fee REAL NOT NULL DEFAULT 0,
                    paid_until_month TEXT,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(router_id, secret_name)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS payment_audit (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    router_id INTEGER NOT NULL,
                    router_label TEXT NOT NULL,
                    router_ip TEXT NOT NULL,
                    secret_name TEXT NOT NULL,
                    action TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS app_state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            self._cleanup_legacy_tables(conn)

    def _cleanup_legacy_tables(self, conn: sqlite3.Connection) -> None:
        # Cleanup skema lama yang sudah tidak dipakai.
        conn.execute("DROP TABLE IF EXISTS registration_inbox")

    def _ensure_router_billing_columns(self, conn: sqlite3.Connection) -> None:
        columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(routers)").fetchall()
        }
        if "address" not in columns:
            conn.execute("ALTER TABLE routers ADD COLUMN address TEXT NOT NULL DEFAULT ''")
        if "monthly_fee" not in columns:
            conn.execute("ALTER TABLE routers ADD COLUMN monthly_fee INTEGER NOT NULL DEFAULT 0")
        if "paid_until_month" not in columns:
            conn.execute("ALTER TABLE routers ADD COLUMN paid_until_month TEXT")
        if "billing_updated_at" not in columns:
            conn.execute("ALTER TABLE routers ADD COLUMN billing_updated_at TEXT")
        if "wa_number" not in columns:
            conn.execute("ALTER TABLE routers ADD COLUMN wa_number TEXT NOT NULL DEFAULT ''")
        if "l2tp_username" not in columns:
            conn.execute("ALTER TABLE routers ADD COLUMN l2tp_username TEXT NOT NULL DEFAULT ''")
        if "l2tp_password" not in columns:
            conn.execute("ALTER TABLE routers ADD COLUMN l2tp_password TEXT NOT NULL DEFAULT ''")
        if "l2tp_secret_id" not in columns:
            conn.execute("ALTER TABLE routers ADD COLUMN l2tp_secret_id TEXT NOT NULL DEFAULT ''")
        if "auto_close_unpaid_end_month" not in columns:
            conn.execute("ALTER TABLE routers ADD COLUMN auto_close_unpaid_end_month INTEGER NOT NULL DEFAULT 0")
        if "auto_close_last_run_month" not in columns:
            conn.execute("ALTER TABLE routers ADD COLUMN auto_close_last_run_month TEXT")

        # Backfill data lama: jika dulu akun L2TP disimpan di kolom username/password,
        # pindahkan ke kolom khusus l2tp_* supaya tidak bentrok dengan kredensial API router.
        prefix = self._normalize_l2tp_prefix()
        if prefix:
            like_prefix = f"{prefix}_%"
            conn.execute(
                """
                UPDATE routers
                SET l2tp_username = CASE WHEN l2tp_username = '' THEN username ELSE l2tp_username END,
                    l2tp_password = CASE WHEN l2tp_password = '' THEN password ELSE l2tp_password END
                WHERE username LIKE ?
                """,
                (like_prefix,),
            )

    def list_routers(self) -> List[Dict[str, Any]]:
        self.run_hourly_router_billing_check()
        with self._db() as conn:
            rows = conn.execute(
                """
                SELECT
                    id, label, address, ip, port, username, wa_number, monthly_fee, paid_until_month, billing_updated_at,
                    CASE
                        WHEN TRIM(COALESCE(username, '')) <> '' AND TRIM(COALESCE(password, '')) <> '' THEN 1
                        ELSE 0
                    END AS has_router_auth
                FROM routers
                ORDER BY label, ip
                """
            ).fetchall()
            user_counts = {
                int(row["router_id"]): int(row["total"])
                for row in conn.execute(
                    """
                    SELECT router_id, COUNT(*) AS total
                    FROM payment_state
                    GROUP BY router_id
                    """
                ).fetchall()
            }
        now = datetime.now()
        result: List[Dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item.update(
                self._build_router_billing_summary_from_row(
                    item,
                    now,
                    user_count=user_counts.get(int(item["id"]), 0),
                )
            )
            result.append(item)
        return result

    def add_router(
        self,
        label: str,
        ip: str,
        port: int,
        username: str,
        password: str,
        wa_number: str = "",
        address: str = "",
        l2tp_username: str = "",
        l2tp_password: str = "",
        l2tp_secret_id: str = "",
        run_initial_billing_sync: bool = True,
    ) -> int:
        router_id = 0
        with self._db() as conn:
            cur = conn.execute(
                """
                INSERT INTO routers (
                    label, address, ip, port, username, password, wa_number,
                    l2tp_username, l2tp_password, l2tp_secret_id,
                    paid_until_month, billing_updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    label.strip(),
                    (address or "").strip(),
                    ip.strip(),
                    int(port),
                    username.strip(),
                    password,
                    self._normalize_wa_number(wa_number),
                    str(l2tp_username or "").strip(),
                    str(l2tp_password or ""),
                    str(l2tp_secret_id or "").strip(),
                    None,
                ),
            )
            router_id = int(cur.lastrowid)

        # Hitung cepat saat router baru ditambahkan (opsional):
        # sync payment_state dari active+secret, lalu update monthly_fee.
        # Jika router belum bisa diakses saat ini, penambahan router tetap sukses.
        if run_initial_billing_sync:
            try:
                with self._db() as conn:
                    self._refresh_router_monthly_fee(
                        conn,
                        router_id,
                        now=datetime.now(),
                        force=True,
                    )
            except CoreError:
                pass

        return router_id

    def update_router(
        self,
        router_id: int,
        label: str,
        ip: str,
        port: int,
        username: str,
        password: str,
        wa_number: str = "",
        address: str = "",
    ) -> None:
        with self._db() as conn:
            conn.execute(
                """
                UPDATE routers
                SET label = ?, address = ?, ip = ?, port = ?, username = ?, password = ?, wa_number = ?
                WHERE id = ?
                """,
                (
                    label.strip(),
                    (address or "").strip(),
                    ip.strip(),
                    int(port),
                    username.strip(),
                    password,
                    self._normalize_wa_number(wa_number),
                    int(router_id),
                ),
            )

    def delete_router(self, router_id: int, sync_l2tp_secret: bool = True) -> Dict[str, Any]:
        router_id_int = int(router_id)
        with self._db() as conn:
            row = conn.execute(
                """
                SELECT id, label, ip, username, l2tp_username, l2tp_secret_id
                FROM routers
                WHERE id = ?
                """,
                (router_id_int,),
            ).fetchone()
        if not row:
            raise CoreError("Router tidak ditemukan di database")

        router_label = str(row["label"] or "")
        router_ip = str(row["ip"] or "").strip()
        l2tp_secret_id = str(row["l2tp_secret_id"] or "").strip()
        l2tp_username = str(row["l2tp_username"] or "").strip()
        if not l2tp_username:
            # Fallback untuk data lama: mungkin dulu akun L2TP disimpan di kolom username.
            fallback_username = str(row["username"] or "").strip()
            if fallback_username.lower().startswith(f"{self._normalize_l2tp_prefix()}_"):
                l2tp_username = fallback_username

        remote_deleted = False
        nat_webfig_deleted = 0
        nat_winbox_deleted = 0
        if sync_l2tp_secret and (l2tp_secret_id or l2tp_username):
            remote_deleted = self._delete_l2tp_secret_safe(
                secret_id=l2tp_secret_id,
                secret_name=l2tp_username,
            )
        if sync_l2tp_secret and router_ip:
            nat_webfig_deleted = int(self.delete_ros7_webfig_dstnat_by_router_ip(router_ip) or 0)
            nat_winbox_deleted = int(self.delete_ros7_winbox_dstnat_by_router_ip(router_ip) or 0)

        with self._db() as conn:
            conn.execute("DELETE FROM payment_audit WHERE router_id = ?", (router_id_int,))
            conn.execute("DELETE FROM payment_state WHERE router_id = ?", (router_id_int,))
            conn.execute("DELETE FROM routers WHERE id = ?", (router_id_int,))
        return {
            "router_id": router_id_int,
            "router_label": router_label,
            "sync_attempted": bool(sync_l2tp_secret and ((l2tp_secret_id or l2tp_username) or router_ip)),
            "l2tp_deleted": bool(remote_deleted),
            "nat_webfig_deleted": nat_webfig_deleted,
            "nat_winbox_deleted": nat_winbox_deleted,
            "nat_deleted": int(nat_webfig_deleted + nat_winbox_deleted),
        }

    def get_router(self, router_id: int) -> RouterConfig:
        with self._db() as conn:
            row = conn.execute("SELECT * FROM routers WHERE id = ?", (router_id,)).fetchone()

        if not row:
            raise CoreError("Router tidak ditemukan di database")

        return RouterConfig(
            id=int(row["id"]),
            label=str(row["label"]),
            address=str(row["address"] or ""),
            ip=str(row["ip"]),
            port=int(row["port"]),
            username=str(row["username"]),
            password=str(row["password"]),
            wa_number=str(row["wa_number"] or ""),
            created_at=str(row["created_at"] or ""),
            l2tp_username=str(row["l2tp_username"] or ""),
            l2tp_password=str(row["l2tp_password"] or ""),
            l2tp_secret_id=str(row["l2tp_secret_id"] or ""),
        )

    def router_exists(self, ip: str, port: int) -> bool:
        with self._db() as conn:
            row = conn.execute(
                "SELECT 1 FROM routers WHERE ip = ? AND port = ? LIMIT 1",
                (str(ip).strip(), int(port)),
            ).fetchone()
        return bool(row)

    def is_router_online(self, ip: str, port: int = 8728, timeout_seconds: float = 1.2) -> bool:
        host = str(ip or "").strip()
        if not host:
            return False
        try:
            with socket.create_connection((host, int(port)), timeout=float(timeout_seconds)):
                return True
        except Exception:
            return False

    @contextmanager
    def _api_with_credentials(self, host: str, port: int, username: str, password: str):
        pool = None
        try:
            pool = routeros_api.RouterOsApiPool(
                host=str(host),
                username=str(username),
                password=password,
                port=int(port),
                plaintext_login=True,
                use_ssl=False,
            )
            api = pool.get_api()
            yield api
        except Exception as exc:
            raise CoreError(f"Gagal konek ke {host}:{port} - {exc}") from exc
        finally:
            if pool is not None:
                pool.disconnect()

    @contextmanager
    def _api(self, router: RouterConfig):
        with self._api_with_credentials(
            host=router.ip,
            port=router.port,
            username=router.username,
            password=router.password,
        ) as api:
            yield api

    def test_router_connection(self, router_id: int) -> Dict[str, Any]:
        router = self.get_router(router_id)
        with self._api(router) as api:
            resource = api.get_resource("/system/identity")
            identity = resource.get()
            return identity[0] if identity else {"name": router.label}

    def login_owner_and_sync_router_credentials(
        self,
        router_id: int,
        username: str,
        password: str,
    ) -> Dict[str, Any]:
        router = self.get_router(router_id)
        owner_username = str(username or "").strip()
        owner_password = password or ""
        if not owner_username or not owner_password:
            raise CoreError("Username dan password router wajib diisi.")

        with self._api_with_credentials(
            host=router.ip,
            port=router.port,
            username=owner_username,
            password=owner_password,
        ) as api:
            identity_rows = api.get_resource("/system/identity").get()

        with self._db() as conn:
            conn.execute(
                """
                UPDATE routers
                SET username = ?, password = ?
                WHERE id = ?
                """,
                (owner_username, owner_password, int(router_id)),
            )

        # Setelah owner login berhasil, langsung sync user active+secret untuk update payment_state
        # dan monthly_fee router. Ini menggantikan mekanisme snapshot H-5.
        try:
            with self._db() as conn:
                self._refresh_router_monthly_fee(
                    conn,
                    int(router_id),
                    now=datetime.now(),
                    force=True,
                )
        except CoreError:
            # Login tetap dianggap sukses meskipun sync billing sesaat gagal.
            pass

        identity = identity_rows[0] if identity_rows else {"name": router.label}
        return {
            "router_id": int(router_id),
            "router_label": router.label,
            "identity": identity,
        }

    def get_router_l2tp_status(self, router_id: int) -> Dict[str, Any]:
        router = self.get_router(router_id)
        l2tp_username = str(router.l2tp_username or "").strip()
        l2tp_password = str(router.l2tp_password or "").strip() or str(ROS7_L2TP_DEFAULT_PASSWORD or "").strip()
        remote_address = str(router.ip or "").strip()
        local_address = str(ROS7_L2TP_LOCAL_ADDRESS or "10.168.0.1").strip()
        connect_to = str(ROS7_L2TP_CONNECT_TO or "server.kipli.net").strip()

        result: Dict[str, Any] = {
            "router_id": int(router.id),
            "router_label": router.label,
            "is_online": False,
            "connect_to": connect_to,
            "username": l2tp_username,
            "password": l2tp_password,
            "local_address": local_address,
            "remote_address": remote_address,
            "active_uptime": "",
            "active_address": "",
            "check_error": "",
        }

        if not l2tp_username:
            result["check_error"] = "Username L2TP belum tersedia."
            return result

        try:
            data = self._ros7_rest_request(
                "GET",
                "/ppp/active",
                query={"service": "l2tp", ".proplist": "name,address,uptime,service,caller-id"},
            )
        except CoreError as exc:
            result["check_error"] = str(exc)
            return result

        rows: List[Dict[str, Any]] = []
        if isinstance(data, dict):
            rows = [data]
        elif isinstance(data, list):
            rows = [item for item in data if isinstance(item, dict)]

        online_row: Optional[Dict[str, Any]] = None
        for item in rows:
            name = str(item.get("name") or "").strip()
            address = str(item.get("address") or "").strip()
            if name == l2tp_username:
                online_row = item
                break
            if remote_address and address == remote_address:
                online_row = item
                break

        if not online_row:
            return result

        result["is_online"] = True
        result["active_uptime"] = str(online_row.get("uptime") or "").strip()
        result["active_address"] = str(online_row.get("address") or "").strip()
        return result

    def list_profiles(self, router_id: int) -> List[Dict[str, Any]]:
        router = self.get_router(router_id)
        with self._api(router) as api:
            rows = api.get_resource("/ppp/profile").get()
        rows.sort(key=lambda x: x.get("name", ""))
        return rows

    def list_active(self, router_id: int) -> List[Dict[str, Any]]:
        router = self.get_router(router_id)
        with self._api(router) as api:
            rows = api.get_resource("/ppp/active").get()
        rows.sort(key=lambda x: x.get("name", ""))
        return rows

    def list_secrets(self, router_id: int) -> List[Dict[str, Any]]:
        router = self.get_router(router_id)
        with self._api(router) as api:
            rows = api.get_resource("/ppp/secret").get()
        rows.sort(key=lambda x: x.get("name", ""))
        return rows

    def _month_index(self, year: int, month: int) -> int:
        return year * 12 + (month - 1)

    def _month_index_from_str(self, month_text: Optional[str]) -> Optional[int]:
        if not month_text:
            return None
        try:
            year_text, month_num_text = month_text.split("-", 1)
            return self._month_index(int(year_text), int(month_num_text))
        except Exception:
            return None

    def _month_str_from_index(self, month_idx: int) -> str:
        year = month_idx // 12
        month = (month_idx % 12) + 1
        return f"{year:04d}-{month:02d}"

    def _month_label(self, month_idx: int) -> str:
        month_names = [
            "Jan",
            "Feb",
            "Mar",
            "Apr",
            "Mei",
            "Jun",
            "Jul",
            "Agu",
            "Sep",
            "Okt",
            "Nov",
            "Des",
        ]
        year = month_idx // 12
        month = (month_idx % 12) + 1
        return f"{month_names[month - 1]} {year}"

    def _current_month_idx(self) -> int:
        now = datetime.now()
        return self._month_index(now.year, now.month)

    def _router_payment_start_idx(self, router: RouterConfig, now: Optional[datetime] = None) -> int:
        current_dt = now or datetime.now()
        created_dt = self._parse_db_datetime(router.created_at)
        if created_dt is None:
            created_dt = current_dt
        return self._month_index(created_dt.year, created_dt.month)

    def _router_cycle_idx(self, dt: datetime) -> int:
        month_idx = self._month_index(dt.year, dt.month)
        return month_idx if dt.day >= ROUTER_BILLING_DAY else (month_idx - 1)

    def _parse_db_datetime(self, raw_text: Optional[str]) -> Optional[datetime]:
        text = (raw_text or "").strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return None

    def _count_router_payment_users(self, conn: sqlite3.Connection, router_id: int) -> int:
        row = conn.execute(
            "SELECT COUNT(*) AS total FROM payment_state WHERE router_id = ?",
            (int(router_id),),
        ).fetchone()
        return int(row["total"] or 0) if row else 0

    def _get_app_state(self, conn: sqlite3.Connection, key: str) -> Optional[str]:
        row = conn.execute(
            "SELECT value FROM app_state WHERE key = ?",
            (key,),
        ).fetchone()
        if not row:
            return None
        return str(row["value"])

    def _set_app_state(self, conn: sqlite3.Connection, key: str, value: str) -> None:
        conn.execute(
            """
            INSERT INTO app_state (key, value, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = CURRENT_TIMESTAMP
            """,
            (key, value),
        )

    def get_payment_preferences(self, router_id: int) -> Dict[str, Any]:
        with self._db() as conn:
            row = conn.execute(
                """
                SELECT auto_close_unpaid_end_month
                FROM routers
                WHERE id = ?
                """,
                (int(router_id),),
            ).fetchone()
        if not row:
            raise CoreError("Router tidak ditemukan di database")
        return {
            "auto_close_unpaid_end_month": bool(int(row["auto_close_unpaid_end_month"] or 0)),
        }

    def set_payment_preferences(
        self,
        router_id: int,
        *,
        auto_close_unpaid_end_month: bool,
    ) -> Dict[str, Any]:
        with self._db() as conn:
            row = conn.execute(
                "SELECT id FROM routers WHERE id = ?",
                (int(router_id),),
            ).fetchone()
            if not row:
                raise CoreError("Router tidak ditemukan di database")

            next_value = 1 if bool(auto_close_unpaid_end_month) else 0
            conn.execute(
                """
                UPDATE routers
                SET auto_close_unpaid_end_month = ?,
                    auto_close_last_run_month = CASE
                        WHEN ? = 0 THEN NULL
                        ELSE auto_close_last_run_month
                    END
                WHERE id = ?
                """,
                (next_value, next_value, int(router_id)),
            )

        return {
            "auto_close_unpaid_end_month": bool(next_value),
        }

    def _is_month_end_cutoff(self, dt: datetime) -> bool:
        if int(dt.hour) < 18:
            return False
        next_day = dt + timedelta(days=1)
        return next_day.month != dt.month

    def _run_auto_close_unpaid_end_month(
        self,
        conn: sqlite3.Connection,
        current_dt: datetime,
    ) -> Dict[str, Any]:
        current_idx = self._month_index(current_dt.year, current_dt.month)
        current_month = self._month_str_from_index(current_idx)
        result: Dict[str, Any] = {
            "ran": False,
            "reason": "",
            "updated_routers": 0,
            "updated_users": 0,
            "month": current_month,
        }

        if not self._is_month_end_cutoff(current_dt):
            result["reason"] = "skip_not_month_end_or_before_cutoff"
            return result

        rows = conn.execute(
            """
            SELECT id, label, ip, created_at, auto_close_last_run_month
            FROM routers
            WHERE COALESCE(auto_close_unpaid_end_month, 0) = 1
            ORDER BY id
            """
        ).fetchall()
        if not rows:
            result["reason"] = "skip_no_router_enabled"
            return result

        updated_routers = 0
        updated_users = 0
        for row in rows:
            router_id = int(row["id"])
            label = str(row["label"] or "-")
            ip = str(row["ip"] or "-")
            last_run_month = str(row["auto_close_last_run_month"] or "").strip()
            if last_run_month == current_month:
                continue

            created_dt = self._parse_db_datetime(row["created_at"])
            if created_dt is None:
                created_dt = current_dt
            start_idx = self._month_index(created_dt.year, created_dt.month)

            items = conn.execute(
                """
                SELECT secret_name, paid_until_month
                FROM payment_state
                WHERE router_id = ?
                """,
                (router_id,),
            ).fetchall()
            changed_this_router = 0
            for item in items:
                secret_name = str(item["secret_name"] or "").strip()
                if not secret_name:
                    continue
                paid_idx = self._month_index_from_str(item["paid_until_month"])
                effective_paid_idx = (start_idx - 1) if paid_idx is None else max(paid_idx, start_idx - 1)
                if effective_paid_idx >= current_idx:
                    continue

                conn.execute(
                    """
                    UPDATE payment_state
                    SET paid_until_month = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE router_id = ? AND secret_name = ?
                    """,
                    (current_month, router_id, secret_name),
                )
                conn.execute(
                    """
                    INSERT INTO payment_audit (router_id, router_label, router_ip, secret_name, action, detail)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        router_id,
                        label,
                        ip,
                        secret_name,
                        "auto_pay_month_end",
                        f"Auto-lunas akhir bulan {self._month_label(current_idx)} (cutoff 18:00)",
                    ),
                )
                changed_this_router += 1

            conn.execute(
                """
                UPDATE routers
                SET auto_close_last_run_month = ?
                WHERE id = ?
                """,
                (current_month, router_id),
            )

            if changed_this_router > 0:
                updated_routers += 1
                updated_users += changed_this_router

        result["ran"] = True
        result["reason"] = "executed"
        result["updated_routers"] = int(updated_routers)
        result["updated_users"] = int(updated_users)
        return result

    def _normalize_wa_number(self, value: Optional[str]) -> str:
        raw = "".join(ch for ch in str(value or "") if ch.isdigit())
        if not raw:
            return ""
        if raw.startswith("0"):
            raw = "62" + raw[1:]
        if raw.startswith("8"):
            raw = "62" + raw
        return raw

    def _format_rupiah(self, amount: int) -> str:
        return f"Rp {int(amount):,}".replace(",", ".")

    def _send_wa_message(self, number: str, message: str) -> Dict[str, Any]:
        target = self._normalize_wa_number(number)
        if not target:
            return {"ok": False, "error": "Nomor WA kosong"}
        if not str(WA_GATEWAY_URL or "").strip():
            return {"ok": False, "error": "Konfigurasi WA_GATEWAY_URL belum diisi", "number": target}
        payload = json.dumps({"number": target, "message": message}).encode("utf-8")
        req = urlrequest.Request(
            WA_GATEWAY_URL,
            data=payload,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json, text/plain, */*",
                "User-Agent": WA_HTTP_USER_AGENT,
            },
        )
        try:
            with urlrequest.urlopen(req, timeout=20) as resp:
                body = resp.read().decode("utf-8", errors="replace")
                return {
                    "ok": True,
                    "status": getattr(resp, "status", 200),
                    "body": body,
                    "number": target,
                }
        except urlerror.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            return {
                "ok": False,
                "status": int(exc.code),
                "error": f"HTTP {exc.code}: {detail}",
                "number": target,
            }
        except Exception as exc:
            return {"ok": False, "error": str(exc), "number": target}

    def _ros7_rest_url(self, path: str, query: Optional[Dict[str, Any]] = None) -> str:
        base = str(ROS7_REST_BASE_URL or "").strip()
        if not base:
            raise CoreError("Konfigurasi ROS7_REST_BASE_URL belum diisi.")
        url = base.rstrip("/") + "/" + str(path or "").lstrip("/")
        if query:
            query_items = {
                str(key): str(value)
                for key, value in query.items()
                if value is not None and str(key).strip()
            }
            if query_items:
                url = url + "?" + urlparse.urlencode(query_items)
        return url

    def _ros7_rest_request(
        self,
        method: str,
        path: str,
        payload: Optional[Dict[str, Any]] = None,
        query: Optional[Dict[str, Any]] = None,
    ) -> Any:
        username = str(ROS7_REST_USERNAME or "").strip()
        password = str(ROS7_REST_PASSWORD or "")
        if not username or not password:
            raise CoreError("Konfigurasi ROS7 REST username/password belum diisi.")

        target_url = self._ros7_rest_url(path, query=query)
        auth_token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
        headers = {
            "Authorization": f"Basic {auth_token}",
            "Accept": "application/json",
            "User-Agent": WA_HTTP_USER_AGENT,
        }
        body = None
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = urlrequest.Request(
            target_url,
            data=body,
            method=str(method or "GET").upper(),
            headers=headers,
        )
        ssl_context = None
        if target_url.lower().startswith("https://") and not ROS7_REST_VERIFY_SSL:
            ssl_context = ssl._create_unverified_context()
        try:
            with urlrequest.urlopen(req, timeout=ROS7_REST_TIMEOUT_SECONDS, context=ssl_context) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
                if not raw.strip():
                    return {}
                try:
                    return json.loads(raw)
                except json.JSONDecodeError:
                    return {"raw": raw}
        except urlerror.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise CoreError(
                f"ROS7 REST {req.get_method()} {target_url} gagal: HTTP {int(exc.code)} - {detail}"
            ) from exc
        except Exception as exc:
            raise CoreError(
                f"ROS7 REST {req.get_method()} {target_url} gagal: {exc}"
            ) from exc

    def test_ros7_rest_connection(self) -> Dict[str, Any]:
        data = self._ros7_rest_request("GET", "/system/resource")
        if isinstance(data, list):
            return dict(data[0]) if data else {}
        if isinstance(data, dict):
            return data
        return {"result": data}

    def create_ros7_l2tp_secret(
        self,
        *,
        name: str,
        password: str,
        profile: str = "default",
        local_address: str = "",
        remote_address: str = "",
        comment: str = "",
    ) -> Dict[str, Any]:
        secret_name = str(name or "").strip()
        secret_password = str(password or "")
        secret_profile = str(profile or "").strip() or "default"
        if not secret_name:
            raise CoreError("Nama secret wajib diisi.")
        if not secret_password:
            raise CoreError("Password secret wajib diisi.")
        payload = {
            "name": secret_name,
            "password": secret_password,
            "service": "l2tp",
            "profile": secret_profile,
        }
        if str(local_address or "").strip():
            payload["local-address"] = str(local_address).strip()
        if str(remote_address or "").strip():
            payload["remote-address"] = str(remote_address).strip()
        if str(comment or "").strip():
            payload["comment"] = str(comment).strip()
        data = self._ros7_rest_request("PUT", "/ppp/secret", payload=payload)
        if isinstance(data, dict):
            return data
        return {"result": data}

    def _normalize_l2tp_prefix(self) -> str:
        prefix = str(ROS7_L2TP_USERNAME_PREFIX or "").strip().lower()
        mapped = []
        for ch in prefix:
            if ch.isalnum() or ch in {"-", "_", "."}:
                mapped.append(ch)
            elif ch.isspace():
                mapped.append("_")
        cleaned = "".join(mapped)
        while "__" in cleaned:
            cleaned = cleaned.replace("__", "_")
        while "--" in cleaned:
            cleaned = cleaned.replace("--", "-")
        return cleaned.strip("-._")

    def _build_l2tp_secret_name(self, label: str) -> str:
        text = str(label or "").strip().lower()
        if not text:
            raise CoreError("Label POP wajib diisi.")
        mapped = []
        for ch in text:
            if ch.isalnum() or ch in {"-", "_", "."}:
                mapped.append(ch)
            elif ch.isspace():
                mapped.append("-")
        cleaned = "".join(mapped)
        while "--" in cleaned:
            cleaned = cleaned.replace("--", "-")
        cleaned = cleaned.strip("-._")
        if not cleaned:
            raise CoreError("Label POP tidak valid untuk username L2TP.")
        prefix = self._normalize_l2tp_prefix()
        max_len = 60
        if prefix:
            max_base_len = max(8, max_len - len(prefix) - 1)
            if len(cleaned) > max_base_len:
                cleaned = cleaned[:max_base_len].rstrip("-._")
            final_name = f"{prefix}_{cleaned}".strip("_")
        else:
            if len(cleaned) > max_len:
                cleaned = cleaned[:max_len].rstrip("-._")
            final_name = cleaned
        if not final_name:
            raise CoreError("Label POP tidak valid untuk username L2TP.")
        return final_name

    def _ros7_secret_exists(self, name: str) -> bool:
        target = str(name or "").strip()
        if not target:
            return False
        data = self._ros7_rest_request(
            "GET",
            "/ppp/secret",
            query={"name": target, ".proplist": "name"},
        )
        if isinstance(data, list):
            return len(data) > 0
        if isinstance(data, dict):
            return bool(data.get("name"))
        return False

    def _build_unique_l2tp_secret_name(self, base_name: str) -> str:
        candidate = self._build_l2tp_secret_name(base_name)
        if not self._ros7_secret_exists(candidate):
            return candidate
        for idx in range(2, 200):
            alt = f"{candidate}-{idx}"
            if len(alt) > 60:
                alt = alt[:60].rstrip("-._")
            if not self._ros7_secret_exists(alt):
                return alt
        raise CoreError("Tidak menemukan username L2TP unik, coba ubah label POP.")

    def _allocate_l2tp_remote_address(self, extra_used_ips: Optional[List[str]] = None) -> str:
        try:
            network = ipaddress.ip_network(str(ROS7_L2TP_REMOTE_NETWORK), strict=False)
        except ValueError as exc:
            raise CoreError(f"Konfigurasi ROS7_L2TP_REMOTE_NETWORK tidak valid: {exc}") from exc
        if not isinstance(network, ipaddress.IPv4Network):
            raise CoreError("ROS7_L2TP_REMOTE_NETWORK harus IPv4 network.")

        try:
            local_addr = ipaddress.ip_address(str(ROS7_L2TP_LOCAL_ADDRESS).strip())
        except ValueError as exc:
            raise CoreError(f"Konfigurasi ROS7_L2TP_LOCAL_ADDRESS tidak valid: {exc}") from exc
        if local_addr not in network:
            raise CoreError("ROS7_L2TP_LOCAL_ADDRESS harus berada dalam range ROS7_L2TP_REMOTE_NETWORK.")

        used: set[ipaddress.IPv4Address] = set()
        used.add(ipaddress.IPv4Address(str(local_addr)))

        with self._db() as conn:
            rows = conn.execute("SELECT ip FROM routers").fetchall()
        for row in rows:
            ip_text = str(row["ip"] or "").strip()
            if not ip_text:
                continue
            try:
                addr = ipaddress.ip_address(ip_text)
            except ValueError:
                continue
            if isinstance(addr, ipaddress.IPv4Address) and addr in network:
                used.add(addr)
        for ip_text in (extra_used_ips or []):
            value = str(ip_text or "").strip()
            if not value:
                continue
            try:
                addr = ipaddress.ip_address(value)
            except ValueError:
                continue
            if isinstance(addr, ipaddress.IPv4Address) and addr in network:
                used.add(addr)

        # Tambahan safety: hindari remote-address yang sudah dipakai secret L2TP di ROS7.
        try:
            secrets = self._ros7_rest_request(
                "GET",
                "/ppp/secret",
                query={"service": "l2tp", ".proplist": "remote-address"},
            )
            if isinstance(secrets, dict):
                secrets = [secrets]
            if isinstance(secrets, list):
                for item in secrets:
                    if not isinstance(item, dict):
                        continue
                    remote_text = str(item.get("remote-address") or "").strip()
                    if not remote_text:
                        continue
                    try:
                        remote_addr = ipaddress.ip_address(remote_text.split("/", 1)[0])
                    except ValueError:
                        continue
                    if isinstance(remote_addr, ipaddress.IPv4Address) and remote_addr in network:
                        used.add(remote_addr)
        except CoreError:
            pass

        for candidate in network.hosts():
            if candidate in used:
                continue
            return str(candidate)
        raise CoreError("IP remote-address 10.168.0.0/24 sudah habis.")

    def _calc_webfig_port_from_router_ip(self, router_ip: str) -> int:
        ip_text = str(router_ip or "").strip()
        try:
            addr = ipaddress.ip_address(ip_text)
        except ValueError as exc:
            raise CoreError(f"IP router tidak valid untuk perhitungan WebFig port: {ip_text}") from exc
        if not isinstance(addr, ipaddress.IPv4Address):
            raise CoreError("IP router untuk WebFig harus IPv4.")
        last_octet = int(ip_text.split(".")[-1])
        port = int(ROS7_WEBFIG_PORT_PREFIX) + int(last_octet)
        if port < 1 or port > 65535:
            raise CoreError(f"Port WebFig hasil perhitungan tidak valid: {port}")
        return port

    def _calc_winbox_port_from_router_ip(self, router_ip: str) -> int:
        ip_text = str(router_ip or "").strip()
        try:
            addr = ipaddress.ip_address(ip_text)
        except ValueError as exc:
            raise CoreError(f"IP router tidak valid untuk perhitungan Winbox port: {ip_text}") from exc
        if not isinstance(addr, ipaddress.IPv4Address):
            raise CoreError("IP router untuk Winbox harus IPv4.")
        last_octet = int(ip_text.split(".")[-1])
        port = int(ROS7_WINBOX_PORT_PREFIX) + int(last_octet)
        if port < 1 or port > 65535:
            raise CoreError(f"Port Winbox hasil perhitungan tidak valid: {port}")
        return port

    def _build_webfig_url_from_router_ip(self, router_ip: str) -> str:
        port = self._calc_webfig_port_from_router_ip(router_ip)
        return f"http://{ROS7_WEBFIG_PROXY_HOST}:{port}"

    def _build_winbox_endpoint_from_router_ip(self, router_ip: str) -> str:
        port = self._calc_winbox_port_from_router_ip(router_ip)
        return f"{ROS7_WINBOX_PROXY_HOST}:{port}"

    def _list_ros7_nat_rules_by_dst_port(self, dst_port: int) -> List[Dict[str, Any]]:
        data = self._ros7_rest_request(
            "GET",
            "/ip/firewall/nat",
            query={
                "dst-port": str(int(dst_port)),
                ".proplist": ".id,chain,action,protocol,dst-port,dst-address,to-addresses,to-ports,disabled,comment",
            },
        )
        if isinstance(data, dict):
            return [data]
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        return []

    def ensure_ros7_webfig_dstnat(self, *, router_ip: str, label: str = "") -> Dict[str, Any]:
        router_ip_text = str(router_ip or "").strip()
        if not router_ip_text:
            raise CoreError("IP router untuk NAT WebFig kosong.")
        webfig_port = self._calc_webfig_port_from_router_ip(router_ip_text)
        to_port = str(int(ROS7_WEBFIG_TO_PORT))

        rules = self._list_ros7_nat_rules_by_dst_port(webfig_port)
        for rule in rules:
            chain = str(rule.get("chain") or "").strip().lower()
            action = str(rule.get("action") or "").strip().lower()
            protocol = str(rule.get("protocol") or "").strip().lower()
            dst_port = str(rule.get("dst-port") or "").strip()
            to_addr = str(rule.get("to-addresses") or "").strip()
            to_ports = str(rule.get("to-ports") or "").strip()
            disabled = str(rule.get("disabled") or "no").strip().lower()
            if chain != "dstnat" or action != "dst-nat" or protocol != "tcp":
                continue
            if dst_port != str(webfig_port):
                continue
            if to_addr == router_ip_text and (not to_ports or to_ports == to_port) and disabled not in {"yes", "true"}:
                return {
                    "created": False,
                    "rule_id": str(rule.get(".id") or ""),
                    "webfig_port": int(webfig_port),
                    "webfig_url": self._build_webfig_url_from_router_ip(router_ip_text),
                }
            if to_addr and to_addr != router_ip_text and disabled not in {"yes", "true"}:
                raise CoreError(
                    f"Port WebFig {webfig_port} sudah dipakai NAT ke {to_addr}. Mohon cek rule firewall NAT di server pusat."
                )

        payload: Dict[str, Any] = {
            "chain": "dstnat",
            "action": "dst-nat",
            "protocol": "tcp",
            "dst-port": str(webfig_port),
            "to-addresses": router_ip_text,
            "to-ports": to_port,
            "disabled": "no",
            "comment": f"auto-webfig pop={str(label or '-').strip() or '-'} ip={router_ip_text}",
        }
        if ROS7_WEBFIG_DST_ADDRESS:
            try:
                ipaddress.ip_address(str(ROS7_WEBFIG_DST_ADDRESS))
                payload["dst-address"] = str(ROS7_WEBFIG_DST_ADDRESS)
            except ValueError as exc:
                raise CoreError(
                    f"Konfigurasi ROS7_WEBFIG_DST_ADDRESS tidak valid: {ROS7_WEBFIG_DST_ADDRESS}"
                ) from exc

        created = self._ros7_rest_request("PUT", "/ip/firewall/nat", payload=payload)
        created_id = str(created.get(".id") or "") if isinstance(created, dict) else ""
        return {
            "created": True,
            "rule_id": created_id,
            "webfig_port": int(webfig_port),
            "webfig_url": self._build_webfig_url_from_router_ip(router_ip_text),
        }

    def ensure_ros7_winbox_dstnat(self, *, router_ip: str, label: str = "") -> Dict[str, Any]:
        router_ip_text = str(router_ip or "").strip()
        if not router_ip_text:
            raise CoreError("IP router untuk NAT Winbox kosong.")
        winbox_port = self._calc_winbox_port_from_router_ip(router_ip_text)
        to_port = str(int(ROS7_WINBOX_TO_PORT))

        rules = self._list_ros7_nat_rules_by_dst_port(winbox_port)
        for rule in rules:
            chain = str(rule.get("chain") or "").strip().lower()
            action = str(rule.get("action") or "").strip().lower()
            protocol = str(rule.get("protocol") or "").strip().lower()
            dst_port = str(rule.get("dst-port") or "").strip()
            to_addr = str(rule.get("to-addresses") or "").strip()
            to_ports = str(rule.get("to-ports") or "").strip()
            disabled = str(rule.get("disabled") or "no").strip().lower()
            if chain != "dstnat" or action != "dst-nat" or protocol != "tcp":
                continue
            if dst_port != str(winbox_port):
                continue
            if to_addr == router_ip_text and (not to_ports or to_ports == to_port) and disabled not in {"yes", "true"}:
                return {
                    "created": False,
                    "rule_id": str(rule.get(".id") or ""),
                    "winbox_port": int(winbox_port),
                    "winbox_endpoint": self._build_winbox_endpoint_from_router_ip(router_ip_text),
                }
            if to_addr and to_addr != router_ip_text and disabled not in {"yes", "true"}:
                raise CoreError(
                    f"Port Winbox {winbox_port} sudah dipakai NAT ke {to_addr}. Mohon cek rule firewall NAT di server pusat."
                )

        payload: Dict[str, Any] = {
            "chain": "dstnat",
            "action": "dst-nat",
            "protocol": "tcp",
            "dst-port": str(winbox_port),
            "to-addresses": router_ip_text,
            "to-ports": to_port,
            "disabled": "no",
            "comment": f"auto-winbox pop={str(label or '-').strip() or '-'} ip={router_ip_text}",
        }
        if ROS7_WEBFIG_DST_ADDRESS:
            try:
                ipaddress.ip_address(str(ROS7_WEBFIG_DST_ADDRESS))
                payload["dst-address"] = str(ROS7_WEBFIG_DST_ADDRESS)
            except ValueError as exc:
                raise CoreError(
                    f"Konfigurasi ROS7_WEBFIG_DST_ADDRESS tidak valid: {ROS7_WEBFIG_DST_ADDRESS}"
                ) from exc

        created = self._ros7_rest_request("PUT", "/ip/firewall/nat", payload=payload)
        created_id = str(created.get(".id") or "") if isinstance(created, dict) else ""
        return {
            "created": True,
            "rule_id": created_id,
            "winbox_port": int(winbox_port),
            "winbox_endpoint": self._build_winbox_endpoint_from_router_ip(router_ip_text),
        }

    def delete_ros7_webfig_dstnat_by_router_ip(self, router_ip: str) -> int:
        router_ip_text = str(router_ip or "").strip()
        if not router_ip_text:
            return 0
        webfig_port = self._calc_webfig_port_from_router_ip(router_ip_text)
        rules = self._list_ros7_nat_rules_by_dst_port(webfig_port)
        deleted = 0
        for rule in rules:
            chain = str(rule.get("chain") or "").strip().lower()
            action = str(rule.get("action") or "").strip().lower()
            protocol = str(rule.get("protocol") or "").strip().lower()
            dst_port = str(rule.get("dst-port") or "").strip()
            to_addr = str(rule.get("to-addresses") or "").strip()
            if chain != "dstnat" or action != "dst-nat" or protocol != "tcp":
                continue
            if dst_port != str(webfig_port):
                continue
            if to_addr != router_ip_text:
                continue
            rule_id = str(rule.get(".id") or "").strip()
            if not rule_id:
                continue
            self._ros7_rest_request("DELETE", f"/ip/firewall/nat/{rule_id}")
            deleted += 1
        return deleted

    def delete_ros7_winbox_dstnat_by_router_ip(self, router_ip: str) -> int:
        router_ip_text = str(router_ip or "").strip()
        if not router_ip_text:
            return 0
        winbox_port = self._calc_winbox_port_from_router_ip(router_ip_text)
        rules = self._list_ros7_nat_rules_by_dst_port(winbox_port)
        deleted = 0
        for rule in rules:
            chain = str(rule.get("chain") or "").strip().lower()
            action = str(rule.get("action") or "").strip().lower()
            protocol = str(rule.get("protocol") or "").strip().lower()
            dst_port = str(rule.get("dst-port") or "").strip()
            to_addr = str(rule.get("to-addresses") or "").strip()
            if chain != "dstnat" or action != "dst-nat" or protocol != "tcp":
                continue
            if dst_port != str(winbox_port):
                continue
            if to_addr != router_ip_text:
                continue
            rule_id = str(rule.get(".id") or "").strip()
            if not rule_id:
                continue
            self._ros7_rest_request("DELETE", f"/ip/firewall/nat/{rule_id}")
            deleted += 1
        return deleted

    def register_and_provision_l2tp(
        self,
        *,
        label: str,
        address: str,
        wa_number: str,
        router_ip: str = "",
    ) -> Dict[str, Any]:
        pop_label = str(label or "").strip()
        pop_address = str(address or "").strip()
        owner_wa = self._normalize_wa_number(wa_number)
        if not pop_label:
            raise CoreError("Label POP wajib diisi.")
        if not owner_wa:
            raise CoreError("Nomor WA pemilik router tidak valid.")

        secret_name = self._build_unique_l2tp_secret_name(pop_label)
        secret_password = str(ROS7_L2TP_DEFAULT_PASSWORD or "").strip()
        if not secret_password:
            raise CoreError("Konfigurasi ROS7_L2TP_DEFAULT_PASSWORD belum diisi.")
        secret_profile = str(ROS7_L2TP_SECRET_PROFILE or "default").strip() or "default"
        local_address = str(ROS7_L2TP_LOCAL_ADDRESS or "10.168.0.1").strip()
        remote_address = self._allocate_l2tp_remote_address(
            extra_used_ips=[router_ip] if str(router_ip or "").strip() else None
        )
        effective_router_ip = str(router_ip or "").strip() or remote_address
        secret_comment = (
            f"auto-register pop={pop_label} wa={owner_wa} "
            f"router_ip={effective_router_ip} "
            f"remote={remote_address} created={datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        created: Dict[str, Any] = {}
        nat_result: Dict[str, Any] = {}
        nat_winbox_result: Dict[str, Any] = {}
        try:
            created = self.create_ros7_l2tp_secret(
                name=secret_name,
                password=secret_password,
                profile=secret_profile,
                local_address=local_address,
                remote_address=remote_address,
                comment=secret_comment,
            )
            nat_result = self.ensure_ros7_webfig_dstnat(
                router_ip=effective_router_ip,
                label=pop_label,
            )
            nat_winbox_result = self.ensure_ros7_winbox_dstnat(
                router_ip=effective_router_ip,
                label=pop_label,
            )
        except Exception as exc:
            created_secret_id = str(created.get(".id") or "").strip()
            if created_secret_id:
                try:
                    self.delete_ros7_secret(created_secret_id)
                except Exception:
                    pass
            if effective_router_ip:
                try:
                    self.delete_ros7_webfig_dstnat_by_router_ip(effective_router_ip)
                except Exception:
                    pass
                try:
                    self.delete_ros7_winbox_dstnat_by_router_ip(effective_router_ip)
                except Exception:
                    pass
            raise CoreError(f"Gagal provisioning L2TP/NAT: {exc}") from exc

        owner_message = (
            "\U0001F389 *Pendaftaran Berhasil - Akun L2TP Aktif*\n"
            f"\U0001F3F7\ufe0f POP: {pop_label}\n"
            f"\U0001F3E0 Alamat: {pop_address or '-'}\n\n"
            "\U0001F4CB *Detail Akun L2TP*\n"
            f"- Connect To: {ROS7_L2TP_CONNECT_TO}\n"
            f"- Username: {secret_name}\n"
            f"- Password: {secret_password}\n"
            f"- IP Router Billing: {effective_router_ip}\n"
            f"- Local Address: {local_address}\n"
            f"- Remote Address: {remote_address}\n"
            f"- WebFig: {str(nat_result.get('webfig_url') or '-')}\n"
            f"- Winbox: {str(nat_winbox_result.get('winbox_endpoint') or '-')}\n"
            f"- Profile: {secret_profile}\n\n"
            "\U0001F6E0\ufe0f Pengaturan di MikroTik:\n"
            "- Interface > Add > L2TP Client > Dial Out\n"
            "- use-peer-dns = no\n"
            "- allow-fast-path = yes\n"
            "- add-default-route = no\n\n"
            "\U0001F91D Jika butuh bantuan, hubungi admin."
        )
        owner_result = self._send_wa_message(owner_wa, owner_message)

        admin_message = (
            "\U0001F4E2 *Auto Register L2TP Berhasil*\n"
            f"\U0001F3F7\ufe0f POP: {pop_label}\n"
            f"\U0001F3E0 Alamat: {pop_address or '-'}\n"
            f"\U0001F4F1 WA: {owner_wa}\n"
            f"\U0001F310 IP Router: {effective_router_ip}\n"
            f"\U0001F464 Username L2TP: {secret_name}\n"
            f"\U0001F310 Connect-To: {ROS7_L2TP_CONNECT_TO}\n"
            f"\U0001F4CD Local/Remote: {local_address} / {remote_address}\n"
            f"\U0001F517 WebFig: {str(nat_result.get('webfig_url') or '-')}\n"
            f"\U0001F4BB Winbox: {str(nat_winbox_result.get('winbox_endpoint') or '-')}\n"
            f"\U0001F194 Secret ID: {str(created.get('.id') or '-')}\n"
            f"\U0001F552 Waktu: {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}"
        )
        admin_result = self._send_wa_message(ADMIN_WA_NUMBER, admin_message)

        return {
            "ok": True,
            "label": pop_label,
            "address": pop_address,
            "wa_number": owner_wa,
            "l2tp_username": secret_name,
            "l2tp_password": secret_password,
            "l2tp_profile": secret_profile,
            "local_address": local_address,
            "remote_address": remote_address,
            "connect_to": ROS7_L2TP_CONNECT_TO,
            "secret_id": str(created.get(".id") or ""),
            "webfig_port": int(nat_result.get("webfig_port") or 0),
            "webfig_url": str(nat_result.get("webfig_url") or ""),
            "nat_rule_created": bool(nat_result.get("created")),
            "nat_rule_id": str(nat_result.get("rule_id") or ""),
            "winbox_port": int(nat_winbox_result.get("winbox_port") or 0),
            "winbox_endpoint": str(nat_winbox_result.get("winbox_endpoint") or ""),
            "nat_winbox_created": bool(nat_winbox_result.get("created")),
            "nat_winbox_rule_id": str(nat_winbox_result.get("rule_id") or ""),
            "owner_notified": bool(owner_result.get("ok")),
            "admin_notified": bool(admin_result.get("ok")),
            "owner_error": None if owner_result.get("ok") else str(owner_result.get("error") or "Gagal kirim WA pemilik"),
            "admin_error": None if admin_result.get("ok") else str(admin_result.get("error") or "Gagal kirim WA admin"),
        }

    def delete_ros7_secret(self, secret_id: str) -> None:
        target = str(secret_id or "").strip()
        if not target:
            raise CoreError("Secret ID tidak valid.")
        self._ros7_rest_request("DELETE", f"/ppp/secret/{target}")

    def delete_ros7_secret_by_name(self, secret_name: str) -> bool:
        target_name = str(secret_name or "").strip()
        if not target_name:
            raise CoreError("Secret name tidak valid.")
        data = self._ros7_rest_request(
            "GET",
            "/ppp/secret",
            query={"name": target_name, ".proplist": ".id,name"},
        )
        items: List[Dict[str, Any]] = []
        if isinstance(data, dict):
            items = [data]
        elif isinstance(data, list):
            items = [item for item in data if isinstance(item, dict)]
        if not items:
            return False

        deleted = False
        for item in items:
            secret_id = str(item.get(".id") or "").strip()
            if not secret_id:
                continue
            self.delete_ros7_secret(secret_id)
            deleted = True
        return deleted

    def _is_ros7_not_found_error(self, exc: Exception) -> bool:
        text = str(exc or "").lower()
        return ("http 404" in text) or ("no such item" in text) or ("not found" in text)

    def _delete_l2tp_secret_safe(self, *, secret_id: str, secret_name: str) -> bool:
        by_id = str(secret_id or "").strip()
        by_name = str(secret_name or "").strip()

        if by_id:
            try:
                self.delete_ros7_secret(by_id)
                return True
            except CoreError as exc:
                if not self._is_ros7_not_found_error(exc):
                    if not by_name:
                        raise
                # fallback by name for stale id

        if by_name:
            try:
                return self.delete_ros7_secret_by_name(by_name)
            except CoreError as exc:
                if self._is_ros7_not_found_error(exc):
                    return False
                raise
        return False

    def _build_router_billing_wa_message(
        self,
        router_label: str,
        summary: Dict[str, Any],
        purpose: str,
    ) -> str:
        due_current_cycle = int(summary.get("amount_due_current_cycle") or 0)
        lines = [
            "\U0001F514 *Informasi Tagihan Router*",
            f"\U0001F3F7\ufe0f Router: {router_label}",
            f"\U0001F4C5 Jatuh tempo: setiap tanggal {summary.get('billing_day', ROUTER_BILLING_DAY)}",
            f"\u2705 Paid until: {summary.get('paid_until_label', '-')}",
            f"\U0001F4CC Wajib lunas: {summary.get('required_paid_label', '-')}",
            f"\U0001F465 Jumlah user: {summary.get('router_user_count', 0)}",
            f"\U0001F4B3 Tagihan bulanan: {self._format_rupiah(int(summary.get('monthly_fee') or 0))}",
            f"\U0001F4B5 Tagihan cycle ini: {self._format_rupiah(due_current_cycle)}",
            f"\U0001F4E3 Status: {summary.get('status_billing', '-')}",
        ]
        if purpose == "reminder":
            lines.append("\u23f0 Pengingat H-3: mohon siapkan pembayaran sebelum jatuh tempo.")
            lines.append("\U0001F64C Admin akan kirim barcode QRIS ke nomor ini.")
        elif purpose == "paid":
            lines.append("\U0001F389 Pembayaran diterima. Terima kasih, layanan tetap aktif.")
        elif purpose == "request_qris":
            lines.append("\U0001F4E9 Mohon kirim kode QRIS pembayaran untuk router ini.")
            lines.append("\U0001F3F7\ufe0f Setelah transfer, admin akan segera mengaktifkannya.")
        else:
            lines.append("\U0001F4EC Notifikasi ini dikirim hanya untuk pengingat saja.")
        return "\n".join(lines)

    def run_hourly_router_billing_check(self, now: Optional[datetime] = None) -> Dict[str, Any]:
        current_dt = now or datetime.now()
        today = current_dt.date().isoformat()
        result: Dict[str, Any] = {
            "billing_day": ROUTER_BILLING_DAY,
            "base_fee_per_user": ROUTER_BASE_FEE_PER_USER,
            "today": today,
            "day": int(current_dt.day),
            "ran_billing": False,
            "updated_routers": 0,
            "billing_reason": "sync_moved_to_login",
            "ran_reminder": False,
            "reminder_day": ROUTER_REMINDER_DAY,
            "reminder_sent": 0,
            "reminder_reason": "",
            "ran_auto_close_unpaid_end_month": False,
            "auto_close_reason": "",
            "auto_close_updated_routers": 0,
            "auto_close_updated_users": 0,
        }

        with self._db() as conn:
            auto_close_result = self._run_auto_close_unpaid_end_month(conn, current_dt)
            result["ran_auto_close_unpaid_end_month"] = bool(auto_close_result.get("ran"))
            result["auto_close_reason"] = str(auto_close_result.get("reason") or "")
            result["auto_close_updated_routers"] = int(auto_close_result.get("updated_routers") or 0)
            result["auto_close_updated_users"] = int(auto_close_result.get("updated_users") or 0)

            if current_dt.day != ROUTER_REMINDER_DAY:
                result["reminder_reason"] = "skip_not_reminder_day"
                return result

            last_reminder_date = self._get_app_state(conn, "router_billing_reminder_last_run_date")
            if last_reminder_date == today:
                result["reminder_reason"] = "skip_reminder_already_ran_today"
                return result

            router_rows = conn.execute(
                """
                SELECT id, label, wa_number, monthly_fee, paid_until_month, billing_updated_at
                FROM routers
                ORDER BY id
                """
            ).fetchall()
            reminder_payloads: List[Dict[str, Any]] = []
            for row in router_rows:
                router_item = dict(row)
                router_id = int(router_item["id"])
                try:
                    source = self._collect_payment_user_sources(router_id)
                    sync_names = list(source.get("user_names") or [])
                    self._sync_payment_state_rows(conn, router_id, sync_names, prune_missing=True)
                    user_count = len(sync_names)
                except CoreError:
                    user_count = self._count_router_payment_users(conn, router_id)
                summary = self._build_router_billing_summary_from_row(
                    router_item,
                    current_dt,
                    user_count=user_count,
                )
                router_item.update(summary)
                message = self._build_router_billing_wa_message(
                    router_label=str(router_item["label"]),
                    summary=router_item,
                    purpose="reminder",
                )
                reminder_payloads.append(
                    {
                        "router_id": router_id,
                        "router_label": str(router_item["label"]),
                        "router_wa": str(router_item.get("wa_number") or ""),
                        "message": message,
                    }
                )
            self._set_app_state(conn, "router_billing_reminder_last_run_date", today)

        sent = 0
        for payload in reminder_payloads:
            router_wa = self._normalize_wa_number(payload["router_wa"])
            if router_wa:
                wa_result = self._send_wa_message(router_wa, payload["message"])
                if wa_result.get("ok"):
                    sent += 1

            admin_result = self._send_wa_message(
                ADMIN_WA_NUMBER,
                f"\U0001F4E2 Reminder untuk router {payload['router_label']}\n\n{payload['message']}\n\n\U0001F4A1 Mohon kirim barcode QRIS ke nomor router terkait.",
            )
            if admin_result.get("ok"):
                sent += 1

        result["ran_reminder"] = True
        result["reminder_sent"] = sent
        result["reminder_reason"] = "executed"

        return result

    def _refresh_router_monthly_fee(
        self,
        conn: sqlite3.Connection,
        router_id: int,
        now: Optional[datetime] = None,
        force: bool = False,
    ) -> None:
        current_dt = now or datetime.now()
        target_cycle_idx = self._router_cycle_idx(current_dt)

        row = conn.execute(
            "SELECT billing_updated_at FROM routers WHERE id = ?",
            (int(router_id),),
        ).fetchone()
        if not row:
            raise CoreError("Router tidak ditemukan di database")

        last_updated_dt = self._parse_db_datetime(row["billing_updated_at"])
        last_cycle_idx = self._router_cycle_idx(last_updated_dt) if last_updated_dt else None
        if (not force) and last_cycle_idx is not None and last_cycle_idx >= target_cycle_idx:
            return

        try:
            source = self._collect_payment_user_sources(router_id)
            sync_names = list(source.get("user_names") or [])
            self._sync_payment_state_rows(conn, router_id, sync_names, prune_missing=True)
            total_users = len(sync_names)
        except CoreError:
            total_users = self._count_router_payment_users(conn, router_id)
        next_fee = int(total_users) * int(ROUTER_BASE_FEE_PER_USER)
        conn.execute(
            """
            UPDATE routers
            SET monthly_fee = ?, billing_updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (next_fee, int(router_id)),
        )

    def _refresh_all_router_monthly_fee(self, conn: sqlite3.Connection, now: Optional[datetime] = None) -> None:
        rows = conn.execute("SELECT id FROM routers").fetchall()
        current_dt = now or datetime.now()
        for row in rows:
            self._refresh_router_monthly_fee(conn, int(row["id"]), current_dt)

    def _build_router_billing_summary_from_row(
        self,
        router_row: Dict[str, Any],
        now: Optional[datetime] = None,
        user_count: int = 0,
    ) -> Dict[str, Any]:
        current_dt = now or datetime.now()
        required_idx = self._router_cycle_idx(current_dt)
        paid_idx = self._month_index_from_str(router_row.get("paid_until_month"))
        effective_paid_idx = (required_idx - 1) if paid_idx is None else paid_idx
        is_paid = effective_paid_idx >= required_idx
        # Model billing router hanya cycle bulan ini (tanpa akumulasi tunggakan multi-bulan).
        unpaid_count = 0 if is_paid else 1
        monthly_fee = int(router_row.get("monthly_fee") or 0)
        amount_due_current_cycle = 0 if is_paid else monthly_fee
        total_due = amount_due_current_cycle
        unpaid_months: List[str] = [] if is_paid else [self._month_label(required_idx)]

        if is_paid:
            status = f"Lunas cycle {self._month_label(required_idx)}"
        else:
            status = f"Belum lunas cycle {self._month_label(required_idx)}"

        return {
            "billing_day": ROUTER_BILLING_DAY,
            "base_fee_per_user": ROUTER_BASE_FEE_PER_USER,
            "router_user_count": int(user_count),
            "required_paid_month": self._month_str_from_index(required_idx),
            "required_paid_label": self._month_label(required_idx),
            "paid_until_label": self._month_label(effective_paid_idx) if paid_idx is not None else "-",
            "is_paid_current_cycle": is_paid,
            "unpaid_count": unpaid_count,
            "unpaid_months": unpaid_months,
            "status_billing": status,
            "amount_due_current_cycle": amount_due_current_cycle,
            "total_due": total_due,
        }

    def get_router_billing_summary(self, router_id: int) -> Dict[str, Any]:
        self.run_hourly_router_billing_check()
        with self._db() as conn:
            row = conn.execute(
                """
                SELECT id, label, ip, wa_number, monthly_fee, paid_until_month, billing_updated_at
                FROM routers
                WHERE id = ?
                """,
                (int(router_id),),
            ).fetchone()
            user_count = self._count_router_payment_users(conn, router_id)

        if not row:
            raise CoreError("Router tidak ditemukan di database")

        item = dict(row)
        item.update(self._build_router_billing_summary_from_row(item, user_count=user_count))
        return item

    def send_router_billing_notification(self, router_id: int, purpose: str = "manual") -> Dict[str, Any]:
        billing = self.get_router_billing_summary(router_id)
        router_label = str(billing.get("label") or "-")
        message = self._build_router_billing_wa_message(router_label, billing, purpose=purpose)
        targets: List[str] = []
        failed: List[Dict[str, Any]] = []
        router_wa = self._normalize_wa_number(billing.get("wa_number"))
        admin_wa = self._normalize_wa_number(ADMIN_WA_NUMBER)

        send_to_router = purpose in {"manual", "reminder", "paid"}
        send_to_admin = purpose in {"manual", "reminder", "request_qris"}

        if send_to_router and router_wa:
            targets.append(router_wa)

        if send_to_admin and admin_wa and admin_wa not in targets:
            admin_message = (
                f"\U0001F4E2 Notifikasi router {router_label}\n\n{message}\n\n"
                "\U0001F4A1 Mohon kirim barcode QRIS ke nomor router terkait dan update status pembayaran router."
            )
            admin_result = self._send_wa_message(admin_wa, admin_message)
            sent_admin = 1 if admin_result.get("ok") else 0
            if not admin_result.get("ok"):
                failed.append(
                    {
                        "target": admin_wa,
                        "scope": "admin",
                        "error": str(admin_result.get("error") or "Gagal kirim ke admin"),
                    }
                )
        else:
            sent_admin = 0

        sent_router = 0
        for number in targets:
            send_result = self._send_wa_message(number, message)
            if send_result.get("ok"):
                sent_router += 1
            else:
                failed.append(
                    {
                        "target": str(number),
                        "scope": "router",
                        "error": str(send_result.get("error") or "Gagal kirim ke router"),
                    }
                )

        return {
            "router_id": int(router_id),
            "router_label": router_label,
            "purpose": purpose,
            "sent_router": sent_router,
            "sent_admin": sent_admin,
            "sent_total": sent_router + sent_admin,
            "message": message,
            "failed": failed,
        }

    def send_router_quick_setup_notification(self, router_id: int) -> Dict[str, Any]:
        router = self.get_router(int(router_id))
        target = self._normalize_wa_number(router.wa_number)
        if not target:
            return {
                "ok": False,
                "error": "Nomor WA router belum diisi.",
                "router_id": int(router.id),
                "router_label": router.label,
                "target": "",
            }

        l2tp_username = str(router.l2tp_username or "").strip() or str(router.label or "").strip()
        l2tp_password = str(router.l2tp_password or "").strip() or str(ROS7_L2TP_DEFAULT_PASSWORD or "").strip()
        if not l2tp_password:
            l2tp_password = "-"
        message = (
            "\U0001F389 *Router Berhasil Diaktifkan di Billing Reseller*\n"
            f"\U0001F3F7\ufe0f POP: {router.label}\n"
            f"\U0001F3E0 Alamat: {router.address or '-'}\n\n"
            "\U0001F4CB *Instruksi setting L2TP Client (manual)*\n"
            "1. Masuk Winbox\n"
            "2. Buka Interface\n"
            "3. Klik Add (+)\n"
            "4. Pilih L2TP Client\n"
            "5. Buka tab Dial Out lalu isi:\n"
            f"   - connect-to = {ROS7_L2TP_CONNECT_TO}\n"
            f"   - user = {l2tp_username}\n"
            f"   - password = {l2tp_password}\n"
            "   - use-peer-dns = no\n"
            "   - ceklis allow-fast-path\n"
            "   - unceklis add-default-route\n\n"
            "\U0001F91D Jika butuh bantuan, balas pesan ini ke admin."
        )
        send_result = self._send_wa_message(target, message)
        return {
            "ok": bool(send_result.get("ok")),
            "error": None if send_result.get("ok") else str(send_result.get("error") or "Gagal kirim notifikasi"),
            "router_id": int(router.id),
            "router_label": router.label,
            "target": target,
            "message": message,
        }

    def pay_router_current_cycle(self, router_id: int) -> Dict[str, Any]:
        self.run_hourly_router_billing_check()
        with self._db() as conn:
            row = conn.execute(
                "SELECT paid_until_month FROM routers WHERE id = ?",
                (int(router_id),),
            ).fetchone()
            if not row:
                raise CoreError("Router tidak ditemukan di database")

            now = datetime.now()
            required_idx = self._router_cycle_idx(now)
            paid_idx = self._month_index_from_str(row["paid_until_month"])
            if paid_idx is None or paid_idx < required_idx:
                next_paid_idx = required_idx
                conn.execute(
                    "UPDATE routers SET paid_until_month = ? WHERE id = ?",
                    (self._month_str_from_index(next_paid_idx), int(router_id)),
                )

        summary = self.get_router_billing_summary(router_id)
        notif = self.send_router_billing_notification(router_id, purpose="paid")
        summary["pay_notification_sent"] = notif.get("sent_total", 0)
        return summary

    def is_router_paid_current_cycle(self, router_id: int) -> bool:
        summary = self.get_router_billing_summary(router_id)
        return bool(summary.get("is_paid_current_cycle"))

    def _build_payment_summary(
        self,
        secret_name: str,
        secret_profile: str,
        monthly_fee: float,
        paid_until_month: Optional[str],
        start_idx: int,
    ) -> Dict[str, Any]:
        monthly_fee_int = int(round(float(monthly_fee or 0)))
        if monthly_fee_int < 0:
            monthly_fee_int = 0
        current_idx = self._current_month_idx()
        paid_until_idx = self._month_index_from_str(paid_until_month)
        effective_paid_idx = start_idx - 1 if paid_until_idx is None else max(paid_until_idx, start_idx - 1)

        if current_idx < start_idx:
            unpaid_month_indexes: List[int] = []
        else:
            unpaid_start_idx = effective_paid_idx + 1
            if unpaid_start_idx > current_idx:
                unpaid_month_indexes = []
            else:
                unpaid_month_indexes = list(range(unpaid_start_idx, current_idx + 1))

        unpaid_count = len(unpaid_month_indexes)
        unpaid_labels = [self._month_label(item) for item in unpaid_month_indexes]
        unpaid_values = [self._month_str_from_index(item) for item in unpaid_month_indexes]
        total_due = monthly_fee_int * unpaid_count
        paid_count = max(0, min(effective_paid_idx, current_idx) - start_idx + 1)
        total_paid = monthly_fee_int * paid_count

        if unpaid_count == 0:
            if current_idx < start_idx:
                status_text = f"Belum mulai tagihan (mulai {self._month_label(start_idx)})"
            elif effective_paid_idx >= current_idx:
                status_text = f"Sudah bayar sampai {self._month_label(effective_paid_idx)}"
            else:
                status_text = "Sudah bayar"
        else:
            status_text = f"Belum bayar {unpaid_count} bulan: {', '.join(unpaid_labels)}"

        return {
            "name": secret_name,
            "profile": secret_profile or "-",
            "monthly_fee": monthly_fee_int,
            "paid_until_month": self._month_str_from_index(effective_paid_idx) if effective_paid_idx >= start_idx else None,
            "paid_until_label": self._month_label(effective_paid_idx) if effective_paid_idx >= start_idx else "-",
            "unpaid_count": unpaid_count,
            "unpaid_months": unpaid_labels,
            "unpaid_month_values": unpaid_values,
            "status": status_text,
            "total_due": total_due,
            "paid_count": paid_count,
            "total_paid": total_paid,
            "start_month": self._month_str_from_index(start_idx),
            "start_month_label": self._month_label(start_idx),
        }

    def _collect_payment_user_sources(self, router_id: int) -> Dict[str, Any]:
        router = self.get_router(router_id)
        with self._api(router) as api:
            active_rows = api.get_resource("/ppp/active").get()
            secret_rows = api.get_resource("/ppp/secret").get()

        active_names: set[str] = set()
        secret_names: set[str] = set()
        profile_by_name: Dict[str, str] = {}

        for row in secret_rows:
            user_name = str(row.get("name", "")).strip()
            if not user_name:
                continue
            secret_names.add(user_name)
            profile_by_name[user_name] = str(row.get("profile", "") or "-")

        for row in active_rows:
            user_name = str(row.get("name", "")).strip()
            if not user_name:
                continue
            active_names.add(user_name)
            if user_name not in profile_by_name:
                profile_by_name[user_name] = str(row.get("service", "") or "-")

        user_names = sorted(secret_names | active_names)
        return {
            "router": router,
            "user_names": user_names,
            "online_names": active_names,
            "profile_by_name": profile_by_name,
        }

    def _sync_payment_state_rows(
        self,
        conn: sqlite3.Connection,
        router_id: int,
        user_names: List[str],
        prune_missing: bool = True,
    ) -> None:
        valid_names = sorted({name.strip() for name in user_names if name and name.strip()})

        for user_name in valid_names:
            conn.execute(
                """
                INSERT INTO payment_state (router_id, secret_name, monthly_fee, paid_until_month)
                VALUES (?, ?, 0, NULL)
                ON CONFLICT(router_id, secret_name) DO NOTHING
                """,
                (router_id, user_name),
            )

        if not prune_missing:
            return

        if valid_names:
            placeholders = ", ".join(["?"] * len(valid_names))
            conn.execute(
                f"""
                DELETE FROM payment_state
                WHERE router_id = ?
                  AND secret_name NOT IN ({placeholders})
                """,
                (int(router_id), *valid_names),
            )
        else:
            conn.execute(
                "DELETE FROM payment_state WHERE router_id = ?",
                (int(router_id),),
            )

    def _insert_payment_audit(
        self,
        conn: sqlite3.Connection,
        router: RouterConfig,
        secret_name: str,
        action: str,
        detail: str,
    ) -> None:
        conn.execute(
            """
            INSERT INTO payment_audit (router_id, router_label, router_ip, secret_name, action, detail)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (router.id, router.label, router.ip, secret_name, action, detail),
        )

    def list_payments(self, router_id: int) -> List[Dict[str, Any]]:
        source = self._collect_payment_user_sources(router_id)
        router = source["router"]
        start_idx = self._router_payment_start_idx(router)
        user_names = list(source.get("user_names") or [])
        profile_by_name = dict(source.get("profile_by_name") or {})
        online_names = set(source.get("online_names") or set())

        with self._db() as conn:
            self._sync_payment_state_rows(conn, router.id, user_names, prune_missing=True)
            rows = conn.execute(
                """
                SELECT secret_name, monthly_fee, paid_until_month
                FROM payment_state
                WHERE router_id = ?
                ORDER BY secret_name
                """,
                (router.id,),
            ).fetchall()

        results: List[Dict[str, Any]] = []
        for row in rows:
            secret_name = str(row["secret_name"])
            profile = str(profile_by_name.get(secret_name, "-"))
            summary = self._build_payment_summary(
                secret_name=secret_name,
                secret_profile=profile,
                monthly_fee=float(row["monthly_fee"] or 0),
                paid_until_month=row["paid_until_month"],
                start_idx=start_idx,
            )
            summary["is_online"] = secret_name in online_names
            results.append(summary)
        return results

    def update_payment_monthly_fee(self, router_id: int, secret_name: str, monthly_fee: float) -> None:
        router = self.get_router(router_id)
        secret_name = (secret_name or "").strip()
        if not secret_name:
            raise CoreError("Nama secret wajib diisi")
        monthly_fee_value = int(round(float(monthly_fee or 0)))
        if monthly_fee_value < 0:
            raise CoreError("Paket bulanan tidak boleh negatif")

        with self._db() as conn:
            conn.execute(
                """
                INSERT INTO payment_state (router_id, secret_name, monthly_fee, paid_until_month, updated_at)
                VALUES (?, ?, ?, NULL, CURRENT_TIMESTAMP)
                ON CONFLICT(router_id, secret_name) DO UPDATE SET
                    monthly_fee = excluded.monthly_fee,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (router.id, secret_name, monthly_fee_value),
            )
            self._insert_payment_audit(
                conn=conn,
                router=router,
                secret_name=secret_name,
                action="set_monthly_fee",
                detail=f"Set paket bulanan menjadi Rp {monthly_fee_value}",
            )

    def apply_payment_action(
        self,
        router_id: int,
        secret_name: str,
        action: str,
        target_month: Optional[str] = None,
    ) -> Dict[str, Any]:
        router = self.get_router(router_id)
        secret_name = (secret_name or "").strip()
        if not secret_name:
            raise CoreError("Nama secret wajib diisi")
        if action == "pay_one":
            action = "pay"
        if action not in {"pay", "pay_full", "cancel_pay"}:
            raise CoreError("Aksi pembayaran tidak valid")
        active_users = self.list_active(router_id)
        secret_profile = "-"
        for item in active_users:
            if str(item.get("name", "")).strip() == secret_name:
                secret_profile = str(item.get("service", "-"))
                break

        start_idx = self._router_payment_start_idx(router)
        current_idx = self._current_month_idx()

        with self._db() as conn:
            row = conn.execute(
                """
                SELECT monthly_fee, paid_until_month
                FROM payment_state
                WHERE router_id = ? AND secret_name = ?
                """,
                (router.id, secret_name),
            ).fetchone()

            if row is None:
                monthly_fee = 0.0
                paid_until_idx = start_idx - 1
                conn.execute(
                    """
                    INSERT INTO payment_state (router_id, secret_name, monthly_fee, paid_until_month)
                    VALUES (?, ?, 0, NULL)
                    """,
                    (router.id, secret_name),
                )
            else:
                monthly_fee = float(row["monthly_fee"] or 0)
                paid_idx_candidate = self._month_index_from_str(row["paid_until_month"])
                paid_until_idx = start_idx - 1 if paid_idx_candidate is None else max(paid_idx_candidate, start_idx - 1)

            old_paid_until_idx = paid_until_idx

            if action == "pay":
                if target_month:
                    target_idx = self._month_index_from_str(target_month)
                    if target_idx is None:
                        raise CoreError("Format target_month harus YYYY-MM")
                    if target_idx < start_idx:
                        raise CoreError(
                            f"Bulan pembayaran minimal {self._month_label(start_idx)}"
                        )
                    if target_idx > current_idx:
                        raise CoreError(
                            f"Bulan pembayaran maksimal {self._month_label(current_idx)}"
                        )
                    paid_until_idx = max(paid_until_idx, target_idx)
                else:
                    # Bayar 1 bulan per klik, maju berurutan mulai bulan awal tagihan.
                    next_idx = paid_until_idx + 1
                    if next_idx < start_idx:
                        next_idx = start_idx
                    if next_idx <= current_idx:
                        paid_until_idx = next_idx
            elif action == "pay_full":
                if current_idx >= start_idx:
                    paid_until_idx = max(paid_until_idx, current_idx)
            elif action == "cancel_pay":
                if paid_until_idx >= start_idx:
                    paid_until_idx -= 1

            paid_until_month = self._month_str_from_index(paid_until_idx) if paid_until_idx >= start_idx else None
            conn.execute(
                """
                UPDATE payment_state
                SET paid_until_month = ?, updated_at = CURRENT_TIMESTAMP
                WHERE router_id = ? AND secret_name = ?
                """,
                (paid_until_month, router.id, secret_name),
            )

            if action == "pay" and target_month:
                target_idx = self._month_index_from_str(target_month)
                if target_idx is not None:
                    action_text = f"Bayar sampai {self._month_label(target_idx)}"
                else:
                    action_text = "Bayar"
            elif action == "pay":
                action_text = "Bayar satu bulan"
            elif action == "pay_full":
                action_text = "Bayar sampai bulan ini"
            else:
                action_text = "Batal bayar (mundur satu bulan)"
            old_label = self._month_label(old_paid_until_idx) if old_paid_until_idx >= start_idx else "-"
            new_label = self._month_label(paid_until_idx) if paid_until_idx >= start_idx else "-"
            self._insert_payment_audit(
                conn=conn,
                router=router,
                secret_name=secret_name,
                action=action,
                detail=f"{action_text}: paid_until {old_label} -> {new_label}",
            )

        return self._build_payment_summary(
            secret_name=secret_name,
            secret_profile=secret_profile,
            monthly_fee=monthly_fee,
            paid_until_month=paid_until_month,
            start_idx=start_idx,
        )

    def list_payment_logs(self, router_id: int, limit: int = 300) -> List[Dict[str, Any]]:
        with self._db() as conn:
            rows = conn.execute(
                """
                SELECT id, router_label, router_ip, secret_name, action, detail, created_at
                FROM payment_audit
                WHERE router_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (router_id, int(limit)),
            ).fetchall()
        return [dict(row) for row in rows]

    def _extract_active_id(self, item: Dict[str, Any]) -> str:
        if not isinstance(item, dict):
            return ""
        candidate = item.get(".id")
        if candidate is None:
            candidate = item.get("id")
        return str(candidate or "").strip()

    def _remove_active_by_name(self, api, username: str) -> int:
        active_resource = api.get_resource("/ppp/active")
        target = str(username or "").strip()
        if not target:
            return 0
        active_list = active_resource.get()
        removed = 0
        for item in active_list:
            current_name = str(item.get("name") or "").strip()
            if current_name != target:
                continue
            item_id = self._extract_active_id(item)
            if not item_id:
                continue
            try:
                active_resource.remove(id=item_id)
            except Exception:
                # Kompatibilitas: sebagian ROS/API menerima argumen "numbers".
                active_resource.remove(numbers=item_id)
            removed += 1
        return removed

    def disconnect_active(self, router_id: int, active_id: Optional[str] = None, username: Optional[str] = None) -> int:
        if not active_id and not username:
            raise CoreError("Butuh active_id atau username untuk disconnect")

        router = self.get_router(router_id)
        with self._api(router) as api:
            active_resource = api.get_resource("/ppp/active")
            removed = 0
            clean_active_id = str(active_id or "").strip()
            clean_username = str(username or "").strip()

            if clean_active_id:
                try:
                    active_resource.remove(id=clean_active_id)
                except Exception:
                    active_resource.remove(numbers=clean_active_id)
                removed += 1

            if clean_username:
                removed += self._remove_active_by_name(api, clean_username)

        return removed

    def add_secret(
        self,
        router_id: int,
        username: str,
        password: str,
        profile: str,
        local_address: str = "",
        remote_address: str = "",
    ) -> None:
        router = self.get_router(router_id)
        username = username.strip()
        profile_value = profile.strip()
        local_addr = (local_address or "").strip()
        remote_addr = (remote_address or "").strip()
        with self._api(router) as api:
            secret_resource = api.get_resource("/ppp/secret")
            payload: Dict[str, Any] = {
                "name": username,
                "password": password,
                "profile": profile_value,
                "service": "pppoe",
                "disabled": "no",
            }
            if local_addr:
                payload["local-address"] = local_addr
            if remote_addr:
                payload["remote-address"] = remote_addr
            secret_resource.add(**payload)
        with self._db() as conn:
            conn.execute(
                """
                INSERT INTO payment_state (router_id, secret_name, monthly_fee, paid_until_month)
                VALUES (?, ?, 0, NULL)
                ON CONFLICT(router_id, secret_name) DO NOTHING
                """,
                (router.id, username),
            )

    def edit_secret(
        self,
        router_id: int,
        current_name: str,
        new_name: str,
        new_password: str,
        new_profile: str,
        new_local_address: Optional[str] = None,
        new_remote_address: Optional[str] = None,
    ) -> None:
        router = self.get_router(router_id)

        with self._api(router) as api:
            secret_resource = api.get_resource("/ppp/secret")
            target = secret_resource.get(name=current_name)
            if not target:
                raise CoreError(f"User secret '{current_name}' tidak ditemukan")

            updates: Dict[str, Any] = {}
            new_name = (new_name or "").strip()
            new_profile = (new_profile or "").strip()

            if new_name and new_name != current_name:
                updates["name"] = new_name
            if new_password:
                updates["password"] = new_password
            if new_profile:
                updates["profile"] = new_profile
            if new_local_address is not None:
                local_address_value = (new_local_address or "").strip()
                if local_address_value:
                    updates["local-address"] = local_address_value
            if new_remote_address is not None:
                remote_address_value = (new_remote_address or "").strip()
                if remote_address_value:
                    updates["remote-address"] = remote_address_value

            if updates:
                secret_resource.set(numbers=current_name, **updates)

            usernames_to_disconnect = {current_name}
            if updates.get("name"):
                usernames_to_disconnect.add(str(updates["name"]))

            for username in usernames_to_disconnect:
                self._remove_active_by_name(api, username)

        if updates.get("name"):
            new_secret_name = str(updates["name"])
            with self._db() as conn:
                conn.execute(
                    """
                    UPDATE payment_state
                    SET secret_name = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE router_id = ? AND secret_name = ?
                    """,
                    (new_secret_name, router.id, current_name),
                )

    def remove_secret(self, router_id: int, username: str) -> None:
        router = self.get_router(router_id)
        with self._api(router) as api:
            secret_resource = api.get_resource("/ppp/secret")
            target = secret_resource.get(name=username)
            if not target:
                raise CoreError(f"User secret '{username}' tidak ditemukan")

            self._remove_active_by_name(api, username)
            secret_resource.remove(numbers=username)
        with self._db() as conn:
            conn.execute(
                "DELETE FROM payment_state WHERE router_id = ? AND secret_name = ?",
                (router.id, username),
            )

    def set_secret_disabled(self, router_id: int, username: str, disabled: bool = True) -> None:
        router = self.get_router(router_id)
        with self._api(router) as api:
            secret_resource = api.get_resource("/ppp/secret")
            target = secret_resource.get(name=username)
            if not target:
                raise CoreError(f"User secret '{username}' tidak ditemukan")
            secret_resource.set(numbers=username, disabled="yes" if disabled else "no")

            if disabled:
                self._remove_active_by_name(api, username)
