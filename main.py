import os
import re
import secrets
import time
import ipaddress
from datetime import timedelta
from datetime import datetime
from functools import wraps
from typing import Any, Callable, Dict, List, Optional

from flask import (
    Flask,
    Response,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from core import (
    CoreError,
    MikroTikCore,
    ROUTER_BASE_FEE_PER_USER,
    ROUTER_BILLING_DAY,
    ROS7_WEBFIG_PROXY_HOST,
    ROS7_WEBFIG_PORT_PREFIX,
    ROS7_WINBOX_PROXY_HOST,
    ROS7_WINBOX_PORT_PREFIX,
    ROS7_WINBOX_TO_PORT,
)


app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "pppoe-local-secret")
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = os.environ.get("SESSION_COOKIE_SECURE", "0") == "1"
app.config["MAX_CONTENT_LENGTH"] = 2 * 1024 * 1024
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=7)

core = MikroTikCore()

ADMIN_USERNAME = os.environ.get("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "password")
WEBFIG_PROXY_HOST = ROS7_WEBFIG_PROXY_HOST
WEBFIG_PROXY_PREFIX = int(ROS7_WEBFIG_PORT_PREFIX)
WINBOX_PROXY_HOST = ROS7_WINBOX_PROXY_HOST
WINBOX_PROXY_PREFIX = int(ROS7_WINBOX_PORT_PREFIX)
WINBOX_ROUTER_PORT = int(ROS7_WINBOX_TO_PORT)
FORM_HONEYPOT_FIELD = "website"

_RATE_LIMIT_STORE: Dict[str, List[float]] = {}
BILLING_LOGIN_MAX_AGE_SECONDS = int(os.environ.get("BILLING_LOGIN_MAX_AGE_SECONDS", str(7 * 24 * 60 * 60)))
LANDING_COPY: Dict[str, str] = {
    "hero_title": "Kelola Router & Billing Lebih Mudah dengan BayarInternet",
    "hero_subtitle": (
        "Dirancang untuk pemilik router yang ingin operasional lebih cepat, "
        "penagihan lebih rapi, dan tim lebih fokus ke layanan pelanggan."
    ),
    "cta_primary": "Mulai Registrasi Router",
    "cta_secondary": "Lihat Portal Client",
}


def _json_error(message: str, status_code: int = 400):
    return jsonify({"ok": False, "message": message}), status_code


def _admin_auth_failed() -> Response:
    return Response(
        "Authentication required",
        401,
        {"WWW-Authenticate": 'Basic realm="Administrator"'},
    )


def _client_ip() -> str:
    forwarded = str(request.headers.get("X-Forwarded-For") or "").strip()
    if forwarded:
        return forwarded.split(",")[0].strip()
    return str(request.remote_addr or "unknown")


def _get_form_csrf_token() -> str:
    token = str(session.get("_form_csrf_token") or "")
    if not token:
        token = secrets.token_urlsafe(32)
        session["_form_csrf_token"] = token
    return token


def _is_valid_form_csrf() -> bool:
    sent = str(request.form.get("_csrf_token") or "")
    saved = str(session.get("_form_csrf_token") or "")
    if not sent or not saved:
        return False
    return secrets.compare_digest(sent, saved)


def _cleanup_rate_limit(key: str, window_seconds: int) -> List[float]:
    now = time.time()
    valid = [
        ts for ts in _RATE_LIMIT_STORE.get(key, [])
        if (now - float(ts)) <= float(window_seconds)
    ]
    _RATE_LIMIT_STORE[key] = valid
    return valid


def _is_rate_limited(key: str, max_attempts: int, window_seconds: int) -> bool:
    return len(_cleanup_rate_limit(key, window_seconds)) >= int(max_attempts)


def _register_rate_attempt(key: str, window_seconds: int) -> None:
    _cleanup_rate_limit(key, window_seconds)
    items = _RATE_LIMIT_STORE.get(key, [])
    items.append(time.time())
    _RATE_LIMIT_STORE[key] = items


def _rate_limit_retry_after(key: str, max_attempts: int, window_seconds: int) -> int:
    valid = _cleanup_rate_limit(key, window_seconds)
    if len(valid) < int(max_attempts):
        return 0
    oldest = min(valid)
    wait = int(window_seconds - (time.time() - oldest))
    return max(1, wait)


def _validate_router_register_values(
    label: str,
    address: str,
    wa_number: str,
) -> Optional[str]:
    if not label:
        return "Nama POP wajib diisi."
    if len(label) > 80:
        return "Nama POP maksimal 80 karakter."
    if not address:
        return "Alamat wajib diisi."
    if len(address) > 160:
        return "Alamat maksimal 160 karakter."
    wa_digits = re.sub(r"\D+", "", wa_number or "")
    if len(wa_digits) < 9 or len(wa_digits) > 16:
        return "Format nomor WA tidak valid."
    return None


def admin_required(view_func: Callable):
    @wraps(view_func)
    def wrapped(*args, **kwargs):
        if session.get("admin_logged_in"):
            return view_func(*args, **kwargs)

        auth = request.authorization
        if not auth:
            return _admin_auth_failed()
        if auth.username != ADMIN_USERNAME or auth.password != ADMIN_PASSWORD:
            return _admin_auth_failed()
        session["admin_logged_in"] = True
        return view_func(*args, **kwargs)

    return wrapped


def _billing_session_key(router_id: int) -> str:
    return f"billing_auth_router_{int(router_id)}"


def _billing_session_time_key(router_id: int) -> str:
    return f"billing_auth_router_{int(router_id)}_ts"


def _is_billing_authenticated(router_id: int) -> bool:
    auth_key = _billing_session_key(router_id)
    ts_key = _billing_session_time_key(router_id)
    if not bool(session.get(auth_key)):
        return False
    ts_raw = session.get(ts_key)
    try:
        ts_value = int(ts_raw)
    except (TypeError, ValueError):
        session.pop(auth_key, None)
        session.pop(ts_key, None)
        return False
    now_ts = int(time.time())
    if now_ts - ts_value > BILLING_LOGIN_MAX_AGE_SECONDS:
        session.pop(auth_key, None)
        session.pop(ts_key, None)
        return False
    return True


def require_billing_login(view_func: Callable):
    @wraps(view_func)
    def wrapped(router_id: int, *args, **kwargs):
        if not _is_billing_authenticated(router_id):
            return redirect(url_for("billing_login", router_id=router_id))
        return view_func(router_id=router_id, *args, **kwargs)

    return wrapped


def require_billing_login_json(view_func: Callable):
    @wraps(view_func)
    def wrapped(router_id: int, *args, **kwargs):
        if not _is_billing_authenticated(router_id):
            return _json_error("Silakan login billing router dulu", 401)
        return view_func(router_id=router_id, *args, **kwargs)

    return wrapped


def require_router_paid_json(view_func: Callable):
    @wraps(view_func)
    def wrapped(router_id: int, *args, **kwargs):
        try:
            is_paid = core.is_router_paid_current_cycle(router_id)
        except CoreError as exc:
            return _json_error(str(exc), 500)
        if not is_paid:
            return _json_error(
                "Menu terkunci. Tagihan router belum lunas untuk cycle ini.",
                403,
            )
        return view_func(router_id=router_id, *args, **kwargs)

    return wrapped


def _parse_port(raw_value: str, default: int = 8728) -> int:
    value = (raw_value or "").strip()
    if not value:
        return default
    port = int(value)
    if port < 1 or port > 65535:
        raise ValueError("Port harus 1-65535")
    return port


def _build_proxy_port(ip_value: str, prefix: int) -> Optional[int]:
    parts = str(ip_value or "").strip().split(".")
    if len(parts) != 4:
        return None
    try:
        last_octet = int(parts[-1])
    except ValueError:
        return None
    if last_octet < 0 or last_octet > 255:
        return None
    port = int(prefix) + last_octet
    if port < 1 or port > 65535:
        return None
    return port


def _build_webfig_url(ip_value: str) -> Optional[str]:
    port = _build_proxy_port(ip_value, WEBFIG_PROXY_PREFIX)
    if port is None:
        return None
    return f"http://{WEBFIG_PROXY_HOST}:{port}"


def _build_winbox_endpoint(ip_value: str) -> Optional[str]:
    port = _build_proxy_port(ip_value, WINBOX_PROXY_PREFIX)
    if port is None:
        return None
    return f"{WINBOX_PROXY_HOST}:{port}"


def _parse_uptime_seconds(raw_uptime: Any) -> Optional[int]:
    text = str(raw_uptime or "").strip()
    if not text:
        return None

    colon_match = re.match(r"^(?:(\d+)w)?(?:(\d+)d)?(\d{1,2}):(\d{2}):(\d{2})$", text, re.I)
    if colon_match:
        weeks = int(colon_match.group(1) or 0)
        days = int(colon_match.group(2) or 0)
        hours = int(colon_match.group(3) or 0)
        minutes = int(colon_match.group(4) or 0)
        seconds = int(colon_match.group(5) or 0)
        return (weeks * 7 * 24 * 3600) + (days * 24 * 3600) + (hours * 3600) + (minutes * 60) + seconds

    total_seconds = 0
    found = False
    unit_map = {"w": 7 * 24 * 3600, "d": 24 * 3600, "h": 3600, "m": 60, "s": 1}
    tokens = re.findall(r"(\d+)([wdhms])", text, re.I)
    for value_text, unit_code in tokens:
        value = int(value_text)
        factor = unit_map.get(unit_code.lower())
        if value and factor:
            total_seconds += value * factor
            found = True
    return total_seconds if found else None


def _format_uptime_readable(raw_uptime: Any) -> str:
    total = _parse_uptime_seconds(raw_uptime)
    if total is None:
        return "-"
    if total <= 0:
        return "0s"

    weeks = total // (7 * 24 * 3600)
    total = total % (7 * 24 * 3600)
    days = total // (24 * 3600)
    total = total % (24 * 3600)
    hours = total // 3600
    total = total % 3600
    minutes = total // 60
    seconds = total % 60

    parts: List[str] = []
    if weeks:
        parts.append(f"{weeks}w")
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}j")
    elif (weeks or days) and (minutes or seconds):
        parts.append("0j")
    if minutes:
        parts.append(f"{minutes}m")
    if seconds and len(parts) < 2:
        parts.append(f"{seconds}s")
    if not parts:
        parts.append("0s")
    return " ".join(parts[:3])


def _format_active_since(raw_uptime: Any) -> str:
    total = _parse_uptime_seconds(raw_uptime)
    if total is None:
        return "-"
    started_at = datetime.now() - timedelta(seconds=total)
    month_map = ["Jan", "Feb", "Mar", "Apr", "Mei", "Jun", "Jul", "Agu", "Sep", "Okt", "Nov", "Des"]
    month_label = month_map[started_at.month - 1]
    return f"{started_at.day:02d} {month_label} {started_at.year} {started_at.hour:02d}:{started_at.minute:02d}"


def _home_router_rows() -> List[Dict[str, Any]]:
    rows = []
    for idx, item in enumerate(core.list_routers(), start=1):
        row = dict(item)
        row["no"] = idx
        is_online = bool(int(row.get("has_router_auth") or 0))
        row["is_online"] = bool(is_online)
        row["status_label"] = "Online" if is_online else "Offline"
        row["status_emoji"] = "\U0001F7E2" if is_online else "\u26AA"
        rows.append(row)
    return rows


def _get_landing_copy() -> Dict[str, str]:
    return dict(LANDING_COPY)


def _build_billing_login_context(router_id: int) -> Dict[str, Any]:
    try:
        status = core.get_router_l2tp_status(router_id)
    except CoreError as exc:
        router = core.get_router(router_id)
        status = {
            "router_id": int(router.id),
            "router_label": router.label,
            "is_online": False,
            "connect_to": "server.kipli.net",
            "username": str(router.l2tp_username or "").strip(),
            "password": str(router.l2tp_password or "").strip(),
            "local_address": "10.168.0.1",
            "remote_address": str(router.ip or "").strip(),
            "active_uptime": "",
            "active_address": "",
            "check_error": str(exc),
        }
    return {
        "l2tp_status": status,
        "login_blocked": not bool(status.get("is_online")),
    }


@app.context_processor
def inject_common_context():
    return {
        "now_year": datetime.now().year,
        "is_admin_logged_in": bool(session.get("admin_logged_in")),
        "form_csrf_token": _get_form_csrf_token(),
    }


@app.get("/")
def root():
    return render_template(
        "home.html",
        title="BayarInternet",
        routers=_home_router_rows(),
        landing_copy=_get_landing_copy(),
        base_fee_per_user=ROUTER_BASE_FEE_PER_USER,
        router_billing_day=ROUTER_BILLING_DAY,
    )


@app.get("/monitor/<int:router_id>")
def public_monitor(router_id: int):
    try:
        router = core.get_router(router_id)
    except CoreError:
        return render_template(
            "monitor.html",
            title="Monitoring",
            router=None,
            active_items=[],
            active_error="Router tidak ditemukan di database.",
        )

    active_items: List[Dict[str, Any]] = []
    active_error: Optional[str] = None
    try:
        active_items = core.list_active(router_id)
        active_items.sort(key=lambda item: str(item.get("name", "")))
        for item in active_items:
            item["uptime_readable"] = _format_uptime_readable(item.get("uptime"))
            item["active_since"] = _format_active_since(item.get("uptime"))
    except CoreError as exc:
        active_error = str(exc)

    return render_template(
        "monitor.html",
        title=f"Monitoring {router.label}",
        router=router,
        active_items=active_items,
        active_error=active_error,
    )


@app.get("/billing/login/<int:router_id>")
def billing_login(router_id: int):
    if _is_billing_authenticated(router_id):
        return redirect(url_for("billing_dashboard", router_id=router_id))

    try:
        router = core.get_router(router_id)
    except CoreError:
        return redirect(url_for("root"))

    extra = _build_billing_login_context(router_id)
    return render_template(
        "billing_login.html",
        title="Login Billing",
        router=router,
        login_error=None,
        **extra,
    )


@app.post("/billing/login/<int:router_id>")
def billing_login_post(router_id: int):
    rate_key = f"billing_login:{router_id}:{_client_ip()}"
    max_attempts = 10
    window_seconds = 300
    if _is_rate_limited(rate_key, max_attempts=max_attempts, window_seconds=window_seconds):
        wait_seconds = _rate_limit_retry_after(rate_key, max_attempts=max_attempts, window_seconds=window_seconds)
        try:
            router = core.get_router(router_id)
        except CoreError:
            return redirect(url_for("root"))
        extra = _build_billing_login_context(router_id)
        return render_template(
            "billing_login.html",
            title="Login Billing",
            router=router,
            login_error=f"Terlalu banyak percobaan login. Coba lagi dalam {wait_seconds} detik.",
            **extra,
        )

    try:
        router = core.get_router(router_id)
    except CoreError:
        return redirect(url_for("root"))

    if str(request.form.get(FORM_HONEYPOT_FIELD) or "").strip():
        extra = _build_billing_login_context(router_id)
        return render_template(
            "billing_login.html",
            title="Login Billing",
            router=router,
            login_error="Permintaan tidak valid.",
            **extra,
        )

    if not _is_valid_form_csrf():
        _register_rate_attempt(rate_key, window_seconds=window_seconds)
        extra = _build_billing_login_context(router_id)
        return render_template(
            "billing_login.html",
            title="Login Billing",
            router=router,
            login_error="Token form tidak valid. Silakan refresh halaman lalu coba lagi.",
            **extra,
        )

    extra = _build_billing_login_context(router_id)
    if extra.get("login_blocked"):
        return render_template(
            "billing_login.html",
            title="Login Billing",
            router=router,
            login_error="Router belum online. Harap koneksikan L2TP terlebih dahulu.",
            **extra,
        )

    username = (request.form.get("username") or "").strip()
    password = request.form.get("password") or ""
    try:
        core.login_owner_and_sync_router_credentials(
            router_id=router_id,
            username=username,
            password=password,
        )
        session[_billing_session_key(router_id)] = True
        session[_billing_session_time_key(router_id)] = int(time.time())
        session["billing_login_ok"] = "Login berhasil. Data user & tagihan tersinkron dari router terbaru."
        session.permanent = True
        _RATE_LIMIT_STORE.pop(rate_key, None)
        session["_form_csrf_token"] = secrets.token_urlsafe(32)
        return redirect(url_for("billing_dashboard", router_id=router_id))
    except CoreError as exc:
        _register_rate_attempt(rate_key, window_seconds=window_seconds)
        return render_template(
            "billing_login.html",
            title="Login Billing",
            router=router,
            login_error=str(exc),
            **extra,
        )


@app.get("/billing/register")
def billing_register():
    return render_template(
        "billing_register.html",
        title="Daftar Router Billing",
        register_error=None,
        register_ok=None,
        register_data=None,
    )


@app.post("/billing/register")
def billing_register_post():
    rate_key = f"billing_register:{_client_ip()}"
    max_attempts = 6
    window_seconds = 900
    if _is_rate_limited(rate_key, max_attempts=max_attempts, window_seconds=window_seconds):
        wait_seconds = _rate_limit_retry_after(rate_key, max_attempts=max_attempts, window_seconds=window_seconds)
        return render_template(
            "billing_register.html",
            title="Daftar Router Billing",
            register_error=f"Terlalu banyak percobaan pendaftaran. Coba lagi dalam {wait_seconds} detik.",
            register_ok=None,
        )

    if str(request.form.get(FORM_HONEYPOT_FIELD) or "").strip():
        return render_template(
            "billing_register.html",
            title="Daftar Router Billing",
            register_error=None,
            register_ok="Permintaan pendaftaran diterima.",
        )

    if not _is_valid_form_csrf():
        _register_rate_attempt(rate_key, window_seconds=window_seconds)
        return render_template(
            "billing_register.html",
            title="Daftar Router Billing",
            register_error="Token form tidak valid. Silakan refresh halaman lalu coba lagi.",
            register_ok=None,
        )

    label = (request.form.get("label") or "").strip()
    address = (request.form.get("address") or "").strip()
    wa_number = (request.form.get("wa_number") or "").strip()

    form_error = _validate_router_register_values(
        label=label,
        address=address,
        wa_number=wa_number,
    )
    if form_error:
        _register_rate_attempt(rate_key, window_seconds=window_seconds)
        return render_template(
            "billing_register.html",
            title="Daftar Router Billing",
            register_error=form_error,
            register_ok=None,
            register_data=None,
        )

    try:
        new_router_id = 0
        provision = core.register_and_provision_l2tp(
            label=label,
            address=address,
            wa_number=wa_number,
            router_ip="",
        )
        router_ip_raw = str(provision.get("remote_address") or "").strip()
        router_ip = router_ip_raw.split("/", 1)[0].strip()
        router_port = 8728
        router_username = str(provision.get("l2tp_username") or "").strip()
        router_password = str(provision.get("l2tp_password") or "")
        if not router_ip or not router_username or not router_password:
            raise CoreError("Detail akun L2TP tidak lengkap untuk menyimpan router.")
        try:
            ipaddress.ip_address(router_ip)
        except ValueError as exc:
            raise CoreError(f"Remote address L2TP tidak valid untuk IP router: {router_ip_raw}") from exc
        if core.router_exists(ip=router_ip, port=router_port):
            raise CoreError(
                f"IP remote {router_ip}:{router_port} sudah ada di data router. Coba daftar ulang dengan label POP lain."
            )
        try:
            new_router_id = core.add_router(
                label=label,
                address=address,
                ip=router_ip,
                port=router_port,
                username="",
                password="",
                wa_number=wa_number,
                l2tp_username=router_username,
                l2tp_password=router_password,
                l2tp_secret_id=str(provision.get("secret_id") or "").strip(),
                run_initial_billing_sync=False,
            )
        except Exception as db_exc:
            # Rollback provisioning L2TP/NAT bila simpan router gagal.
            secret_id = str(provision.get("secret_id") or "").strip()
            rollback_ip = str(provision.get("remote_address") or "").split("/", 1)[0].strip()
            if secret_id:
                try:
                    core.delete_ros7_secret(secret_id)
                except Exception:
                    pass
            if rollback_ip:
                try:
                    core.delete_ros7_webfig_dstnat_by_router_ip(rollback_ip)
                except Exception:
                    pass
                try:
                    core.delete_ros7_winbox_dstnat_by_router_ip(rollback_ip)
                except Exception:
                    pass
            raise CoreError(f"Gagal simpan data router ke database: {db_exc}")

        _RATE_LIMIT_STORE.pop(rate_key, None)
        session["_form_csrf_token"] = secrets.token_urlsafe(32)

        notify_note = []
        if not provision.get("owner_notified"):
            notify_note.append(f"WA pemilik gagal: {provision.get('owner_error') or '-'}")
        if not provision.get("admin_notified"):
            notify_note.append(f"WA admin gagal: {provision.get('admin_error') or '-'}")
        notify_suffix = ""
        if notify_note:
            notify_suffix = " | Notifikasi: " + " ; ".join(notify_note)

        return render_template(
            "billing_register.html",
            title="Daftar Router Billing",
            register_error=None,
            register_ok="Registrasi berhasil. Akun L2TP aktif dan data router tersimpan.",
            register_data={
                "connect_to": provision.get("connect_to"),
                "username": provision.get("l2tp_username"),
                "password": provision.get("l2tp_password"),
                "local_address": provision.get("local_address"),
                "remote_address": provision.get("remote_address"),
                "profile": provision.get("l2tp_profile"),
                "webfig_port": provision.get("webfig_port"),
                "webfig_url": provision.get("webfig_url"),
                "winbox_port": provision.get("winbox_port"),
                "winbox_endpoint": provision.get("winbox_endpoint"),
                "router_id": int(new_router_id) if int(new_router_id) > 0 else None,
                "note": notify_suffix.lstrip(" |"),
            },
        )
    except CoreError as exc:
        _register_rate_attempt(rate_key, window_seconds=window_seconds)
        return render_template(
            "billing_register.html",
            title="Daftar Router Billing",
            register_error=f"Gagal memproses registrasi: {exc}",
            register_ok=None,
            register_data=None,
        )


@app.get("/billing/logout/<int:router_id>")
def billing_logout(router_id: int):
    session.pop(_billing_session_key(router_id), None)
    session.pop(_billing_session_time_key(router_id), None)
    return redirect(url_for("billing_login", router_id=router_id))


@app.get("/administrator/logout")
def administrator_logout():
    session.pop("admin_logged_in", None)
    return redirect(url_for("root"))


@app.get("/billing/dashboard/<int:router_id>")
@require_billing_login
def billing_dashboard(router_id: int):
    try:
        router = core.get_router(router_id)
        billing_summary = core.get_router_billing_summary(router_id)
    except CoreError:
        session.pop(_billing_session_key(router_id), None)
        session.pop(_billing_session_time_key(router_id), None)
        return redirect(url_for("root"))

    billing_locked = not bool(billing_summary.get("is_paid_current_cycle"))
    webfig_url = _build_webfig_url(router.ip)
    winbox_endpoint = _build_winbox_endpoint(router.ip)
    winbox_server_port = ""
    if winbox_endpoint and ":" in winbox_endpoint:
        winbox_server_port = winbox_endpoint.split(":", 1)[1]
    warning = (
        "Akses menu selain Active Connection dikunci sampai tagihan router lunas"
        if billing_locked
        else ""
    )

    return render_template(
        "billing_dashboard.html",
        title=f"Billing - {router.label}",
        router=router,
        webfig_url=webfig_url,
        winbox_endpoint=winbox_endpoint,
        winbox_server_host=WINBOX_PROXY_HOST,
        winbox_server_port=winbox_server_port,
        winbox_router_port=WINBOX_ROUTER_PORT,
        billing_locked=billing_locked,
        billing_warning=warning,
        billing_summary=billing_summary,
        billing_login_ok=session.pop("billing_login_ok", None),
    )


@app.get("/billing/api/<int:router_id>/active")
@require_billing_login_json
def billing_api_active(router_id: int):
    try:
        items = core.list_active(router_id)
        return jsonify({"ok": True, "items": items})
    except CoreError as exc:
        return _json_error(str(exc), 500)


@app.post("/billing/api/<int:router_id>/active/disconnect")
@require_billing_login_json
def billing_api_disconnect_active(router_id: int):
    data: Dict[str, Any] = request.get_json(silent=True) or {}
    active_id = str(data.get("id") or "").strip() or None
    username = str(data.get("name") or "").strip() or None
    if not active_id and not username:
        return _json_error("Butuh id atau name untuk disconnect")
    try:
        removed = core.disconnect_active(router_id, active_id=active_id, username=username)
        return jsonify({"ok": True, "removed": removed})
    except CoreError as exc:
        return _json_error(str(exc), 500)


@app.get("/billing/api/<int:router_id>/profiles")
@require_billing_login_json
@require_router_paid_json
def billing_api_profiles(router_id: int):
    try:
        items = core.list_profiles(router_id)
        return jsonify({"ok": True, "items": items})
    except CoreError as exc:
        return _json_error(str(exc), 500)


@app.get("/billing/api/<int:router_id>/secrets")
@require_billing_login_json
@require_router_paid_json
def billing_api_secrets(router_id: int):
    try:
        items = core.list_secrets(router_id)
        return jsonify({"ok": True, "items": items})
    except CoreError as exc:
        return _json_error(str(exc), 500)


@app.post("/billing/api/<int:router_id>/secrets/add")
@require_billing_login_json
@require_router_paid_json
def billing_api_secret_add(router_id: int):
    data: Dict[str, Any] = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    password = data.get("password") or ""
    profile = (data.get("profile") or "").strip()
    local_address = (data.get("local_address") or "").strip()
    remote_address = (data.get("remote_address") or "").strip()
    if not name or not password or not profile:
        return _json_error("name, password, profile wajib diisi")
    try:
        core.add_secret(
            router_id,
            name,
            password,
            profile,
            local_address=local_address,
            remote_address=remote_address,
        )
        return jsonify({"ok": True})
    except CoreError as exc:
        return _json_error(str(exc), 500)


@app.post("/billing/api/<int:router_id>/secrets/edit")
@require_billing_login_json
@require_router_paid_json
def billing_api_secret_edit(router_id: int):
    data: Dict[str, Any] = request.get_json(silent=True) or {}
    current_name = (data.get("current_name") or "").strip()
    new_name = (data.get("new_name") or "").strip()
    new_password = data.get("new_password") or ""
    new_profile = (data.get("new_profile") or "").strip()
    new_local_address = (data.get("new_local_address") or "").strip()
    new_remote_address = (data.get("new_remote_address") or "").strip()
    if not current_name or not new_name or not new_profile:
        return _json_error("current_name, new_name, new_profile wajib diisi")
    try:
        core.edit_secret(
            router_id,
            current_name,
            new_name,
            new_password,
            new_profile,
            new_local_address=new_local_address,
            new_remote_address=new_remote_address,
        )
        return jsonify({"ok": True})
    except CoreError as exc:
        return _json_error(str(exc), 500)


@app.post("/billing/api/<int:router_id>/secrets/disable")
@require_billing_login_json
@require_router_paid_json
def billing_api_secret_disable(router_id: int):
    data: Dict[str, Any] = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    if not name:
        return _json_error("name wajib diisi")
    disabled = bool(data.get("disabled", True))
    try:
        core.set_secret_disabled(router_id, name, disabled=disabled)
        return jsonify({"ok": True})
    except CoreError as exc:
        return _json_error(str(exc), 500)


@app.post("/billing/api/<int:router_id>/secrets/remove")
@require_billing_login_json
@require_router_paid_json
def billing_api_secret_remove(router_id: int):
    data: Dict[str, Any] = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    if not name:
        return _json_error("name wajib diisi")
    try:
        core.remove_secret(router_id, name)
        return jsonify({"ok": True})
    except CoreError as exc:
        return _json_error(str(exc), 500)


@app.get("/billing/api/<int:router_id>/payments")
@require_billing_login_json
@require_router_paid_json
def billing_api_payments(router_id: int):
    try:
        items = core.list_payments(router_id)
        settings = core.get_payment_preferences(router_id)
        return jsonify({"ok": True, "items": items, "settings": settings})
    except CoreError as exc:
        return _json_error(str(exc), 500)


@app.post("/billing/api/<int:router_id>/payments/fee")
@require_billing_login_json
@require_router_paid_json
def billing_api_payments_fee(router_id: int):
    data: Dict[str, Any] = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    if not name:
        return _json_error("name wajib diisi")
    try:
        monthly_fee = float(data.get("monthly_fee", 0))
    except (TypeError, ValueError):
        return _json_error("monthly_fee harus angka")
    try:
        core.update_payment_monthly_fee(router_id, name, monthly_fee)
        return jsonify({"ok": True})
    except CoreError as exc:
        return _json_error(str(exc), 500)


@app.post("/billing/api/<int:router_id>/payments/preferences")
@require_billing_login_json
@require_router_paid_json
def billing_api_payments_preferences(router_id: int):
    data: Dict[str, Any] = request.get_json(silent=True) or {}
    next_auto_close = bool(data.get("auto_close_unpaid_end_month", False))
    try:
        settings = core.set_payment_preferences(
            router_id,
            auto_close_unpaid_end_month=next_auto_close,
        )
        return jsonify({"ok": True, "settings": settings})
    except CoreError as exc:
        return _json_error(str(exc), 500)


@app.post("/billing/api/<int:router_id>/payments/action")
@require_billing_login_json
@require_router_paid_json
def billing_api_payments_action(router_id: int):
    data: Dict[str, Any] = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    action = (data.get("action") or "").strip()
    target_month = (data.get("target_month") or "").strip() or None
    if not name or not action:
        return _json_error("name dan action wajib diisi")
    try:
        item = core.apply_payment_action(
            router_id,
            name,
            action,
            target_month=target_month,
        )
        return jsonify({"ok": True, "item": item})
    except CoreError as exc:
        return _json_error(str(exc), 500)


@app.get("/billing/api/<int:router_id>/logs")
@require_billing_login_json
@require_router_paid_json
def billing_api_logs(router_id: int):
    try:
        items = core.list_payment_logs(router_id)
        return jsonify({"ok": True, "items": items})
    except CoreError as exc:
        return _json_error(str(exc), 500)


@app.post("/billing/api/<int:router_id>/request-qris")
@require_billing_login_json
def billing_api_request_qris(router_id: int):
    try:
        result = core.send_router_billing_notification(router_id, purpose="request_qris")
    except CoreError as exc:
        return _json_error(str(exc), 500)

    sent_admin = int(result.get("sent_admin", 0) or 0)
    if sent_admin <= 0:
        failed_items = result.get("failed") or []
        if failed_items:
            detail = "; ".join(
                f"{item.get('scope', 'target')} {item.get('target', '-')}: {item.get('error', '-')}"
                for item in failed_items
            )
        else:
            detail = "Nomor admin tidak tersedia atau gateway menolak request."
        return _json_error(f"Permintaan QRIS gagal dikirim ke admin. {detail}", 502)

    return jsonify({"ok": True, **result})


@app.get("/administrator")
@admin_required
def administrator():
    return render_template(
        "administrator.html",
        title="Administrator",
        routers=core.list_routers(),
        index_error=session.pop("index_error", None),
        index_ok=session.pop("index_ok", None),
    )


@app.post("/routers/add")
@admin_required
def add_router():
    session["index_error"] = "Penambahan router lewat administrator dinonaktifkan. Gunakan halaman registrasi."
    return redirect(url_for("administrator"))


@app.post("/routers/edit")
@admin_required
def edit_router():
    router_id = (request.form.get("router_id") or "").strip()
    label = (request.form.get("label") or "").strip()
    address = (request.form.get("address") or "").strip()
    ip = (request.form.get("ip") or "").strip()
    username = (request.form.get("username") or "").strip()
    password = request.form.get("password") or ""
    wa_number = (request.form.get("wa_number") or "").strip()

    if not router_id:
        session["index_error"] = "Router ID tidak valid."
        return redirect(url_for("administrator"))

    try:
        router_id_int = int(router_id)
        port = _parse_port(request.form.get("port", "8728"))
    except ValueError as exc:
        session["index_error"] = str(exc)
        return redirect(url_for("administrator"))

    if not label or not ip or not username:
        session["index_error"] = "Label, IP, dan username wajib diisi."
        return redirect(url_for("administrator"))
    if len(label) > 80:
        session["index_error"] = "Label maksimal 80 karakter."
        return redirect(url_for("administrator"))
    if len(username) > 80:
        session["index_error"] = "Username maksimal 80 karakter."
        return redirect(url_for("administrator"))
    try:
        ipaddress.ip_address(ip)
    except ValueError:
        session["index_error"] = "Format IP router tidak valid."
        return redirect(url_for("administrator"))

    try:
        router = core.get_router(router_id_int)
        final_password = password if password else router.password
        core.update_router(
            router_id=router_id_int,
            label=label,
            address=address,
            ip=ip,
            port=port,
            username=username,
            password=final_password,
            wa_number=wa_number,
        )
        session["index_ok"] = "Router berhasil diperbarui."
    except Exception as exc:
        session["index_error"] = f"Gagal edit router: {exc}"
    return redirect(url_for("administrator"))


@app.post("/routers/delete")
@admin_required
def delete_router_modal():
    router_id = (request.form.get("router_id") or "").strip()
    if not router_id:
        session["index_error"] = "Router ID tidak valid."
        return redirect(url_for("administrator"))
    try:
        result = core.delete_router(int(router_id), sync_l2tp_secret=True)
        if result.get("sync_attempted"):
            nat_webfig_deleted = int(result.get("nat_webfig_deleted") or 0)
            nat_winbox_deleted = int(result.get("nat_winbox_deleted") or 0)
            if result.get("l2tp_deleted"):
                session["index_ok"] = (
                    "Router berhasil dihapus, secret L2TP di server pusat juga terhapus. "
                    f"Rule NAT terhapus: WebFig {nat_webfig_deleted}, Winbox {nat_winbox_deleted}."
                )
            else:
                session["index_ok"] = (
                    "Router berhasil dihapus. Secret L2TP pusat tidak ditemukan (kemungkinan sudah terhapus sebelumnya). "
                    f"Rule NAT terhapus: WebFig {nat_webfig_deleted}, Winbox {nat_winbox_deleted}."
                )
        else:
            session["index_ok"] = "Router berhasil dihapus."
    except Exception as exc:
        session["index_error"] = f"Gagal hapus router: {exc}"
    return redirect(url_for("administrator"))


@app.post("/routers/pay")
@admin_required
def pay_router():
    router_id = (request.form.get("router_id") or "").strip()
    if not router_id:
        session["index_error"] = "Router ID tidak valid."
        return redirect(url_for("administrator"))
    try:
        summary = core.pay_router_current_cycle(int(router_id))
        session["index_ok"] = (
            f"Pembayaran router {summary.get('label', '')} berhasil. "
            f"Notifikasi terkirim: {summary.get('pay_notification_sent', 0)}."
        )
    except Exception as exc:
        session["index_error"] = f"Gagal proses pembayaran router: {exc}"
    return redirect(url_for("administrator"))


@app.post("/api/router-billing/check")
@admin_required
def api_router_billing_check():
    try:
        result = core.run_hourly_router_billing_check()
        return jsonify({"ok": True, **result})
    except Exception as exc:
        return _json_error(f"Gagal cek billing router: {exc}", 500)


@app.post("/api/router-billing/notify")
@admin_required
def api_router_billing_notify():
    data: Dict[str, Any] = request.get_json(silent=True) or {}
    router_id = data.get("router_id")
    try:
        router_id_int = int(router_id)
    except (TypeError, ValueError):
        return _json_error("router_id wajib angka")

    try:
        result = core.send_router_billing_notification(router_id_int, purpose="manual")
        if int(result.get("sent_total", 0)) <= 0:
            failed_items = result.get("failed") or []
            if failed_items:
                detail = "; ".join(
                    f"{item.get('scope', 'target')} {item.get('target', '-')}: {item.get('error', '-')}"
                    for item in failed_items
                )
            else:
                detail = "Tidak ada nomor tujuan valid."
            return _json_error(f"Notifikasi gagal dikirim. {detail}", 502)
        return jsonify({"ok": True, **result})
    except Exception as exc:
        return _json_error(f"Gagal kirim notifikasi: {exc}", 500)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
