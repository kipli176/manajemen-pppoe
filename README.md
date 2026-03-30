# BayarInternet - Manajemen PPPoE & Billing Router MikroTik

Aplikasi Flask untuk membantu pemilik router mengelola:
- Active connection PPP
- PPP secret (tambah, edit, disable, hapus)
- Rekap pembayaran pelanggan
- Audit log pembayaran
- Billing bulanan per router

Sistem menggunakan:
- API RouterOS (`8728`) untuk manajemen router pemilik
- SQLite lokal (`pppoe_manager.db`)
- Integrasi REST API ROS7 (server pusat) untuk otomasi L2TP dan remote NAT (WebFig/Winbox)

## Struktur Proyek

- `main.py`: route web, auth, halaman admin/public/billing
- `core.py`: logika utama (DB, RouterOS API, billing, notifikasi)
- `templates/`: semua template Jinja2
- `static/`: CSS/JS/assets
- `docker-compose.yml`: runtime container
- `requirements.txt`: dependency Python

## Prasyarat

- Python `3.11+`
- MikroTik yang bisa diakses API (`8728`)
- (Opsional) Docker + Docker Compose

## Instalasi Lokal (Python)

1. Clone repo:
```bash
git clone https://github.com/kipli176/manajemen-pppoe.git
cd manajemen-pppoe
```

2. Buat virtualenv dan install dependency:
```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

3. Siapkan konfigurasi `.env`:
```bash
copy .env.example .env
```

4. Edit `.env` lalu isi credential sesuai server Anda.

5. Jalankan aplikasi:
```bash
python main.py
```

6. Buka:
- `http://localhost:5000`

## Instalasi Docker Compose

`docker-compose.yml` memakai network eksternal `cloudflared`.

Jika belum ada, buat sekali:
```bash
docker network create cloudflared
```

Lalu jalankan:
```bash
docker compose up -d
```

Aplikasi akan tersedia di:
- `http://localhost:5000`

## Login Default

- Administrator (Basic Auth):
  - Username: `admin`
  - Password: `password`

Endpoint admin:
- `http://localhost:5000/administrator`

## Environment Variable Penting

### Security
- `FLASK_SECRET_KEY` (default: `pppoe-local-secret`)
- `ADMIN_USERNAME` (default: `admin`)
- `ADMIN_PASSWORD` (default: `password`)
- `SESSION_COOKIE_SECURE` (`0`/`1`, default: `0`)
- `BILLING_LOGIN_MAX_AGE_SECONDS` (default: `604800`, 7 hari)

### ROS7 REST (server pusat)
- `ROS7_REST_BASE_URL` (default: `https://server.kipli.net/rest`)
- `ROS7_REST_USERNAME` (default: `kipli`)
- `ROS7_REST_PASSWORD` (default: `rahasia`)
- `ROS7_REST_TIMEOUT_SECONDS` (default: `20`)
- `ROS7_REST_VERIFY_SSL` (`0`/`1`, default: `1`)

### L2TP Default
- `ROS7_L2TP_SECRET_PROFILE` (default: `default`)
- `ROS7_L2TP_DEFAULT_PASSWORD` (default: `1234`)
- `ROS7_L2TP_CONNECT_TO` (default: `server.kipli.net`)
- `ROS7_L2TP_LOCAL_ADDRESS` (default: `10.168.0.1`)
- `ROS7_L2TP_REMOTE_NETWORK` (default: `10.168.0.0/24`)
- `ROS7_L2TP_USERNAME_PREFIX` (default: `billing`)

### Proxy Port Remote
- `WEBFIG_PROXY_HOST` (default: `server.kipli.net`)
- `WEBFIG_PROXY_PREFIX` (default: `6000`)
- `ROS7_WEBFIG_TO_PORT` (default: `80`)
- `WINBOX_PROXY_HOST` (default: ikut `WEBFIG_PROXY_HOST`)
- `WINBOX_PROXY_PREFIX` (default: `7000`)
- `ROS7_WINBOX_TO_PORT` (default: `8291`)

## Alur Singkat Penggunaan

1. Pemilik router daftar dari halaman register billing.
2. Sistem membuat akun L2TP ke server pusat + simpan data router.
3. Pemilik login billing per router.
4. Sistem sinkron data active/secret/payment.
5. Jika billing router belum lunas, menu tertentu dikunci sampai admin menandai bayar.

## Catatan Operasional

- Database lokal: `pppoe_manager.db` (backup file ini secara berkala).
- `web.py` sudah di-ignore dan tidak dipakai untuk runtime utama.
- Mode debug saat ini aktif di `main.py` (`debug=True`) untuk development.

## Rekomendasi Produksi

- Ganti `ADMIN_PASSWORD` dan `FLASK_SECRET_KEY`.
- Jalankan di reverse proxy HTTPS.
- Set `SESSION_COOKIE_SECURE=1`.
- Batasi akses endpoint administrator.
