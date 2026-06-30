# 🏬 Data Warehouse Mini Project — Sales Analytics

Membangun *data warehouse* **skema bintang** dari data transaksi penjualan: pipeline
ter-orkestrasi **Apache Airflow**, warehouse di **Neon PostgreSQL**, dan dashboard di **Metabase**.

> Implementasi mengikuti dokumen *mapping* **"(DE) Mapping & Metadata Data Mart"** — staging,
> dimensi, dan fact dibuat persis sesuai spek (urutan kolom, tipe data, SCD type, logika join).

---

## 📌 Ringkasan

| Aspek | Detail |
|---|---|
| Sumber | Data transaksi penjualan (CSV harian) |
| Warehouse | Neon PostgreSQL — database `dwh_project`, schema `stg` + `dwh` |
| Orkestrasi | Apache Airflow (DAG `dag_manuel`, connection `neon_manuel`) |
| Visualisasi | Metabase |
| **Total transaksi** | **99.051** |
| Rentang data | Jan – 9 Mei 2018 |

---

## 🧱 Arsitektur — satu project, dua source

```
  (1) CSV (zip, 2 tanggal) ──[Airflow]──┐
                                        ▼
                          Staging (stg) ─► Transform / SCD ─► Star Schema (dwh) ─► Metabase
                                        ▲                      fact_sales + 4 dimensi
  (2) MariaDB stg_categories ─[CDC/JDBC]┘
      (schema dna_project)
```

- **Pipeline atas (base / batch)** — CSV 2 tanggal di-load Airflow ke `stg`, lalu dibentuk dimensi & fact di `dwh`. **Selesai.**
- **Pipeline bawah (streaming)** — satu tabel `stg_categories` dari MariaDB (`dna_project`) di-stream ke Postgres yang sama.

Diagram lengkap (editable): **[`docs/erd_star_schema.drawio`](docs/erd_star_schema.drawio)**
→ buka di [draw.io](https://app.diagrams.net) lalu ekspor ke `docs/erd.png`.

> Dokumen mapping menulis schema `dm`; implementasi di Neon memakai `dwh`.

---

## ⭐ Skema Bintang (`dwh`)

| Tabel | Peran | SCD | Baris |
|---|---|---|---:|
| `fact_sales` | Fact | – | **99.051** |
| `dim_time` | Dimensi waktu | Type 0 | 36.890 |
| `dim_product` | Dimensi produk | Type 1 | 452 |
| `dim_customer` | Dimensi pelanggan | Type 2 | 98.759 |
| `dim_employee` | Dimensi pegawai | Type 2 | 23 |

- **`fact_sales`** — `quantity`, `discount`, `total_price` + 4 *surrogate key* (FK ke tiap dimensi).
- **SCD Type 1** (`dim_product`) — data ditimpa saat berubah.
- **SCD Type 2** (`dim_customer`, `dim_employee`) — histori disimpan via `start_date`/`end_date`.
- **SCD Type 0** (`dim_time`) — statis.

> **Revenue** dihitung `quantity × product_price × (1 − discount)` karena `sales.totalprice`
> di sumber bernilai 0 semua.

---

## 📂 Struktur Repo

```
dwh-project/
├── README.md
├── docker-compose.yml          # Postgres + Airflow + Metabase
├── .env.example                # contoh env (AIRFLOW_UID, dsb.)
├── .gitignore
├── run_all.sh                  # jalankan pipeline manual (tanpa Airflow)
├── airflow/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── dags/
│       ├── dwh_common.py       # helper: runner file SQL + pemanggil loader
│       ├── dwh_init_dag.py     # DAG setup sekali: schema + DDL + dim_time
│       └── dwh_batch_dag.py    # DAG harian: load → dim → fact
├── scripts/
│   └── load_staging.py         # CSV → schema stg (+ pembersihan data)
├── sql/
│   ├── 00_schemas.sql          # schema stg & dwh
│   ├── 01_staging_ddl.sql      # 7 tabel staging
│   ├── 02_dimension_ddl.sql    # dim_product / dim_customer / dim_employee
│   ├── 03_dim_time.sql         # dim_time + populate
│   ├── 04_fact_ddl.sql         # fact_sales
│   └── transform/
│       ├── dim_product.sql     # SCD1 (upsert)
│       ├── dim_customer.sql    # SCD2
│       ├── dim_employee.sql    # SCD2
│       └── fact_sales.sql      # lookup surrogate key (idempoten)
├── INPUT/                      # taruh CSV di sini (yyyyMMdd_*.csv)
├── notebooks/
│   └── storytelling.ipynb      # narasi + analisis + screenshot Metabase
└── docs/
    ├── erd_star_schema.drawio  # ERD editable
    ├── erd.png                 # hasil ekspor ERD (PNG)
    ├── airflow.jpeg            # screenshot graph Airflow (semua task success)
    └── metabase.jpeg           # screenshot Dashboard Penjualan
```

---

## 🚀 Cara Menjalankan

### Opsi A — Manual (tanpa Airflow)
```bash
cp .env.example .env            # isi NEON_DSN / kredensial
# taruh CSV di INPUT/, lalu:
./run_all.sh 20180508           # init + load tanggal pertama
./run_all.sh 20180509           # incremental
```

### Opsi B — Airflow (Docker)
```bash
docker compose up -d
# Airflow UI → http://localhost:8080
# 1. Trigger DAG  dwh_init   (sekali, setup schema + dim_time)
# 2. Trigger DAG  dag_manuel dengan config { "load_date": "20180508" }
# 3. Ulangi untuk 20180509 (incremental)
```

Urutan task DAG harian: `load_staging → dim_product / dim_customer / dim_employee → fact_sales`.

### Dashboard
Metabase → connect ke Neon (schema `dwh`) → buat dashboard dari 4 chart
(volume, per kategori, per kota, tren waktu).

---

## ✅ Verifikasi (end-to-end)

| Tanggal | Mode | `fact_sales` | Catatan |
|---|---|---:|---|
| 2018-05-08 | full | 98.255 | 0 surrogate key NULL |
| 2018-05-09 | incremental | 99.051 (+796) | dimensi lain tidak berubah |
| 2018-05-09 | **trigger ulang** | 99.051 | **idempotent** — tidak ada duplikat |

---

## 📊 Storytelling

Narasi & temuan bisnis lengkap ada di **[`notebooks/storytelling.ipynb`](notebooks/storytelling.ipynb)**:
volume transaksi, distribusi per produk/kategori, sebaran geografis, tren waktu, catatan kualitas
data, dan rekomendasi.

---

## 🧰 Stack

`PostgreSQL (Neon)` · `Apache Airflow` · `Metabase` · `Python (pandas, psycopg2/sqlalchemy)` · `Docker`
