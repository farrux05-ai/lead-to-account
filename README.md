# Lead-to-Account: B2B SaaS RevOps Revenue Engine

> **Marketing Analytics Warehouse** — Reklama klikidan tortib $100K+ Enterprise Bitishuvigacha bo'lgan to'liq B2B Customer Journey'ni bir platformada kuzating.

[![dbt Docs](https://img.shields.io/badge/Documentation-dbt--Docs-FF694B)](https://farrux05-ai.github.io/lead-to-account/)
[![Stack](https://img.shields.io/badge/Stack-Modern--Data--Stack-blue)](https://github.com/farrux05-ai/lead-to-account)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

---

## 🏗️ Loyiha Arxitekturasi

Loyiha **to'liq avtomatlashtirilgan** ma'lumotlar konveyeri (pipeline) asosida qurilgan. Har bir qator ma'lumot quyidagi bosqichlardan o'tadi:

```
HubSpot CRM (Mock API)
        │  dlt (Data Load Tool)
        ▼
    DuckDB (Local Warehouse)
        │  dbt Core (50+ modell va test)
        ▼
   Staging → Intermediate → Marts
        │  Dagster (Orchestration)
        ▼
   Streamlit Dashboard
```

![Data Pipeline Architecture](screenshots/data_pipeline_architecture.svg)

### Stack:
| Qatlam | Vosita | Vazifasi |
|---|---|---|
| **Ingestion** | `dlt` | HubSpot'dan ma'lumotlarni avtomatik yuklash |
| **Warehouse** | `DuckDB` | Mahalliy, tezkor analitik omborxona |
| **Transformation** | `dbt Core` | 3 qatlamli ma'lumot modellashtirish |
| **Orchestration** | `Dagster` | Asset-based pipeline boshqaruvi |
| **Visualization** | `Streamlit` | Interaktiv RevOps dashboardi |

---

## ⚙️ Dagster Orkestratsiyasi (Asset Lineage)

Loyiha **`dagster-dbt`** integratsiyasi orqali har bir dbt modelini alohida "asset" sifatida boshqaradi. Bu shuni anglatadiki — qaysi model qachon ishlagani, uning bog'liqliklari va holati Dagster UI orqali to'liq nazorat qilinadi.

![Dagster Asset Lineage](screenshots/dagster_linage.png)

> Yuqoridagi rasmda **56 ta dbt modeli** (staging, intermediate, marts) va ularning o'zaro bog'liqligi (lineage) ko'rsatilgan. Ingestion jarayoni muvaffaqiyatli bo'lgandan keyingina transformatsiya boshladi.

---

## 📊 To'liq Funnel Dashboard

### 1. KPI va Daromad Nazorati
Marketing xarajatlari va yopilgan bitishuvlar orasidagi to'g'ridan-to'g'ri aloqa.
![Revenue Performance](screenshots/kpi.png)

### 2. Platforma Samaradorligi (CPC & CTR)
Qaysi reklama platforma eng arzon va eng samarali ekanligini ko'rsatadi.
![Ad Efficiency](screenshots/platform_effciency.png)

### 3. Traffic va Sessiyalar (GA4)
Kunlik trafik oqimi va manbalar bo'yicha taqsimot.
![GA4 Traffic](screenshots/traffic_source_daily_session.png)

### 4. Lead Funnel
MQL → SQL → Deal bosqichlarida qancha lead qolayotgani va qanchasi yo'qolayotgani.
![Lead Funnel](screenshots/lead_by_stage.png)

### 5. dbt Data Lineage
Ma'lumot qayerdan keladi va qaysi modellarga ta'sir qiladi — to'liq grafik ko'rinishida.
![Data Lineage](screenshots/linage_of_data.png)

---

## 🚀 Ishga Tushirish

```bash
# 1. Repozitoriyani klonlash
git clone https://github.com/farrux05-ai/lead-to-account.git
cd lead-to-account/my_marketing_project

# 2. Virtual muhit yaratish va kutubxonalarni o'rnatish
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 3. dbt paketlarini yuklash
dbt deps

# 4. To'liq pipeline'ni ishga tushirish (Ingestion + Modeling + Testing)
dagster asset materialize --select "*" -f scripts/dagster_orchestrator.py

# 5. Dashboardni ochish
streamlit run streamlit_app.py
```

---

## 🛡️ Ma'lumotlar Sifati

Loyiha `dbt_expectations` orqali qat'iy sifat nazoratini amalga oshiradi:
- **Domain validatsiyasi:** Gmail/Yahoo kabi bepul email domenlarini B2B hisobotlaridan chiqaradi.
- **Surrogat kalit yaxlitligi:** Barcha dimension va fact jadvallarida 100% join darajasini ta'minlaydi.
- **Identity Resolution:** Floating lead'larni domen razvedkasi asosida Virtual Accountlarga birlashtiradi.
- **SCD Type 2:** dbt Snapshots orqali CRM lifecycle tarixiy o'zgarishlarini kuzatadi.

---

## 🔗 Foydali Havolalar

- 📚 **[Interaktiv dbt Dokumentatsiyasi](https://farrux05-ai.github.io/lead-to-account/)** — To'liq model katalogi va lineage
- 🐙 **[GitHub Repo](https://github.com/farrux05-ai/lead-to-account)** — Barcha manba kodlar
