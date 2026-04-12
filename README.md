# Marketing Analytics Warehouse

> **Part of the B2B SaaS RevOps Data Platform — Marketing Module**

A production-grade **marketing data warehouse** built with dbt + DuckDB.
Designed to be **Metabase-ready** from day one, following a strict layered
architecture with full test coverage and inline documentation.

---

## 📦 Stack

| Component | Technology |
|---|---|
| Warehouse | DuckDB (local dev / production-ready) |
| Transformation | dbt Core ≥ 1.5 |
| dbt Packages | dbt_utils |
| BI Tool | Metabase |
| Source (dev) | dbt Seeds (CSV dummy data) |

---

## 📡 Marketing Sources

| Source | What it represents |
|---|---|
| **Google Ads** | Search & Display paid campaigns |
| **Meta Ads** | Facebook + Instagram paid campaigns |
| **LinkedIn Ads** | B2B paid campaigns |
| **HubSpot** | CRM, email marketing, form submissions |
| **Google Analytics 4** | Web sessions, page views, events |
| **Segment** | Product event tracking, identify calls |

---

## 🏛️ Warehouse Architecture

```
seeds/                      ← Raw CSV source data (dummy in dev)
  ├── google_ads/
  ├── meta_ads/
  ├── linkedin_ads/
  ├── hubspot/
  ├── ga4/
  └── segment/

models/
  ├── staging/               ← 1:1 with source tables, typed + renamed
  │   ├── google_ads/
  │   ├── meta_ads/
  │   ├── linkedin_ads/
  │   ├── hubspot/
  │   ├── ga4/
  │   └── segment/
  │
  ├── intermediate/          ← Business logic, dedup, cross-source joins
  │   └── int_*.sql
  │
  └── marts/                 ← Dashboard-ready tables (Metabase connects here)
      └── marketing/
          ├── dim_date.sql
          ├── dim_campaigns.sql
          ├── dim_contacts.sql
          ├── fct_ad_performance_daily.sql
          ├── fct_web_sessions.sql
          ├── fct_email_performance.sql
          └── fct_pipeline_influenced.sql

macros/                     ← Reusable SQL macros
tests/                      ← Custom generic + singular tests
```

---

## 🚀 Quick Start

```bash
# 1. Activate the virtual environment
source /home/farrux/data_projects/marketing_analytics/venv/bin/activate

# 2. Move into the project directory
cd /home/farrux/data_projects/marketing_analytics/my_marketing_project

# 3. Install dbt packages
dbt deps

# 4. Verify connection
dbt debug

# 5. Load seed (dummy) data
dbt seed

# 6. Build all models
dbt run

# 7. Run all tests
dbt test

# 8. Generate + serve docs
dbt docs generate && dbt docs serve
```

---

## 🧱 Layer Conventions

### Staging (`stg_*`)
- **One model per source table**
- Rename columns to snake_case standard
- Cast all data types explicitly
- Deduplicate via `ROW_NUMBER()` on source primary key
- Never join across sources at this layer
- Materialized as **views**

### Intermediate (`int_*`)
- Cross-source joins and transformations
- Build spine tables (e.g. campaign-date spines)
- Apply business logic (e.g. currency normalisation)
- Flag anomalies (`is_anomaly`, `is_active`)
- Materialized as **ephemeral** (compiled inline)

### Marts (`fct_*`, `dim_*`)
- **Dashboard-ready**: wide, denormalised tables
- Pre-calculated metrics (CTR, CPC, CPM, ROAS, CVR)
- All monetary values in USD
- All timestamps in UTC, with `date_day` for Metabase time series
- Surrogate keys for all entities
- Materialized as **tables** for fast BI queries

---

## 🛡️ Anti-Fragile Design Decisions

| Pattern | Implementation |
|---|---|
| Cross-channel surrogate keys | `channel_code \|\| '_' \|\| source_id` |
| Currency normalisation | `amount_usd` + `original_currency` preserved |
| NULL metric handling | `COALESCE(metric, 0)` in staging |
| API retry deduplication | `ROW_NUMBER()` dedup in every staging model |
| Timezone normalisation | All times cast to UTC, `date_day` extracted |
| Soft deletes | `is_active` boolean flag on all dimension records |
| Anomaly detection | `is_anomaly` flag where spend > 0 but impressions = 0 |
| Contact merges (HubSpot) | `merged_into_contact_id` tracked |
| Future-proof UTM parsing | Source/Medium/Campaign extracted with fallback |

---

## 📐 Naming Conventions

| Object | Pattern | Example |
|---|---|---|
| Staging model | `stg_{source}__{table}` | `stg_google_ads__campaigns` |
| Intermediate model | `int_{description}` | `int_unified_ad_performance` |
| Fact table | `fct_{entity}_{grain}` | `fct_ad_performance_daily` |
| Dimension table | `dim_{entity}` | `dim_campaigns` |
| Surrogate key | `{entity}_key` | `campaign_key` |
| Source ID | `{source}_{entity}_id` | `google_ads_campaign_id` |
| Boolean flag | `is_{state}` | `is_active`, `is_anomaly` |
| Timestamp | `{event}_at` | `created_at`, `updated_at` |
| Date | `{event}_date` | `report_date` |
| Amount | `{metric}_usd` | `spend_usd` |

---

## 🔗 Related Modules

This warehouse is designed to integrate with the B2B SaaS RevOps platform:

- **CRM Module** — HubSpot/Salesforce contacts + pipeline (coming soon)
- **Billing Module** — Stripe/Chargebee MRR + churn (coming soon)
- **Product Module** — Mixpanel/Segment product usage (coming soon)
