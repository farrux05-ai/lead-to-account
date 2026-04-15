# Lead-to-Account: B2B SaaS RevOps Revenue Engine

> **High-Performance Marketing Analytics Warehouse** mapping the entire B2B Customer Journey — from anonymous Ad clicks to $100K+ Enterprise Deals.

[![dbt Docs](https://img.shields.io/badge/Documentation-dbt--Docs-FF694B)](https://farrux05-ai.github.io/lead-to-account/)
[![Stack](https://img.shields.io/badge/Stack-Modern--Data--Stack-blue)](https://github.com/farrux05-ai/lead-to-account)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

---

## 📖 Project Overview

This is a production-grade **Modern Data Stack (MDS)** implementation designed for **B2B SaaS Revenue Operations (RevOps)**. Unlike generic marketing dashboards, this project solves the critical disconnect between **Top-of-Funnel (ToFu)** marketing activity and **Bottom-of-Funnel (BoFu)** CRM revenue.

By unifying data from **Google Ads, Meta, LinkedIn, HubSpot, and Segment**, this warehouse provides a single source of truth for Sales and Marketing alignment.

### 🎯 Key Business Problems Solved

1.  **Lead-to-Account Matching (Identity Resolution):** Maps individual "leads" to "accounts" using email domain logic, allowing for true Account-Based Marketing (ABM) attribution.
2.  **Revenue Attribution:** Connects marketing spend directly to Closed-Won deals. We attribute revenue to the *First Touch* of the entire company, not just the single converting contact.
3.  **Pipeline Velocity Analysis:** Uses **SCD Type 2 (dbt Snapshots)** to track how long leads spend in each lifecycle stage (MQL -> SQL -> Deal), identifying bottleneck stages in the sales funnel.
4.  **DRY Analytics Engineering:** Implements reusable Jinja macros to standardize ad performance across fragmented schemas (cents vs micros vs USD), reducing code volume by 80%.

---

## 🛠️ The Tech Stack

| Layer | Component | Description |
|---|---|---|
| **Ingestion** | **dlt** (Data Load Tool) | Automated ELT pipelines extracting from CRM APIs into DuckDB. |
| **Warehouse** | **DuckDB** | Fast, local-first analytical engine powering the warehouse. |
| **Transformation** | **dbt Core** | Dimensional modeling (Star Schema), snapshots, and automated testing. |
| **Orchestration** | **Dagster** | Asset-based orchestration managing the sequential flow of Ingestion -> dbt. |
| **Visualization** | **Streamlit** | Real-time Python dashboard for ROI and Pipeline tracking. |

---

## 🏛️ Warehouse Architecture

We follow a modular **Medallion-inspired** architecture within dbt:

*   **Staging (`stg_`)**: Standardized naming, `TRY_CAST` defensive typing, and `ROW_NUMBER` deduplication.
*   **Intermediate (`int_`)**: The "Brain" of the warehouse. Handles Identity Resolution, Contact-to-Account mapping, and historical stage tracking.
*   **Marts (`fct_`, `dim_`)**: Dashboard-ready tables.
    *   `fct_pipeline_revenue`: The **Crown Jewel** linking marketing channels to actual dollars.
    *   `dim_accounts`: A unified 360-view of B2B accounts.

---

## 📈 Dashboard Highlights

Our **Streamlit** application (`streamlit_app.py`) provides mission-critical metrics:
*   **Total ROI (Return on Investment):** Real-time spend vs. won revenue.
*   **Win Rate by Channel:** Which acquisition channels deliver the highest quality leads?
*   **Daily Spend Trends:** Cross-channel spend monitoring.

---

## 🚀 Getting Started

### 1. Requirements
* Python 3.9+
* dbt-core

### 2. Setup & Execution
```bash
# Clone the repository
git clone https://github.com/farrux05-ai/lead-to-account.git
cd lead-to-account/my_marketing_project

# Set up environment
pip install -r requirements.txt

# Run the full orchestrated pipeline (DLT + dbt Build)
python scripts/dagster_orchestrator.py

# Launch the dashboard
streamlit run streamlit_app.py
```

---

## 🛡️ Data Quality & Testing
We enforce strict data quality using `dbt_expectations`:
*   **Domain Validity:** Ensures email domains are not free providers (Gmail/Yahoo) in the Account dimension.
*   **Unique Keys:** Asserts surrogate key integrity across all fact tables.
*   **Relationship Tests:** Validates that every Deal has an associated Account in the mart layer.

---

## 🔗 Documentation
Interactive lineage and data dictionary are hosted via GitHub Pages:
👉 **[View Data Documentation](https://farrux05-ai.github.io/lead-to-account/)**
