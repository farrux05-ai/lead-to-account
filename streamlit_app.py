import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px

# Set page config
st.set_page_config(page_title="B2B RevOps Dashboard", layout="wide", page_icon="📈")

st.title("📈 B2B RevOps: Marketing to Pipeline Attribution")
st.markdown("This dashboard maps Top-of-Funnel marketing campaigns directly to Bottom-of-Funnel Closed Won revenue using Data Build Tool (dbt) and DuckDB.")

# Connect to DuckDB
@st.cache_resource
def get_connection():
    return duckdb.connect('dev.duckdb', read_only=True)

# Load Data
@st.cache_data
def load_data():
    conn = get_connection()
    ad_perf = conn.execute("SELECT * FROM main_marts.fct_ad_performance_daily").df()
    revenue = conn.execute("SELECT * FROM main_marts.fct_pipeline_revenue").df()
    return ad_perf, revenue

try:
    ad_perf, revenue = load_data()
    
    # ── KPIs ───────────────────────────────────────
    col1, col2, col3, col4 = st.columns(4)
    
    total_spend = ad_perf['daily_spend_usd'].sum()
    total_revenue = revenue[revenue['is_closed_won'] == True]['amount_usd'].sum()
    total_deals = revenue['deal_key'].nunique()
    won_deals = revenue[revenue['is_closed_won'] == True]['deal_key'].nunique()
    
    roi = (total_revenue/total_spend)*100 if total_spend > 0 else 0
    win_rate = (won_deals/total_deals)*100 if total_deals > 0 else 0
    
    col1.metric("Total Ad Spend (USD)", f"${total_spend:,.0f}")
    col2.metric("Closed Won Revenue", f"${total_revenue:,.0f}")
    col3.metric("Total ROI", f"{roi:.1f}%")
    col4.metric("Win Rate", f"{win_rate:.1f}%")
    
    st.markdown("---")

    # ── Charts ─────────────────────────────────────
    col_a, col_b = st.columns(2)
    
    with col_a:
        st.subheader("Attribution: Revenue by Original Source")
        rev_by_source = revenue[revenue['is_closed_won'] == True].groupby('attribution_channel')['amount_usd'].sum().reset_index()
        fig_rev = px.pie(rev_by_source, names='attribution_channel', values='amount_usd', hole=0.4, title="Closed Won Revenue by Acquisition Channel")
        st.plotly_chart(fig_rev, use_container_width=True)
        
    with col_b:
        st.subheader("Marketing Spend by Platform over Time")
        spend_trend = ad_perf.groupby(['date_day', 'channel'])['daily_spend_usd'].sum().reset_index()
        fig_spend = px.line(spend_trend, x='date_day', y='daily_spend_usd', color='channel', title="Daily Spend Trend by Platform")
        st.plotly_chart(fig_spend, use_container_width=True)
        
    st.markdown("---")
    
    # ── Deal Explorer ──────────────────────────────
    st.subheader("🔍 Deals Tracker (B2B Virtual Accounts)")
    st.dataframe(revenue[['deal_name', 'company_domain', 'deal_stage', 'amount_usd', 'attribution_channel', 'deal_created_at']].sort_values('amount_usd', ascending=False), use_container_width=True)

except Exception as e:
    st.error(f"Error loading data. Make sure 'dbt build' has been run successfully. Details: {e}")
