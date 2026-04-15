import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px

import os

# 1. Page Configuration
st.set_page_config(
    page_title="RevOps Full-Funnel Analytics", 
    layout="wide", 
    page_icon="📈"
)

st.title("RevOps: B2B Full-Funnel Analytics Platform")
st.markdown("""
This platform integrates **Google Analytics 4**, **HubSpot CRM**, and **Paid Ad Channels** (Google, Meta, LinkedIn) 
into a unified B2B Revenue Operations dashboard. Powered by **dbt** and **DuckDB**.
""")

# 2. Data Connection
@st.cache_resource
def get_connection():
    # Robust pathing: find dev.duckdb relative to this script
    current_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(current_dir, 'dev.duckdb')
    return duckdb.connect(db_path, read_only=True)


@st.cache_data
def load_all_data():
    conn = get_connection()
    
    # Marts
    ad_perf = conn.execute("SELECT * FROM main_marts.fct_ad_performance_daily").df()
    revenue = conn.execute("SELECT * FROM main_marts.fct_pipeline_revenue").df()
    traffic = conn.execute("SELECT * FROM main_marts.fct_web_sessions_daily").df()
    contacts = conn.execute("SELECT * FROM main_marts.dim_contacts").df()
    accounts = conn.execute("SELECT * FROM main_marts.dim_accounts").df()
    
    return ad_perf, revenue, traffic, contacts, accounts

try:
    ad_perf, revenue, traffic, contacts, accounts = load_all_data()

    # 3. Sidebar Filters
    st.sidebar.header("Global Filters")
    all_channels = sorted(list(set(ad_perf['channel'].unique()) | set(revenue['attribution_channel'].unique())))
    selected_channels = st.sidebar.multiselect("Acquisition Channels", all_channels, default=all_channels)
    
    # Filter Data (Basic filtering logic)
    filtered_rev = revenue[revenue['attribution_channel'].isin(selected_channels)]
    filtered_ads = ad_perf[ad_perf['channel'].isin(selected_channels)]

    # 4. Main Tabs
    tab_rev, tab_ads, tab_traffic, tab_funnel = st.tabs([
        "Revenue & ROI", 
        "Ad Efficiency", 
        "Web Traffic (GA4)", 
        "Lead Funnel"
    ])


    # ── TAB 1: REVENUE ────────────────────────────────────────────────────────
    with tab_rev:
        col1, col2, col3, col4 = st.columns(4)
        
        spend = filtered_ads['daily_spend_usd'].sum()
        rev = filtered_rev[filtered_rev['is_closed_won'] == True]['amount_usd'].sum()
        roi = (rev/spend)*100 if spend > 0 else 0
        wr = (filtered_rev[filtered_rev['is_closed_won'] == True].shape[0] / filtered_rev.shape[0])*100 if filtered_rev.shape[0] > 0 else 0
        
        col1.metric("Total Spend", f"${spend:,.0f}")
        col2.metric("Won Revenue", f"${rev:,.0f}")
        col3.metric("Total ROI", f"{roi:.1f}%")
        col4.metric("Win Rate", f"{wr:.1f}%")
        
        st.markdown("---")
        c1, c2 = st.columns(2)
        with c1:
            # Handle NaN in sunburst path to avoid "Non-leaves rows" error
            plot_rev = filtered_rev[filtered_rev['is_closed_won'] == True].copy()
            plot_rev['attribution_channel'] = plot_rev['attribution_channel'].fillna('UNKNOWN')
            plot_rev['company_domain'] = plot_rev['company_domain'].fillna('(no domain)')
            
            fig_pie = px.sunburst(plot_rev, 
                                  path=['attribution_channel', 'company_domain'], 
                                  values='amount_usd', 
                                  title="Revenue Distribution by Channel & Account")
            st.plotly_chart(fig_pie, use_container_width=True)

        with c2:
            st.subheader("Top B2B Accounts (by Lead Count)")
            top_accs = accounts.sort_values('total_contacts_in_account', ascending=False).head(10)
            fig_acc = px.bar(top_accs, x='total_contacts_in_account', y='email_domain', orientation='h',
                             labels={'email_domain': 'Account Domain', 'total_contacts_in_account': 'Contacts'},
                             title="Account Penetration (ABM)")
            st.plotly_chart(fig_acc, use_container_width=True)

    # ── TAB 2: AD EFFICIENCY ──────────────────────────────────────────────────
    with tab_ads:
        col_ads_a, col_ads_b = st.columns(2)
        with col_ads_a:
            st.subheader("Daily Spend Trend")
            fig_line = px.line(filtered_ads, x='date_day', y='daily_spend_usd', color='channel')
            st.plotly_chart(fig_line, use_container_width=True)
        with col_ads_b:
            st.subheader("Platform Efficiency (CPC & CTR)")
            avg_metrics = filtered_ads.groupby('channel')[['daily_cpc_usd', 'daily_ctr']].mean().reset_index()
            fig_eff = px.scatter(avg_metrics, x='daily_cpc_usd', y='daily_ctr', text='channel', size='daily_cpc_usd',
                                 title="CPC vs. CTR by Platform (Lower Left = Good CPC, Higher = Good Engagement)")
            st.plotly_chart(fig_eff, use_container_width=True)

    # ── TAB 3: WEB TRAFFIC ───────────────────────────────────────────────────
    with tab_traffic:
        t1, t2 = st.columns([1, 2])
        with t1:
            st.subheader("Traffic Source Mix")
            traffic_mix = traffic.groupby('channel_grouping')['daily_sessions'].sum().reset_index()
            fig_t_pie = px.pie(traffic_mix, names='channel_grouping', values='daily_sessions', hole=0.5)
            st.plotly_chart(fig_t_pie, use_container_width=True)
        with t2:
            st.subheader("Daily Session Trends (GA4)")
            t_trend = traffic.groupby(['date_day', 'channel_grouping'])['daily_sessions'].sum().reset_index()
            fig_t_line = px.area(t_trend, x='date_day', y='daily_sessions', color='channel_grouping')
            st.plotly_chart(fig_t_line, use_container_width=True)
        
        st.subheader("Engagement: Engagement Rate by Source")
        eng_rate = traffic.groupby('channel_grouping')['engagement_rate'].mean().reset_index()
        fig_eng = px.bar(eng_rate, x='channel_grouping', y='engagement_rate', color='engagement_rate')
        st.plotly_chart(fig_eng, use_container_width=True)

    # ── TAB 4: LEAD FUNNEL ───────────────────────────────────────────────────
    with tab_funnel:
        f1, f2 = st.columns(2)
        with f1:
            st.subheader("Leads by Lifecycle Stage")
            funnel_data = contacts['lifecycle_stage'].value_counts().reset_index()
            funnel_data.columns = ['stage', 'count']
            fig_fun = px.funnel(funnel_data, x='count', y='stage', title="CRM Lead Funnel")
            st.plotly_chart(fig_fun, use_container_width=True)
        with f2:
            st.subheader("Lead Acquisition Sources")
            lead_source = contacts['original_source'].value_counts().reset_index()
            lead_source.columns = ['source', 'count']
            fig_src = px.bar(lead_source, x='count', y='source', orientation='h', color='count')
            st.plotly_chart(fig_src, use_container_width=True)

except Exception as e:
    st.error(f"Error loading dashboard data. Ensure 'dbt build' has run. Details: {e}")
    st.info("Tip: Try running 'dbt build' and 'python scripts/dlt_hubspot_pipeline.py' to populate the warehouse.")
