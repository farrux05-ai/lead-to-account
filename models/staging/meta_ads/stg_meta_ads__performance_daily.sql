{{ generate_stg_ad_performance(
    source_name='meta_ads',
    table_name='meta_ads_performance_daily',
    date_col='date_start',
    spend_col='spend_cents',
    spend_divisor=100.0,
    currency_col='currency'
) }}
