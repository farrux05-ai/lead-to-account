{{ generate_stg_ad_performance(
    source_name='google_ads',
    table_name='google_ads_performance_daily',
    date_col='date',
    spend_col='cost_micros',
    spend_divisor=1000000.0,
    currency_col='currency_code'
) }}
