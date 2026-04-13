{{ generate_stg_ad_performance(
    source_name='linkedin_ads',
    table_name='linkedin_ads_performance_daily',
    date_col='date',
    spend_col='cost_in_usd',
    spend_divisor=1.0,
    currency_col='local_currency'
) }}
