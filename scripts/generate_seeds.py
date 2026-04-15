import csv, random, os, json, uuid
from datetime import date, datetime, timedelta

def wcsv(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not rows: return
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"Written: {path}")

def rand_date():
    return f"2026-01-01T{random.randint(0,23):02d}:{random.randint(0,59):02d}:00Z"

BASE_DIR = "/home/farrux/data_projects/marketing_analytics/my_marketing_project/seeds"
START, END = date(2026, 1, 1), date(2026, 3, 31)
DAYS = [START + timedelta(days=i) for i in range((END - START).days + 1)]
random.seed(42)
NOW = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def generate_google_ads():
    camps = [{"campaign_id": f"GC{i:03d}", "campaign_name": f"Google Camp {i}", "campaign_type": "SEARCH", "status": "ENABLED", "currency_code": "USD", "_loaded_at": NOW} for i in range(1, 6)]
    wcsv(f"{BASE_DIR}/google_ads/google_ads_campaigns.csv", camps)
    
    perf = []
    for d in DAYS:
        for c in camps:
            if random.random() < 0.05: continue # some missing
            imps = random.randint(100, 1000)
            clicks = int(imps * random.uniform(0.02, 0.08))
            perf.append({"date": str(d), "campaign_id": c["campaign_id"], "impressions": imps, "clicks": clicks, "cost_micros": clicks * random.randint(1000000, 5000000), "currency_code": "USD", "_loaded_at": NOW})
    wcsv(f"{BASE_DIR}/google_ads/google_ads_performance_daily.csv", perf)

def generate_meta_ads():
    camps = [{"campaign_id": f"MC{i:03d}", "campaign_name": f"Meta Camp {i}", "objective": "CONVERSIONS", "status": "ACTIVE", "currency": "USD", "_loaded_at": NOW} for i in range(1, 5)]
    wcsv(f"{BASE_DIR}/meta_ads/meta_ads_campaigns.csv", camps)
    
    perf = []
    for d in DAYS:
        for c in camps:
            imps = random.randint(500, 5000)
            clicks = int(imps * random.uniform(0.01, 0.05))
            perf.append({"date_start": str(d), "date_stop": str(d), "campaign_id": c["campaign_id"], "impressions": imps, "clicks": clicks, "spend_cents": clicks * random.randint(50, 200), "currency": "USD", "_loaded_at": NOW})
    wcsv(f"{BASE_DIR}/meta_ads/meta_ads_performance_daily.csv", perf)

def generate_linkedin_ads():
    camps = [{"campaign_id": f"LC{i:03d}", "campaign_name": f"LI Camp {i}", "status": "ACTIVE", "daily_budget_currency": "USD", "_loaded_at": NOW} for i in range(1, 4)]
    wcsv(f"{BASE_DIR}/linkedin_ads/linkedin_ads_campaigns.csv", camps)
    
    perf = []
    for d in DAYS:
        for c in camps:
            imps = random.randint(50, 500)
            clicks = int(imps * random.uniform(0.005, 0.03))
            perf.append({"date": str(d), "campaign_id": c["campaign_id"], "impressions": imps, "clicks": clicks, "cost_in_usd": clicks * random.uniform(2.0, 10.0), "local_currency": "USD", "_loaded_at": NOW})
    wcsv(f"{BASE_DIR}/linkedin_ads/linkedin_ads_performance_daily.csv", perf)

def generate_hubspot():
    b2b_domains = ['acme.com', 'stark.com', 'wayne.com', 'oscorp.com', 'cyberdyne.com', 'initech.com', 'hooli.com', 'piedpiper.com', 'umbrella.com', 'gmail.com', 'yahoo.com']
    sources = ['PAID_SEARCH', 'PAID_SOCIAL', 'ORGANIC_SEARCH', 'DIRECT']
    stages = ['lead', 'mql', 'sql', 'customer']
    
    contacts = []
    deals = []
    
    # Generate 100 Contacts across 11 domains
    for i in range(1, 101):
        domain = random.choice(b2b_domains)
        stage = random.choice(stages)
        source = random.choice(sources)
        contact_id = f"HSCT{i:05d}"
        created_at = rand_date()
        
        contacts.append({
            "contact_id": contact_id, 
            "email": f"user{i}@{domain}", 
            "first_name": f"User{i}", 
            "last_name": f"Last{i}", 
            "lifecycle_stage": stage, 
            "original_source": source, 
            "created_at": created_at, 
            "merged_into_contact_id": None
        })
        
        # If the contact is an SQL or Customer, 50% chance they have a Deal
        if stage in ['sql', 'customer'] and random.random() > 0.5:
            deal_stage = 'closed_won' if stage == 'customer' else random.choice(['discovery', 'presentation', 'negotiation', 'closed_lost'])
            amount = random.randint(5000, 50000) if deal_stage != 'closed_lost' else 0
            deals.append({
                "deal_id": f"HSDL{len(deals)+1:05d}",
                "contact_id": contact_id, # Single-contact association to keep it simple
                "deal_name": f"{domain.upper().split('.')[0]} - Enterprise License",
                "deal_stage": deal_stage,
                "amount": amount,
                "created_at": created_at,
                "closed_at": rand_date() if deal_stage in ['closed_won', 'closed_lost'] else None,
                "_loaded_at": NOW
            })

    # merge test:
    contacts.append({"contact_id": "HSCT00101", "email": "user1@acme.com", "first_name": "User1", "last_name": "Last1", "lifecycle_stage": "lead", "original_source": "PAID_SEARCH", "created_at": rand_date(), "merged_into_contact_id": "HSCT00001"})
    wcsv(f"{BASE_DIR}/hubspot/hubspot_contacts.csv", contacts)
    wcsv(f"{BASE_DIR}/hubspot/hubspot_deals.csv", deals)

def generate_ga4():
    sess = []
    for d in DAYS:
        for s, m in [("google", "cpc"), ("(direct)", "(none)"), ("linkedin", "paid_social")]:
            sess.append({"date": str(d), "source": s, "medium": m, "campaign": "brand_search_q1", "sessions": random.randint(10, 100), "engaged_sessions": random.randint(5, 50), "bounces": random.randint(0, 5), "_loaded_at": NOW})
    wcsv(f"{BASE_DIR}/ga4/ga4_sessions_daily.csv", sess)

def generate_segment():
    tracks = []
    for d in DAYS:
        for i in range(10):
            tracks.append({"message_id": str(uuid.uuid4()), "user_id": f"usr_hsct{random.randint(1,100):05d}", "event": random.choice(["Page Viewed", "Pricing Visited"]), "timestamp": f"{d}T10:00:00Z", "_loaded_at": NOW})
    wcsv(f"{BASE_DIR}/segment/segment_tracks.csv", tracks)

if __name__ == "__main__":
    generate_google_ads()
    generate_meta_ads()
    generate_linkedin_ads()
    generate_hubspot()
    generate_ga4()
    generate_segment()
