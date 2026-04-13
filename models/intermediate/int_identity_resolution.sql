-- ============================================================================
-- MODEL: int_identity_resolution
-- ============================================================================
-- GRAIN: One row per Lead (Contact) resolving up to a Virtual Account.
-- PURPOSE: B2B Identity Resolution. We extract email domains, filter out
--          free providers, and use Window Functions to attribute Account-level
--          "First Touch" to the very first contact created for that domain.
-- ============================================================================

WITH contacts AS (
    SELECT * FROM {{ ref('int_contacts_resolved') }}
),

-- 1. Extract domain from email
extracted_domains AS (
    SELECT
        *,
        SPLIT_PART(email, '@', 2) AS email_domain
    FROM contacts
),

-- 2. Flag B2B domains vs Free domains
flagged_domains AS (
    SELECT 
        *,
        CASE 
            WHEN email_domain IN ('gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'icloud.com') THEN TRUE 
            ELSE FALSE 
        END AS is_free_email
    FROM extracted_domains
),

-- 3. Identity Resolution via Window Functions
account_resolution AS (
    SELECT
        source_contact_id,
        email,
        first_name,
        last_name,
        lifecycle_stage,
        email_domain,
        is_free_email,
        
        -- Virtual Account ID: NULL for free emails, Hash for B2B domains
        CASE 
            WHEN is_free_email = FALSE AND email_domain IS NOT NULL 
            THEN {{ dbt_utils.generate_surrogate_key(['email_domain']) }} 
            ELSE NULL 
        END AS virtual_account_id,
        
        -- Original Source for the Account
        CASE 
            WHEN is_free_email = FALSE AND email_domain IS NOT NULL 
            THEN FIRST_VALUE(original_source) OVER (
                PARTITION BY email_domain
                ORDER BY created_at ASC
            ) 
            ELSE original_source 
        END AS account_original_source,

        -- Target the first person who signed up
        CASE 
            WHEN is_free_email = FALSE AND email_domain IS NOT NULL 
            THEN FIRST_VALUE(source_contact_id) OVER (
                PARTITION BY email_domain
                ORDER BY created_at ASC
            ) 
            ELSE source_contact_id 
        END AS account_champion_contact_id,
        
        created_at
    FROM flagged_domains
)

SELECT * FROM account_resolution
