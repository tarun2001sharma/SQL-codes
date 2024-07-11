WITH SolutionDates AS (
    SELECT DISTINCT
        DATE,
        YEAR_FISCAL,
        YEAR,
        MONTH_CALENDAR,
        YEAR || '-' || LPAD(MONTH_CALENDAR::VARCHAR, 2, '0') AS YEAR_MONTH
    FROM REF.DATE
    WHERE DAY_OF_MONTH = 1
        AND DATE >= DATEADD(MONTH, -24, CURRENT_DATE())  -- Filter for past 24 months
),

FilteredCSM AS (
    SELECT
        SOLUTION_UID,
        DATE,
        PLAN_UID,
        USERS_HVT,
        INTEGRATIONS,
        WORKFLOWS,
        API_INTEGRATIONS,
        QUARTERS_ACTIVE,
        OWNER_UID,
        EXTERNAL,
        WT_COMPLEXITY
    FROM
        PRODUCT.CONTAINER_SOLUTION_MONTHLY
    WHERE
        QUARTERS_ACTIVE >= 1
        AND DATE <= CURRENT_DATE()
        AND PLAN_UID IS NOT NULL
),

FilteredPlans AS (
    SELECT
        PLAN_UID,
        ACCOUNT_ID
    FROM
        nexus.base.plan
    WHERE
        PLAN_UID IS NOT NULL
        AND ACCOUNT_ID IS NOT NULL
),

FilteredPlanDaily AS (
    SELECT
        pd.PLAN_UID,
        pd.DATE,
        fp.ACCOUNT_ID,
        pd.USERS_LICENSED,
        pd.USERS_LICENSED_ACTIVE,
        pd.USERS_COLLAB_INTERNAL,
        pd.USERS_COLLAB_EXTERNAL,
        pd.PLAN_ARR
    FROM
        nexus.base.plan_daily pd
    JOIN 
        FilteredPlans fp ON pd.PLAN_UID = fp.PLAN_UID
    WHERE
        pd.PLAN_UID IS NOT NULL
        AND pd.DATE IS NOT NULL
),

AccountARR AS (
    SELECT 
        ACCOUNT_ID,
        SUM(PLAN_ARR) AS total_ARR
    FROM 
        FilteredPlanDaily
    GROUP BY 
        ACCOUNT_ID
    HAVING 
        SUM(PLAN_ARR) >= 5000
),

OwnerSolutionCounts AS (
    SELECT 
        OWNER_UID,
        COUNT(SOLUTION_UID) AS total_solutions_per_owner
    FROM 
        FilteredCSM
    GROUP BY 
        OWNER_UID
),

UniqueHVTs AS (
    SELECT
        fp.ACCOUNT_ID,
        csm.SOLUTION_UID,
        csm.USERS_HVT
    FROM
        FilteredCSM csm
    LEFT JOIN
        FilteredPlans fp ON csm.PLAN_UID = fp.PLAN_UID
    WHERE
        csm.USERS_HVT > 0
),

BrandfolderPresence AS (
    SELECT
        ACCOUNT_ID,
        1 AS brandfolder_presence
    FROM
        PRODUCT.RM_PLAN
    GROUP BY
        ACCOUNT_ID
),

AggregatedData AS (
    SELECT 
        fp.ACCOUNT_ID,
        csm.PLAN_UID,
        csm.SOLUTION_UID,
        csm.USERS_HVT,
        csm.INTEGRATIONS,
        csm.WORKFLOWS,
        csm.API_INTEGRATIONS,
        pd.DATE,
        pd.USERS_LICENSED,
        pd.USERS_LICENSED_ACTIVE,
        pd.USERS_COLLAB_INTERNAL,
        pd.USERS_COLLAB_EXTERNAL,
        sd.YEAR_FISCAL,
        sd.YEAR,
        sd.MONTH_CALENDAR,
        sd.YEAR_MONTH,
        COALESCE(bfp.brandfolder_presence, 0) AS brandfolder_presence,
        -- Calculate total active users
        pd.USERS_COLLAB_INTERNAL + pd.USERS_COLLAB_EXTERNAL AS total_active_users,
        -- Calculate unique solution owners per account and fiscal month
        COUNT(DISTINCT csm.OWNER_UID) OVER (PARTITION BY fp.ACCOUNT_ID, sd.YEAR_MONTH) AS unique_solution_owners,
        -- Calculate owners with more than 10 solutions per account and fiscal month
        COUNT(DISTINCT CASE WHEN osc.total_solutions_per_owner > 10 THEN csm.OWNER_UID END) OVER (PARTITION BY fp.ACCOUNT_ID, sd.YEAR_MONTH) AS power_developers,
        -- Calculate total solutions per account and fiscal month
        COUNT(csm.SOLUTION_UID) OVER (PARTITION BY fp.ACCOUNT_ID, sd.YEAR_MONTH) AS total_solutions_per_account,
        -- Calculate unique internal collaborators per solution
        COUNT(DISTINCT pd.USERS_COLLAB_INTERNAL) OVER (PARTITION BY csm.SOLUTION_UID) AS unique_internal_collaborators,
        -- Calculate unique external collaborators per solution
        COUNT(DISTINCT pd.USERS_COLLAB_EXTERNAL) OVER (PARTITION BY csm.SOLUTION_UID) AS unique_external_collaborators,
        -- Calculate solutions with >=1 HVT per account and fiscal month
        COUNT(DISTINCT CASE WHEN csm.USERS_HVT > 0 THEN csm.SOLUTION_UID END) OVER (PARTITION BY fp.ACCOUNT_ID, sd.YEAR_MONTH) AS solutions_with_HVT,
        -- Calculate solutions with >=1 external user per account and fiscal month
        COUNT(DISTINCT CASE WHEN pd.USERS_COLLAB_EXTERNAL > 0 THEN csm.SOLUTION_UID END) OVER (PARTITION BY fp.ACCOUNT_ID, sd.YEAR_MONTH) AS solutions_with_external_users,
        -- Calculate solutions created in a month per account and fiscal month
        COUNT(csm.SOLUTION_UID) OVER (PARTITION BY fp.ACCOUNT_ID, sd.YEAR_MONTH) AS solutions_created_monthly,
        -- Calculate unique HVT collaborators per account and fiscal month
        COUNT(DISTINCT uhv.USERS_HVT) OVER (PARTITION BY fp.ACCOUNT_ID, sd.YEAR_MONTH) AS unique_HVT_collaborators,
        -- Calculate external integrations per account and fiscal month
        COUNT(DISTINCT CASE WHEN csm.EXTERNAL = TRUE THEN csm.SOLUTION_UID END) OVER (PARTITION BY fp.ACCOUNT_ID, sd.YEAR_MONTH) AS external_integrations,
        -- Calculate internal integrations per account and fiscal month
        COUNT(DISTINCT CASE WHEN ARRAY_SIZE(csm.INTEGRATIONS) > 0 THEN csm.SOLUTION_UID END) OVER (PARTITION BY fp.ACCOUNT_ID, sd.YEAR_MONTH) AS internal_integrations,
        -- Calculate total complexity score per account and fiscal month
        SUM(csm.WT_COMPLEXITY) OVER (PARTITION BY fp.ACCOUNT_ID, sd.YEAR_MONTH) AS total_complexity_score,
        -- Calculate solutions with automation per account and fiscal month
        COUNT(DISTINCT CASE WHEN csm.WORKFLOWS > 0 THEN csm.SOLUTION_UID END) OVER (PARTITION BY fp.ACCOUNT_ID, sd.YEAR_MONTH) AS solutions_with_automation,
        -- Calculate total workflows per account and fiscal month
        SUM(csm.WORKFLOWS) OVER (PARTITION BY fp.ACCOUNT_ID, sd.YEAR_MONTH) AS total_workflows
    FROM 
        FilteredCSM csm
    LEFT JOIN 
        FilteredPlans fp ON csm.PLAN_UID = fp.PLAN_UID
    LEFT JOIN 
        FilteredPlanDaily pd ON csm.PLAN_UID = pd.PLAN_UID AND csm.DATE = pd.DATE
    JOIN
        SolutionDates sd ON pd.DATE = sd.DATE
    LEFT JOIN 
        OwnerSolutionCounts osc ON csm.OWNER_UID = osc.OWNER_UID
    LEFT JOIN
        UniqueHVTs uhv ON csm.SOLUTION_UID = uhv.SOLUTION_UID AND fp.ACCOUNT_ID = uhv.ACCOUNT_ID
    LEFT JOIN
        BrandfolderPresence bfp ON fp.ACCOUNT_ID = bfp.ACCOUNT_ID
    JOIN 
        AccountARR aar ON fp.ACCOUNT_ID = aar.ACCOUNT_ID
    WHERE
        fp.ACCOUNT_ID IS NOT NULL
)

SELECT 
    ACCOUNT_ID,
    YEAR_FISCAL,
    YEAR,
    MONTH_CALENDAR,
    YEAR_MONTH,
    SUM(USERS_LICENSED) AS total_licensed_users,
    SUM(USERS_LICENSED_ACTIVE) AS total_active_licensed_users,
    SUM(USERS_COLLAB_INTERNAL) AS total_internal_collaborators,
    SUM(USERS_COLLAB_EXTERNAL) AS total_external_collaborators,
    SUM(total_active_users) AS total_active_users,
    
    -- KPI Calculations
    
    -- % Licensed Users
    (SUM(USERS_LICENSED_ACTIVE) / NULLIF(SUM(USERS_LICENSED), 0)) * 100 AS pct_licensed_users,
    -- % Active Users
    (SUM(USERS_HVT) / NULLIF(SUM(USERS_LICENSED_ACTIVE), 0)) * 100 AS pct_active_users,
    -- % Internal Collaborator
    (SUM(USERS_COLLAB_INTERNAL) / NULLIF(SUM(total_active_users), 0)) * 100 AS pct_internal_collaborator,
    -- % External Collaborator
    (SUM(USERS_COLLAB_EXTERNAL) / NULLIF(SUM(total_active_users), 0)) * 100 AS pct_external_collaborator,
    -- % Active Creators
    (SUM(unique_solution_owners) / NULLIF(SUM(total_solutions_per_account), 0)) * 100 AS pct_active_creators,
    -- % Power Developer
    (SUM(power_developers) / NULLIF(SUM(USERS_LICENSED), 0)) * 100 AS pct_power_developer,
    -- % Solution Creators
    (SUM(unique_solution_owners) / NULLIF(SUM(USERS_LICENSED), 0)) * 100 AS pct_solution_creators,
    -- Average Internal Collaboration per Solution
    (SUM(unique_internal_collaborators) / NULLIF(SUM(total_solutions_per_account), 0)) AS avg_internal_collab_per_solution,
        -- Average unique External Collaboration per Solution
    (SUM(solutions_with_external_users) / NULLIF(SUM(total_solutions_per_account), 0)) AS avg_unique_external_collab_per_solution,
    -- Average External Collaboration per Solution
    (SUM(unique_external_collaborators) / NULLIF(SUM(total_solutions_per_account), 0)) AS avg_external_collab_per_solution,
    -- Average unique HVT Collaboration per Solution
    (SUM(solutions_with_HVT) / NULLIF(SUM(total_solutions_per_account), 0)) AS avg_unique_HVT_collab_per_solution,
    -- % HVT per solution
    (SUM(unique_HVT_collaborators) / NULLIF(SUM(total_solutions_per_account), 0)) AS pct_HVT_per_solution,
    -- Monthly Solution Creation rate
    (SUM(solutions_created_monthly) / NULLIF(SUM(total_solutions_per_account), 0)) AS monthly_solution_creation_rate,
    -- External Integration percentage
    (SUM(external_integrations) / NULLIF(SUM(total_solutions_per_account), 0)) * 100 AS external_integration_pct,
    -- Internal Integration percentage
    (SUM(internal_integrations) / NULLIF(SUM(total_solutions_per_account), 0)) * 100 AS internal_integration_pct,
    -- Average complexity per solution
    (SUM(total_complexity_score) / NULLIF(SUM(total_solutions_per_account), 0)) AS avg_complexity_per_solution,
    -- % Solution Automation
    (SUM(solutions_with_automation) / NULLIF(SUM(total_solutions_per_account), 0)) * 100 AS pct_solution_automation,
    -- % Automation Complexity
    (SUM(total_workflows) / NULLIF(SUM(total_solutions_per_account), 0)) AS pct_automation_complexity,
    -- Presence of Brandfolder
    MAX(brandfolder_presence) AS brandfolder_presence
FROM 
    AggregatedData
GROUP BY 
    ACCOUNT_ID, YEAR_FISCAL, YEAR, MONTH_CALENDAR, YEAR_MONTH
ORDER BY 
    ACCOUNT_ID, YEAR_MONTH
LIMIT 50;
