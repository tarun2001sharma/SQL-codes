WITH AccountDetails AS (
    SELECT
        ACCOUNT_ID,
        NAME AS ACCOUNT_NAME,
        ARR,
        TERRITORY_TYPE,
        COMPANY:industry::STRING AS INDUSTRY,
        COMPANY:subIndustry::STRING AS SUB_INDUSTRY
    FROM
        base.account
),

SolutionDates AS (
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
        DEPARTMENT,
        WT_COMPLEXITY,
        API_APPS
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
        pd.USERS_COLLAB_INTERNAL_ACTIVE,
        pd.USERS_COLLAB_EXTERNAL,
        pd.USERS_COLLAB_EXTERNAL_ACTIVE,
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

FilteredWorkApps AS (
    SELECT
        CONTAINER_UID,
        EFFECTIVE_FROM,
        EXTRACT(YEAR FROM EFFECTIVE_FROM) AS YEAR,
        EXTRACT(MONTH FROM EFFECTIVE_FROM) AS MONTH_CALENDAR,
        SPLIT_PART(OWNER_PLAN_UID, '.', 3) AS PLAN_UID,
        OWNER_USER_UID,
        STATUS,
        CHANGES
    FROM
        PRODUCT.CONTAINER_WORKAPP_J
    WHERE
        STATUS = 'Active'
),


FilteredUsers AS (
    SELECT
        USER_UID,
        PLAN_UID
    FROM
        BASE.USER_J
    WHERE
        PLAN_UID IS NOT NULL
),


MobileSessions AS (
    SELECT
        su.USER_UID,
        fu.PLAN_UID,
        COUNT(DISTINCT su.SESSION_UID) AS mobile_sessions
    FROM
        BASE.SESSION su
    JOIN
        FilteredUsers fu ON su.USER_UID = fu.USER_UID
    WHERE
        su.DEVICE LIKE 'Mobile%'
    GROUP BY
        su.USER_UID, fu.PLAN_UID
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
        pd.USERS_COLLAB_INTERNAL_ACTIVE,
        pd.USERS_COLLAB_EXTERNAL,
        pd.USERS_COLLAB_EXTERNAL_ACTIVE,
        sd.YEAR_FISCAL,
        sd.YEAR,
        sd.MONTH_CALENDAR,
        sd.YEAR_MONTH,
        -- Calculate total active users
        pd.USERS_COLLAB_INTERNAL_ACTIVE + pd.USERS_COLLAB_EXTERNAL_ACTIVE AS total_active_users,
  
        -- Calculate unique solution owners per account and fiscal month
        COUNT(DISTINCT csm.OWNER_UID) OVER (PARTITION BY fp.ACCOUNT_ID, sd.YEAR_MONTH) AS unique_solution_owners,
        -- Calculate owners with more than 10 solutions per account and fiscal month
        COUNT(DISTINCT CASE WHEN osc.total_solutions_per_owner > 10 THEN csm.OWNER_UID END) OVER (PARTITION BY fp.ACCOUNT_ID, sd.YEAR_MONTH) AS power_developers,
        -- Calculate total solutions per account and fiscal month
        COUNT(csm.SOLUTION_UID) OVER (PARTITION BY fp.ACCOUNT_ID, sd.YEAR_MONTH) AS total_solutions_per_account,
  
        -- Calculate external integrations per account and fiscal month
        COUNT(DISTINCT CASE WHEN csm.API_INTEGRATIONS IS NOT NULL THEN csm.SOLUTION_UID END) OVER (PARTITION BY fp.ACCOUNT_ID, sd.YEAR_MONTH) AS external_integrations,
        -- Calculate internal integrations per account and fiscal month
        COUNT(DISTINCT CASE WHEN csm.API_APPS IS NOT NULL THEN csm.SOLUTION_UID END) OVER (PARTITION BY fp.ACCOUNT_ID, sd.YEAR_MONTH) AS internal_integrations,
        -- Calculate solutions with >=1 HVT per account and fiscal month
        COUNT(DISTINCT CASE WHEN csm.USERS_HVT > 0 THEN csm.SOLUTION_UID END) OVER (PARTITION BY fp.ACCOUNT_ID, sd.YEAR_MONTH) AS solutions_with_HVT,
        
        -- Calculate solutions with >=1 external user per account and fiscal month
        COUNT(DISTINCT CASE WHEN pd.USERS_COLLAB_EXTERNAL > 0 THEN csm.SOLUTION_UID END) OVER (PARTITION BY fp.ACCOUNT_ID, sd.YEAR_MONTH) AS solutions_with_external_users,

        -- Calculate unique external collaborators per solution
        COUNT(DISTINCT pd.USERS_COLLAB_EXTERNAL) OVER (PARTITION BY csm.SOLUTION_UID) AS unique_external_collaborators,

        
        -- Calculate solutions created in the past 90 days per account and fiscal month
        COUNT(DISTINCT CASE WHEN csm.DATE >= DATEADD(DAY, -90, CURRENT_DATE()) THEN csm.SOLUTION_UID END) OVER (PARTITION BY fp.ACCOUNT_ID, sd.YEAR_MONTH) AS solutions_created_last_90_days,
        
        -- Calculate number of departments per solution
        COUNT(DISTINCT CASE WHEN csm.DEPARTMENT IS NOT NULL THEN csm.DEPARTMENT END) OVER (PARTITION BY csm.SOLUTION_UID) AS num_departments_per_solution,

  
        -- Calculate unique HVT collaborators per account and fiscal month
        COUNT(DISTINCT uhv.USERS_HVT) OVER (PARTITION BY fp.ACCOUNT_ID, sd.YEAR_MONTH) AS unique_HVT_collaborators,
        -- Calculate total complexity score per account and fiscal month
        SUM(csm.WT_COMPLEXITY) OVER (PARTITION BY fp.ACCOUNT_ID, sd.YEAR_MONTH) AS total_complexity_score,
  
        
        -- Calculate total automation per account and fiscal month
        COUNT(DISTINCT CASE WHEN csm.WORKFLOWS > 0 THEN csm.SOLUTION_UID END) OVER (PARTITION BY fp.ACCOUNT_ID, sd.YEAR_MONTH) AS automation_count,

        -- Calculate sum of workflows for automation complexity
        SUM(csm.WORKFLOWS) OVER (PARTITION BY fp.ACCOUNT_ID, sd.YEAR_MONTH) AS total_workflows,
       
        
        -- Calculate unique internal collaborators per solution
        COUNT(DISTINCT pd.USERS_COLLAB_INTERNAL) OVER (PARTITION BY csm.SOLUTION_UID) AS unique_internal_collaborators,
        -- Check for presence of Brandfolder
        MAX(CASE WHEN bfp.ACCOUNT_ID IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY fp.ACCOUNT_ID) AS brandfolder_presence,
  
        -- Calculate total work apps per account and fiscal month
        COUNT(DISTINCT fwa.CONTAINER_UID) OVER (PARTITION BY csm.SOLUTION_UID, sd.YEAR, sd.MONTH_CALENDAR) AS work_app_count,
        -- Calculate external views per solution with work app
        COUNT(DISTINCT fwa.OWNER_USER_UID) OVER (PARTITION BY csm.SOLUTION_UID, sd.YEAR, sd.MONTH_CALENDAR) AS external_views_with_work_app,

        -- Calculate the number of active users who used mobile sessions
        COUNT(DISTINCT CASE WHEN ms.mobile_sessions > 0 THEN ms.USER_UID END) OVER (PARTITION BY fp.ACCOUNT_ID, sd.YEAR_MONTH) AS active_mobile_users,
  
        -- -- Calculate the total active users per account and fiscal month
        -- COUNT(DISTINCT pd.USERS_LICENSED_ACTIVE) OVER (PARTITION BY fp.ACCOUNT_ID, sd.YEAR_MONTH) AS total_active_users,

        -- Calculate the percentage of active users who used mobile sessions
        (COUNT(DISTINCT CASE WHEN ms.mobile_sessions > 0 THEN ms.USER_UID END) OVER (PARTITION BY fp.ACCOUNT_ID, sd.YEAR_MONTH) / 
         NULLIF(COUNT(DISTINCT pd.USERS_LICENSED_ACTIVE) OVER (PARTITION BY fp.ACCOUNT_ID, sd.YEAR_MONTH), 0)) * 100 AS pct_active_users_mobile_sessions,

  
        -- Additional columns from account
        MAX(ad.ACCOUNT_NAME) OVER (PARTITION BY fp.ACCOUNT_ID) AS ACCOUNT_NAME,
        MAX(ad.ARR) OVER (PARTITION BY fp.ACCOUNT_ID) AS ACCOUNT_ARR,
        MAX(ad.TERRITORY_TYPE) OVER (PARTITION BY fp.ACCOUNT_ID) AS TERRITORY_TYPE,
        MAX(ad.INDUSTRY) OVER (PARTITION BY fp.ACCOUNT_ID) AS INDUSTRY,
        MAX(ad.SUB_INDUSTRY) OVER (PARTITION BY fp.ACCOUNT_ID) AS SUB_INDUSTRY,
  
//        -- Add percentile ranks for each KPI
//        PERCENT_RANK() OVER (ORDER BY SUM(USERS_LICENSED) DESC) AS pct_rank_licensed_users,
//        PERCENT_RANK() OVER (ORDER BY SUM(USERS_LICENSED_ACTIVE) DESC) AS pct_rank_active_licensed_users,
//        PERCENT_RANK() OVER (ORDER BY SUM(total_active_users) DESC) AS pct_rank_total_active_users,
//        PERCENT_RANK() OVER (ORDER BY SUM(unique_solution_owners) DESC) AS pct_rank_unique_solution_owners,
//        PERCENT_RANK() OVER (ORDER BY SUM(power_developers) DESC) AS pct_rank_power_developers,
//        PERCENT_RANK() OVER (ORDER BY SUM(total_solutions_per_account) DESC) AS pct_rank_total_solutions_per_account,
//        PERCENT_RANK() OVER (ORDER BY SUM(external_integrations) DESC) AS pct_rank_external_integrations,
//        PERCENT_RANK() OVER (ORDER BY SUM(internal_integrations) DESC) AS pct_rank_internal_integrations,
//        PERCENT_RANK() OVER (ORDER BY SUM(solutions_with_HVT) DESC) AS pct_rank_solutions_with_HVT,
//        PERCENT_RANK() OVER (ORDER BY SUM(solutions_with_external_users) DESC) AS pct_rank_solutions_with_external_users,
//        PERCENT_RANK() OVER (ORDER BY SUM(unique_external_collaborators) DESC) AS pct_rank_unique_external_collaborators,
//        PERCENT_RANK() OVER (ORDER BY SUM(solutions_created_last_90_days) DESC) AS pct_rank_solutions_created_last_90_days,
//        PERCENT_RANK() OVER (ORDER BY SUM(num_departments_per_solution) DESC) AS pct_rank_num_departments_per_solution,
//        PERCENT_RANK() OVER (ORDER BY SUM(unique_HVT_collaborators) DESC) AS pct_rank_unique_HVT_collaborators,
//        PERCENT_RANK() OVER (ORDER BY SUM(total_complexity_score) DESC) AS pct_rank_total_complexity_score,
//        PERCENT_RANK() OVER (ORDER BY SUM(automation_count) DESC) AS pct_rank_automation_count,
//        PERCENT_RANK() OVER (ORDER BY SUM(total_workflows) DESC) AS pct_rank_total_workflows,
//        PERCENT_RANK() OVER (ORDER BY SUM(unique_internal_collaborators) DESC) AS pct_rank_unique_internal_collaborators,
//        PERCENT_RANK() OVER (ORDER BY SUM(work_app_count) DESC) AS pct_rank_work_app_count,
//        PERCENT_RANK() OVER (ORDER BY SUM(external_views_with_work_app) DESC) AS pct_rank_external_views_with_work_app,
//        PERCENT_RANK() OVER (ORDER BY MAX(brandfolder_presence) DESC) AS pct_rank_brandfolder_presence
  
  
  
  
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
    JOIN 
        AccountARR aar ON fp.ACCOUNT_ID = aar.ACCOUNT_ID
    LEFT JOIN 
        BrandfolderPresence bfp ON fp.ACCOUNT_ID = bfp.ACCOUNT_ID
    LEFT JOIN
        FilteredWorkApps fwa ON csm.PLAN_UID = fwa.PLAN_UID AND sd.YEAR = fwa.YEAR AND sd.MONTH_CALENDAR = fwa.MONTH_CALENDAR
    LEFT JOIN
        AccountDetails ad ON fp.ACCOUNT_ID = ad.ACCOUNT_ID
    LEFT JOIN
        MobileSessions ms ON csm.PLAN_UID = ms.PLAN_UID
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
    -- Additional columns from account
    MAX(ACCOUNT_NAME) AS ACCOUNT_NAME,
    MAX(ACCOUNT_ARR) AS ACCOUNT_ARR,
    MAX(TERRITORY_TYPE) AS TERRITORY_TYPE,
    MAX(INDUSTRY) AS INDUSTRY,
    MAX(SUB_INDUSTRY) AS SUB_INDUSTRY,
    
    
    -- KPI Calculations
    -- % Licensed Users
     (SUM(USERS_LICENSED_ACTIVE) / NULLIF(SUM(USERS_LICENSED), 0)) * 100 AS pct_licensed_users,
    PERCENT_RANK() OVER (ORDER BY (SUM(USERS_LICENSED_ACTIVE) / NULLIF(SUM(USERS_LICENSED), 0))) AS pct_rank_licensed_users,
    CASE 
        WHEN pct_rank_licensed_users >= 0.90 THEN 7
        WHEN pct_rank_licensed_users >= 0.70 AND pct_rank_licensed_users < 0.90 THEN 4
        WHEN pct_rank_licensed_users >= 0.35 AND pct_rank_licensed_users < 0.70 THEN 3
        WHEN pct_rank_licensed_users > 0 AND pct_rank_licensed_users < 0.35 THEN 1
        ELSE 0
      END AS score_licensed_users,

    -- % Active Users
    (SUM(total_active_users) / NULLIF(SUM(USERS_LICENSED_ACTIVE), 0)) * 100 AS pct_active_users,
    PERCENT_RANK() OVER (ORDER BY (SUM(total_active_users) / NULLIF(SUM(USERS_LICENSED_ACTIVE), 0))) AS pct_rank_active_users,
    CASE 
        WHEN pct_rank_active_users >= 0.90 THEN 7
        WHEN pct_rank_active_users >= 0.70 AND pct_rank_active_users < 0.90 THEN 4
        WHEN pct_rank_active_users >= 0.35 AND pct_rank_active_users < 0.70 THEN 3
        WHEN pct_rank_active_users > 0 AND pct_rank_active_users < 0.35 THEN 1
        ELSE 0
      END AS score_active_users,

    -- % Internal Collaborator
    (SUM(USERS_COLLAB_INTERNAL_ACTIVE) / NULLIF(SUM(total_active_users), 0)) * 100 AS pct_internal_collaborator,
    PERCENT_RANK() OVER (ORDER BY (SUM(USERS_COLLAB_INTERNAL_ACTIVE) / NULLIF(SUM(total_active_users), 0))) AS pct_rank_internal_collaborator,
    CASE 
        WHEN pct_rank_internal_collaborator >= 0.90 THEN 7
        WHEN pct_rank_internal_collaborator >= 0.70 AND pct_rank_internal_collaborator < 0.90 THEN 4
        WHEN pct_rank_internal_collaborator >= 0.35 AND pct_rank_internal_collaborator < 0.70 THEN 3
        WHEN pct_rank_internal_collaborator > 0 AND pct_rank_internal_collaborator < 0.35 THEN 1
        ELSE 0
      END AS score_internal_collaborator,

    -- % External Collaborator
    (SUM(USERS_COLLAB_EXTERNAL_ACTIVE) / NULLIF(SUM(total_active_users), 0)) * 100 AS pct_external_collaborator,
    PERCENT_RANK() OVER (ORDER BY (SUM(USERS_COLLAB_EXTERNAL_ACTIVE) / NULLIF(SUM(total_active_users), 0))) AS pct_rank_external_collaborator,
    CASE 
        WHEN pct_rank_external_collaborator >= 0.90 THEN 7
        WHEN pct_rank_external_collaborator >= 0.70 AND pct_rank_external_collaborator < 0.90 THEN 4
        WHEN pct_rank_external_collaborator >= 0.35 AND pct_rank_external_collaborator < 0.70 THEN 3
        WHEN pct_rank_external_collaborator > 0 AND pct_rank_external_collaborator < 0.35 THEN 1
        ELSE 0
      END AS score_external_collaborator,

    -- % Active Creators
    (SUM(unique_solution_owners) / NULLIF(SUM(total_solutions_per_account), 0)) * 100 AS pct_active_creators,
    PERCENT_RANK() OVER (ORDER BY (SUM(unique_solution_owners) / NULLIF(SUM(total_solutions_per_account), 0))) AS pct_rank_active_creators,
    CASE 
        WHEN pct_rank_active_creators >= 0.90 THEN 7
        WHEN pct_rank_active_creators >= 0.70 AND pct_rank_active_creators < 0.90 THEN 4
        WHEN pct_rank_active_creators >= 0.35 AND pct_rank_active_creators < 0.70 THEN 3
        WHEN pct_rank_active_creators > 0 AND pct_rank_active_creators < 0.35 THEN 1
        ELSE 0
      END AS score_active_creators,

    -- % Power Developer
    (SUM(power_developers) / NULLIF(SUM(USERS_LICENSED), 0)) * 100 AS pct_power_developer,
    PERCENT_RANK() OVER (ORDER BY (SUM(power_developers) / NULLIF(SUM(USERS_LICENSED), 0))) AS pct_rank_power_developer,
    CASE 
        WHEN pct_rank_power_developer >= 0.90 THEN 7
        WHEN pct_rank_power_developer >= 0.70 AND pct_rank_power_developer < 0.90 THEN 4
        WHEN pct_rank_power_developer >= 0.35 AND pct_rank_power_developer < 0.70 THEN 3
        WHEN pct_rank_power_developer > 0 AND pct_rank_power_developer < 0.35 THEN 1
        ELSE 0
      END AS score_power_developer,

    -- % Solution Creators
    (SUM(unique_solution_owners) / NULLIF(SUM(USERS_LICENSED), 0)) * 100 AS pct_solution_creators,
    PERCENT_RANK() OVER (ORDER BY (SUM(unique_solution_owners) / NULLIF(SUM(USERS_LICENSED), 0))) AS pct_rank_solution_creators,
    CASE 
        WHEN pct_rank_solution_creators >= 0.90 THEN 7
        WHEN pct_rank_solution_creators >= 0.70 AND pct_rank_solution_creators < 0.90 THEN 4
        WHEN pct_rank_solution_creators >= 0.35 AND pct_rank_solution_creators < 0.70 THEN 3
        WHEN pct_rank_solution_creators > 0 AND pct_rank_solution_creators < 0.35 THEN 1
        ELSE 0
      END AS score_solution_creators,

    -- Average Internal Collaboration per Solution
    (SUM(unique_internal_collaborators) / NULLIF(SUM(total_solutions_per_account), 0)) AS avg_internal_collab_per_solution,
    PERCENT_RANK() OVER (ORDER BY (SUM(unique_internal_collaborators) / NULLIF(SUM(total_solutions_per_account), 0))) AS pct_rank_internal_collab_per_solution,
    CASE 
        WHEN pct_rank_internal_collab_per_solution >= 0.90 THEN 7
        WHEN pct_rank_internal_collab_per_solution >= 0.70 AND pct_rank_internal_collab_per_solution < 0.90 THEN 4
        WHEN pct_rank_internal_collab_per_solution >= 0.35 AND pct_rank_internal_collab_per_solution < 0.70 THEN 3
        WHEN pct_rank_internal_collab_per_solution > 0 AND pct_rank_internal_collab_per_solution < 0.35 THEN 1
        ELSE 0
      END AS score_internal_collab_per_solution,

    -- Average unique External Collaboration per Solution
    (SUM(solutions_with_external_users) / NULLIF(SUM(total_solutions_per_account), 0)) AS avg_unique_external_collab_per_solution,
    PERCENT_RANK() OVER (ORDER BY (SUM(solutions_with_external_users) / NULLIF(SUM(total_solutions_per_account), 0))) AS pct_rank_unique_external_collab_per_solution,
    CASE 
        WHEN pct_rank_unique_external_collab_per_solution >= 0.90 THEN 7
        WHEN pct_rank_unique_external_collab_per_solution >= 0.70 AND pct_rank_unique_external_collab_per_solution < 0.90 THEN 4
        WHEN pct_rank_unique_external_collab_per_solution >= 0.35 AND pct_rank_unique_external_collab_per_solution < 0.70 THEN 3
        WHEN pct_rank_unique_external_collab_per_solution > 0 AND pct_rank_unique_external_collab_per_solution < 0.35 THEN 1
        ELSE 0
      END AS score_unique_external_collab_per_solution,

    -- Average External Collaboration per Solution
    (SUM(unique_external_collaborators) / NULLIF(SUM(total_solutions_per_account), 0)) AS avg_external_collab_per_solution,
    PERCENT_RANK() OVER (ORDER BY (SUM(unique_external_collaborators) / NULLIF(SUM(total_solutions_per_account), 0))) AS pct_rank_external_collab_per_solution,
    CASE 
        WHEN pct_rank_external_collab_per_solution >= 0.90 THEN 7
        WHEN pct_rank_external_collab_per_solution >= 0.70 AND pct_rank_external_collab_per_solution < 0.90 THEN 4
        WHEN pct_rank_external_collab_per_solution >= 0.35 AND pct_rank_external_collab_per_solution < 0.70 THEN 3
        WHEN pct_rank_external_collab_per_solution > 0 AND pct_rank_external_collab_per_solution < 0.35 THEN 1
        ELSE 0
      END AS score_external_collab_per_solution,

    -- Average unique HVT Collaboration per Solution
    (SUM(solutions_with_HVT) / NULLIF(SUM(total_solutions_per_account), 0)) AS avg_unique_HVT_collab_per_solution,
    PERCENT_RANK() OVER (ORDER BY (SUM(solutions_with_HVT) / NULLIF(SUM(total_solutions_per_account), 0))) AS pct_rank_unique_HVT_collab_per_solution,
    CASE 
        WHEN pct_rank_unique_HVT_collab_per_solution >= 0.90 THEN 7
        WHEN pct_rank_unique_HVT_collab_per_solution >= 0.70 AND pct_rank_unique_HVT_collab_per_solution < 0.90 THEN 4
        WHEN pct_rank_unique_HVT_collab_per_solution >= 0.35 AND pct_rank_unique_HVT_collab_per_solution < 0.70 THEN 3
        WHEN pct_rank_unique_HVT_collab_per_solution > 0 AND pct_rank_unique_HVT_collab_per_solution < 0.35 THEN 1
        ELSE 0
      END AS score_unique_HVT_collab_per_solution,

    -- % HVT per solution
    (SUM(unique_HVT_collaborators) / NULLIF(SUM(total_solutions_per_account), 0)) AS pct_HVT_per_solution,
    PERCENT_RANK() OVER (ORDER BY (SUM(unique_HVT_collaborators) / NULLIF(SUM(total_solutions_per_account), 0))) AS pct_rank_HVT_per_solution,
    CASE 
        WHEN pct_rank_HVT_per_solution >= 0.90 THEN 7
        WHEN pct_rank_HVT_per_solution >= 0.70 AND pct_rank_HVT_per_solution < 0.90 THEN 4
        WHEN pct_rank_HVT_per_solution >= 0.35 AND pct_rank_HVT_per_solution < 0.70 THEN 3
        WHEN pct_rank_HVT_per_solution > 0 AND pct_rank_HVT_per_solution < 0.35 THEN 1
        ELSE 0
      END AS score_HVT_per_solution,

    -- Average Departments per Solution
    (SUM(num_departments_per_solution) / NULLIF(SUM(total_solutions_per_account), 0)) AS avg_departments_per_solution,
    PERCENT_RANK() OVER (ORDER BY (SUM(num_departments_per_solution) / NULLIF(SUM(total_solutions_per_account), 0))) AS pct_rank_departments_per_solution,
    CASE 
        WHEN pct_rank_departments_per_solution >= 0.90 THEN 7
        WHEN pct_rank_departments_per_solution >= 0.70 AND pct_rank_departments_per_solution < 0.90 THEN 4
        WHEN pct_rank_departments_per_solution >= 0.35 AND pct_rank_departments_per_solution < 0.70 THEN 3
        WHEN pct_rank_departments_per_solution > 0 AND pct_rank_departments_per_solution < 0.35 THEN 1
        ELSE 0
      END AS score_departments_per_solution ,

    -- Quarterly Solution Creation rate
    (SUM(solutions_created_last_90_days) / NULLIF(SUM(total_solutions_per_account), 0)) AS quarterly_solution_creation_rate,
    PERCENT_RANK() OVER (ORDER BY (SUM(solutions_created_last_90_days) / NULLIF(SUM(total_solutions_per_account), 0))) AS pct_rank_quarterly_solution_creation_rate,
    CASE 
        WHEN pct_rank_quarterly_solution_creation_rate >= 0.90 THEN 7
        WHEN pct_rank_quarterly_solution_creation_rate >= 0.70 AND pct_rank_quarterly_solution_creation_rate < 0.90 THEN 4
        WHEN pct_rank_quarterly_solution_creation_rate >= 0.35 AND pct_rank_quarterly_solution_creation_rate < 0.70 THEN 3
        WHEN pct_rank_quarterly_solution_creation_rate > 0 AND pct_rank_quarterly_solution_creation_rate < 0.35 THEN 1
        ELSE 0
      END AS score_quarterly_solution_creation_rate,

    -- % Solution Automation
    (SUM(CASE WHEN automation_count > 0 THEN 1 ELSE 0 END) / NULLIF(SUM(total_solutions_per_account), 0)) * 100 AS pct_solution_automation,
    PERCENT_RANK() OVER (ORDER BY (SUM(CASE WHEN automation_count > 0 THEN 1 ELSE 0 END) / NULLIF(SUM(total_solutions_per_account), 0))) AS pct_rank_solution_automation,
    CASE 
        WHEN pct_rank_solution_automation >= 0.90 THEN 7
        WHEN pct_rank_solution_automation >= 0.70 AND pct_rank_solution_automation < 0.90 THEN 4
        WHEN pct_rank_solution_automation >= 0.35 AND pct_rank_solution_automation < 0.70 THEN 3
        WHEN pct_rank_solution_automation > 0 AND pct_rank_solution_automation < 0.35 THEN 1
        ELSE 0
      END AS score_solution_automation,

    -- % Automation Complexity
    (SUM(total_workflows) / NULLIF(SUM(total_solutions_per_account), 0)) AS pct_automation_complexity,
    PERCENT_RANK() OVER (ORDER BY (SUM(total_workflows) / NULLIF(SUM(total_solutions_per_account), 0))) AS pct_rank_automation_complexity,
    CASE 
        WHEN pct_rank_automation_complexity >= 0.90 THEN 7
        WHEN pct_rank_automation_complexity >= 0.70 AND pct_rank_automation_complexity < 0.90 THEN 4
        WHEN pct_rank_automation_complexity >= 0.35 AND pct_rank_automation_complexity < 0.70 THEN 3
        WHEN pct_rank_automation_complexity > 0 AND pct_rank_automation_complexity < 0.35 THEN 1
        ELSE 0
      END AS score_automation_complexity,

    -- Presence of Brandfolder
    MAX(brandfolder_presence) AS brandfolder_presence,
    PERCENT_RANK() OVER (ORDER BY MAX(brandfolder_presence)) AS pct_rank_brandfolder_presence,
    CASE 
        WHEN pct_rank_brandfolder_presence >= 0.90 THEN 7
        WHEN pct_rank_brandfolder_presence >= 0.70 AND pct_rank_brandfolder_presence < 0.90 THEN 4
        WHEN pct_rank_brandfolder_presence >= 0.35 AND pct_rank_brandfolder_presence < 0.70 THEN 3
        WHEN pct_rank_brandfolder_presence > 0 AND pct_rank_brandfolder_presence < 0.35 THEN 1
        ELSE 0
      END AS score_brandfolder_presence,

    -- % Low Code
    (SUM(work_app_count) / NULLIF(SUM(total_solutions_per_account), 0)) * 100 AS pct_low_code,
    PERCENT_RANK() OVER (ORDER BY (SUM(work_app_count) / NULLIF(SUM(total_solutions_per_account), 0))) AS pct_rank_low_code,
    CASE 
        WHEN pct_rank_low_code >= 0.90 THEN 7
        WHEN pct_rank_low_code >= 0.70 AND pct_rank_low_code < 0.90 THEN 4
        WHEN pct_rank_low_code >= 0.35 AND pct_rank_low_code < 0.70 THEN 3
        WHEN pct_rank_low_code > 0 AND pct_rank_low_code < 0.35 THEN 1
        ELSE 0
      END AS score_low_code,

    -- % Low Code external collab
    (SUM(external_views_with_work_app) / NULLIF(SUM(work_app_count), 0)) * 100 AS pct_low_code_external_collab,
    PERCENT_RANK() OVER (ORDER BY (SUM(external_views_with_work_app) / NULLIF(SUM(work_app_count), 0))) AS pct_rank_low_code_external_collab,
    CASE 
        WHEN pct_rank_low_code_external_collab >= 0.90 THEN 7
        WHEN pct_rank_low_code_external_collab >= 0.70 AND pct_rank_low_code_external_collab < 0.90 THEN 4
        WHEN pct_rank_low_code_external_collab >= 0.35 AND pct_rank_low_code_external_collab < 0.70 THEN 3
        WHEN pct_rank_low_code_external_collab > 0 AND pct_rank_low_code_external_collab < 0.35 THEN 1
        ELSE 0
      END AS score_low_code_external_collab,

     -- % Active users who used mobile sessions
    (SUM(active_mobile_users) / NULLIF(SUM(total_active_users), 0)) * 100 AS pct_active_users_mobile_sessions,
    PERCENT_RANK() OVER (ORDER BY (SUM(active_mobile_users) / NULLIF(SUM(total_active_users), 0))) AS pct_rank_active_users_mobile_sessions,
    CASE 
        WHEN pct_rank_active_users_mobile_sessions >= 0.90 THEN 7
        WHEN pct_rank_active_users_mobile_sessions >= 0.70 AND pct_rank_active_users_mobile_sessions < 0.90 THEN 4
        WHEN pct_rank_active_users_mobile_sessions >= 0.35 AND pct_rank_active_users_mobile_sessions < 0.70 THEN 3
        WHEN pct_rank_active_users_mobile_sessions > 0 AND pct_rank_active_users_mobile_sessions < 0.35 THEN 1
        ELSE 0
      END AS score_active_users_mobile_sessions,
    
FROM 
    AggregatedData
GROUP BY 
    ACCOUNT_ID, YEAR_FISCAL, YEAR, MONTH_CALENDAR, YEAR_MONTH
ORDER BY 
    ACCOUNT_ID, YEAR_MONTH
    
LIMIT 10;
