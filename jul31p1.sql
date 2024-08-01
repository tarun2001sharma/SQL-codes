
WITH FilteredCSM AS (
    SELECT 
        CSM.SOLUTION_UID,
        CSM.DATE,
        CSM.PLAN_UID,
        CSM.USERS_HVT,
        CSM.INTEGRATIONS,
        CSM.WORKFLOWS,
        CSM.API_INTEGRATIONS,
        CSM.QUARTERS_ACTIVE,
        CSM.USERS_ACTIVE,
        CSM.USERS_EXTERNAL,
        CSM.OWNER_UID,
        CSM.EXTERNAL,
        CSM.USERS_EXTERNAL_VIEW,
        REPLACE(CSM.DEPARTMENT, ' ', '_') AS DEPARTMENT, 
        CSM.WT_COMPLEXITY,
        CSM.API_APPS,
        BA.ACCOUNT_ID AS ACCOUNT_ID,
        BA.NAME AS ACCOUNT_NAME,
        ROUND(BA.ARR, 0) AS ACCOUNT_ARR,
        BA.company:industry::varchar AS INDUSTRY,
        BA.company:industry_sub::varchar AS SUB_INDUSTRY,
        BA.TERRITORY_TYPE,
        pd.USERS_LICENSED,
        pd.USERS_LICENSED_ACTIVE,
        pd.USERS_COLLAB_INTERNAL,
        pd.USERS_COLLAB_INTERNAL_ACTIVE,
        pd.USERS_COLLAB_EXTERNAL,
        pd.USERS_COLLAB_EXTERNAL_ACTIVE
    FROM 
        NEXUS.PRODUCT.CONTAINER_SOLUTION_MONTHLY AS CSM
    LEFT JOIN 
        NEXUS.BASE.PLAN AS BP ON BP.PLAN_UID = CSM.PLAN_UID
    LEFT JOIN 
        NEXUS.BASE.ACCOUNT AS BA ON BA.ACCOUNT_ID = BP.ACCOUNT_ID
    LEFT JOIN 
        NEXUS.BASE.PLAN_DAILY AS PD ON PD.PLAN_UID = BP.PLAN_UID AND CSM.DATE = PD.DATE
    WHERE 
        BA.ARR >= 5000
        AND CSM.DATE = '2024-07-01'
        AND BA.ACCOUNT_ID = '0013300001cDTdAAAW'
        AND CSM.USERS_ACTIVE >= 1
        AND PD.PLAN_ARR >= 5000
),

AccountsWithMoreThan10Solutions AS (
    SELECT 
        BA.account_id,
        COUNT(CSM.solution_uid) AS solution_count
    FROM 
        NEXUS.PRODUCT.CONTAINER_SOLUTION_MONTHLY AS CSM
    LEFT JOIN 
        NEXUS.BASE.PLAN AS BP ON BP.PLAN_UID = CSM.PLAN_UID
    LEFT JOIN 
        NEXUS.BASE.ACCOUNT AS BA ON BA.ACCOUNT_ID = BP.ACCOUNT_ID
    WHERE 
        BA.ARR >= 5000
        AND CSM.DATE = '2024-07-01'
        AND BA.ACCOUNT_ID IN ('0013300001cDTdAAAW') 
    GROUP BY 
        BA.account_id
    HAVING 
        COUNT(csm.solution_uid) > 10
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

rm_presence AS (
    SELECT
        ACCOUNT_ID,
        1 AS rm_plan
    FROM
        PRODUCT.RM_PLAN
    GROUP BY
        ACCOUNT_ID
),

MobileSessions AS (
    SELECT
        up.PLAN_UID,
        fp.ACCOUNT_ID,
        up.USER_UID,
        COALESCE(ms.mobile_sessions_count, 0) AS mobile_sessions_count
    FROM
        BASE.USER_J up
    JOIN
        (SELECT PLAN_UID, ACCOUNT_ID FROM NEXUS.BASE.PLAN) fp ON up.PLAN_UID = fp.PLAN_UID
    LEFT JOIN
        (SELECT USER_UID, COUNT(SESSION_UID) AS mobile_sessions_count FROM BASE.SESSION WHERE DEVICE LIKE 'Mobile%' GROUP BY USER_UID) ms ON up.USER_UID = ms.USER_UID
),

AccountMobileSessions AS (
    SELECT
        ups.ACCOUNT_ID,
        COUNT(DISTINCT ups.USER_UID) AS mobile_users_count
    FROM
        MobileSessions ups
    GROUP BY
        ups.ACCOUNT_ID
),

-- Filter and Aggregate CONTAINER_WORKAPP_J
FilteredWorkApp AS (
    SELECT 
        FWA.CONTAINER_UID,
        FWA.OWNER_PLAN_UID,
        FWA.OWNER_USER_UID
    FROM 
        PRODUCT.CONTAINER_WORKAPP_J FWA
    WHERE 
        FWA.STATUS = 'Active' -- Assuming you only want active records
        AND FWA.EFFECTIVE_FROM <= '2024-07-01'
        AND (FWA.EFFECTIVE_TO IS NULL OR FWA.EFFECTIVE_TO >= '2024-07-01')
    GROUP BY 
        FWA.CONTAINER_UID, FWA.OWNER_PLAN_UID, FWA.OWNER_USER_UID
),

AggregatedData AS (
    SELECT 
        FSM.ACCOUNT_ID, 
        TO_CHAR(FSM.DATE, 'YYYY-MM') AS month_year,
        FSM.ACCOUNT_NAME AS account_name,
        FSM.ACCOUNT_ARR AS account_arr,
        FSM.TERRITORY_TYPE AS territory_type,
        FSM.INDUSTRY AS industry,
        FSM.SUB_INDUSTRY AS sub_industry,
        
        SUM(DISTINCT FSM.USERS_LICENSED) AS total_licensed_users,
        SUM(DISTINCT FSM.USERS_LICENSED_ACTIVE) AS total_active_licensed_users,
        SUM(DISTINCT FSM.USERS_COLLAB_INTERNAL_ACTIVE) AS total_internal_collaborators_active,
        SUM(DISTINCT FSM.USERS_COLLAB_EXTERNAL_ACTIVE) AS total_external_collaborators_active,
        SUM(DISTINCT FSM.USERS_COLLAB_INTERNAL) AS total_internal_collaborators,
        SUM(DISTINCT FSM.USERS_COLLAB_EXTERNAL) AS total_external_collaborators,
        SUM(DISTINCT (FSM.USERS_COLLAB_INTERNAL_ACTIVE + FSM.USERS_COLLAB_EXTERNAL_ACTIVE + FSM.USERS_LICENSED_ACTIVE)) AS total_active_users,
  
        COUNT(DISTINCT FSM.OWNER_UID) AS unique_solution_owners,
        COUNT(DISTINCT CASE WHEN OSC.total_solutions_per_owner > 10 THEN FSM.OWNER_UID END) AS power_developers,
        COUNT(CASE WHEN FSM.USERS_ACTIVE > 0 THEN FSM.SOLUTION_UID END) AS total_solutions_per_account,
        COUNT(CASE WHEN len(FSM.API_INTEGRATIONS) > 0 THEN FSM.SOLUTION_UID END) AS external_integrations,
        COUNT(CASE WHEN len(FSM.API_APPS) > 0 THEN FSM.SOLUTION_UID END) AS internal_integrations,
        COUNT(CASE WHEN FSM.USERS_HVT > 0 THEN FSM.SOLUTION_UID END) AS solutions_with_HVT,
  
        COUNT(DISTINCT CASE WHEN FSM.USERS_HVT > 0 THEN FSM.USERS_HVT END) AS unique_hvt_collaborators_per_solution,

        COUNT(CASE WHEN FSM.USERS_EXTERNAL > 0 THEN FSM.SOLUTION_UID END) AS solutions_with_external_users,
        COUNT(FSM.USERS_COLLAB_EXTERNAL) AS unique_external_collaborators,

        COUNT(CASE WHEN FSM.QUARTERS_ACTIVE = 1 THEN FSM.SOLUTION_UID END) AS solutions_created_last_90_days,
        COUNT(DISTINCT FSM.USERS_HVT) AS unique_HVT_collaborators,
        AVG(DISTINCT FSM.WT_COMPLEXITY) AS total_complexity_score,
        COUNT(CASE WHEN FSM.WORKFLOWS > 0 THEN FSM.SOLUTION_UID END) AS solutions_automation,
        SUM(DISTINCT FSM.WORKFLOWS) AS total_workflows,
        COUNT(FSM.USERS_COLLAB_INTERNAL) AS unique_internal_collaborators,
        MAX(CASE WHEN BFP.ACCOUNT_ID IS NOT NULL THEN 1 ELSE 0 END) AS brandfolder_presence,
        MAX(AMS.mobile_users_count) AS mobile_users_count,
        COALESCE(MAX(RM.rm_plan), 0) AS presence_of_rm_plan,
  
        -- Calculate Work App-related KPIs using FilteredWorkApp
        COUNT(DISTINCT CASE WHEN FWA.CONTAINER_UID IS NOT NULL THEN FSM.SOLUTION_UID END) AS work_app_count,
        SUM(CASE WHEN FWA.CONTAINER_UID IS NOT NULL THEN FSM.USERS_EXTERNAL_VIEW ELSE 0 END) AS external_views_with_work_app
    FROM 
        FilteredCSM FSM
    JOIN
        AccountsWithMoreThan10Solutions A10 ON FSM.ACCOUNT_ID = A10.ACCOUNT_ID
    LEFT JOIN 
        OwnerSolutionCounts OSC ON FSM.OWNER_UID = OSC.OWNER_UID
    LEFT JOIN 
        rm_presence RM ON FSM.ACCOUNT_ID = RM.ACCOUNT_ID
    LEFT JOIN 
        (SELECT ACCOUNT_ID, 1 AS brandfolder_presence FROM PRODUCT.RM_PLAN GROUP BY ACCOUNT_ID) BFP ON FSM.ACCOUNT_ID = BFP.ACCOUNT_ID
    LEFT JOIN 
        FilteredWorkApp FWA ON FWA.CONTAINER_UID = FSM.SOLUTION_UID
    LEFT JOIN 
        AccountMobileSessions AMS ON FSM.ACCOUNT_ID = AMS.ACCOUNT_ID
    WHERE
        TO_CHAR(FSM.DATE, 'YYYY-MM') = '2024-07'
    GROUP BY 
        FSM.ACCOUNT_ID, TO_CHAR(FSM.DATE, 'YYYY-MM'), account_name, account_arr, territory_type, industry, sub_industry
)

SELECT * FROM AggregatedData;
