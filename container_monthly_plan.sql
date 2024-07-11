with object_data AS (
  SELECT
    account_id,
    date AS date,
    work_type,
    OBJECT_AGG(DISTINCT item_pa, cnt_pa) AS premium_apps,
    OBJECT_AGG(DISTINCT item_ia, cnt_ia) AS integrations,
    OBJECT_AGG(DISTINCT item_ca, cnt_ca) AS connectors,
    OBJECT_AGG(DISTINCT item_assets, cnt_assets) AS assets
  FROM (
    SELECT
      csm.account_id AS account_id,
      csm.date AS date,
      csm.work_type AS work_type,
      pa.key::STRING AS item_pa,
      sum(value)::int AS cnt_pa,
      NULL::STRING AS item_ia,
      0 AS cnt_ia,
      NULL::STRING AS item_ca,
      0 AS cnt_ca,
      NULL AS item_assets,
      0 AS cnt_assets
    FROM (select * from "NEXUS"."PRODUCT"."CONTAINER_SOLUTION_PLAN_MONTHLY" where WORK_TYPE not in ('Not Active')
         ) AS csm,
      LATERAL FLATTEN(input => csm.premium_apps) AS pa
    GROUP BY csm.account_id, csm.date, csm.work_type, item_pa
    UNION ALL
    SELECT
      csm.account_id AS account_id,
      csm.date AS date,
      csm.work_type AS work_type,
      NULL::STRING AS item_pa,
      0 AS cnt_pa,
      ia.key::STRING AS item_ia,
      sum(value)::int AS cnt_ia,
      NULL::STRING AS item_ca,
      0 AS cnt_ca,
      NULL AS item_assets,
      0 AS cnt_assets
    FROM (select * from "NEXUS"."PRODUCT"."CONTAINER_SOLUTION_PLAN_MONTHLY" where WORK_TYPE not in ('Not Active')
         ) AS csm,
      LATERAL FLATTEN(input => csm.integrations) AS ia
    GROUP BY csm.account_id, csm.date, csm.work_type, item_ia
    UNION ALL
    SELECT
      csm.account_id AS account_id,
      csm.date AS date,
      csm.work_type AS work_type,
      NULL::STRING AS item_pa,
      0 AS cnt_pa,
      NULL::STRING AS item_ia,
      0 AS cnt_ia,
      ca.key::STRING AS item_ca,
      sum(value)::int AS cnt_ca,
      NULL AS item_assets,
      0 AS cnt_assets
    FROM (select * from "NEXUS"."PRODUCT"."CONTAINER_SOLUTION_PLAN_MONTHLY" where WORK_TYPE not in ('Not Active')
         ) AS csm,
      LATERAL FLATTEN(input => csm.connectors) AS ca
    GROUP BY csm.account_id, csm.date, csm.work_type, item_ca
        UNION ALL
    SELECT
      csm.account_id AS account_id,
      csm.date AS date,
      csm.work_type AS work_type,
      NULL::STRING AS item_pa,
      0 AS cnt_pa,
      NULL AS item_ia,
      0 AS cnt_ia,
      NULL::STRING AS item_ca,
      0 AS cnt_ca,
      assets.key::STRING AS item_assets,
      sum(value)::int AS cnt_assets
    FROM (select * from "NEXUS"."PRODUCT"."CONTAINER_SOLUTION_PLAN_MONTHLY" where WORK_TYPE not in ('Not Active')
         ) AS csm,
      LATERAL FLATTEN(input => csm.assets) AS assets
    GROUP BY csm.account_id, csm.date, csm.work_type, item_assets
  )
  GROUP BY account_id, date, work_type
),
account_ppid as
(
select distinct a.account_id,c.name,
  concat(c.name,' (',listagg(distinct replace(replace(a.plan_uid,'plan.com-us.',''),'plan.com-eu.',''),', ') over(partition by a.account_id),')') account_name_ppid
  from "NEXUS"."PRODUCT"."CONTAINER_SOLUTION_PLAN_MONTHLY" a
  left join nexus.base.plan b on a.plan_uid = b.plan_uid
  and a.account_id = b.account_id
  left join nexus.base.account c on a.account_id = c.account_id
  where
  b.arr > 0 and
  b.licenses > 1 and
  c.arr >= '10000' and
  a.account_id is not null
),
agg as
(
select
  coalesce(account_ppid.account_name_ppid,data.name) account_name_ppid,
  data.*,ac.territory,
        ac.territory_type,
        coalesce(ac.first_paid,data.plan_first_paid,ac.session_first) as partner_since,ac.first_paid,ac.session_first,
        ac.USERS_LICENSED_PAID,
        ac.USERS_LICENSED_PAID_ACTIVE, ac.LICENSES_PAID from
      ( select a.date,a.account_id,acc.name,
        max(acc.arr) arr,max(plan_arr) plan_arr,min(plan_first_paid) plan_first_paid,max(LICENSES) as LICENSES,max(USERS_LICENSED) as USERS_LICENSED,max(USERS_LICENSED_ACTIVE) USERS_LICENSED_ACTIVE,
        max(USERS_COLLAB_EXTERNAL) USERS_COLLAB_EXTERNAL, max(USERS_COLLAB_INTERNAL) USERS_COLLAB_INTERNAL,
        sum(DAYS_EDIT) DAYS_EDIT, sum(DAYS_EDIT_HVT) DAYS_EDIT_HVT,sum(DAYS_EDIT_NON_HVT)DAYS_EDIT_NON_HVT,max(quarters_active)quarters_active,sum(total_count) total_count,sum(users_active) users_active,
        sum(users_edit) users_edit, sum(users_external) users_external,sum(users_hvt)users_hvt,sum(users_hvt_editor)users_hvt_editor,sum(users_hvt_viewer)users_hvt_viewer,
        sum(users_non_hvt_editor)users_non_hvt_editor,sum(users_non_hvt_viewer)users_non_hvt_viewer,
        sum(users_view)users_view,sum(DAYS_VIEW)DAYS_VIEW,sum(DAYS_VIEW_HVT)DAYS_VIEW_HVT,sum(DAYS_VIEW_NON_HVT)DAYS_VIEW_NON_HVT,work_type
        from "NEXUS"."PRODUCT"."CONTAINER_SOLUTION_PLAN_MONTHLY" a join
                  (
                    select account_id,date,name,max(arr) arr,sum(plan_arr) plan_arr,min(plan_first_paid) plan_first_paid,
                    sum(LICENSES) LICENSES,sum(USERS_LICENSED) USERS_LICENSED,sum(USERS_LICENSED_ACTIVE) USERS_LICENSED_ACTIVE,
                    sum(USERS_COLLAB_EXTERNAL) USERS_COLLAB_EXTERNAL, sum(USERS_COLLAB_INTERNAL) USERS_COLLAB_INTERNAL from
                        (
                          select a.plan_uid,d.account_id,a.plan_arr as plan_arr,d.arr as arr,a.date,d.name, C.first_paid as plan_first_paid ,
                          a.LICENSES,a.USERS_LICENSED,a.USERS_LICENSED_ACTIVE, USERS_COLLAB_EXTERNAL, USERS_COLLAB_INTERNAL
                          from nexus.base.plan_daily a
                          LEFT JOIN NEXUS.BASE.PLAN C
                          ON A.PLAN_UID = C.PLAN_UID
                          LEFT JOIN NEXUS.BASE.ACCOUNT D
                            ON C.ACCOUNT_ID=D.ACCOUNT_ID where c.arr > '0' and c.licenses > '1' --and d.account_id = '0010b00002Lm1wZAAR'
                          )group by account_id,date,name
                  ) acc on a.account_id = acc.account_id and a.date = acc.date
        where acc.arr>= '10000'  and WORK_TYPE not in ('Not Active')
        group by work_type,a.account_id,name,a.date order by arr desc
      )
      data
      join nexus.base.account ac
      on data.account_id = ac.account_id
      left join account_ppid on data.account_id = account_ppid.account_id
)
SELECT
  names.date,names.account_name_ppid,
  names.account_id,names.NAME,names.plan_arr,names.arr,names.first_paid,names.session_first,a.LICENSES_PAID,a.USERS_COLLAB_INTERNAL,a.USERS_COLLAB_EXTERNAL,a.LICENSES, a.USERS_LICENSED,a.USERS_LICENSED_ACTIVE,
  a.DAYS_EDIT,a.DAYS_EDIT_HVT,a.DAYS_EDIT_NON_HVT,a.QUARTERS_ACTIVE,a.TOTAL_COUNT,a.USERS_ACTIVE,a.USERS_EDIT,a.USERS_EXTERNAL,a.USERS_HVT,
  a.USERS_HVT_EDITOR,a.USERS_HVT_VIEWER,a.USERS_NON_HVT_EDITOR,a.USERS_NON_HVT_VIEWER,a.USERS_VIEW,a.DAYS_VIEW,a.DAYS_VIEW_HVT,a.DAYS_VIEW_NON_HVT,worktypes.work_type,
  a.TERRITORY,a.TERRITORY_TYPE,names.partner_since,a.USERS_LICENSED_PAID,a.USERS_LICENSED_PAID_ACTIVE,a.PREMIUM_APPS,a.INTEGRATIONS,a.CONNECTORS,a.ASSETS
FROM
  (SELECT DISTINCT account_id,date,name,coalesce(plan_arr ,0) as plan_arr,coalesce(arr ,0) as arr,session_first,first_paid,partner_since,account_name_ppid FROM agg WHERE date >= DATEADD(MONTH, -24, CURRENT_DATE())) names
CROSS JOIN
  (SELECT DISTINCT work_type FROM agg) worktypes
LEFT JOIN
 (select agg.*,o.PREMIUM_APPS,o.INTEGRATIONS,o.CONNECTORS,o.ASSETS
from object_data o join agg on o.account_id = agg.account_id and o.work_type = agg.work_type and o.date = agg.date ) a
ON
  names.account_id = a.account_id
  AND names.date = a.date
  AND names.name = a.name
  AND names.plan_arr = a.plan_arr
  AND names.arr = a.arr
  AND names.partner_since = a.partner_since
  AND names.account_name_ppid = a.account_name_ppid
  AND worktypes.work_type = a.work_type
  order by date desc
