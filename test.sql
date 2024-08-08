--SA edited
CALL
  `pypl-edw`.pp_monitor.drop_table (
    '${dataset_name_tmp}.check_decline_rule_base'
  );


CREATE TABLE
  `pypl-edw.${dataset_name_tmp}.check_decline_rule_base`
CLUSTER BY
  rule_id AS
SELECT
  st.rule_id                     ,
  st.priority                    ,
  st.rule_name                   ,
  st.cg          AS control_group,
  crs.crs_team                   ,
  crs.owner_ntid                 ,
  mo.rule_mo                     ,
  st.tags                        ,
  CASE
    WHEN DATE_DIFF(
      DATE_SUB(
        CURRENT_DATE('America/Los_Angeles'),
        INTERVAL 7 DAY
      )      ,
      EXTRACT(
        DATE
        FROM
          st.created_ts
      ) ,
      DAY
    ) + 1 >= 84 THEN 84
    ELSE DATE_DIFF(
      DATE_SUB(
        CURRENT_DATE('America/Los_Angeles'),
        INTERVAL 7 DAY
      )      ,
      EXTRACT(
        DATE
        FROM
          st.created_ts
      ) ,
      DAY
    ) + 1
  END AS observing_window_raw,
  DATE_DIFF(
    CURRENT_DATE('America/Los_Angeles'),
    EXTRACT(
      DATE
      FROM
        activation_updated_ts
    ) ,
    DAY
  ) + 1 AS last_release_length
FROM
  (
    SELECT
      --  how many days has the rule been running since the last update
      *
    FROM
      `pypl-edw.${dataset_name_tmp}.urm_lite` AS live_metadata_table
    WHERE
      UPPER(
        live_metadata_table.chkpnt_delimited_str
      ) LIKE '%CONSOLIDATEDFUNDING%'
      AND (
        UPPER(
          live_metadata_table.action_raw_str
        ) LIKE '%DECLINE_PMT%'
        OR UPPER(
          live_metadata_table.action_raw_str
        ) LIKE '%AUTH_FLOW%'
        OR UPPER(
          live_metadata_table.action_raw_str
        ) LIKE '%DISALLOW%'
      )
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%WHITE_LIST%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%WHITELIST%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%ADDLMTNINPUTMAP%SETISDISALLOW%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%SET_IS_DISALLOW%'
  ) AS st
  INNER JOIN `pypl-edw.${dataset_name_tmp}.crs_all_funding` AS crs ON st.rule_id = crs.rule_id
  INNER JOIN (
    SELECT
      *
    FROM
      `pypl-edw`.pp_risk_crs_core.rule_mo_funding_uni AS rule_mo_funding_table
    QUALIFY
      ROW_NUMBER() OVER (
        PARTITION BY
          rule_mo_funding_table.rule_id
        ORDER BY
          rule_mo_funding_table.baseline DESC
      ) = 1
  ) AS mo ON crs.rule_id = mo.rule_id;


--  on commit preserve rows
CALL
  `pypl-edw`.pp_monitor.drop_table (
    '${dataset_name_tmp}.decline_eval_end_date'
  );


CREATE TABLE
  `pypl-edw.${dataset_name_tmp}.decline_eval_end_date`
CLUSTER BY
  rule_id AS
SELECT
  unified_rule_metadata_history.rule_id    ,
  unified_rule_metadata_history.baseline_ts,
  PARSE_DATE(
    '%Y%m%d',
    SUBSTR(
      unified_rule_metadata_history.baseline,
      1                                     ,
      8
    )
  ) AS last_eval_baseline
FROM
  `pypl-edw`.pp_dama_ces_prototype_tables.unified_rule_metadata_history AS unified_rule_metadata_history
WHERE
  UPPER(
    unified_rule_metadata_history.action_raw_str
  ) LIKE '%EVALUATE%'
  AND UPPER(
    unified_rule_metadata_history.action_raw_str
  ) NOT LIKE '%WHITE_LIST%'
  AND UPPER(
    unified_rule_metadata_history.action_raw_str
  ) NOT LIKE '%WHITELIST%'
  AND UPPER(
    unified_rule_metadata_history.action_raw_str
  ) NOT LIKE '%RESTRICT%'
  AND UPPER(
    unified_rule_metadata_history.action_raw_str
  ) NOT LIKE '%DECLINE%'
  AND UPPER(
    unified_rule_metadata_history.action_raw_str
  ) NOT LIKE '%DISALLOW%'
  AND UPPER(
    unified_rule_metadata_history.action_raw_str
  ) NOT LIKE '%BLOCK%'
  AND UPPER(
    unified_rule_metadata_history.action_raw_str
  ) NOT LIKE '%AUTH%'
  AND UPPER(
    unified_rule_metadata_history.chkpnt_delimited_str
  ) LIKE '%CONSOLIDATEDFUNDING%'
  AND unified_rule_metadata_history.rule_id IN (
    SELECT
      rule_id AS rule_id
    FROM
      `pypl-edw.pp_risk_crs_core.check_decline_rule_base`
  )
  AND CAST(
    unified_rule_metadata_history.baseline_ts AS DATE
  ) >= DATE_SUB(
    CURRENT_DATE('America/Los_Angeles'),
    INTERVAL 120 DAY
  ) --((extract(YEAR from CAST(unified_rule_metadata_history.baseline_ts as DATE)) - 1900) * 10000 + extract(MONTH from CAST(unified_rule_metadata_history.baseline_ts as DATE)) * 100 + extract(DAY from CAST(unified_rule_metadata_history.baseline_ts as DATE))) >= date_diff(date_sub(current_date('America/Los_Angeles'), interval 120 DAY))
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY
      unified_rule_metadata_history.rule_id
    ORDER BY
      UPPER(
        unified_rule_metadata_history.baseline
      ) DESC
  ) = 1;


-- - (rule_team like '%crs%' or rule_team like '%crm%')
--  look at only the past 120 days to make the code run faster
--  on commit preserve rows
CALL
  `pypl-edw`.pp_monitor.drop_table (
    '${dataset_name_tmp}.decline_action_date'
  );


CREATE TABLE
  `pypl-edw.${dataset_name_tmp}.decline_action_date`
CLUSTER BY
  rule_id AS
SELECT
  a.rule_id                              ,
  DATE(a.baseline_ts) AS action_start_date
FROM
  `pypl-edw`.pp_dama_ces_prototype_tables.unified_rule_metadata_history AS a
  INNER JOIN `pypl-edw.${dataset_name_tmp}.decline_eval_end_date` AS b ON a.rule_id = b.rule_id
  AND a.baseline_ts > b.baseline_ts
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY
      a.rule_id
    ORDER BY
      UNIX_SECONDS(
        CAST(a.baseline_ts AS TIMESTAMP)
      )
  ) = 1;


--  on commit preserve rows
CALL
  `pypl-edw`.pp_monitor.drop_table (
    '${dataset_name_tmp}.check_action_rule_id'
  );


CREATE TABLE
  `pypl-edw.${dataset_name_tmp}.check_action_rule_id`
CLUSTER BY
  rule_id AS
SELECT
  a.rule_id            ,
  a.priority           ,
  a.rule_name          ,
  a.control_group      ,
  a.crs_team           ,
  a.owner_ntid         ,
  a.rule_mo            ,
  a.tags               ,
  a.last_release_length,
  CASE
    WHEN CURRENT_DATE('America/Los_Angeles') - 90 <= '2020-08-26' THEN CASE
      WHEN b.rule_id IS NOT NULL THEN CASE
        WHEN DATE_DIFF(
          DATE_SUB(
            CURRENT_DATE('America/Los_Angeles'),
            INTERVAL 7 DAY
          )                  ,
          b.action_start_date,
          DAY
        ) + 1 >= 84 THEN CAST(84 - 8 AS BIGNUMERIC) --WHEN current_date('America/Los_Angeles')- 7 - b.action_start_date + 1 >= 84 THEN CAST(84 - 8 as BIGNUMERIC)
        ELSE DATE_DIFF(
          DATE_SUB(
            CURRENT_DATE('America/Los_Angeles'),
            INTERVAL 7 DAY
          )                  ,
          b.action_start_date,
          DAY
        ) + 1 - 8
      END
      WHEN b.rule_id IS NULL THEN CAST(84 - 8 AS BIGNUMERIC)
    END
    WHEN CURRENT_DATE('America/Los_Angeles') - 90 <= '2020-09-10' THEN CASE
      WHEN b.rule_id IS NOT NULL THEN CASE
        WHEN DATE_DIFF(
          DATE_SUB(
            CURRENT_DATE('America/Los_Angeles'),
            INTERVAL 7 DAY
          )                  ,
          b.action_start_date,
          DAY
        ) + 1 >= 84 THEN CAST(84 - 3 AS BIGNUMERIC)
        ELSE DATE_DIFF(
          DATE_SUB(
            CURRENT_DATE('America/Los_Angeles'),
            INTERVAL 7 DAY
          )                  ,
          b.action_start_date,
          DAY
        ) + 1 - 3
      END
      WHEN b.rule_id IS NULL THEN CAST(84 - 3 AS BIGNUMERIC)
    END
    WHEN CURRENT_DATE('America/Los_Angeles') - 90 <= '2022-09-09' THEN CASE
      WHEN b.rule_id IS NOT NULL THEN CASE
        WHEN DATE_DIFF(
          DATE_SUB(
            CURRENT_DATE('America/Los_Angeles'),
            INTERVAL 7 DAY
          )                  ,
          b.action_start_date,
          DAY
        ) + 1 >= 84 THEN CAST(84 - 5 AS BIGNUMERIC)
        ELSE DATE_DIFF(
          DATE_SUB(
            CURRENT_DATE('America/Los_Angeles'),
            INTERVAL 7 DAY
          )                  ,
          b.action_start_date,
          DAY
        ) + 1 - 5
      END
      WHEN b.rule_id IS NULL THEN a.observing_window_raw - 5
    END
    ELSE CASE
      WHEN b.rule_id IS NOT NULL THEN CASE
        WHEN DATE_DIFF(
          DATE_SUB(
            CURRENT_DATE('America/Los_Angeles'),
            INTERVAL 7 DAY
          )                  ,
          b.action_start_date,
          DAY
        ) + 1 >= 84 THEN CAST(84 AS BIGNUMERIC)
        ELSE DATE_DIFF(
          DATE_SUB(
            CURRENT_DATE('America/Los_Angeles'),
            INTERVAL 7 DAY
          )                  ,
          b.action_start_date,
          DAY
        ) + 1
      END
      WHEN b.rule_id IS NULL THEN a.observing_window_raw
    END
  END AS observing_window
FROM
  `pypl-edw.${dataset_name_tmp}.check_decline_rule_base` AS a
  LEFT OUTER JOIN `pypl-edw.${dataset_name_tmp}.decline_action_date` AS b ON a.rule_id = b.rule_id;


--  where observing_window>=23 -- remove this filter action to have all decline rules appear in the output table
--  on commit preserve rows
CALL
  `pypl-edw`.pp_monitor.drop_table (
    '${dataset_name_tmp}.decline_metrics'
  );


CREATE TABLE
  `pypl-edw.${dataset_name_tmp}.decline_metrics`
CLUSTER BY
  rule_id      ,
  action_type AS
SELECT
  `pypl-edw`.cw_udf.cw_round_half_even (aview.rule_id, 0) AS rule_id    ,
  aview.action_type                                       AS action_type,
  CONCAT(
    CAST(
      CURRENT_DATE('America/Los_Angeles') - 90 AS STRING
    )     ,
    ' to ',
    CAST(
      CURRENT_DATE('America/Los_Angeles') - 7 AS STRING
    )
  ) AS monitoring_window,
  SUM(
    COALESCE(aview.attempt_cnt, 0)
  ) AS fire_vol          ,
  -- --not txn_cnt? not 1?
  SUM(
    CASE
      WHEN RTRIM(aview.transaction_type) = 'U' THEN COALESCE(aview.attempt_cnt, 0)
      ELSE 0
    END
  ) AS u2u_fire_vol,
  SUM(
    COALESCE(
      aview.attempt_amt,
      CAST(0 AS NUMERIC)
    )
  ) AS fire_amt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.txn_cnt
      ELSE 0
    END
  ) AS cg_txn_cnt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.txn_wcnt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS cg_txn_wcnt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.txn_amt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS cg_txn_amt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.txn_wamt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS cg_txn_wamt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.brm_bad_wcnt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS cg_brm_bad_wcnt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.brm_bad_wcnt_adj
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS cg_brm_bad_wcnt_adj,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.brm_bad_wamt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS cg_brm_bad_wamt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.brm_bad_wamt_adj
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS cg_brm_bad_wamt_adj,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND aview.is_str_overlap = 0
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.txn_cnt
      ELSE 0
    END
  ) AS str_cg_txn_cnt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND aview.is_str_overlap = 0
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.txn_wcnt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS str_cg_txn_wcnt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND aview.is_str_overlap = 0
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.txn_amt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS str_cg_txn_amt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND aview.is_str_overlap = 0
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.txn_wamt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS str_cg_txn_wamt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND aview.is_str_overlap = 0
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.brm_bad_wcnt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS str_cg_brm_bad_wcnt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND aview.is_str_overlap = 0
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.brm_bad_wcnt_adj
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS str_cg_brm_bad_wcnt_adj,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND aview.is_str_overlap = 0
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.brm_bad_wamt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS str_cg_brm_bad_wamt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND aview.is_str_overlap = 0
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.brm_bad_wamt_adj
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS str_cg_brm_bad_wamt_adj,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND aview.is_actn_overlap = 0
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.txn_cnt
      ELSE 0
    END
  ) AS actn_cg_txn_cnt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND aview.is_actn_overlap = 0
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.txn_wcnt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS actn_cg_txn_wcnt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND aview.is_actn_overlap = 0
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.txn_amt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS actn_cg_txn_amt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND aview.is_actn_overlap = 0
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.txn_wamt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS actn_cg_txn_wamt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND aview.is_actn_overlap = 0
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.brm_bad_wcnt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS actn_cg_brm_bad_wcnt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND aview.is_actn_overlap = 0
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.brm_bad_wcnt_adj
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS actn_cg_brm_bad_wcnt_adj,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND aview.is_actn_overlap = 0
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.brm_bad_wamt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS actn_cg_brm_bad_wamt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND aview.is_actn_overlap = 0
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.brm_bad_wamt_adj
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS actn_cg_brm_bad_wamt_adj,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_txn_cnt
      ELSE aview.match_txn_cnt
    END
  ) AS match_txn_cnt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_txn_cnt
      ELSE aview.match_txn_wcnt
    END
  ) AS match_txn_wcnt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_txn_amt
      ELSE aview.match_txn_amt
    END
  ) AS match_txn_amt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_txn_amt
      ELSE aview.match_txn_wamt
    END
  ) AS match_txn_wamt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_bad_cnt_new
      ELSE aview.match_bad_wcnt
    END
  ) AS match_bad_wcnt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_bad_cnt_adj_new
      ELSE aview.match_bad_wcnt_adj
    END
  ) AS match_bad_wcnt_adj,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_bad_amt_new
      ELSE aview.match_bad_wamt
    END
  ) AS match_bad_wamt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_bad_amt_adj_new
      ELSE aview.match_bad_wamt_adj
    END
  ) AS match_bad_wamt_adj,
  SUM(
    CASE
      WHEN aview.is_str_overlap = 0 THEN CASE
        WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_txn_cnt
        ELSE aview.match_txn_cnt
      END
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS str_match_txn_cnt,
  SUM(
    CASE
      WHEN aview.is_str_overlap = 0 THEN CASE
        WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_txn_cnt
        ELSE aview.match_txn_wcnt
      END
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS str_match_txn_wcnt,
  SUM(
    CASE
      WHEN aview.is_str_overlap = 0 THEN CASE
        WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_txn_amt
        ELSE aview.match_txn_amt
      END
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS str_match_txn_amt,
  SUM(
    CASE
      WHEN aview.is_str_overlap = 0 THEN CASE
        WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_txn_amt
        ELSE aview.match_txn_wamt
      END
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS str_match_txn_wamt,
  SUM(
    CASE
      WHEN aview.is_str_overlap = 0 THEN CASE
        WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_bad_cnt_new
        ELSE aview.match_bad_wcnt
      END
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS str_match_bad_wcnt,
  SUM(
    CASE
      WHEN aview.is_str_overlap = 0 THEN CASE
        WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_bad_cnt_adj_new
        ELSE aview.match_bad_wcnt_adj
      END
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS str_match_bad_wcnt_adj,
  SUM(
    CASE
      WHEN aview.is_str_overlap = 0 THEN CASE
        WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_bad_amt_new
        ELSE aview.match_bad_wamt
      END
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS str_match_bad_wamt,
  SUM(
    CASE
      WHEN aview.is_str_overlap = 0 THEN CASE
        WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_bad_amt_adj_new
        ELSE aview.match_bad_wamt_adj
      END
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS str_match_bad_wamt_adj,
  SUM(
    CASE
      WHEN aview.is_actn_overlap = 0 THEN CASE
        WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_txn_cnt
        ELSE aview.match_txn_cnt
      END
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS actn_match_txn_cnt,
  SUM(
    CASE
      WHEN aview.is_actn_overlap = 0 THEN CASE
        WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_txn_cnt
        ELSE aview.match_txn_wcnt
      END
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS actn_match_txn_wcnt,
  SUM(
    CASE
      WHEN aview.is_actn_overlap = 0 THEN CASE
        WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_txn_amt
        ELSE aview.match_txn_amt
      END
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS actn_match_txn_amt,
  SUM(
    CASE
      WHEN aview.is_actn_overlap = 0 THEN CASE
        WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_txn_amt
        ELSE aview.match_txn_wamt
      END
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS actn_match_txn_wamt,
  SUM(
    CASE
      WHEN aview.is_actn_overlap = 0 THEN CASE
        WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_bad_cnt_new
        ELSE aview.match_bad_wcnt
      END
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS actn_match_bad_wcnt,
  SUM(
    CASE
      WHEN aview.is_actn_overlap = 0 THEN CASE
        WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_bad_cnt_adj_new
        ELSE aview.match_bad_wcnt_adj
      END
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS actn_match_bad_wcnt_adj,
  SUM(
    CASE
      WHEN aview.is_actn_overlap = 0 THEN CASE
        WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_bad_amt_new
        ELSE aview.match_bad_wamt
      END
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS actn_match_bad_wamt,
  SUM(
    CASE
      WHEN aview.is_actn_overlap = 0 THEN CASE
        WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_bad_amt_adj_new
        ELSE aview.match_bad_wamt_adj
      END
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS actn_match_bad_wamt_adj,
  SUM(
    COALESCE(aview.is_wh_actn, 0) * aview.attempt_cnt
  ) AS sum_wh_actn                        ,
  -- ----------------? not sum(is_wh_actn)?
  SUM(
    CASE
      WHEN aview.is_wh_actn > 0
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.txn_cnt
      ELSE 0
    END
  ) AS whed_cnt,
  SUM(
    CASE
      WHEN aview.is_wh_actn = 1
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.txn_amt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS whed_amt,
  SUM(
    CASE
      WHEN aview.is_wh_actn = 1
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.brm_bad_amt_adj
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS bad_amt_adj,
  SUM(
    CASE
      WHEN aview.is_str_overlap = 0 THEN aview.attempt_cnt
      ELSE 0
    END
  ) AS incre_str_fire_vol,
  SUM(
    CASE
      WHEN aview.is_rule_overlap = 0 THEN aview.attempt_cnt
      ELSE 0
    END
  ) AS incre_rule_fire_vol,
  SUM(
    CASE
      WHEN aview.is_actn_overlap = 0 THEN aview.attempt_cnt
      ELSE 0
    END
  ) AS incre_actn_fire_vol                ,
  SUM(aview.catch_bad) AS catch_bad       ,
  --  add metrics for automated retire tier
  SUM(
    CASE
      WHEN aview.is_str_overlap = 0 THEN aview.attempt_amt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS incre_str_fire_amt,
  SUM(
    CASE
      WHEN aview.is_str_overlap = 0
      AND aview.is_wh_actn = 0 THEN aview.attempt_amt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS incre_str_non_whed_fire_amt                ,
  --  add metrics for Omni declined bad calculation
  SUM(aview.decled_cnt) AS decled_cnt        ,
  SUM(aview.decled_amt) AS decled_amt        ,
  SUM(aview.decled_bad_cnt) AS decled_bad_cnt,
  SUM(aview.decled_bad_amt) AS decled_bad_amt,
  SUM(
    CASE
      WHEN aview.is_str_overlap = 0 THEN aview.decled_cnt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS incre_str_decled_cnt,
  SUM(
    CASE
      WHEN aview.is_str_overlap = 0 THEN aview.decled_amt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS incre_str_decled_amt,
  SUM(
    CASE
      WHEN aview.is_str_overlap = 0 THEN aview.decled_bad_cnt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS incre_str_decled_bad_cnt,
  SUM(
    CASE
      WHEN aview.is_str_overlap = 0 THEN aview.decled_bad_amt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS incre_str_decled_bad_amt,
  SUM(
    CASE
      WHEN aview.is_actn_overlap = 0 THEN aview.decled_cnt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS incre_actn_decled_cnt,
  SUM(
    CASE
      WHEN aview.is_actn_overlap = 0 THEN aview.decled_amt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS incre_actn_decled_amt,
  SUM(
    CASE
      WHEN aview.is_actn_overlap = 0 THEN aview.decled_bad_cnt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS incre_actn_decled_bad_cnt,
  SUM(
    CASE
      WHEN aview.is_actn_overlap = 0 THEN aview.decled_bad_amt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS incre_actn_decled_bad_amt
FROM
  (
    SELECT
      funding_actn_summary.pymt_start_date  ,
      funding_actn_summary.transaction_type ,
      funding_actn_summary.bad_type         ,
      funding_actn_summary.bad_type_name    ,
      funding_actn_summary.bad_days         ,
      funding_actn_summary.is_col_bad       ,
      funding_actn_summary.rule_mo          ,
      funding_actn_summary.rule_id          ,
      funding_actn_summary.action_type      ,
      funding_actn_summary.rule_name        ,
      funding_actn_summary.is_wh_actn       ,
      funding_actn_summary.is_str_overlap   ,
      funding_actn_summary.is_rule_overlap  ,
      funding_actn_summary.is_actn_overlap  ,
      funding_actn_summary.cg_cat           ,
      funding_actn_summary.rule_type        ,
      funding_actn_summary.attempt_cnt      ,
      funding_actn_summary.attempt_amt      ,
      funding_actn_summary.txn_cnt          ,
      funding_actn_summary.txn_amt          ,
      `pypl-edw`.cw_udf.cw_round_half_even  (
        funding_actn_summary.attempt_wcnt,
        2
      ) AS attempt_wcnt                    ,
      --  DECIMAL(18,6)                    ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.attempt_wamt,
        2
      ) AS attempt_wamt                    ,
      --  DECIMAL(18,8)                    ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.txn_wcnt,
        2
      ) AS txn_wcnt                        ,
      --  DECIMAL(18,6)                    ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.txn_wamt,
        2
      ) AS txn_wamt    ,
      --  DECIMAL(18,8),
      CAST(
        funding_actn_summary.brm_bad_cnt AS NUMERIC
      ) AS brm_bad_cnt                               ,
      --  INTEGER                                    ,
      funding_actn_summary.brm_bad_amt AS brm_bad_amt,
      --  DECIMAL(18,2)                              ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.brm_bad_wcnt,
        2
      ) AS brm_bad_wcnt                    ,
      --  DECIMAL(18,6)                    ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.brm_bad_wamt,
        2
      ) AS brm_bad_wamt,
      --  DECIMAL(18,8),
      CAST(
        funding_actn_summary.col_bad_cnt AS NUMERIC
      ) AS col_bad_cnt                               ,
      --  INTEGER                                    ,
      funding_actn_summary.col_bad_amt AS col_bad_amt,
      --  DECIMAL(18,2)                              ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.col_bad_wcnt,
        2
      ) AS col_bad_wcnt                    ,
      --  DECIMAL(18,6)                    ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.col_bad_wamt,
        2
      ) AS col_bad_wamt                    ,
      --  DECIMAL(18,8)                    ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        CAST(
          funding_actn_summary.pp_loss AS BIGNUMERIC
        ),
        2
      ) AS pp_loss                         ,
      --  FLOAT                            ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.usd_margin,
        2
      ) AS usd_margin                      ,
      --  DECIMAL(18,4)                    ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.usd_revenue,
        2
      ) AS usd_revenue                     ,
      --  DECIMAL(18,4)                    ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        CAST(
          funding_actn_summary.rmc_loss AS BIGNUMERIC
        ),
        2
      ) AS rmc_loss                        ,
      --  FLOAT                            ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        CAST(
          funding_actn_summary.rms_loss AS BIGNUMERIC
        ),
        2
      ) AS rms_loss                        ,
      --  FLOAT                            ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        CAST(
          funding_actn_summary.pp_wloss AS BIGNUMERIC
        ),
        2
      ) AS pp_wloss                        ,
      --  FLOAT                            ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.usd_wmargin,
        2
      ) AS usd_wmargin                     ,
      --  DECIMAL(18,10)                   ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.usd_wrevenue,
        2
      ) AS usd_wrevenue                    ,
      --  DECIMAL(18,10)                   ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        CAST(
          funding_actn_summary.rmc_wloss AS BIGNUMERIC
        ),
        2
      ) AS rmc_wloss                       ,
      --  FLOAT                            ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        CAST(
          funding_actn_summary.rms_wloss AS BIGNUMERIC
        ),
        2
      ) AS rms_wloss,
      --  FLOAT     ,
      CAST(
        funding_actn_summary.match_txn_cnt AS NUMERIC
      ) AS match_txn_cnt                                 ,
      --  INTEGER                                        ,
      funding_actn_summary.match_txn_amt AS match_txn_amt,
      --  DECIMAL(18,2)                                  ,
      CAST(
        funding_actn_summary.match_bad_cnt AS NUMERIC
      ) AS match_bad_cnt,
      --  INTEGER       ,
      CAST(
        funding_actn_summary.match_bad_cnt_new AS NUMERIC
      ) AS match_bad_cnt_new                                     ,
      --  INTEGER                                                ,
      funding_actn_summary.match_bad_amt AS match_bad_amt        ,
      --  DECIMAL(18,2)                                          ,
      funding_actn_summary.match_bad_amt_new AS match_bad_amt_new,
      --  DECIMAL(18,2)                                          ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.match_txn_wcnt,
        2
      ) AS match_txn_wcnt                  ,
      --  DECIMAL(18,6)                    ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.match_txn_wamt,
        2
      ) AS match_txn_wamt                  ,
      --  DECIMAL(18,8)                    ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.match_bad_wcnt,
        2
      ) AS match_bad_wcnt                  ,
      --  DECIMAL(18,6)                    ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.match_bad_wamt,
        2
      ) AS match_bad_wamt                  ,
      --  DECIMAL(18,8)                    ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        CAST(
          funding_actn_summary.tlt_bad AS BIGNUMERIC
        ),
        2
      ) AS tlt_bad                                           ,
      --  FLOAT                                              ,
      funding_actn_summary.catch_bad AS catch_bad            ,
      --  DECIMAL(18,2)                                      ,
      funding_actn_summary.brm_bad_cnt_adj AS brm_bad_cnt_adj,
      --  DECIMAL(18,2)                                      ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.brm_bad_amt_adj,
        2
      ) AS brm_bad_amt_adj                 ,
      --  DECIMAL(18,4)                    ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.brm_bad_wcnt_adj,
        2
      ) AS brm_bad_wcnt_adj                ,
      --  DECIMAL(18,8)                    ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.brm_bad_wamt_adj,
        2
      ) AS brm_bad_wamt_adj                                              ,
      --  DECIMAL(18,10)                                                 ,
      funding_actn_summary.match_bad_cnt_adj AS match_bad_cnt_adj        ,
      --  DECIMAL(18,2)                                                  ,
      funding_actn_summary.match_bad_cnt_adj_new AS match_bad_cnt_adj_new,
      --  DECIMAL(18,2)                                                  ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.match_bad_amt_adj,
        2
      ) AS match_bad_amt_adj               ,
      --  DECIMAL(18,4)                    ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.match_bad_amt_adj_new,
        2
      ) AS match_bad_amt_adj_new           ,
      --  DECIMAL(18,4)                    ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.match_bad_wcnt_adj,
        2
      ) AS match_bad_wcnt_adj              ,
      --  DECIMAL(18,8)                    ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.match_bad_wamt_adj,
        2
      ) AS match_bad_wamt_adj              ,
      --  DECIMAL(18,10)                   ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.catch_bad_adj,
        2
      ) AS catch_bad_adj,
      --  DECIMAL(18,4) ,
      CAST(
        funding_actn_summary.decled_cnt AS NUMERIC
      ) AS decled_cnt                              ,
      --  INTEGER                                  ,
      funding_actn_summary.decled_amt AS decled_amt,
      --  DECIMAL(18,2)                            ,
      CAST(
        funding_actn_summary.decled_bad_cnt AS NUMERIC
      ) AS decled_bad_cnt                                  ,
      --  INTEGER                                          ,
      funding_actn_summary.decled_bad_amt AS decled_bad_amt,
      --  DECIMAL(18,2)                                    ,
      CAST(
        funding_actn_summary.fallback_honor_attempt_cnt AS NUMERIC
      ) AS fallback_honor_attempt_cnt                                             ,
      --  INTEGER                                                                 ,
      funding_actn_summary.fallback_honor_attempt_amt AS fallback_honor_attempt_amt
    FROM
      `pypl-edw`.pp_fastr_dama_views.funding_actn_summary
  ) AS aview
WHERE
  aview.rule_id IN (
    SELECT
      --  DECIMAL(18,2)
      rule_id AS rule_id
    FROM
      `pypl-edw.${dataset_name_tmp}.check_action_rule_id`
  )
  AND aview.action_type IN (
    16 ,
    17 ,
    18 ,
    19 ,
    20 ,
    22 ,
    23 ,
    29 ,
    97 ,
    98 ,
    146,
    203,
    1354
  )
  AND aview.pymt_start_date BETWEEN CURRENT_DATE('America/Los_Angeles') - 90
  AND CURRENT_DATE('America/Los_Angeles') - 7
  AND aview.pymt_start_date NOT IN (
    DATE '2020-08-21',
    DATE '2020-08-22',
    DATE '2020-08-23',
    DATE '2020-08-24',
    DATE '2020-08-26',
    DATE '2020-09-08',
    DATE '2020-09-09',
    DATE '2020-09-10'
  )
  AND aview.pymt_start_date NOT IN (
    DATE '2022-09-01',
    DATE '2022-09-02',
    DATE '2022-09-07',
    DATE '2022-09-08',
    DATE '2022-09-09'
  )
GROUP BY
  1,
  2,
  3;


--  on commit preserve rows
-- ----------append delta------------------
CALL
  `pypl-edw`.pp_monitor.drop_table (
    '${dataset_name_tmp}.funding_actn_base'
  );


CREATE TABLE
  `pypl-edw.${dataset_name_tmp}.funding_actn_base`
CLUSTER BY
  trans_id ,
  rule_id AS
SELECT
  `pypl-edw`.cw_udf.cw_round_half_even_bignumeric (
    funding_actn_master.pymt_flow_id,
    0
  ) AS pymt_flow_id                               ,
  `pypl-edw`.cw_udf.cw_round_half_even_bignumeric (
    funding_actn_master.trans_id,
    0
  ) AS trans_id                        ,
  funding_actn_master.pymt_start_date  ,
  `pypl-edw`.cw_udf.cw_round_half_even (
    funding_actn_master.usd_amt,
    2
  ) AS usd_amt                         ,
  funding_actn_master.transaction_type ,
  `pypl-edw`.cw_udf.cw_round_half_even (
    funding_actn_master.rule_id,
    0
  ) AS rule_id                  ,
  funding_actn_master.action_type
FROM
  `pypl-edw`.pp_fastr_dama_views.funding_actn_master AS funding_actn_master
WHERE
  funding_actn_master.action_type IN (
    16 ,
    17 ,
    18 ,
    19 ,
    20 ,
    22 ,
    23 ,
    29 ,
    97 ,
    98 ,
    146,
    203,
    1354
  )
  AND UPPER(
    funding_actn_master.checkpoint_name
  ) LIKE '%CONSOLIDATEDFUNDING%'
  AND funding_actn_master.pymt_start_date BETWEEN CURRENT_DATE('America/Los_Angeles') - 90
  AND CURRENT_DATE('America/Los_Angeles') - 7
  AND funding_actn_master.pymt_start_date NOT IN (
    DATE '2020-08-21',
    DATE '2020-08-22',
    DATE '2020-08-23',
    DATE '2020-08-24',
    DATE '2020-08-26',
    DATE '2020-09-08',
    DATE '2020-09-09',
    DATE '2020-09-10'
  )
  AND funding_actn_master.pymt_start_date NOT IN (
    DATE '2022-09-01',
    DATE '2022-09-02',
    DATE '2022-09-07',
    DATE '2022-09-08',
    DATE '2022-09-09'
  )
  AND funding_actn_master.rule_id IN (
    SELECT
      rule_id AS rule_id
    FROM
      `pypl-edw.${dataset_name_tmp}.check_action_rule_id`
  );


--  on commit preserve rows
-- COLLECT STATISTICS is not supported in this dialect.
CALL
  `pypl-edw`.pp_monitor.drop_table (
    '${dataset_name_tmp}.delta_temp'
  );


CREATE TABLE
  `pypl-edw.${dataset_name_tmp}.delta_temp`
CLUSTER BY
  rule_id AS
SELECT
  `pypl-edw`.cw_udf.cw_round_half_even_bignumeric (a.pymt_flow_id, 0) AS pymt_flow_id,
  `pypl-edw`.cw_udf.cw_round_half_even_bignumeric (a.trans_id, 0)     AS trans_id    ,
  a.pymt_start_date                                                                  ,
  `pypl-edw`.cw_udf.cw_round_half_even (a.usd_amt, 2)                 AS usd_amt     ,
  a.transaction_type                                                                 ,
  `pypl-edw`.cw_udf.cw_round_half_even (a.rule_id, 0)                 AS rule_id     ,
  a.action_type                                                                      ,
  delta.delta_bad_tag
FROM
  `pypl-edw.${dataset_name_tmp}.funding_actn_base` AS a
  INNER JOIN (
    SELECT
      delta_results.trans_id           ,
      delta_results.pmt_start_date     ,
      delta_results.delta_model_version,
      delta_results.delta_bad_tag      ,
      delta_results.delta_bad_prob
    FROM
      `pypl-edw`.pp_risk_roe_views.delta_results
    WHERE
      UPPER(
        RTRIM(
          delta_results.delta_model_version
        )
      ) = 'DELTA_V1'
  ) AS delta ON a.pymt_start_date = delta.pmt_start_date
  AND a.trans_id = delta.trans_id;


--  on commit preserve rows
CALL
  `pypl-edw`.pp_monitor.drop_table ('${dataset_name_tmp}.delta');


CREATE TABLE
  `pypl-edw.${dataset_name_tmp}.delta`
CLUSTER BY
  rule_id      ,
  action_type AS
SELECT
  `pypl-edw`.cw_udf.cw_round_half_even (a.rule_id, 0) AS rule_id,
  a.action_type                                                 ,
  SUM(
    CASE
      WHEN a.delta_bad_tag IS NOT NULL
      AND CASE
        a.delta_bad_tag
        WHEN '' THEN 0.0
        ELSE CAST(a.delta_bad_tag AS FLOAT64)
      END = 1
      AND a.trans_id < 0 THEN 1
      ELSE 0
    END
  ) AS delta_bad_cnt,
  SUM(
    CASE
      WHEN a.trans_id < 0
      AND a.delta_bad_tag IS NOT NULL THEN 1
      ELSE 0
    END
  ) AS delta_txn_cnt,
  SUM(
    CASE
      WHEN a.delta_bad_tag IS NOT NULL
      AND CASE
        a.delta_bad_tag
        WHEN '' THEN 0.0
        ELSE CAST(a.delta_bad_tag AS FLOAT64)
      END = 1
      AND a.trans_id < 0 THEN a.usd_amt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS delta_bad_amt,
  SUM(
    CASE
      WHEN a.trans_id < 0
      AND a.delta_bad_tag IS NOT NULL THEN a.usd_amt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS delta_txn_amt,
  CASE
    WHEN SUM(
      CASE
        WHEN a.trans_id < 0
        AND a.delta_bad_tag IS NOT NULL THEN 1
        ELSE 0
      END
    ) > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f',
          SUM(
            CASE
              WHEN a.delta_bad_tag IS NOT NULL
              AND CASE
                a.delta_bad_tag
                WHEN '' THEN 0.0
                ELSE CAST(a.delta_bad_tag AS FLOAT64)
              END = 1
              AND a.trans_id < 0 THEN 1
              ELSE 0
            END
          ) * NUMERIC '1.000' / SUM(
            CASE
              WHEN a.trans_id < 0
              AND a.delta_bad_tag IS NOT NULL THEN 1
              ELSE 0
            END
          )
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS delta_cnt_bad_rate,
  CASE
    WHEN SUM(
      CASE
        WHEN a.trans_id < 0
        AND a.delta_bad_tag IS NOT NULL THEN a.usd_amt
        ELSE CAST(0 AS NUMERIC)
      END
    ) > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f',
          SUM(
            CASE
              WHEN a.delta_bad_tag IS NOT NULL
              AND CASE
                a.delta_bad_tag
                WHEN '' THEN 0.0
                ELSE CAST(a.delta_bad_tag AS FLOAT64)
              END = 1
              AND a.trans_id < 0 THEN a.usd_amt
              ELSE CAST(0 AS NUMERIC)
            END
          ) * NUMERIC '1.000' / SUM(
            CASE
              WHEN a.trans_id < 0
              AND a.delta_bad_tag IS NOT NULL THEN a.usd_amt
              ELSE CAST(0 AS NUMERIC)
            END
          )
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS delta_amt_bad_rate
FROM
  `pypl-edw.${dataset_name_tmp}.delta_temp` AS a
GROUP BY
  1,
  2;


--  on commit preserve rows
--  CALL pp_monitor.drop_table(''${decline_gms}'');
--  CREATE multiset TABLE ${decline_gms} AS (
--  SEL
--  bad.rule_id                                                                               ,
--  bad.action_type                                                                           ,
--  SUM(1)              AS cnt_total                                                          ,
--  SUM(bad.usd_amt)    AS amt_total                                                          ,
--  SUM(bad.bad_weight) AS bad_wcnt                                                           ,
--  SUM(1.0000 * bad_weight * bad.usd_amt) AS bad_wamt                                        ,
--  CASE WHEN cnt_total>0 THEN bad_wcnt*1.000 /cnt_total ELSE 'NA' END AS decline_cnt_bad_rate,
--  CASE WHEN amt_total>0 THEN bad_wamt*1.000 /amt_total ELSE 'NA' END AS decline_amt_bad_rate
--  FROM pp_oap_qiqi_t.mit_fct_dclbad bad
--  where pymt_start_date BETWEEN current_date-90 and current_date-7
--  GROUP BY 1,2
--  ) with data primary index(rule_id,action_type)
--  -- on commit preserve rows
--  ;
-- -------------------append overlap-----------------------
CALL
  `pypl-edw`.pp_monitor.drop_table (
    '${dataset_name_tmp}.overlaprule_temp'
  );


CREATE TABLE
  `pypl-edw.${dataset_name_tmp}.overlaprule_temp`
CLUSTER BY
  rule_id AS
SELECT
  `pypl-edw`.cw_udf.cw_round_half_even (c.rule_id, 0)     AS rule_id    ,
  c.action_type                                                         ,
  `pypl-edw`.cw_udf.cw_round_half_even (c.rule_id_n, 0)   AS rule_id_n  ,
  c.overlap_cnt                                                         ,
  c.tlt_cnt                                                             ,
  `pypl-edw`.cw_udf.cw_round_half_even (c.overlap_pct, 3) AS overlap_pct,
  c.rank_num
FROM
  (
    SELECT
      a.rule_id                                                 ,
      a.action_type                                             ,
      a.rule_id_n                                               ,
      a.overlap_cnt                                             ,
      b.tlt_cnt                                                 ,
      a.overlap_cnt * NUMERIC '1.000' / b.tlt_cnt AS overlap_pct,
      ROW_NUMBER() OVER                           (
        PARTITION BY
          a.rule_id   ,
          a.action_type
        ORDER BY
          a.overlap_cnt * NUMERIC '1.000' / b.tlt_cnt DESC
      ) AS rank_num
    FROM
      (
        SELECT
          funding_actn_rule_overlap_master.rule_id    ,
          funding_actn_rule_overlap_master.action_type,
          funding_actn_rule_overlap_master.rule_id_n  ,
          SUM(
            funding_actn_rule_overlap_master.overlap_cnt
          ) AS overlap_cnt
        FROM
          `pypl-edw`.pp_fastr_dama_views.funding_actn_rule_overlap_master AS funding_actn_rule_overlap_master
        WHERE
          funding_actn_rule_overlap_master.pymt_start_date BETWEEN CURRENT_DATE('America/Los_Angeles') - 90
          AND CURRENT_DATE('America/Los_Angeles') - 7
          AND funding_actn_rule_overlap_master.pymt_start_date NOT IN (
            DATE '2020-08-21',
            DATE '2020-08-22',
            DATE '2020-08-23',
            DATE '2020-08-24',
            DATE '2020-08-26',
            DATE '2020-09-08',
            DATE '2020-09-09',
            DATE '2020-09-10'
          )
          AND funding_actn_rule_overlap_master.pymt_start_date NOT IN (
            DATE '2022-09-01',
            DATE '2022-09-02',
            DATE '2022-09-07',
            DATE '2022-09-08',
            DATE '2022-09-09'
          )
          AND funding_actn_rule_overlap_master.rule_id IN (
            SELECT
              rule_id AS rule_id
            FROM
              `pypl-edw.${dataset_name_tmp}.check_action_rule_id`
            GROUP BY
              1
          )
        GROUP BY
          1,
          2,
          3
      ) AS a
      INNER JOIN (
        SELECT
          tmp.rule_id               ,
          tmp.action_type           ,
          SUM(tmp.tlt_cnt) AS tlt_cnt
        FROM
          (
            SELECT
              funding_actn_rule_overlap_master_0.pymt_start_date,
              funding_actn_rule_overlap_master_0.rule_id        ,
              funding_actn_rule_overlap_master_0.rule_id_n      ,
              funding_actn_rule_overlap_master_0.action_type    ,
              funding_actn_rule_overlap_master_0.overlap_cnt    ,
              funding_actn_rule_overlap_master_0.tlt_cnt        ,
              funding_actn_rule_overlap_master_0.overlap_pct    ,
              funding_actn_rule_overlap_master_0.rank_num
            FROM
              `pypl-edw`.pp_fastr_dama_views.funding_actn_rule_overlap_master AS funding_actn_rule_overlap_master_0
            QUALIFY
              ROW_NUMBER() OVER (
                PARTITION BY
                  funding_actn_rule_overlap_master_0.pymt_start_date,
                  funding_actn_rule_overlap_master_0.rule_id        ,
                  funding_actn_rule_overlap_master_0.action_type
                ORDER BY
                  funding_actn_rule_overlap_master_0.rule_id_n DESC
              ) = 1
          ) AS tmp
        WHERE
          tmp.pymt_start_date BETWEEN CURRENT_DATE('America/Los_Angeles') - 90
          AND CURRENT_DATE('America/Los_Angeles') - 7
          AND tmp.pymt_start_date NOT IN (
            DATE '2020-08-21',
            DATE '2020-08-22',
            DATE '2020-08-23',
            DATE '2020-08-24',
            DATE '2020-08-26',
            DATE '2020-09-08',
            DATE '2020-09-09',
            DATE '2020-09-10'
          )
          AND tmp.pymt_start_date NOT IN (
            DATE '2022-09-01',
            DATE '2022-09-02',
            DATE '2022-09-07',
            DATE '2022-09-08',
            DATE '2022-09-09'
          )
        GROUP BY
          1,
          2
      ) AS b ON a.rule_id = b.rule_id
      AND a.action_type = b.action_type
  ) AS c
WHERE
  c.rank_num <= 4;


--  on commit preserve rows
CALL
  `pypl-edw`.pp_monitor.drop_table (
    '${dataset_name_tmp}.overlaprule'
  );


CREATE TABLE
  `pypl-edw.${dataset_name_tmp}.overlaprule`
CLUSTER BY
  rule_id AS
SELECT
  `pypl-edw`.cw_udf.cw_round_half_even (a.rule_id, 0) AS rule_id,
  a.action_type                                                 ,
  CASE
    WHEN ov1.rule_id_n IS NOT NULL THEN FORMAT('%20.0f', ov1.rule_id_n)
    ELSE 'NA'
  END AS ov_rule_1,
  CASE
    WHEN ov1.overlap_pct IS NOT NULL THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT('%6.3f', ov1.overlap_pct),
        r'^( *?)(-)?0(\..*)'            ,
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS ov_pct_1,
  CASE
    WHEN d1.owner_ntid IS NOT NULL THEN d1.owner_ntid
    ELSE 'NA'
  END AS ov_ntid_1,
  CASE
    WHEN ov2.rule_id_n IS NOT NULL THEN FORMAT('%20.0f', ov2.rule_id_n)
    ELSE 'NA'
  END AS ov_rule_2,
  CASE
    WHEN ov2.overlap_pct IS NOT NULL THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT('%6.3f', ov2.overlap_pct),
        r'^( *?)(-)?0(\..*)'            ,
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS ov_pct_2,
  CASE
    WHEN d2.owner_ntid IS NOT NULL THEN d2.owner_ntid
    ELSE 'NA'
  END AS ov_ntid_2,
  CASE
    WHEN ov3.rule_id_n IS NOT NULL THEN FORMAT('%20.0f', ov3.rule_id_n)
    ELSE 'NA'
  END AS ov_rule_3,
  CASE
    WHEN ov3.overlap_pct IS NOT NULL THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT('%6.3f', ov3.overlap_pct),
        r'^( *?)(-)?0(\..*)'            ,
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS ov_pct_3,
  CASE
    WHEN d3.owner_ntid IS NOT NULL THEN d3.owner_ntid
    ELSE 'NA'
  END AS ov_ntid_3,
  CASE
    WHEN ov4.rule_id_n IS NOT NULL THEN FORMAT('%20.0f', ov4.rule_id_n)
    ELSE 'NA'
  END AS ov_rule_4,
  CASE
    WHEN ov4.overlap_pct IS NOT NULL THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT('%6.3f', ov4.overlap_pct),
        r'^( *?)(-)?0(\..*)'            ,
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS ov_pct_4,
  CASE
    WHEN d4.owner_ntid IS NOT NULL THEN d4.owner_ntid
    ELSE 'NA'
  END AS ov_ntid_4
FROM
  (
    SELECT
      overlaprule_temp.rule_id   ,
      overlaprule_temp.action_type
    FROM
      `pypl-edw.${dataset_name_tmp}.overlaprule_temp` AS overlaprule_temp
    WHERE
      overlaprule_temp.rank_num = 1
  ) AS a
  LEFT OUTER JOIN (
    SELECT
      overlaprule_temp_0.rule_id    ,
      overlaprule_temp_0.action_type,
      overlaprule_temp_0.rule_id_n  ,
      overlaprule_temp_0.overlap_cnt,
      overlaprule_temp_0.tlt_cnt    ,
      overlaprule_temp_0.overlap_pct,
      overlaprule_temp_0.rank_num
    FROM
      `pypl-edw.${dataset_name_tmp}.overlaprule_temp` AS overlaprule_temp_0
    WHERE
      overlaprule_temp_0.rank_num = 1
  ) AS ov1 ON a.rule_id = ov1.rule_id
  AND a.action_type = ov1.action_type
  LEFT OUTER JOIN (
    SELECT
      overlaprule_temp_1.rule_id    ,
      overlaprule_temp_1.action_type,
      overlaprule_temp_1.rule_id_n  ,
      overlaprule_temp_1.overlap_cnt,
      overlaprule_temp_1.tlt_cnt    ,
      overlaprule_temp_1.overlap_pct,
      overlaprule_temp_1.rank_num
    FROM
      `pypl-edw.${dataset_name_tmp}.overlaprule_temp` AS overlaprule_temp_1
    WHERE
      overlaprule_temp_1.rank_num = 2
  ) AS ov2 ON a.rule_id = ov2.rule_id
  AND a.action_type = ov2.action_type
  LEFT OUTER JOIN (
    SELECT
      overlaprule_temp_2.rule_id    ,
      overlaprule_temp_2.action_type,
      overlaprule_temp_2.rule_id_n  ,
      overlaprule_temp_2.overlap_cnt,
      overlaprule_temp_2.tlt_cnt    ,
      overlaprule_temp_2.overlap_pct,
      overlaprule_temp_2.rank_num
    FROM
      `pypl-edw.${dataset_name_tmp}.overlaprule_temp` AS overlaprule_temp_2
    WHERE
      overlaprule_temp_2.rank_num = 3
  ) AS ov3 ON a.rule_id = ov3.rule_id
  AND a.action_type = ov3.action_type
  LEFT OUTER JOIN (
    SELECT
      overlaprule_temp_3.rule_id    ,
      overlaprule_temp_3.action_type,
      overlaprule_temp_3.rule_id_n  ,
      overlaprule_temp_3.overlap_cnt,
      overlaprule_temp_3.tlt_cnt    ,
      overlaprule_temp_3.overlap_pct,
      overlaprule_temp_3.rank_num
    FROM
      `pypl-edw.${dataset_name_tmp}.overlaprule_temp` AS overlaprule_temp_3
    WHERE
      overlaprule_temp_3.rank_num = 4
  ) AS ov4 ON a.rule_id = ov4.rule_id
  AND a.action_type = ov4.action_type
  LEFT OUTER JOIN `pypl-edw.${dataset_name_tmp}.crs_all_funding` AS d1 ON ov1.rule_id_n = d1.rule_id
  LEFT OUTER JOIN `pypl-edw.${dataset_name_tmp}.crs_all_funding` AS d2 ON ov2.rule_id_n = d2.rule_id
  LEFT OUTER JOIN `pypl-edw.${dataset_name_tmp}.crs_all_funding` AS d3 ON ov3.rule_id_n = d3.rule_id
  LEFT OUTER JOIN `pypl-edw.${dataset_name_tmp}.crs_all_funding` AS d4 ON ov4.rule_id_n = d4.rule_id;


--  on commit preserve rows
-- -------------------append contact rate (BQ version)-----------------------
DROP TABLE IF EXISTS
  `pypl-edw.${dataset_name_tmp}.contact_metrics_tmp`;


CREATE TABLE
  `pypl-edw.${dataset_name_tmp}.contact_metrics_tmp`
CLUSTER BY
  rule_id AS
SELECT
  a.rule_id                                 ,
  a.action_type                             ,
  SUM(COALESCE(1, 0))         AS decline_cnt,
  SUM(COALESCE(cntct_cnt, 0)) AS cntct_cnt
FROM
  `pypl-edw.pp_risk_crs_core.rule_decline_cntct_master` a
WHERE
  action_type IN (
    16 ,
    17 ,
    18 ,
    19 ,
    20 ,
    22 ,
    23 ,
    29 ,
    97 ,
    98 ,
    146,
    203,
    1354
  )
  AND pmt_start_date BETWEEN CURRENT_DATE('America/Los_Angeles') - 90
  AND CURRENT_DATE('America/Los_Angeles') - 7
  AND pmt_start_date NOT IN (
    '2020-08-21',
    '2020-08-22',
    '2020-08-23',
    '2020-08-24',
    '2020-08-26',
    '2020-09-08',
    '2020-09-09',
    '2020-09-10'
  )
  AND pmt_start_date NOT IN (
    '2022-09-01',
    '2022-09-02',
    '2022-09-07',
    '2022-09-08',
    '2022-09-09'
  )
GROUP BY
  1,
  2;


CALL
  `pypl-edw`.pp_monitor.drop_table (
    '${dataset_name_tmp}.contact_metrics'
  );


CREATE TABLE
  `pypl-edw.${dataset_name_tmp}.contact_metrics`
CLUSTER BY
  rule_id      ,
  action_type AS
SELECT
  `pypl-edw`.cw_udf.cw_round_half_even (sub.rule_id, 0) AS rule_id,
  sub.action_type                                                 ,
  sub.decline_cnt                                                 ,
  sub.cntct_cnt                                                   ,
  sub.cntct_cnt /                                       CASE
    WHEN sub.decline_cnt = 0 THEN 1
    ELSE sub.decline_cnt
  END AS cntct_rate,
  CASE
    WHEN sub.cntct_cnt >= 30
    AND sub.rule_mo IN ('ATO', 'Collusion', 'StolenID')
    AND sub.cntct_cnt / CASE
      WHEN sub.decline_cnt = 0 THEN 1
      ELSE sub.decline_cnt
    END >= NUMERIC '0.02' THEN 1
    WHEN sub.cntct_cnt >= 30
    AND LOWER(sub.crs_team) IN ('gfr_credit', 'goal_credit')
    AND sub.cntct_cnt / CASE
      WHEN sub.decline_cnt = 0 THEN 1
      ELSE sub.decline_cnt
    END >= NUMERIC '0.02' THEN 1
    WHEN sub.cntct_cnt >= 30
    AND (
      sub.rule_mo NOT IN ('ATO', 'Collusion', 'StolenID')
      OR sub.rule_mo IS NULL
    )
    AND sub.cntct_cnt / CASE
      WHEN sub.decline_cnt = 0 THEN 1
      ELSE sub.decline_cnt
    END >= NUMERIC '0.015' THEN 1
    ELSE 0
  END AS high_cntct_rate_
FROM
  (
    SELECT
      a.rule_id    ,
      a.action_type,
      a.decline_cnt,
      a.cntct_cnt  ,
      b.rule_mo    ,
      b.crs_team
    FROM
      `pypl-edw.${dataset_name_tmp}.contact_metrics_tmp` AS a
      LEFT OUTER JOIN `pypl-edw.${dataset_name_tmp}.check_action_rule_id` AS b ON a.rule_id = b.rule_id
    WHERE
      b.rule_id IS NOT NULL
      AND a.action_type IN (
        16 ,
        17 ,
        18 ,
        19 ,
        20 ,
        22 ,
        23 ,
        29 ,
        97 ,
        98 ,
        146,
        203,
        1354
      )
  ) AS sub;


-- ----------------treatment for af pld pair rules----------------------------------
CALL
  `pypl-edw`.pp_monitor.drop_table (
    '${dataset_name_tmp}.auth_flow_pld_pair'
  );


CREATE TABLE
  `pypl-edw.${dataset_name_tmp}.auth_flow_pld_pair`
CLUSTER BY
  rule_id AS
SELECT
  a.rule_id
FROM
  `pypl-edw.${dataset_name_tmp}.check_action_rule_id` AS a
  INNER JOIN (
    SELECT
      *
    FROM
      `pypl-edw.${dataset_name_tmp}.urm_lite` AS live_metadata_table
    WHERE
      UPPER(
        live_metadata_table.action_raw_str
      ) LIKE '%DECLINE_PMT%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) LIKE '%REDIRECT_AUTH_FLOW%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%WHITE_LIST%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%WHITELIST%'
  ) AS b ON a.rule_id = b.rule_id;


--  on commit preserve rows
CALL
  `pypl-edw`.pp_monitor.drop_table (
    '${dataset_name_tmp}.decline_metrics_auth_flow_pld'
  );


CREATE TABLE
  `pypl-edw.${dataset_name_tmp}.decline_metrics_auth_flow_pld`
CLUSTER BY
  rule_id AS
SELECT
  `pypl-edw`.cw_udf.cw_round_half_even (aview.rule_id, 0) AS rule_id,
  CONCAT(
    CAST(
      CURRENT_DATE('America/Los_Angeles') - 90 AS STRING
    )     ,
    ' to ',
    CAST(
      CURRENT_DATE('America/Los_Angeles') - 7 AS STRING
    )
  ) AS monitoring_window,
  SUM(
    COALESCE(aview.attempt_cnt, 0)
  ) AS fire_vol          ,
  -- --not txn_cnt? not 1?
  SUM(
    CASE
      WHEN RTRIM(aview.transaction_type) = 'U' THEN COALESCE(aview.attempt_cnt, 0)
      ELSE 0
    END
  ) AS u2u_fire_vol,
  SUM(
    COALESCE(
      aview.attempt_amt,
      CAST(0 AS NUMERIC)
    )
  ) AS fire_amt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.txn_cnt
      ELSE 0
    END
  ) AS cg_txn_cnt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.txn_wcnt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS cg_txn_wcnt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.txn_amt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS cg_txn_amt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.txn_wamt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS cg_txn_wamt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.brm_bad_wcnt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS cg_brm_bad_wcnt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.brm_bad_wcnt_adj
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS cg_brm_bad_wcnt_adj,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.brm_bad_wamt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS cg_brm_bad_wamt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.brm_bad_wamt_adj
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS cg_brm_bad_wamt_adj,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND aview.is_str_overlap = 0
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.txn_cnt
      ELSE 0
    END
  ) AS str_cg_txn_cnt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND aview.is_str_overlap = 0
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.txn_wcnt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS str_cg_txn_wcnt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND aview.is_str_overlap = 0
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.txn_amt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS str_cg_txn_amt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND aview.is_str_overlap = 0
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.txn_wamt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS str_cg_txn_wamt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND aview.is_str_overlap = 0
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.brm_bad_wcnt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS str_cg_brm_bad_wcnt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND aview.is_str_overlap = 0
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.brm_bad_wcnt_adj
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS str_cg_brm_bad_wcnt_adj,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND aview.is_str_overlap = 0
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.brm_bad_wamt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS str_cg_brm_bad_wamt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND aview.is_str_overlap = 0
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.brm_bad_wamt_adj
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS str_cg_brm_bad_wamt_adj,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND aview.is_actn_overlap = 0
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.txn_cnt
      ELSE 0
    END
  ) AS actn_cg_txn_cnt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND aview.is_actn_overlap = 0
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.txn_wcnt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS actn_cg_txn_wcnt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND aview.is_actn_overlap = 0
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.txn_amt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS actn_cg_txn_amt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND aview.is_actn_overlap = 0
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.txn_wamt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS actn_cg_txn_wamt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND aview.is_actn_overlap = 0
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.brm_bad_wcnt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS actn_cg_brm_bad_wcnt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND aview.is_actn_overlap = 0
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.brm_bad_wcnt_adj
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS actn_cg_brm_bad_wcnt_adj,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND aview.is_actn_overlap = 0
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.brm_bad_wamt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS actn_cg_brm_bad_wamt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.cg_cat)) = 'CG'
      AND aview.is_actn_overlap = 0
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.brm_bad_wamt_adj
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS actn_cg_brm_bad_wamt_adj,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_txn_cnt
      ELSE aview.match_txn_cnt
    END
  ) AS match_txn_cnt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_txn_cnt
      ELSE aview.match_txn_wcnt
    END
  ) AS match_txn_wcnt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_txn_amt
      ELSE aview.match_txn_amt
    END
  ) AS match_txn_amt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_txn_amt
      ELSE aview.match_txn_wamt
    END
  ) AS match_txn_wamt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_bad_cnt_new
      ELSE aview.match_bad_wcnt
    END
  ) AS match_bad_wcnt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_bad_cnt_adj_new
      ELSE aview.match_bad_wcnt_adj
    END
  ) AS match_bad_wcnt_adj,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_bad_amt_new
      ELSE aview.match_bad_wamt
    END
  ) AS match_bad_wamt,
  SUM(
    CASE
      WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_bad_amt_adj_new
      ELSE aview.match_bad_wamt_adj
    END
  ) AS match_bad_wamt_adj,
  SUM(
    CASE
      WHEN aview.is_str_overlap = 0 THEN CASE
        WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_txn_cnt
        ELSE aview.match_txn_cnt
      END
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS str_match_txn_cnt,
  SUM(
    CASE
      WHEN aview.is_str_overlap = 0 THEN CASE
        WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_txn_cnt
        ELSE aview.match_txn_wcnt
      END
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS str_match_txn_wcnt,
  SUM(
    CASE
      WHEN aview.is_str_overlap = 0 THEN CASE
        WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_txn_amt
        ELSE aview.match_txn_amt
      END
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS str_match_txn_amt,
  SUM(
    CASE
      WHEN aview.is_str_overlap = 0 THEN CASE
        WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_txn_amt
        ELSE aview.match_txn_wamt
      END
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS str_match_txn_wamt,
  SUM(
    CASE
      WHEN aview.is_str_overlap = 0 THEN CASE
        WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_bad_cnt_new
        ELSE aview.match_bad_wcnt
      END
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS str_match_bad_wcnt,
  SUM(
    CASE
      WHEN aview.is_str_overlap = 0 THEN CASE
        WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_bad_cnt_adj_new
        ELSE aview.match_bad_wcnt_adj
      END
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS str_match_bad_wcnt_adj,
  SUM(
    CASE
      WHEN aview.is_str_overlap = 0 THEN CASE
        WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_bad_amt_new
        ELSE aview.match_bad_wamt
      END
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS str_match_bad_wamt,
  SUM(
    CASE
      WHEN aview.is_str_overlap = 0 THEN CASE
        WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_bad_amt_adj_new
        ELSE aview.match_bad_wamt_adj
      END
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS str_match_bad_wamt_adj,
  SUM(
    CASE
      WHEN aview.is_actn_overlap = 0 THEN CASE
        WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_txn_cnt
        ELSE aview.match_txn_cnt
      END
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS actn_match_txn_cnt,
  SUM(
    CASE
      WHEN aview.is_actn_overlap = 0 THEN CASE
        WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_txn_cnt
        ELSE aview.match_txn_wcnt
      END
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS actn_match_txn_wcnt,
  SUM(
    CASE
      WHEN aview.is_actn_overlap = 0 THEN CASE
        WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_txn_amt
        ELSE aview.match_txn_amt
      END
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS actn_match_txn_amt,
  SUM(
    CASE
      WHEN aview.is_actn_overlap = 0 THEN CASE
        WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_txn_amt
        ELSE aview.match_txn_wamt
      END
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS actn_match_txn_wamt,
  SUM(
    CASE
      WHEN aview.is_actn_overlap = 0 THEN CASE
        WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_bad_cnt_new
        ELSE aview.match_bad_wcnt
      END
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS actn_match_bad_wcnt,
  SUM(
    CASE
      WHEN aview.is_actn_overlap = 0 THEN CASE
        WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_bad_cnt_adj_new
        ELSE aview.match_bad_wcnt_adj
      END
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS actn_match_bad_wcnt_adj,
  SUM(
    CASE
      WHEN aview.is_actn_overlap = 0 THEN CASE
        WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_bad_amt_new
        ELSE aview.match_bad_wamt
      END
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS actn_match_bad_wamt,
  SUM(
    CASE
      WHEN aview.is_actn_overlap = 0 THEN CASE
        WHEN UPPER(RTRIM(aview.rule_mo)) = 'UBSM' THEN aview.match_bad_amt_adj_new
        ELSE aview.match_bad_wamt_adj
      END
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS actn_match_bad_wamt_adj,
  SUM(
    COALESCE(aview.is_wh_actn, 0) * aview.attempt_cnt
  ) AS sum_wh_actn                        ,
  -- ----------------? not sum(is_wh_actn)?
  SUM(
    CASE
      WHEN aview.is_wh_actn > 0
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.txn_cnt
      ELSE 0
    END
  ) AS whed_cnt,
  SUM(
    CASE
      WHEN aview.is_wh_actn = 1
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.txn_amt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS whed_amt,
  SUM(
    CASE
      WHEN aview.is_wh_actn = 1
      AND RTRIM(aview.transaction_type) = 'U' THEN aview.brm_bad_amt_adj
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS bad_amt_adj,
  SUM(
    CASE
      WHEN aview.is_str_overlap = 0 THEN aview.attempt_cnt
      ELSE 0
    END
  ) AS incre_str_fire_vol,
  SUM(
    CASE
      WHEN aview.is_rule_overlap = 0 THEN aview.attempt_cnt
      ELSE 0
    END
  ) AS incre_rule_fire_vol,
  SUM(
    CASE
      WHEN aview.is_actn_overlap = 0 THEN aview.attempt_cnt
      ELSE 0
    END
  ) AS incre_actn_fire_vol                ,
  SUM(aview.catch_bad) AS catch_bad       ,
  --  add metrics for automated retire tier
  SUM(
    CASE
      WHEN aview.is_str_overlap = 0 THEN aview.attempt_amt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS incre_str_fire_amt,
  SUM(
    CASE
      WHEN aview.is_str_overlap = 0
      AND aview.is_wh_actn = 0 THEN aview.attempt_amt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS incre_str_non_whed_fire_amt                ,
  --  add metrics for Omni declined bad calculation
  SUM(aview.decled_cnt) AS decled_cnt        ,
  SUM(aview.decled_amt) AS decled_amt        ,
  SUM(aview.decled_bad_cnt) AS decled_bad_cnt,
  SUM(aview.decled_bad_amt) AS decled_bad_amt,
  SUM(
    CASE
      WHEN aview.is_str_overlap = 0 THEN aview.decled_cnt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS incre_str_decled_cnt,
  SUM(
    CASE
      WHEN aview.is_str_overlap = 0 THEN aview.decled_amt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS incre_str_decled_amt,
  SUM(
    CASE
      WHEN aview.is_str_overlap = 0 THEN aview.decled_bad_cnt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS incre_str_decled_bad_cnt,
  SUM(
    CASE
      WHEN aview.is_str_overlap = 0 THEN aview.decled_bad_amt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS incre_str_decled_bad_amt,
  SUM(
    CASE
      WHEN aview.is_actn_overlap = 0 THEN aview.decled_cnt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS incre_actn_decled_cnt,
  SUM(
    CASE
      WHEN aview.is_actn_overlap = 0 THEN aview.decled_amt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS incre_actn_decled_amt,
  SUM(
    CASE
      WHEN aview.is_actn_overlap = 0 THEN aview.decled_bad_cnt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS incre_actn_decled_bad_cnt,
  SUM(
    CASE
      WHEN aview.is_actn_overlap = 0 THEN aview.decled_bad_amt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS incre_actn_decled_bad_amt
FROM
  (
    SELECT
      funding_actn_summary.pymt_start_date  ,
      funding_actn_summary.transaction_type ,
      funding_actn_summary.bad_type         ,
      funding_actn_summary.bad_type_name    ,
      funding_actn_summary.bad_days         ,
      funding_actn_summary.is_col_bad       ,
      funding_actn_summary.rule_mo          ,
      funding_actn_summary.rule_id          ,
      funding_actn_summary.action_type      ,
      funding_actn_summary.rule_name        ,
      funding_actn_summary.is_wh_actn       ,
      funding_actn_summary.is_str_overlap   ,
      funding_actn_summary.is_rule_overlap  ,
      funding_actn_summary.is_actn_overlap  ,
      funding_actn_summary.cg_cat           ,
      funding_actn_summary.rule_type        ,
      funding_actn_summary.attempt_cnt      ,
      funding_actn_summary.attempt_amt      ,
      funding_actn_summary.txn_cnt          ,
      funding_actn_summary.txn_amt          ,
      `pypl-edw`.cw_udf.cw_round_half_even  (
        funding_actn_summary.attempt_wcnt,
        2
      ) AS attempt_wcnt                      ,
      --  DECIMAL(18,6)                      ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.attempt_wamt,
        2
      ) AS attempt_wamt                      ,
      --  DECIMAL(18,8)                      ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.txn_wcnt,
        2
      ) AS txn_wcnt                          ,
      --  DECIMAL(18,6)                      ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.txn_wamt,
        2
      ) AS txn_wamt    ,
      --  DECIMAL(18,8),
      CAST(
        funding_actn_summary.brm_bad_cnt AS NUMERIC
      ) AS brm_bad_cnt                               ,
      --  INTEGER                                    ,
      funding_actn_summary.brm_bad_amt AS brm_bad_amt,
      --  DECIMAL(18,2)                              ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.brm_bad_wcnt,
        2
      ) AS brm_bad_wcnt                      ,
      --  DECIMAL(18,6)                      ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.brm_bad_wamt,
        2
      ) AS brm_bad_wamt,
      --  DECIMAL(18,8),
      CAST(
        funding_actn_summary.col_bad_cnt AS NUMERIC
      ) AS col_bad_cnt                               ,
      --  INTEGER                                    ,
      funding_actn_summary.col_bad_amt AS col_bad_amt,
      --  DECIMAL(18,2)                              ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.col_bad_wcnt,
        2
      ) AS col_bad_wcnt                      ,
      --  DECIMAL(18,6)                      ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.col_bad_wamt,
        2
      ) AS col_bad_wamt                      ,
      --  DECIMAL(18,8)                      ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        CAST(
          funding_actn_summary.pp_loss AS BIGNUMERIC
        ),
        2
      ) AS pp_loss                           ,
      --  FLOAT                              ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.usd_margin,
        2
      ) AS usd_margin                        ,
      --  DECIMAL(18,4)                      ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.usd_revenue,
        2
      ) AS usd_revenue                       ,
      --  DECIMAL(18,4)                      ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        CAST(
          funding_actn_summary.rmc_loss AS BIGNUMERIC
        ),
        2
      ) AS rmc_loss                          ,
      --  FLOAT                              ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        CAST(
          funding_actn_summary.rms_loss AS BIGNUMERIC
        ),
        2
      ) AS rms_loss                          ,
      --  FLOAT                              ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        CAST(
          funding_actn_summary.pp_wloss AS BIGNUMERIC
        ),
        2
      ) AS pp_wloss                          ,
      --  FLOAT                              ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.usd_wmargin,
        2
      ) AS usd_wmargin                       ,
      --  DECIMAL(18,10)                     ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.usd_wrevenue,
        2
      ) AS usd_wrevenue                      ,
      --  DECIMAL(18,10)                     ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        CAST(
          funding_actn_summary.rmc_wloss AS BIGNUMERIC
        ),
        2
      ) AS rmc_wloss                         ,
      --  FLOAT                              ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        CAST(
          funding_actn_summary.rms_wloss AS BIGNUMERIC
        ),
        2
      ) AS rms_wloss,
      --  FLOAT     ,
      CAST(
        funding_actn_summary.match_txn_cnt AS NUMERIC
      ) AS match_txn_cnt                                 ,
      --  INTEGER                                        ,
      funding_actn_summary.match_txn_amt AS match_txn_amt,
      --  DECIMAL(18,2)                                  ,
      CAST(
        funding_actn_summary.match_bad_cnt AS NUMERIC
      ) AS match_bad_cnt,
      --  INTEGER       ,
      CAST(
        funding_actn_summary.match_bad_cnt_new AS NUMERIC
      ) AS match_bad_cnt_new                                     ,
      --  INTEGER                                                ,
      funding_actn_summary.match_bad_amt AS match_bad_amt        ,
      --  DECIMAL(18,2)                                          ,
      funding_actn_summary.match_bad_amt_new AS match_bad_amt_new,
      --  DECIMAL(18,2)                                          ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.match_txn_wcnt,
        2
      ) AS match_txn_wcnt                    ,
      --  DECIMAL(18,6)                      ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.match_txn_wamt,
        2
      ) AS match_txn_wamt                    ,
      --  DECIMAL(18,8)                      ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.match_bad_wcnt,
        2
      ) AS match_bad_wcnt                    ,
      --  DECIMAL(18,6)                      ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.match_bad_wamt,
        2
      ) AS match_bad_wamt                    ,
      --  DECIMAL(18,8)                      ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        CAST(
          funding_actn_summary.tlt_bad AS BIGNUMERIC
        ),
        2
      ) AS tlt_bad                                           ,
      --  FLOAT                                              ,
      funding_actn_summary.catch_bad AS catch_bad            ,
      --  DECIMAL(18,2)                                      ,
      funding_actn_summary.brm_bad_cnt_adj AS brm_bad_cnt_adj,
      --  DECIMAL(18,2)                                      ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.brm_bad_amt_adj,
        2
      ) AS brm_bad_amt_adj                   ,
      --  DECIMAL(18,4)                      ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.brm_bad_wcnt_adj,
        2
      ) AS brm_bad_wcnt_adj                  ,
      --  DECIMAL(18,8)                      ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.brm_bad_wamt_adj,
        2
      ) AS brm_bad_wamt_adj                                              ,
      --  DECIMAL(18,10)                                                 ,
      funding_actn_summary.match_bad_cnt_adj AS match_bad_cnt_adj        ,
      --  DECIMAL(18,2)                                                  ,
      funding_actn_summary.match_bad_cnt_adj_new AS match_bad_cnt_adj_new,
      --  DECIMAL(18,2)                                                  ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.match_bad_amt_adj,
        2
      ) AS match_bad_amt_adj                 ,
      --  DECIMAL(18,4)                      ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.match_bad_amt_adj_new,
        2
      ) AS match_bad_amt_adj_new             ,
      --  DECIMAL(18,4)                      ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.match_bad_wcnt_adj,
        2
      ) AS match_bad_wcnt_adj                ,
      --  DECIMAL(18,8)                      ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.match_bad_wamt_adj,
        2
      ) AS match_bad_wamt_adj                ,
      --  DECIMAL(18,10)                     ,
      `pypl-edw`.cw_udf.cw_round_half_even (
        funding_actn_summary.catch_bad_adj,
        2
      ) AS catch_bad_adj,
      --  DECIMAL(18,4) ,
      CAST(
        funding_actn_summary.decled_cnt AS NUMERIC
      ) AS decled_cnt                              ,
      --  INTEGER                                  ,
      funding_actn_summary.decled_amt AS decled_amt,
      --  DECIMAL(18,2)                            ,
      CAST(
        funding_actn_summary.decled_bad_cnt AS NUMERIC
      ) AS decled_bad_cnt                                  ,
      --  INTEGER                                          ,
      funding_actn_summary.decled_bad_amt AS decled_bad_amt,
      --  DECIMAL(18,2)                                    ,
      CAST(
        funding_actn_summary.fallback_honor_attempt_cnt AS NUMERIC
      ) AS fallback_honor_attempt_cnt                                             ,
      --  INTEGER                                                                 ,
      funding_actn_summary.fallback_honor_attempt_amt AS fallback_honor_attempt_amt
    FROM
      `pypl-edw`.pp_fastr_dama_views.funding_actn_summary
  ) AS aview
WHERE
  aview.rule_id IN (
    SELECT
      --  DECIMAL(18,2)
      auth_flow_pld_pair.rule_id AS rule_id
    FROM
      `pypl-edw.${dataset_name_tmp}.auth_flow_pld_pair` AS auth_flow_pld_pair
  )
  AND aview.action_type IN (
    16 ,
    17 ,
    18 ,
    19 ,
    20 ,
    22 ,
    23 ,
    29 ,
    97 ,
    98 ,
    146,
    203,
    1354
  )
  AND aview.pymt_start_date BETWEEN CURRENT_DATE('America/Los_Angeles') - 90
  AND CURRENT_DATE('America/Los_Angeles') - 7
  AND aview.pymt_start_date NOT IN (
    DATE '2020-08-21',
    DATE '2020-08-22',
    DATE '2020-08-23',
    DATE '2020-08-24',
    DATE '2020-08-26',
    DATE '2020-09-08',
    DATE '2020-09-09',
    DATE '2020-09-10'
  )
  AND aview.pymt_start_date NOT IN (
    DATE '2022-09-01',
    DATE '2022-09-02',
    DATE '2022-09-07',
    DATE '2022-09-08',
    DATE '2022-09-09'
  )
GROUP BY
  1,
  2;


--  on commit preserve rows
CALL
  `pypl-edw`.pp_monitor.drop_table (
    '${dataset_name_tmp}.delta_auth_flow_pld'
  );


CREATE TABLE
  `pypl-edw.${dataset_name_tmp}.delta_auth_flow_pld`
CLUSTER BY
  rule_id AS
SELECT
  `pypl-edw`.cw_udf.cw_round_half_even (delta_temp.rule_id, 0) AS rule_id,
  SUM(
    CASE
      WHEN delta_temp.delta_bad_tag IS NOT NULL
      AND CASE
        delta_temp.delta_bad_tag
        WHEN '' THEN 0.0
        ELSE CAST(
          delta_temp.delta_bad_tag AS FLOAT64
        )
      END = 1
      AND delta_temp.trans_id < 0 THEN 1
      ELSE 0
    END
  ) AS delta_bad_cnt,
  SUM(
    CASE
      WHEN delta_temp.trans_id < 0
      AND delta_temp.delta_bad_tag IS NOT NULL THEN 1
      ELSE 0
    END
  ) AS delta_txn_cnt,
  SUM(
    CASE
      WHEN delta_temp.delta_bad_tag IS NOT NULL
      AND CASE
        delta_temp.delta_bad_tag
        WHEN '' THEN 0.0
        ELSE CAST(
          delta_temp.delta_bad_tag AS FLOAT64
        )
      END = 1
      AND delta_temp.trans_id < 0 THEN delta_temp.usd_amt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS delta_bad_amt,
  SUM(
    CASE
      WHEN delta_temp.trans_id < 0
      AND delta_temp.delta_bad_tag IS NOT NULL THEN delta_temp.usd_amt
      ELSE CAST(0 AS NUMERIC)
    END
  ) AS delta_txn_amt,
  CASE
    WHEN SUM(
      CASE
        WHEN delta_temp.trans_id < 0
        AND delta_temp.delta_bad_tag IS NOT NULL THEN 1
        ELSE 0
      END
    ) > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f',
          SUM(
            CASE
              WHEN delta_temp.delta_bad_tag IS NOT NULL
              AND CASE
                delta_temp.delta_bad_tag
                WHEN '' THEN 0.0
                ELSE CAST(
                  delta_temp.delta_bad_tag AS FLOAT64
                )
              END = 1
              AND delta_temp.trans_id < 0 THEN 1
              ELSE 0
            END
          ) * NUMERIC '1.000' / SUM(
            CASE
              WHEN delta_temp.trans_id < 0
              AND delta_temp.delta_bad_tag IS NOT NULL THEN 1
              ELSE 0
            END
          )
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS delta_cnt_bad_rate,
  CASE
    WHEN SUM(
      CASE
        WHEN delta_temp.trans_id < 0
        AND delta_temp.delta_bad_tag IS NOT NULL THEN delta_temp.usd_amt
        ELSE CAST(0 AS NUMERIC)
      END
    ) > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f',
          SUM(
            CASE
              WHEN delta_temp.delta_bad_tag IS NOT NULL
              AND CASE
                delta_temp.delta_bad_tag
                WHEN '' THEN 0.0
                ELSE CAST(
                  delta_temp.delta_bad_tag AS FLOAT64
                )
              END = 1
              AND delta_temp.trans_id < 0 THEN delta_temp.usd_amt
              ELSE CAST(0 AS NUMERIC)
            END
          ) * NUMERIC '1.000' / SUM(
            CASE
              WHEN delta_temp.trans_id < 0
              AND delta_temp.delta_bad_tag IS NOT NULL THEN delta_temp.usd_amt
              ELSE CAST(0 AS NUMERIC)
            END
          )
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS delta_amt_bad_rate
FROM
  `pypl-edw.${dataset_name_tmp}.delta_temp` delta_temp
WHERE
  delta_temp.rule_id IN (
    SELECT
      auth_flow_pld_pair.rule_id AS rule_id
    FROM
      `pypl-edw.${dataset_name_tmp}.auth_flow_pld_pair` AS auth_flow_pld_pair
  )
GROUP BY
  1;


--  on commit preserve rows
--  CALL pp_monitor.drop_table(''${decline_gms_auth_flow_pld}'');
--  CREATE multiset TABLE ${decline_gms_auth_flow_pld} AS (
--  SEL
--  bad.rule_id                                                                                ,
--  SUM(1)              AS cnt_total                                                           ,
--  SUM(bad.usd_amt)    AS amt_total                                                           ,
--  SUM(bad.bad_weight) AS bad_wcnt                                                            ,
--  SUM(1.0000 * bad_weight * bad.usd_amt) AS bad_wamt                                         ,
--  CASE WHEN cnt_total>0 THEN bad_wcnt*1.000 /cnt_total ELSE 'NA' END  AS decline_cnt_bad_rate,
--  CASE WHEN amt_total>0 THEN bad_wamt*1.000 /amt_total ELSE 'NA' END  AS decline_amt_bad_rate
--  FROM pp_oap_qiqi_t.mit_fct_dclbad bad
--  where pymt_start_date BETWEEN current_date-90 and current_date-7 and rule_id in (sel rule_id from ${dataset_name_tmp}.auth_flow_pld_pair
--  GROUP BY 1
--  ) with data primary index(rule_id)
--  -- on commit preserve rows
--  ;
CALL
  `pypl-edw`.pp_monitor.drop_table (
    '${dataset_name_tmp}.overlaprule_auth_flow_pld'
  );


CREATE TABLE
  `pypl-edw.${dataset_name_tmp}.overlaprule_auth_flow_pld`
CLUSTER BY
  rule_id AS
SELECT
  `pypl-edw`.cw_udf.cw_round_half_even (a.rule_id, 0) AS rule_id,
  a.action_type                                                 ,
  a.ov_rule_1                                                   ,
  a.ov_pct_1                                                    ,
  a.ov_ntid_1                                                   ,
  a.ov_rule_2                                                   ,
  a.ov_pct_2                                                    ,
  a.ov_ntid_2                                                   ,
  a.ov_rule_3                                                   ,
  a.ov_pct_3                                                    ,
  a.ov_ntid_3                                                   ,
  a.ov_rule_4                                                   ,
  a.ov_pct_4                                                    ,
  a.ov_ntid_4
FROM
  `pypl-edw.${dataset_name_tmp}.overlaprule` AS a
WHERE
  a.rule_id IN (
    SELECT
      auth_flow_pld_pair.rule_id AS rule_id
    FROM
      `pypl-edw.${dataset_name_tmp}.auth_flow_pld_pair` AS auth_flow_pld_pair
  )
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY
      rule_id
    ORDER BY
      a.ov_pct_1 DESC
  ) = 1;


--  on commit preserve rows
-- -------------------append contact rate (old TD version) -----------------------
-- call pp_monitor.drop_table(''${contact_metrics_auth_flow_pld}'');
--create multiset table ${contact_metrics_auth_flow_pld} as (
--select
--    rule_id
--    ,decline_cnt
--    ,cntct_cnt
--    ,cast(cntct_cnt as float)/case when decline_cnt = 0 then 1 else decline_cnt end as cntct_rate
--    ,case when cntct_cnt >= 30 and rule_mo in ('ATO','Collusion','StolenID') and cntct_rate >= 0.02 then 1
--          when cntct_cnt >= 30 and crs_team in ('gfr_credit') and cntct_rate >= 0.02 then 1
--          when cntct_cnt >= 30 and (rule_mo not in ('ATO','Collusion','StolenID') or rule_mo is null) and cntct_rate >= 0.015 then 1
--          else 0 end as high_cntct_rate_
--from(
--select
--    a.rule_id
--    ,b.rule_mo
--    ,b.crs_team
--    ,sum(coalesce(1, 0))                             as decline_cnt
--    ,sum(coalesce(cntct_cnt, 0))                     as cntct_cnt
--from pp_risk_crs_core.rule_decline_cntct_master a
--left join ${dataset_name_tmp}.check_action_rule_id b on a.rule_id = b.rule_id
--where a.rule_id in (sel rule_id from ${dataset_name_tmp}.auth_flow_pld_pair
--        and action_type in (16,17,18,19,20,22,23,29,97,98,146,203,1354)
--        and pmt_start_date BETWEEN current_date-90 and current_date-7
--        and pmt_start_date not in ('2020-08-21', '2020-08-22', '2020-08-23', '2020-08-24', '2020-08-26', '2020-09-08', '2020-09-09', '2020-09-10')
--        and pmt_start_date not in ('2022-09-01', '2022-09-02', '2022-09-07', '2022-09-08', '2022-09-09')
--group by 1,2,3
--)sub
--)with data primary index(rule_id)
-- -------------------append contact rate (BQ version) -----------------------
DROP TABLE IF EXISTS
  `pypl-edw.${dataset_name_tmp}.contact_metrics_auth_flow_pld_tmp`;


CREATE TABLE
  `pypl-edw.${dataset_name_tmp}.contact_metrics_auth_flow_pld_tmp`
CLUSTER BY
  rule_id AS
SELECT
  a.rule_id                                 ,
  SUM(COALESCE(1, 0))         AS decline_cnt,
  SUM(COALESCE(cntct_cnt, 0)) AS cntct_cnt
FROM
  `pypl-edw.pp_risk_crs_core.rule_decline_cntct_master` a
WHERE
  action_type IN (
    16 ,
    17 ,
    18 ,
    19 ,
    20 ,
    22 ,
    23 ,
    29 ,
    97 ,
    98 ,
    146,
    203,
    1354
  )
  AND pmt_start_date BETWEEN CURRENT_DATE('America/Los_Angeles') - 90
  AND CURRENT_DATE('America/Los_Angeles') - 7
  AND pmt_start_date NOT IN (
    '2020-08-21',
    '2020-08-22',
    '2020-08-23',
    '2020-08-24',
    '2020-08-26',
    '2020-09-08',
    '2020-09-09',
    '2020-09-10'
  )
  AND pmt_start_date NOT IN (
    '2022-09-01',
    '2022-09-02',
    '2022-09-07',
    '2022-09-08',
    '2022-09-09'
  )
GROUP BY
  1;


CALL
  `pypl-edw`.pp_monitor.drop_table (
    '${dataset_name_tmp}.contact_metrics_auth_flow_pld'
  );


CREATE TABLE
  `pypl-edw.${dataset_name_tmp}.contact_metrics_auth_flow_pld`
CLUSTER BY
  rule_id AS
SELECT
  `pypl-edw`.cw_udf.cw_round_half_even (sub.rule_id, 0) AS rule_id,
  sub.decline_cnt                                                 ,
  sub.cntct_cnt                                                   ,
  sub.cntct_cnt /                                       CASE
    WHEN sub.decline_cnt = 0 THEN 1
    ELSE sub.decline_cnt
  END AS cntct_rate,
  CASE
    WHEN sub.cntct_cnt >= 30
    AND sub.rule_mo IN ('ATO', 'Collusion', 'StolenID')
    AND sub.cntct_cnt / CASE
      WHEN sub.decline_cnt = 0 THEN 1
      ELSE sub.decline_cnt
    END >= NUMERIC '0.02' THEN 1
    WHEN sub.cntct_cnt >= 30
    AND LOWER(sub.crs_team) IN ('gfr_credit', 'goal_credit')
    AND sub.cntct_cnt / CASE
      WHEN sub.decline_cnt = 0 THEN 1
      ELSE sub.decline_cnt
    END >= NUMERIC '0.02' THEN 1
    WHEN sub.cntct_cnt >= 30
    AND (
      sub.rule_mo NOT IN ('ATO', 'Collusion', 'StolenID')
      OR sub.rule_mo IS NULL
    )
    AND sub.cntct_cnt / CASE
      WHEN sub.decline_cnt = 0 THEN 1
      ELSE sub.decline_cnt
    END >= NUMERIC '0.015' THEN 1
    ELSE 0
  END AS high_cntct_rate_
FROM
  (
    SELECT
      a.rule_id    ,
      a.decline_cnt,
      a.cntct_cnt  ,
      b.rule_mo    ,
      b.crs_team
    FROM
      `pypl-edw.${dataset_name_tmp}.contact_metrics_auth_flow_pld_tmp` AS a
      LEFT OUTER JOIN `pypl-edw.${dataset_name_tmp}.check_action_rule_id` AS b ON a.rule_id = b.rule_id
    WHERE
      a.rule_id IN (
        SELECT
          rule_id
        FROM
          `pypl-edw.${dataset_name_tmp}.auth_flow_pld_pair`
      )
  ) AS sub;


------------------------------------update bad count/amount for UBSM rules-----------------
-- ----------------------------------treatment for restriction_pld combo-----------------
CALL
  `pypl-edw`.pp_monitor.drop_table (
    '${dataset_name_tmp}.restrict_pld_pair'
  );


CREATE TABLE
  `pypl-edw.${dataset_name_tmp}.restrict_pld_pair`
CLUSTER BY
  rule_id AS
SELECT
  a.rule_id
FROM
  `pypl-edw.${dataset_name_tmp}.check_action_rule_id` AS a
  INNER JOIN (
    SELECT
      *
    FROM
      `pypl-edw.${dataset_name_tmp}.urm_lite` AS live_metadata_table
    WHERE
      UPPER(
        live_metadata_table.action_raw_str
      ) LIKE '%DECLINE_PMT%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) LIKE '%RESTRICT%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%LIFT_RESTRICTION%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%WHITE_LIST%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%WHITELIST%'
  ) AS b ON a.rule_id = b.rule_id;


--  on commit preserve rows
-- ----------------------------------treatment for restriction_disallow combo-----------------
CALL
  `pypl-edw`.pp_monitor.drop_table (
    '${dataset_name_tmp}.restrict_disallow_pair'
  );


CREATE TABLE
  `pypl-edw.${dataset_name_tmp}.restrict_disallow_pair`
CLUSTER BY
  rule_id AS
SELECT
  a.rule_id
FROM
  `pypl-edw.${dataset_name_tmp}.check_action_rule_id` AS a
  INNER JOIN (
    SELECT
      *
    FROM
      `pypl-edw.${dataset_name_tmp}.urm_lite` AS live_metadata_table
    WHERE
      UPPER(
        live_metadata_table.action_raw_str
      ) LIKE '%DISALLOW%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) LIKE '%RESTRICT%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%LIFT_RESTRICTION%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%WHITE_LIST%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%WHITELIST%'
  ) AS b ON a.rule_id = b.rule_id;


--  on commit preserve rows
CALL
  `pypl-edw`.pp_monitor.drop_table (
    '${dataset_name_tmp}.funding_decline_alert_v0305'
  );


CREATE TABLE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305`
CLUSTER BY
  action_type,
  rule_id AS
SELECT
  -- -CREATE multiset TABLE pp_oap_xsheng1_t.funding_decline_alert_0305 AS (
  -- ---------------------------ordinary rules------------------------------------
  r.crs_team           ,
  r.owner_ntid         ,
  r.rule_id            ,
  a.action_type        ,
  a.monitoring_window  ,
  r.observing_window   ,
  r.rule_mo            ,
  r.rule_name          ,
  a.fire_vol           ,
  a.u2u_fire_vol       ,
  a.fire_amt           ,
  a.cg_txn_cnt         ,
  a.cg_txn_wcnt        ,
  a.cg_txn_amt         ,
  a.cg_txn_wamt        ,
  a.cg_brm_bad_wcnt    ,
  a.cg_brm_bad_wcnt_adj,
  a.cg_brm_bad_wamt    ,
  a.cg_brm_bad_wamt_adj,
  cg_wcnt_bad_rate     ,
  CASE
    WHEN a.cg_txn_wamt > 0 THEN CAST(
      a.cg_brm_bad_wamt / a.cg_txn_wamt AS STRING
    )
    ELSE 'NA'
  END AS cg_wamt_bad_rate,
  CASE
    WHEN a.cg_txn_wcnt > 0 THEN CAST(
      a.cg_brm_bad_wcnt_adj / a.cg_txn_wcnt AS STRING
    )
    ELSE 'NA'
  END AS cg_wcnt_bad_rate_adj,
  CASE
    WHEN a.cg_txn_wamt > 0 THEN CAST(
      a.cg_brm_bad_wamt_adj / a.cg_txn_wamt AS STRING
    )
    ELSE 'NA'
  END AS cg_wamt_bad_rate_adj,
  a.str_cg_txn_cnt           ,
  a.str_cg_txn_wcnt          ,
  a.str_cg_txn_amt           ,
  a.str_cg_txn_wamt          ,
  a.str_cg_brm_bad_wcnt      ,
  a.str_cg_brm_bad_wcnt_adj  ,
  a.str_cg_brm_bad_wamt      ,
  a.str_cg_brm_bad_wamt_adj  ,
  CASE
    WHEN a.str_cg_txn_wcnt > 0 THEN CAST(
      a.str_cg_brm_bad_wcnt / a.str_cg_txn_wcnt AS STRING
    )
    ELSE 'NA'
  END AS str_cg_wcnt_bad_rate,
  CASE
    WHEN a.str_cg_txn_wamt > 0 THEN CAST(
      a.str_cg_brm_bad_wamt / a.str_cg_txn_wamt AS STRING
    )
    ELSE 'NA'
  END AS str_cg_wamt_bad_rate                     ,
  -- ------------used for gms/nextgen--------------
  CASE
    WHEN a.str_cg_txn_wcnt > 0 THEN CAST(
      a.str_cg_brm_bad_wcnt_adj / a.str_cg_txn_wcnt AS STRING
    )
    ELSE 'NA'
  END AS str_cg_wcnt_bad_rate_adj,
  CASE
    WHEN a.str_cg_txn_wamt > 0 THEN CAST(
      a.str_cg_brm_bad_wamt_adj / a.str_cg_txn_wamt AS STRING
    )
    ELSE 'NA'
  END AS str_cg_wamt_bad_rate_adj,
  a.actn_cg_txn_cnt              ,
  a.actn_cg_txn_wcnt             ,
  a.actn_cg_txn_amt              ,
  a.actn_cg_txn_wamt             ,
  a.actn_cg_brm_bad_wcnt         ,
  a.actn_cg_brm_bad_wcnt_adj     ,
  a.actn_cg_brm_bad_wamt         ,
  a.actn_cg_brm_bad_wamt_adj     ,
  CASE
    WHEN a.actn_cg_txn_wcnt > 0 THEN CAST(
      a.actn_cg_brm_bad_wcnt / a.actn_cg_txn_wcnt AS STRING
    )
    ELSE 'NA'
  END AS actn_cg_wcnt_bad_rate,
  CASE
    WHEN a.actn_cg_txn_wamt > 0 THEN CAST(
      a.actn_cg_brm_bad_wamt / a.actn_cg_txn_wamt AS STRING
    )
    ELSE 'NA'
  END AS actn_cg_wamt_bad_rate                    ,
  -- ------------used for gms/nextgen--------------
  CASE
    WHEN a.actn_cg_txn_wcnt > 0 THEN CAST(
      a.actn_cg_brm_bad_wcnt_adj / a.actn_cg_txn_wcnt AS STRING
    )
    ELSE 'NA'
  END AS actn_cg_wcnt_bad_rate_adj,
  CASE
    WHEN a.actn_cg_txn_wamt > 0 THEN CAST(
      a.actn_cg_brm_bad_wamt_adj / a.actn_cg_txn_wamt AS STRING
    )
    ELSE 'NA'
  END AS actn_cg_wamt_bad_rate_adj,
  a.match_txn_cnt                 ,
  a.match_txn_wcnt                ,
  a.match_txn_amt                 ,
  a.match_txn_wamt                ,
  a.match_bad_wcnt                ,
  a.match_bad_wcnt_adj            ,
  a.match_bad_wamt                ,
  a.match_bad_wamt_adj            ,
  CASE
    WHEN a.match_txn_wcnt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                             ,
          a.match_bad_wcnt * NUMERIC '1.000' / a.match_txn_wcnt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS match_wcnt_bad_rate,
  CASE
    WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                             ,
          a.match_bad_wamt * NUMERIC '1.000' / a.match_txn_wamt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS match_wamt_bad_rate,
  CASE
    WHEN a.match_txn_wcnt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                                 ,
          a.match_bad_wcnt_adj * NUMERIC '1.000' / a.match_txn_wcnt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS match_wcnt_bad_rate_adj,
  CASE
    WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                                 ,
          a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS match_wamt_bad_rate_adj                                      ,
  -- ---------------used for collusion and other teams-----------------
  CASE
    WHEN a.match_bad_wamt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                                 ,
          a.match_txn_wamt * NUMERIC '1.000' / a.match_bad_wamt - 1
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS match_wamt_fpr,
  CASE
    WHEN a.match_bad_wamt_adj > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                                     ,
          a.match_txn_wamt * NUMERIC '1.000' / a.match_bad_wamt_adj - 1
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS match_wamt_fpr_adj,
  a.str_match_txn_cnt      ,
  a.str_match_txn_wcnt     ,
  a.str_match_txn_amt      ,
  a.str_match_txn_wamt     ,
  a.str_match_bad_wcnt     ,
  a.str_match_bad_wcnt_adj ,
  a.str_match_bad_wamt     ,
  a.str_match_bad_wamt_adj ,
  CASE
    WHEN a.str_match_txn_wcnt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                                     ,
          a.str_match_bad_wcnt * NUMERIC '1.000' / a.str_match_txn_wcnt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS str_match_wcnt_bad_rate,
  CASE
    WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                                     ,
          a.str_match_bad_wamt * NUMERIC '1.000' / a.str_match_txn_wamt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS str_match_wamt_bad_rate,
  CASE
    WHEN a.str_match_txn_wcnt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                                         ,
          a.str_match_bad_wcnt_adj * NUMERIC '1.000' / a.str_match_txn_wcnt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS str_match_wcnt_bad_rate_adj,
  CASE
    WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                                         ,
          a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS str_match_wamt_bad_rate_adj                                               ,
  -- ---------------used for non-collusion and non-gms/nextgen teams----------------
  a.actn_match_txn_cnt     ,
  a.actn_match_txn_wcnt    ,
  a.actn_match_txn_amt     ,
  a.actn_match_txn_wamt    ,
  a.actn_match_bad_wcnt    ,
  a.actn_match_bad_wcnt_adj,
  a.actn_match_bad_wamt    ,
  a.actn_match_bad_wamt_adj,
  CASE
    WHEN a.actn_match_txn_wcnt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                                       ,
          a.actn_match_bad_wcnt * NUMERIC '1.000' / a.actn_match_txn_wcnt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS actn_match_wcnt_bad_rate,
  CASE
    WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                                       ,
          a.actn_match_bad_wamt * NUMERIC '1.000' / a.actn_match_txn_wamt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS actn_match_wamt_bad_rate,
  CASE
    WHEN a.actn_match_txn_wcnt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                                           ,
          a.actn_match_bad_wcnt_adj * NUMERIC '1.000' / a.actn_match_txn_wcnt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS actn_match_wcnt_bad_rate_adj,
  CASE
    WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                                           ,
          a.actn_match_bad_wamt_adj * NUMERIC '1.000' / a.actn_match_txn_wamt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS actn_match_wamt_bad_rate_adj,
  d.delta_bad_cnt                    ,
  d.delta_txn_cnt                    ,
  d.delta_bad_amt                    ,
  d.delta_txn_amt                    ,
  d.delta_cnt_bad_rate               ,
  d.delta_amt_bad_rate               ,
  a.decled_cnt                       ,
  a.decled_amt                       ,
  a.decled_bad_cnt                   ,
  a.decled_bad_amt                   ,
  a.incre_str_decled_cnt             ,
  a.incre_str_decled_amt             ,
  a.incre_str_decled_bad_cnt         ,
  a.incre_str_decled_bad_amt         ,
  a.incre_actn_decled_cnt            ,
  a.incre_actn_decled_amt            ,
  a.incre_actn_decled_bad_cnt        ,
  a.incre_actn_decled_bad_amt        ,
  CASE
    WHEN a.decled_cnt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                         ,
          a.decled_bad_cnt * NUMERIC '1.000' / a.decled_cnt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS decline_cnt_bad_rate,
  CASE
    WHEN a.decled_amt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                         ,
          a.decled_bad_amt * NUMERIC '1.000' / a.decled_amt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS decline_amt_bad_rate,
  CASE
    WHEN a.incre_str_decled_cnt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                                             ,
          a.incre_str_decled_bad_cnt * NUMERIC '1.000' / a.incre_str_decled_cnt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS incre_str_decline_cnt_bad_rate,
  CASE
    WHEN a.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                                             ,
          a.incre_str_decled_bad_amt * NUMERIC '1.000' / a.incre_str_decled_amt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS incre_str_decline_amt_bad_rate,
  CASE
    WHEN a.incre_actn_decled_cnt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                                               ,
          a.incre_actn_decled_bad_cnt * NUMERIC '1.000' / a.incre_actn_decled_cnt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS incre_actn_decline_cnt_bad_rate,
  CASE
    WHEN a.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                                               ,
          a.incre_actn_decled_bad_amt * NUMERIC '1.000' / a.incre_actn_decled_amt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS incre_actn_decline_amt_bad_rate,
  a.sum_wh_actn                         ,
  CASE
    WHEN a.fire_vol > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                    ,
          a.sum_wh_actn * NUMERIC '1.000' / a.fire_vol
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS whed_pct,
  a.whed_cnt     ,
  a.whed_amt     ,
  a.bad_amt_adj  ,
  CASE
    WHEN a.whed_amt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                    ,
          a.bad_amt_adj * NUMERIC '1.000' / a.whed_amt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS wh_badrate    ,
  a.incre_str_fire_vol ,
  a.incre_rule_fire_vol,
  a.incre_actn_fire_vol,
  CASE
    WHEN a.fire_vol > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                                ,
          1 - a.incre_rule_fire_vol * NUMERIC '1.000' / a.fire_vol
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS ov_pct                                ,
  a.catch_bad                                  ,
  a.incre_str_fire_amt                         ,
  a.incre_str_non_whed_fire_amt                ,
  b.ov_rule_1                                  ,
  b.ov_pct_1                                   ,
  b.ov_ntid_1                                  ,
  b.ov_rule_2                                  ,
  b.ov_pct_2                                   ,
  b.ov_ntid_2                                  ,
  b.ov_rule_3                                  ,
  b.ov_pct_3                                   ,
  b.ov_ntid_3                                  ,
  b.ov_rule_4                                  ,
  b.ov_pct_4                                   ,
  b.ov_ntid_4                                  ,
  COALESCE(cntct.decline_cnt, 0) AS decline_cnt,
  COALESCE(cntct.cntct_cnt, 0) AS cntct_cnt    ,
  COALESCE(
    cntct.cntct_rate ,
    CAST(0 AS FLOAT64)
  ) AS cntct_rate,
  CASE
    WHEN CASE
      WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
      ELSE 0
    END = 1 THEN 0
    ELSE COALESCE(cntct.high_cntct_rate_, 0)
  END AS high_cntct_rate,
  CASE
    WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
    ELSE 0
  END AS business_policy,
  CASE
    WHEN e.rule_id IS NOT NULL THEN 1
    ELSE 0
  END AS exclusion_flg,
  CASE
    WHEN e2.rule_id IS NOT NULL
    OR res_dis.rule_id IS NOT NULL THEN 1
    ELSE 0
  END AS incre_exempt,
  CASE
    WHEN res_dis.rule_id IS NOT NULL THEN 1
    ELSE 0
  END AS excl_high_overlap,
  CASE
    WHEN e_ow.rule_id IS NOT NULL THEN 1
    ELSE 0
  END AS excl_over_whitelisted,
  CASE
    WHEN st.rule_id IS NOT NULL THEN st.is_strategy
    ELSE 0
  END AS is_strategy                                        ,
  COALESCE(st_ov1.is_strategy_ov1_raw, 0) AS is_strategy_ov1,
  CASE
    WHEN (
      a.fire_vol < 10 * r.observing_window
      AND (
        SAFE_CAST(
          CASE
            WHEN a.cg_txn_wamt > 0 THEN CAST(
              a.cg_brm_bad_wamt / a.cg_txn_wamt AS STRING
            )
            ELSE 'NA'
          END AS BIGNUMERIC
        ) < NUMERIC '0.1'
        OR RTRIM(
          CASE
            WHEN a.cg_txn_wamt > 0 THEN CAST(
              a.cg_brm_bad_wamt / a.cg_txn_wamt AS STRING
            )
            ELSE 'NA'
          END
        ) = 'NA'
      )
      OR CASE
        WHEN e2.rule_id IS NOT NULL
        OR res_dis.rule_id IS NOT NULL THEN 1
        ELSE 0
      END = 0
      AND a.action_type <> 1354
      AND a.incre_rule_fire_vol < 5 * r.observing_window
    )
    AND CASE
      WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
      ELSE 0
    END = 0
    AND CASE
      WHEN e.rule_id IS NOT NULL THEN 1
      ELSE 0
    END = 0 THEN 1
    ELSE 0
  END AS low_firing,
  CASE
    WHEN CASE
      WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
      ELSE 0
    END = 0
    AND SAFE_CAST(b.ov_pct_1 AS BIGNUMERIC) >= NUMERIC '0.8'
    AND CASE
      WHEN e2.rule_id IS NOT NULL
      OR res_dis.rule_id IS NOT NULL THEN 1
      ELSE 0
    END = 0
    AND a.action_type <> 1354 THEN CASE
      WHEN UPPER(r.tags) LIKE '%AUTOSOLUTION%'
      AND UPPER(r.tags) LIKE '%HYPERNET MERCHANT%'
      AND COALESCE(st_ov1.is_strategy_ov1_raw, 0) = 1 THEN 1
      WHEN UPPER(r.tags) LIKE '%AUTOSOLUTION%'
      AND UPPER(r.tags) LIKE '%HYPERNET MERCHANT%'
      AND COALESCE(st_ov1.is_strategy_ov1_raw, 0) = 0 THEN 0
      ELSE 1
    END
    ELSE 0
  END AS high_overlap,
  CASE
    WHEN CASE
      WHEN e_ow.rule_id IS NOT NULL THEN 1
      ELSE 0
    END = 0
    AND SAFE_CAST(
      CASE
        WHEN a.fire_vol > 0 THEN REGEXP_REPLACE(
          REGEXP_REPLACE(
            FORMAT(
              '%6.3f'                                    ,
              a.sum_wh_actn * NUMERIC '1.000' / a.fire_vol
            )                   ,
            r'^( *?)(-)?0(\..*)',
            r'\2\3 \1'
          )                     ,
          r'^( *?)(-)?(\d*\..*)',
          r'\2\3\1'
        )
        ELSE 'NA'
      END AS BIGNUMERIC
    ) >= NUMERIC '0.7'
    AND SAFE_CAST(
      CASE
        WHEN a.whed_amt > 0 THEN REGEXP_REPLACE(
          REGEXP_REPLACE(
            FORMAT(
              '%6.3f'                                    ,
              a.bad_amt_adj * NUMERIC '1.000' / a.whed_amt
            )                   ,
            r'^( *?)(-)?0(\..*)',
            r'\2\3 \1'
          )                     ,
          r'^( *?)(-)?(\d*\..*)',
          r'\2\3\1'
        )
        ELSE 'NA'
      END AS BIGNUMERIC
    ) < NUMERIC '0.1'
    AND a.whed_cnt >= 30
    AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%' THEN 1
    ELSE 0
  END AS over_whitelisted,
  CASE
    WHEN CASE
      WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
      ELSE 0
    END = 1 THEN 0
    WHEN excl_accuracy.rule_id IS NOT NULL THEN 0
    WHEN st.is_strategy = 1
    AND (
      UPPER(r.rule_name) LIKE '%FALLBACK%'
      OR UPPER(r.tags) LIKE '%SAFETY%NET%'
    ) THEN CASE
      WHEN LOWER(r.crs_team) IN (
        'gfr_omni_gms'    ,
        'gfr_omni_nextgen',
        'gfr_omni'        ,
        'goal_nextgen'    ,
        'goal_gms'
      ) THEN CASE
        WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
          WHEN a.cg_txn_cnt >= 30
          AND SAFE_CAST(
            CASE
              WHEN a.cg_txn_wamt > 0 THEN CAST(
                a.cg_brm_bad_wamt / a.cg_txn_wamt AS STRING
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.10'
          OR a.cg_txn_cnt < 30
          AND SAFE_CAST(
            CASE
              WHEN a.decled_amt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                         ,
                    a.decled_bad_amt * NUMERIC '1.000' / a.decled_amt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.10' THEN 1
          ELSE 0
        END
        WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN CASE
          WHEN a.match_txn_cnt >= 30
          AND SAFE_CAST(
            CASE
              WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                 ,
                    a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.10' THEN 1
          ELSE 0
        END
        WHEN UPPER(RTRIM(r.rule_mo)) = 'CC' THEN CASE
          WHEN a.match_txn_cnt >= 30
          AND SAFE_CAST(
            CASE
              WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                 ,
                    a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.10'
          OR a.match_txn_cnt < 30
          AND SAFE_CAST(
            CASE
              WHEN a.decled_amt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                         ,
                    a.decled_bad_amt * NUMERIC '1.000' / a.decled_amt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.10' THEN 1
          ELSE 0
        END
        WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
          WHEN a.match_txn_cnt >= 30
          AND SAFE_CAST(
            CASE
              WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                 ,
                    a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.10' THEN 1
          ELSE 0
        END
        ELSE 0
      END
      WHEN r.rule_mo IN ('Collusion', 'UBSM')
      AND xoom_team.xoom_ntid IS NULL THEN CASE
        WHEN SAFE_CAST(
          CASE
            WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
              REGEXP_REPLACE(
                FORMAT(
                  '%6.3f'                                                 ,
                  a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                )                   ,
                r'^( *?)(-)?0(\..*)',
                r'\2\3 \1'
              )                     ,
              r'^( *?)(-)?(\d*\..*)',
              r'\2\3\1'
            )
            ELSE 'NA'
          END AS BIGNUMERIC
        ) < NUMERIC '0.10'
        AND a.match_txn_cnt >= 30 THEN 1
        ELSE 0
      END
      ELSE CASE
        WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
        AND xoom_team.xoom_ntid IS NULL
        AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
        AND r.rule_id NOT IN (467812)
        AND (
          SAFE_CAST(
            CASE
              WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                 ,
                    a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.10'
          AND a.match_txn_cnt >= 30
        ) THEN 1
        ELSE 0
      END
    END
    ELSE CASE
      WHEN LOWER(r.crs_team) IN (
        'gfr_omni_gms'    ,
        'gfr_omni_nextgen',
        'gfr_omni'        ,
        'goal_nextgen'    ,
        'goal_gms'
      ) THEN CASE
        WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
          WHEN a.cg_txn_cnt >= 30
          AND SAFE_CAST(
            CASE
              WHEN a.cg_txn_wamt > 0 THEN CAST(
                a.cg_brm_bad_wamt / a.cg_txn_wamt AS STRING
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.15'
          OR a.cg_txn_cnt < 30
          AND SAFE_CAST(
            CASE
              WHEN a.decled_amt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                         ,
                    a.decled_bad_amt * NUMERIC '1.000' / a.decled_amt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.15' THEN 1
          ELSE 0
        END
        WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN CASE
          WHEN a.match_txn_cnt >= 30
          AND SAFE_CAST(
            CASE
              WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                 ,
                    a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.15' THEN 1
          ELSE 0
        END
        WHEN UPPER(RTRIM(r.rule_mo)) = 'CC' THEN CASE
          WHEN a.match_txn_cnt >= 30
          AND SAFE_CAST(
            CASE
              WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                 ,
                    a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.15'
          OR a.match_txn_cnt < 30
          AND SAFE_CAST(
            CASE
              WHEN a.decled_amt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                         ,
                    a.decled_bad_amt * NUMERIC '1.000' / a.decled_amt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.15' THEN 1
          ELSE 0
        END
        WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
          WHEN a.match_txn_cnt >= 30
          AND SAFE_CAST(
            CASE
              WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                 ,
                    a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.15' THEN 1
          ELSE 0
        END
        ELSE 0
      END
      WHEN r.rule_mo IN ('Collusion', 'UBSM')
      AND xoom_team.xoom_ntid IS NULL THEN CASE
        WHEN SAFE_CAST(
          CASE
            WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
              REGEXP_REPLACE(
                FORMAT(
                  '%6.3f'                                                 ,
                  a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                )                   ,
                r'^( *?)(-)?0(\..*)',
                r'\2\3 \1'
              )                     ,
              r'^( *?)(-)?(\d*\..*)',
              r'\2\3\1'
            )
            ELSE 'NA'
          END AS BIGNUMERIC
        ) < NUMERIC '0.15'
        AND a.match_txn_cnt >= 30 THEN 1
        ELSE 0
      END
      ELSE CASE
        WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
        AND xoom_team.xoom_ntid IS NULL
        AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
        AND r.rule_id NOT IN (467812)
        AND (
          SAFE_CAST(
            CASE
              WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                 ,
                    a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.15'
          AND a.match_txn_cnt >= 30
        ) THEN 1
        ELSE 0
      END
    END
  END AS sub_poor_accuracy_bad_rate,
  CASE
    WHEN CASE
      WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
      ELSE 0
    END = 1 THEN 0
    WHEN excl_accuracy.rule_id IS NOT NULL THEN 0
    WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN 0
    WHEN st.is_strategy = 1
    AND (
      UPPER(r.rule_name) LIKE '%FALLBACK%'
      OR UPPER(r.tags) LIKE '%SAFETY%NET%'
    ) THEN CASE
      WHEN LOWER(r.crs_team) IN (
        'gfr_omni_gms'    ,
        'gfr_omni_nextgen',
        'gfr_omni'        ,
        'goal_nextgen'    ,
        'goal_gms'
      ) THEN CASE
        WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
          WHEN CASE
            WHEN e2.rule_id IS NOT NULL
            OR res_dis.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND a.action_type <> 1354
          AND (
            a.str_cg_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a.str_cg_txn_wamt > 0 THEN CAST(
                  a.str_cg_brm_bad_wamt / a.str_cg_txn_wamt AS STRING
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            OR a.str_cg_txn_cnt < 30
            AND SAFE_CAST(
              CASE
                WHEN a.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                             ,
                      a.incre_str_decled_bad_amt * NUMERIC '1.000' / a.incre_str_decled_amt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
          ) THEN 1
          ELSE 0
        END
        WHEN UPPER(RTRIM(r.rule_mo)) = 'CC' THEN CASE
          WHEN CASE
            WHEN e2.rule_id IS NOT NULL
            OR res_dis.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND a.action_type <> 1354
          AND (
            a.str_match_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                         ,
                      a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            OR a.str_match_txn_cnt < 30
            AND SAFE_CAST(
              CASE
                WHEN a.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                             ,
                      a.incre_str_decled_bad_amt * NUMERIC '1.000' / a.incre_str_decled_amt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
          ) THEN 1
          ELSE 0
        END
        WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
          WHEN CASE
            WHEN e2.rule_id IS NOT NULL
            OR res_dis.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND a.action_type <> 1354
          AND (
            a.str_match_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                         ,
                      a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
          ) THEN 1
          ELSE 0
        END
        ELSE 0
      END
      ELSE CASE
        WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
        AND xoom_team.xoom_ntid IS NULL
        AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
        AND r.rule_id NOT IN (467812)
        AND (
          CASE
            WHEN e2.rule_id IS NOT NULL
            OR res_dis.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND a.action_type <> 1354
          AND SAFE_CAST(
            CASE
              WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                         ,
                    a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.10'
          AND a.str_match_txn_cnt >= 30
        ) THEN 1
        ELSE 0
      END
    END
    ELSE CASE
      WHEN LOWER(r.crs_team) IN (
        'gfr_omni_gms'    ,
        'gfr_omni_nextgen',
        'gfr_omni'        ,
        'goal_nextgen'    ,
        'goal_gms'
      ) THEN CASE
        WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
          WHEN CASE
            WHEN e2.rule_id IS NOT NULL
            OR res_dis.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND a.action_type <> 1354
          AND (
            a.str_cg_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a.str_cg_txn_wamt > 0 THEN CAST(
                  a.str_cg_brm_bad_wamt / a.str_cg_txn_wamt AS STRING
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
            OR a.str_cg_txn_cnt < 30
            AND SAFE_CAST(
              CASE
                WHEN a.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                             ,
                      a.incre_str_decled_bad_amt * NUMERIC '1.000' / a.incre_str_decled_amt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
          ) THEN 1
          ELSE 0
        END
        WHEN UPPER(RTRIM(r.rule_mo)) = 'CC' THEN CASE
          WHEN CASE
            WHEN e2.rule_id IS NOT NULL
            OR res_dis.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND a.action_type <> 1354
          AND (
            a.str_match_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                         ,
                      a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
            OR a.str_match_txn_cnt < 30
            AND SAFE_CAST(
              CASE
                WHEN a.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                             ,
                      a.incre_str_decled_bad_amt * NUMERIC '1.000' / a.incre_str_decled_amt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
          ) THEN 1
          ELSE 0
        END
        WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
          WHEN CASE
            WHEN e2.rule_id IS NOT NULL
            OR res_dis.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND a.action_type <> 1354
          AND (
            a.str_match_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                         ,
                      a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
          ) THEN 1
          ELSE 0
        END
        ELSE 0
      END
      ELSE CASE
        WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
        AND xoom_team.xoom_ntid IS NULL
        AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
        AND r.rule_id NOT IN (467812)
        AND (
          CASE
            WHEN e2.rule_id IS NOT NULL
            OR res_dis.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND a.action_type <> 1354
          AND SAFE_CAST(
            CASE
              WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                         ,
                    a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.15'
          AND a.str_match_txn_cnt >= 30
        ) THEN 1
        ELSE 0
      END
    END
  END AS sub_poor_accuracy_incre_str_bad_rate,
  CASE
    WHEN CASE
      WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
      ELSE 0
    END = 1 THEN 0
    WHEN excl_accuracy.rule_id IS NOT NULL THEN 0
    WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN 0
    WHEN st.is_strategy = 1
    AND (
      UPPER(r.rule_name) LIKE '%FALLBACK%'
      OR UPPER(r.tags) LIKE '%SAFETY%NET%'
    ) THEN CASE
      WHEN LOWER(r.crs_team) IN (
        'gfr_omni_gms'    ,
        'gfr_omni_nextgen',
        'gfr_omni'        ,
        'goal_nextgen'    ,
        'goal_gms'
      ) THEN CASE
        WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
          WHEN CASE
            WHEN e2.rule_id IS NOT NULL
            OR res_dis.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND a.action_type <> 1354
          AND (
            a.actn_cg_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a.actn_cg_txn_wamt > 0 THEN CAST(
                  a.actn_cg_brm_bad_wamt / a.actn_cg_txn_wamt AS STRING
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            OR a.actn_cg_txn_cnt < 30
            AND SAFE_CAST(
              CASE
                WHEN a.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                               ,
                      a.incre_actn_decled_bad_amt * NUMERIC '1.000' / a.incre_actn_decled_amt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
          ) THEN 1
          ELSE 0
        END
        WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
          WHEN CASE
            WHEN e2.rule_id IS NOT NULL
            OR res_dis.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND a.action_type <> 1354
          AND (
            a.actn_match_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                           ,
                      a.actn_match_bad_wamt_adj * NUMERIC '1.000' / a.actn_match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            OR a.actn_match_txn_cnt < 30
            AND SAFE_CAST(
              CASE
                WHEN a.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                               ,
                      a.incre_actn_decled_bad_amt * NUMERIC '1.000' / a.incre_actn_decled_amt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
          ) THEN 1
          ELSE 0
        END
        ELSE 0
      END
      ELSE CASE
        WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
        AND xoom_team.xoom_ntid IS NULL
        AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
        AND r.rule_id NOT IN (467812)
        AND (
          CASE
            WHEN e2.rule_id IS NOT NULL
            OR res_dis.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND a.action_type <> 1354
          AND SAFE_CAST(
            CASE
              WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                           ,
                    a.actn_match_bad_wamt_adj * NUMERIC '1.000' / a.actn_match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.10'
          AND a.actn_match_txn_cnt >= 30
        ) THEN 1
        ELSE 0
      END
    END
    ELSE CASE
      WHEN LOWER(r.crs_team) IN (
        'gfr_omni_gms'    ,
        'gfr_omni_nextgen',
        'gfr_omni'        ,
        'goal_nextgen'    ,
        'goal_gms'
      ) THEN CASE
        WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
          WHEN CASE
            WHEN e2.rule_id IS NOT NULL
            OR res_dis.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND a.action_type <> 1354
          AND (
            a.actn_cg_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a.actn_cg_txn_wamt > 0 THEN CAST(
                  a.actn_cg_brm_bad_wamt / a.actn_cg_txn_wamt AS STRING
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
            OR a.actn_cg_txn_cnt < 30
            AND SAFE_CAST(
              CASE
                WHEN a.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                               ,
                      a.incre_actn_decled_bad_amt * NUMERIC '1.000' / a.incre_actn_decled_amt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
          ) THEN 1
          ELSE 0
        END
        WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
          WHEN CASE
            WHEN e2.rule_id IS NOT NULL
            OR res_dis.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND a.action_type <> 1354
          AND (
            a.actn_match_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                           ,
                      a.actn_match_bad_wamt_adj * NUMERIC '1.000' / a.actn_match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
            OR a.actn_match_txn_cnt < 30
            AND SAFE_CAST(
              CASE
                WHEN a.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                               ,
                      a.incre_actn_decled_bad_amt * NUMERIC '1.000' / a.incre_actn_decled_amt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
          ) THEN 1
          ELSE 0
        END
        ELSE 0
      END
      ELSE CASE
        WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
        AND xoom_team.xoom_ntid IS NULL
        AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
        AND r.rule_id NOT IN (467812)
        AND (
          CASE
            WHEN e2.rule_id IS NOT NULL
            OR res_dis.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND a.action_type <> 1354
          AND SAFE_CAST(
            CASE
              WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                           ,
                    a.actn_match_bad_wamt_adj * NUMERIC '1.000' / a.actn_match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.15'
          AND a.actn_match_txn_cnt >= 30
        ) THEN 1
        ELSE 0
      END
    END
  END AS sub_poor_accuracy_incre_actn_bad_rate,
  CASE
    WHEN CASE
      WHEN CASE
        WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
        ELSE 0
      END = 1 THEN 0
      WHEN excl_accuracy.rule_id IS NOT NULL THEN 0
      WHEN st.is_strategy = 1
      AND (
        UPPER(r.rule_name) LIKE '%FALLBACK%'
        OR UPPER(r.tags) LIKE '%SAFETY%NET%'
      ) THEN CASE
        WHEN LOWER(r.crs_team) IN (
          'gfr_omni_gms'    ,
          'gfr_omni_nextgen',
          'gfr_omni'        ,
          'goal_nextgen'    ,
          'goal_gms'
        ) THEN CASE
          WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
            WHEN a.cg_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a.cg_txn_wamt > 0 THEN CAST(
                  a.cg_brm_bad_wamt / a.cg_txn_wamt AS STRING
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            OR a.cg_txn_cnt < 30
            AND SAFE_CAST(
              CASE
                WHEN a.decled_amt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                         ,
                      a.decled_bad_amt * NUMERIC '1.000' / a.decled_amt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10' THEN 1
            ELSE 0
          END
          WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN CASE
            WHEN a.match_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                 ,
                      a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10' THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r.rule_mo)) = 'CC' THEN CASE
            WHEN a.match_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                 ,
                      a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            OR a.match_txn_cnt < 30
            AND SAFE_CAST(
              CASE
                WHEN a.decled_amt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                         ,
                      a.decled_bad_amt * NUMERIC '1.000' / a.decled_amt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10' THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
            WHEN a.match_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                 ,
                      a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10' THEN 1
            ELSE 0
          END
          ELSE 0
        END
        WHEN r.rule_mo IN ('Collusion', 'UBSM')
        AND xoom_team.xoom_ntid IS NULL THEN CASE
          WHEN SAFE_CAST(
            CASE
              WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                 ,
                    a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.10'
          AND a.match_txn_cnt >= 30 THEN 1
          ELSE 0
        END
        ELSE CASE
          WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
          AND xoom_team.xoom_ntid IS NULL
          AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
          AND r.rule_id NOT IN (467812)
          AND (
            SAFE_CAST(
              CASE
                WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                 ,
                      a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            AND a.match_txn_cnt >= 30
          ) THEN 1
          ELSE 0
        END
      END
      ELSE CASE
        WHEN LOWER(r.crs_team) IN (
          'gfr_omni_gms'    ,
          'gfr_omni_nextgen',
          'gfr_omni'        ,
          'goal_nextgen'    ,
          'goal_gms'
        ) THEN CASE
          WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
            WHEN a.cg_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a.cg_txn_wamt > 0 THEN CAST(
                  a.cg_brm_bad_wamt / a.cg_txn_wamt AS STRING
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
            OR a.cg_txn_cnt < 30
            AND SAFE_CAST(
              CASE
                WHEN a.decled_amt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                         ,
                      a.decled_bad_amt * NUMERIC '1.000' / a.decled_amt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15' THEN 1
            ELSE 0
          END
          WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN CASE
            WHEN a.match_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                 ,
                      a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15' THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r.rule_mo)) = 'CC' THEN CASE
            WHEN a.match_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                 ,
                      a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
            OR a.match_txn_cnt < 30
            AND SAFE_CAST(
              CASE
                WHEN a.decled_amt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                         ,
                      a.decled_bad_amt * NUMERIC '1.000' / a.decled_amt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15' THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
            WHEN a.match_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                 ,
                      a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15' THEN 1
            ELSE 0
          END
          ELSE 0
        END
        WHEN r.rule_mo IN ('Collusion', 'UBSM')
        AND xoom_team.xoom_ntid IS NULL THEN CASE
          WHEN SAFE_CAST(
            CASE
              WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                 ,
                    a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.15'
          AND a.match_txn_cnt >= 30 THEN 1
          ELSE 0
        END
        ELSE CASE
          WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
          AND xoom_team.xoom_ntid IS NULL
          AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
          AND r.rule_id NOT IN (467812)
          AND (
            SAFE_CAST(
              CASE
                WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                 ,
                      a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
            AND a.match_txn_cnt >= 30
          ) THEN 1
          ELSE 0
        END
      END
    END + CASE
      WHEN CASE
        WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
        ELSE 0
      END = 1 THEN 0
      WHEN excl_accuracy.rule_id IS NOT NULL THEN 0
      WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN 0
      WHEN st.is_strategy = 1
      AND (
        UPPER(r.rule_name) LIKE '%FALLBACK%'
        OR UPPER(r.tags) LIKE '%SAFETY%NET%'
      ) THEN CASE
        WHEN LOWER(r.crs_team) IN (
          'gfr_omni_gms'    ,
          'gfr_omni_nextgen',
          'gfr_omni'        ,
          'goal_nextgen'    ,
          'goal_gms'
        ) THEN CASE
          WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
            WHEN CASE
              WHEN e2.rule_id IS NOT NULL
              OR res_dis.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND a.action_type <> 1354
            AND (
              a.actn_cg_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a.actn_cg_txn_wamt > 0 THEN CAST(
                    a.actn_cg_brm_bad_wamt / a.actn_cg_txn_wamt AS STRING
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              OR a.actn_cg_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                               ,
                        a.incre_actn_decled_bad_amt * NUMERIC '1.000' / a.incre_actn_decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
            ) THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
            WHEN CASE
              WHEN e2.rule_id IS NOT NULL
              OR res_dis.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND a.action_type <> 1354
            AND (
              a.actn_match_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                           ,
                        a.actn_match_bad_wamt_adj * NUMERIC '1.000' / a.actn_match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              OR a.actn_match_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                               ,
                        a.incre_actn_decled_bad_amt * NUMERIC '1.000' / a.incre_actn_decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
            ) THEN 1
            ELSE 0
          END
          ELSE 0
        END
        ELSE CASE
          WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
          AND xoom_team.xoom_ntid IS NULL
          AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
          AND r.rule_id NOT IN (467812)
          AND (
            CASE
              WHEN e2.rule_id IS NOT NULL
              OR res_dis.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND a.action_type <> 1354
            AND SAFE_CAST(
              CASE
                WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                           ,
                      a.actn_match_bad_wamt_adj * NUMERIC '1.000' / a.actn_match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            AND a.actn_match_txn_cnt >= 30
          ) THEN 1
          ELSE 0
        END
      END
      ELSE CASE
        WHEN LOWER(r.crs_team) IN (
          'gfr_omni_gms'    ,
          'gfr_omni_nextgen',
          'gfr_omni'        ,
          'goal_nextgen'    ,
          'goal_gms'
        ) THEN CASE
          WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
            WHEN CASE
              WHEN e2.rule_id IS NOT NULL
              OR res_dis.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND a.action_type <> 1354
            AND (
              a.actn_cg_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a.actn_cg_txn_wamt > 0 THEN CAST(
                    a.actn_cg_brm_bad_wamt / a.actn_cg_txn_wamt AS STRING
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
              OR a.actn_cg_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                               ,
                        a.incre_actn_decled_bad_amt * NUMERIC '1.000' / a.incre_actn_decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
            ) THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
            WHEN CASE
              WHEN e2.rule_id IS NOT NULL
              OR res_dis.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND a.action_type <> 1354
            AND (
              a.actn_match_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                           ,
                        a.actn_match_bad_wamt_adj * NUMERIC '1.000' / a.actn_match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
              OR a.actn_match_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                               ,
                        a.incre_actn_decled_bad_amt * NUMERIC '1.000' / a.incre_actn_decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
            ) THEN 1
            ELSE 0
          END
          ELSE 0
        END
        ELSE CASE
          WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
          AND xoom_team.xoom_ntid IS NULL
          AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
          AND r.rule_id NOT IN (467812)
          AND (
            CASE
              WHEN e2.rule_id IS NOT NULL
              OR res_dis.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND a.action_type <> 1354
            AND SAFE_CAST(
              CASE
                WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                           ,
                      a.actn_match_bad_wamt_adj * NUMERIC '1.000' / a.actn_match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
            AND a.actn_match_txn_cnt >= 30
          ) THEN 1
          ELSE 0
        END
      END
    END + CASE
      WHEN CASE
        WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
        ELSE 0
      END = 1 THEN 0
      WHEN excl_accuracy.rule_id IS NOT NULL THEN 0
      WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN 0
      WHEN st.is_strategy = 1
      AND (
        UPPER(r.rule_name) LIKE '%FALLBACK%'
        OR UPPER(r.tags) LIKE '%SAFETY%NET%'
      ) THEN CASE
        WHEN LOWER(r.crs_team) IN (
          'gfr_omni_gms'    ,
          'gfr_omni_nextgen',
          'gfr_omni'        ,
          'goal_nextgen'    ,
          'goal_gms'
        ) THEN CASE
          WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
            WHEN CASE
              WHEN e2.rule_id IS NOT NULL
              OR res_dis.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND a.action_type <> 1354
            AND (
              a.str_cg_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a.str_cg_txn_wamt > 0 THEN CAST(
                    a.str_cg_brm_bad_wamt / a.str_cg_txn_wamt AS STRING
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              OR a.str_cg_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                             ,
                        a.incre_str_decled_bad_amt * NUMERIC '1.000' / a.incre_str_decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
            ) THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r.rule_mo)) = 'CC' THEN CASE
            WHEN CASE
              WHEN e2.rule_id IS NOT NULL
              OR res_dis.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND a.action_type <> 1354
            AND (
              a.str_match_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                         ,
                        a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              OR a.str_match_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                             ,
                        a.incre_str_decled_bad_amt * NUMERIC '1.000' / a.incre_str_decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
            ) THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
            WHEN CASE
              WHEN e2.rule_id IS NOT NULL
              OR res_dis.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND a.action_type <> 1354
            AND (
              a.str_match_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                         ,
                        a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
            ) THEN 1
            ELSE 0
          END
          ELSE 0
        END
        ELSE CASE
          WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
          AND xoom_team.xoom_ntid IS NULL
          AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
          AND r.rule_id NOT IN (467812)
          AND (
            CASE
              WHEN e2.rule_id IS NOT NULL
              OR res_dis.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND a.action_type <> 1354
            AND SAFE_CAST(
              CASE
                WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                         ,
                      a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            AND a.str_match_txn_cnt >= 30
          ) THEN 1
          ELSE 0
        END
      END
      ELSE CASE
        WHEN LOWER(r.crs_team) IN (
          'gfr_omni_gms'    ,
          'gfr_omni_nextgen',
          'gfr_omni'        ,
          'goal_nextgen'    ,
          'goal_gms'
        ) THEN CASE
          WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
            WHEN CASE
              WHEN e2.rule_id IS NOT NULL
              OR res_dis.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND a.action_type <> 1354
            AND (
              a.str_cg_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a.str_cg_txn_wamt > 0 THEN CAST(
                    a.str_cg_brm_bad_wamt / a.str_cg_txn_wamt AS STRING
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
              OR a.str_cg_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                             ,
                        a.incre_str_decled_bad_amt * NUMERIC '1.000' / a.incre_str_decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
            ) THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r.rule_mo)) = 'CC' THEN CASE
            WHEN CASE
              WHEN e2.rule_id IS NOT NULL
              OR res_dis.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND a.action_type <> 1354
            AND (
              a.str_match_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                         ,
                        a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
              OR a.str_match_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                             ,
                        a.incre_str_decled_bad_amt * NUMERIC '1.000' / a.incre_str_decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
            ) THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
            WHEN CASE
              WHEN e2.rule_id IS NOT NULL
              OR res_dis.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND a.action_type <> 1354
            AND (
              a.str_match_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                         ,
                        a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
            ) THEN 1
            ELSE 0
          END
          ELSE 0
        END
        ELSE CASE
          WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
          AND xoom_team.xoom_ntid IS NULL
          AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
          AND r.rule_id NOT IN (467812)
          AND (
            CASE
              WHEN e2.rule_id IS NOT NULL
              OR res_dis.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND a.action_type <> 1354
            AND SAFE_CAST(
              CASE
                WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                         ,
                      a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
            AND a.str_match_txn_cnt >= 30
          ) THEN 1
          ELSE 0
        END
      END
    END > 0 THEN 1
    ELSE 0
  END AS poor_accuracy,
  CASE
    WHEN a.fire_vol < 10 * r.observing_window
    AND (
      SAFE_CAST(
        CASE
          WHEN a.cg_txn_wamt > 0 THEN CAST(
            a.cg_brm_bad_wamt / a.cg_txn_wamt AS STRING
          )
          ELSE 'NA'
        END AS BIGNUMERIC
      ) < NUMERIC '0.1'
      OR RTRIM(
        CASE
          WHEN a.cg_txn_wamt > 0 THEN CAST(
            a.cg_brm_bad_wamt / a.cg_txn_wamt AS STRING
          )
          ELSE 'NA'
        END
      ) = 'NA'
    )
    AND CASE
      WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
      ELSE 0
    END = 0
    AND CASE
      WHEN e.rule_id IS NOT NULL THEN 1
      ELSE 0
    END = 0 THEN 1
    ELSE 0
  END AS sub_low_firing_all_firing,
  CASE
    WHEN CASE
      WHEN e2.rule_id IS NOT NULL
      OR res_dis.rule_id IS NOT NULL THEN 1
      ELSE 0
    END = 0
    AND a.incre_rule_fire_vol < 5 * r.observing_window
    AND a.action_type <> 1354
    AND CASE
      WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
      ELSE 0
    END = 0
    AND CASE
      WHEN e.rule_id IS NOT NULL THEN 1
      ELSE 0
    END = 0 THEN 1
    ELSE 0
  END AS sub_low_firing_incre_firing,
  CASE
    WHEN (
      r.crs_team LIKE 'gfr_goal_ato%'
      OR r.crs_team LIKE 'gfr_goal_ach%'
      OR r.crs_team LIKE 'gfr_goal_cc%'
      OR r.crs_team LIKE 'gfr_goal_col%'
      OR r.crs_team LIKE 'gfr_goal_auto%'
      OR LOWER(r.crs_team) LIKE ANY (
        'goal_ato'  ,
        'goal_ach'  ,
        'goal_addfi',
        'goal_cc'   ,
        'goal_col'  ,
        'goal_auto'
      )
    )
    AND (
      UPPER(r.tags) LIKE '%AUTOSOLUTION%'
      AND UPPER(r.tags) LIKE '%HYPERNET%'
    )
    AND (
      UPPER(r.tags) NOT LIKE '%MERCHANT%'
      AND UPPER(r.tags) NOT LIKE '%CLUSTER%'
    )
    AND a.incre_str_fire_vol < r.observing_window THEN 1
    WHEN UPPER(r.tags) LIKE '%AUTOSOLUTION%'
    AND UPPER(r.tags) LIKE '%HYPERNET MERCHANT%'
    AND a.incre_str_fire_vol < 1 * r.observing_window THEN 1
    ELSE 0
  END AS automated_low_firing_cnt,
  CASE
    WHEN (
      r.crs_team LIKE 'gfr_goal_ato%'
      OR r.crs_team LIKE 'gfr_goal_ach%'
      OR r.crs_team LIKE 'gfr_goal_cc%'
      OR r.crs_team LIKE 'gfr_goal_col%'
      OR r.crs_team LIKE 'gfr_goal_auto%'
      OR LOWER(r.crs_team) LIKE ANY (
        'goal_ato'  ,
        'goal_ach'  ,
        'goal_addfi',
        'goal_cc'   ,
        'goal_col'  ,
        'goal_auto'
      )
    )
    AND (
      UPPER(r.tags) LIKE '%AUTOSOLUTION%'
      AND UPPER(r.tags) LIKE '%HYPERNET%'
    )
    AND (
      UPPER(r.tags) NOT LIKE '%MERCHANT%'
      AND UPPER(r.tags) NOT LIKE '%CLUSTER%'
    ) THEN CASE
      WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
        WHEN SAFE_CAST(
          CASE
            WHEN a.cg_txn_wamt > 0 THEN CAST(
              a.cg_brm_bad_wamt / a.cg_txn_wamt AS STRING
            )
            ELSE 'NA'
          END AS BIGNUMERIC
        ) < NUMERIC '0.10'
        AND a.cg_txn_cnt >= 30 THEN 1
        ELSE 0
      END
      WHEN UPPER(RTRIM(r.rule_mo)) <> 'ATO' THEN CASE
        WHEN SAFE_CAST(
          CASE
            WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
              REGEXP_REPLACE(
                FORMAT(
                  '%6.3f'                                                 ,
                  a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                )                   ,
                r'^( *?)(-)?0(\..*)',
                r'\2\3 \1'
              )                     ,
              r'^( *?)(-)?(\d*\..*)',
              r'\2\3\1'
            )
            ELSE 'NA'
          END AS BIGNUMERIC
        ) < NUMERIC '0.10'
        AND a.match_txn_cnt >= 30 THEN 1
        ELSE 0
      END
      ELSE 0
    END
    WHEN UPPER(r.tags) LIKE '%AUTOSOLUTION%'
    AND UPPER(r.tags) LIKE '%HYPERNET MERCHANT%' THEN CASE
      WHEN CASE
        WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
        ELSE 0
      END = 1 THEN 0
      WHEN excl_accuracy.rule_id IS NOT NULL THEN 0
      WHEN st.is_strategy = 1
      AND (
        UPPER(r.rule_name) LIKE '%FALLBACK%'
        OR UPPER(r.tags) LIKE '%SAFETY%NET%'
      ) THEN CASE
        WHEN LOWER(r.crs_team) IN (
          'gfr_omni_gms'    ,
          'gfr_omni_nextgen',
          'gfr_omni'        ,
          'goal_nextgen'    ,
          'goal_gms'
        ) THEN CASE
          WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
            WHEN a.cg_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a.cg_txn_wamt > 0 THEN CAST(
                  a.cg_brm_bad_wamt / a.cg_txn_wamt AS STRING
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            OR a.cg_txn_cnt < 30
            AND SAFE_CAST(
              CASE
                WHEN a.decled_amt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                         ,
                      a.decled_bad_amt * NUMERIC '1.000' / a.decled_amt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10' THEN 1
            ELSE 0
          END
          WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN CASE
            WHEN a.match_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                 ,
                      a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10' THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r.rule_mo)) = 'CC' THEN CASE
            WHEN a.match_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                 ,
                      a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            OR a.match_txn_cnt < 30
            AND SAFE_CAST(
              CASE
                WHEN a.decled_amt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                         ,
                      a.decled_bad_amt * NUMERIC '1.000' / a.decled_amt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10' THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
            WHEN a.match_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                 ,
                      a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10' THEN 1
            ELSE 0
          END
          ELSE 0
        END
        WHEN r.rule_mo IN ('Collusion', 'UBSM')
        AND xoom_team.xoom_ntid IS NULL THEN CASE
          WHEN SAFE_CAST(
            CASE
              WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                 ,
                    a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.10'
          AND a.match_txn_cnt >= 30 THEN 1
          ELSE 0
        END
        ELSE CASE
          WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
          AND xoom_team.xoom_ntid IS NULL
          AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
          AND r.rule_id NOT IN (467812)
          AND (
            SAFE_CAST(
              CASE
                WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                 ,
                      a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            AND a.match_txn_cnt >= 30
          ) THEN 1
          ELSE 0
        END
      END
      ELSE CASE
        WHEN LOWER(r.crs_team) IN (
          'gfr_omni_gms'    ,
          'gfr_omni_nextgen',
          'gfr_omni'        ,
          'goal_nextgen'    ,
          'goal_gms'
        ) THEN CASE
          WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
            WHEN a.cg_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a.cg_txn_wamt > 0 THEN CAST(
                  a.cg_brm_bad_wamt / a.cg_txn_wamt AS STRING
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
            OR a.cg_txn_cnt < 30
            AND SAFE_CAST(
              CASE
                WHEN a.decled_amt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                         ,
                      a.decled_bad_amt * NUMERIC '1.000' / a.decled_amt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15' THEN 1
            ELSE 0
          END
          WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN CASE
            WHEN a.match_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                 ,
                      a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15' THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r.rule_mo)) = 'CC' THEN CASE
            WHEN a.match_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                 ,
                      a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
            OR a.match_txn_cnt < 30
            AND SAFE_CAST(
              CASE
                WHEN a.decled_amt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                         ,
                      a.decled_bad_amt * NUMERIC '1.000' / a.decled_amt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15' THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
            WHEN a.match_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                 ,
                      a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15' THEN 1
            ELSE 0
          END
          ELSE 0
        END
        WHEN r.rule_mo IN ('Collusion', 'UBSM')
        AND xoom_team.xoom_ntid IS NULL THEN CASE
          WHEN SAFE_CAST(
            CASE
              WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                 ,
                    a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.15'
          AND a.match_txn_cnt >= 30 THEN 1
          ELSE 0
        END
        ELSE CASE
          WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
          AND xoom_team.xoom_ntid IS NULL
          AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
          AND r.rule_id NOT IN (467812)
          AND (
            SAFE_CAST(
              CASE
                WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                 ,
                      a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
            AND a.match_txn_cnt >= 30
          ) THEN 1
          ELSE 0
        END
      END
    END
    ELSE 0
  END AS sub_automated_poor_accuracy_bad_rate,
  CASE
    WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN 0
    WHEN (
      r.crs_team LIKE 'gfr_goal_ato%'
      OR r.crs_team LIKE 'gfr_goal_ach%'
      OR r.crs_team LIKE 'gfr_goal_cc%'
      OR r.crs_team LIKE 'gfr_goal_col%'
      OR r.crs_team LIKE 'gfr_goal_auto%'
      OR LOWER(r.crs_team) LIKE ANY (
        'goal_ato'  ,
        'goal_ach'  ,
        'goal_addfi',
        'goal_cc'   ,
        'goal_col'  ,
        'goal_auto'
      )
    )
    AND (
      UPPER(r.tags) LIKE '%AUTOSOLUTION%'
      AND UPPER(r.tags) LIKE '%HYPERNET%'
    )
    AND (
      UPPER(r.tags) NOT LIKE '%MERCHANT%'
      AND UPPER(r.tags) NOT LIKE '%CLUSTER%'
    ) THEN CASE
      WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
        WHEN CASE
          WHEN e2.rule_id IS NOT NULL
          OR res_dis.rule_id IS NOT NULL THEN 1
          ELSE 0
        END = 0
        AND a.action_type <> 1354
        AND SAFE_CAST(
          CASE
            WHEN a.str_cg_txn_wamt > 0 THEN CAST(
              a.str_cg_brm_bad_wamt / a.str_cg_txn_wamt AS STRING
            )
            ELSE 'NA'
          END AS BIGNUMERIC
        ) < NUMERIC '0.10'
        AND a.str_cg_txn_cnt >= 30 THEN 0
        ELSE 0
      END
      WHEN UPPER(RTRIM(r.rule_mo)) <> 'ATO' THEN CASE
        WHEN CASE
          WHEN e2.rule_id IS NOT NULL
          OR res_dis.rule_id IS NOT NULL THEN 1
          ELSE 0
        END = 0
        AND a.action_type <> 1354
        AND SAFE_CAST(
          CASE
            WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
              REGEXP_REPLACE(
                FORMAT(
                  '%6.3f'                                                         ,
                  a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                )                   ,
                r'^( *?)(-)?0(\..*)',
                r'\2\3 \1'
              )                     ,
              r'^( *?)(-)?(\d*\..*)',
              r'\2\3\1'
            )
            ELSE 'NA'
          END AS BIGNUMERIC
        ) < NUMERIC '0.10'
        AND a.str_match_txn_cnt >= 30 THEN 0
        ELSE 0
      END
      ELSE 0
    END
    WHEN UPPER(r.tags) LIKE '%AUTOSOLUTION%'
    AND UPPER(r.tags) LIKE '%HYPERNET MERCHANT%' THEN CASE
      WHEN CASE
        WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
        ELSE 0
      END = 1 THEN 0
      WHEN excl_accuracy.rule_id IS NOT NULL THEN 0
      WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN 0
      WHEN st.is_strategy = 1
      AND (
        UPPER(r.rule_name) LIKE '%FALLBACK%'
        OR UPPER(r.tags) LIKE '%SAFETY%NET%'
      ) THEN CASE
        WHEN LOWER(r.crs_team) IN (
          'gfr_omni_gms'    ,
          'gfr_omni_nextgen',
          'gfr_omni'        ,
          'goal_nextgen'    ,
          'goal_gms'
        ) THEN CASE
          WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
            WHEN CASE
              WHEN e2.rule_id IS NOT NULL
              OR res_dis.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND a.action_type <> 1354
            AND (
              a.str_cg_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a.str_cg_txn_wamt > 0 THEN CAST(
                    a.str_cg_brm_bad_wamt / a.str_cg_txn_wamt AS STRING
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              OR a.str_cg_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                             ,
                        a.incre_str_decled_bad_amt * NUMERIC '1.000' / a.incre_str_decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
            ) THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r.rule_mo)) = 'CC' THEN CASE
            WHEN CASE
              WHEN e2.rule_id IS NOT NULL
              OR res_dis.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND a.action_type <> 1354
            AND (
              a.str_match_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                         ,
                        a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              OR a.str_match_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                             ,
                        a.incre_str_decled_bad_amt * NUMERIC '1.000' / a.incre_str_decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
            ) THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
            WHEN CASE
              WHEN e2.rule_id IS NOT NULL
              OR res_dis.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND a.action_type <> 1354
            AND (
              a.str_match_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                         ,
                        a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
            ) THEN 1
            ELSE 0
          END
          ELSE 0
        END
        ELSE CASE
          WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
          AND xoom_team.xoom_ntid IS NULL
          AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
          AND r.rule_id NOT IN (467812)
          AND (
            CASE
              WHEN e2.rule_id IS NOT NULL
              OR res_dis.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND a.action_type <> 1354
            AND SAFE_CAST(
              CASE
                WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                         ,
                      a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            AND a.str_match_txn_cnt >= 30
          ) THEN 1
          ELSE 0
        END
      END
      ELSE CASE
        WHEN LOWER(r.crs_team) IN (
          'gfr_omni_gms'    ,
          'gfr_omni_nextgen',
          'gfr_omni'        ,
          'goal_nextgen'    ,
          'goal_gms'
        ) THEN CASE
          WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
            WHEN CASE
              WHEN e2.rule_id IS NOT NULL
              OR res_dis.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND a.action_type <> 1354
            AND (
              a.str_cg_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a.str_cg_txn_wamt > 0 THEN CAST(
                    a.str_cg_brm_bad_wamt / a.str_cg_txn_wamt AS STRING
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
              OR a.str_cg_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                             ,
                        a.incre_str_decled_bad_amt * NUMERIC '1.000' / a.incre_str_decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
            ) THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r.rule_mo)) = 'CC' THEN CASE
            WHEN CASE
              WHEN e2.rule_id IS NOT NULL
              OR res_dis.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND a.action_type <> 1354
            AND (
              a.str_match_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                         ,
                        a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
              OR a.str_match_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                             ,
                        a.incre_str_decled_bad_amt * NUMERIC '1.000' / a.incre_str_decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
            ) THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
            WHEN CASE
              WHEN e2.rule_id IS NOT NULL
              OR res_dis.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND a.action_type <> 1354
            AND (
              a.str_match_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                         ,
                        a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
            ) THEN 1
            ELSE 0
          END
          ELSE 0
        END
        ELSE CASE
          WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
          AND xoom_team.xoom_ntid IS NULL
          AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
          AND r.rule_id NOT IN (467812)
          AND (
            CASE
              WHEN e2.rule_id IS NOT NULL
              OR res_dis.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND a.action_type <> 1354
            AND SAFE_CAST(
              CASE
                WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                         ,
                      a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
            AND a.str_match_txn_cnt >= 30
          ) THEN 1
          ELSE 0
        END
      END
    END
    ELSE 0
  END AS sub_automated_poor_accuracy_incre_str_bad_rate,
  CASE
    WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN 0
    WHEN (
      r.crs_team LIKE 'gfr_goal_ato%'
      OR r.crs_team LIKE 'gfr_goal_ach%'
      OR r.crs_team LIKE 'gfr_goal_cc%'
      OR r.crs_team LIKE 'gfr_goal_col%'
      OR r.crs_team LIKE 'gfr_goal_auto%'
      OR LOWER(r.crs_team) LIKE ANY (
        'goal_ato'  ,
        'goal_ach'  ,
        'goal_addfi',
        'goal_cc'   ,
        'goal_col'  ,
        'goal_auto'
      )
    )
    AND (
      UPPER(r.tags) LIKE '%AUTOSOLUTION%'
      AND UPPER(r.tags) LIKE '%HYPERNET%'
    )
    AND (
      UPPER(r.tags) NOT LIKE '%MERCHANT%'
      AND UPPER(r.tags) NOT LIKE '%CLUSTER%'
    ) THEN CASE
      WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
        WHEN CASE
          WHEN e2.rule_id IS NOT NULL
          OR res_dis.rule_id IS NOT NULL THEN 1
          ELSE 0
        END = 0
        AND a.action_type <> 1354
        AND SAFE_CAST(
          CASE
            WHEN a.actn_cg_txn_wamt > 0 THEN CAST(
              a.actn_cg_brm_bad_wamt / a.actn_cg_txn_wamt AS STRING
            )
            ELSE 'NA'
          END AS BIGNUMERIC
        ) < NUMERIC '0.10'
        AND a.actn_cg_txn_cnt >= 30 THEN 1
        ELSE 0
      END
      WHEN UPPER(RTRIM(r.rule_mo)) <> 'ATO' THEN CASE
        WHEN CASE
          WHEN e2.rule_id IS NOT NULL
          OR res_dis.rule_id IS NOT NULL THEN 1
          ELSE 0
        END = 0
        AND a.action_type <> 1354
        AND SAFE_CAST(
          CASE
            WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
              REGEXP_REPLACE(
                FORMAT(
                  '%6.3f'                                                           ,
                  a.actn_match_bad_wamt_adj * NUMERIC '1.000' / a.actn_match_txn_wamt
                )                   ,
                r'^( *?)(-)?0(\..*)',
                r'\2\3 \1'
              )                     ,
              r'^( *?)(-)?(\d*\..*)',
              r'\2\3\1'
            )
            ELSE 'NA'
          END AS BIGNUMERIC
        ) < NUMERIC '0.10'
        AND a.actn_match_txn_cnt >= 30 THEN 1
        ELSE 0
      END
      ELSE 0
    END
    WHEN UPPER(r.tags) LIKE '%AUTOSOLUTION%'
    AND UPPER(r.tags) LIKE '%HYPERNET MERCHANT%' THEN CASE
      WHEN CASE
        WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
        ELSE 0
      END = 1 THEN 0
      WHEN excl_accuracy.rule_id IS NOT NULL THEN 0
      WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN 0
      WHEN st.is_strategy = 1
      AND (
        UPPER(r.rule_name) LIKE '%FALLBACK%'
        OR UPPER(r.tags) LIKE '%SAFETY%NET%'
      ) THEN CASE
        WHEN LOWER(r.crs_team) IN (
          'gfr_omni_gms'    ,
          'gfr_omni_nextgen',
          'gfr_omni'        ,
          'goal_nextgen'    ,
          'goal_gms'
        ) THEN CASE
          WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
            WHEN CASE
              WHEN e2.rule_id IS NOT NULL
              OR res_dis.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND a.action_type <> 1354
            AND (
              a.actn_cg_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a.actn_cg_txn_wamt > 0 THEN CAST(
                    a.actn_cg_brm_bad_wamt / a.actn_cg_txn_wamt AS STRING
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              OR a.actn_cg_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                               ,
                        a.incre_actn_decled_bad_amt * NUMERIC '1.000' / a.incre_actn_decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
            ) THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
            WHEN CASE
              WHEN e2.rule_id IS NOT NULL
              OR res_dis.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND a.action_type <> 1354
            AND (
              a.actn_match_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                           ,
                        a.actn_match_bad_wamt_adj * NUMERIC '1.000' / a.actn_match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              OR a.actn_match_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                               ,
                        a.incre_actn_decled_bad_amt * NUMERIC '1.000' / a.incre_actn_decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
            ) THEN 1
            ELSE 0
          END
          ELSE 0
        END
        ELSE CASE
          WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
          AND xoom_team.xoom_ntid IS NULL
          AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
          AND r.rule_id NOT IN (467812)
          AND (
            CASE
              WHEN e2.rule_id IS NOT NULL
              OR res_dis.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND a.action_type <> 1354
            AND SAFE_CAST(
              CASE
                WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                           ,
                      a.actn_match_bad_wamt_adj * NUMERIC '1.000' / a.actn_match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            AND a.actn_match_txn_cnt >= 30
          ) THEN 1
          ELSE 0
        END
      END
      ELSE CASE
        WHEN LOWER(r.crs_team) IN (
          'gfr_omni_gms'    ,
          'gfr_omni_nextgen',
          'gfr_omni'        ,
          'goal_nextgen'    ,
          'goal_gms'
        ) THEN CASE
          WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
            WHEN CASE
              WHEN e2.rule_id IS NOT NULL
              OR res_dis.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND a.action_type <> 1354
            AND (
              a.actn_cg_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a.actn_cg_txn_wamt > 0 THEN CAST(
                    a.actn_cg_brm_bad_wamt / a.actn_cg_txn_wamt AS STRING
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
              OR a.actn_cg_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                               ,
                        a.incre_actn_decled_bad_amt * NUMERIC '1.000' / a.incre_actn_decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
            ) THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
            WHEN CASE
              WHEN e2.rule_id IS NOT NULL
              OR res_dis.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND a.action_type <> 1354
            AND (
              a.actn_match_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                           ,
                        a.actn_match_bad_wamt_adj * NUMERIC '1.000' / a.actn_match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
              OR a.actn_match_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                               ,
                        a.incre_actn_decled_bad_amt * NUMERIC '1.000' / a.incre_actn_decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
            ) THEN 1
            ELSE 0
          END
          ELSE 0
        END
        ELSE CASE
          WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
          AND xoom_team.xoom_ntid IS NULL
          AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
          AND r.rule_id NOT IN (467812)
          AND (
            CASE
              WHEN e2.rule_id IS NOT NULL
              OR res_dis.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND a.action_type <> 1354
            AND SAFE_CAST(
              CASE
                WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                           ,
                      a.actn_match_bad_wamt_adj * NUMERIC '1.000' / a.actn_match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
            AND a.actn_match_txn_cnt >= 30
          ) THEN 1
          ELSE 0
        END
      END
    END
    ELSE 0
  END AS sub_automated_poor_accuracy_incre_actn_bad_rate,
  CASE
    WHEN CASE
      WHEN (
        r.crs_team LIKE 'gfr_goal_ato%'
        OR r.crs_team LIKE 'gfr_goal_ach%'
        OR r.crs_team LIKE 'gfr_goal_cc%'
        OR r.crs_team LIKE 'gfr_goal_col%'
        OR r.crs_team LIKE 'gfr_goal_auto%'
        OR LOWER(r.crs_team) LIKE ANY (
          'goal_ato'  ,
          'goal_ach'  ,
          'goal_addfi',
          'goal_cc'   ,
          'goal_col'  ,
          'goal_auto'
        )
      )
      AND (
        UPPER(r.tags) LIKE '%AUTOSOLUTION%'
        AND UPPER(r.tags) LIKE '%HYPERNET%'
      )
      AND (
        UPPER(r.tags) NOT LIKE '%MERCHANT%'
        AND UPPER(r.tags) NOT LIKE '%CLUSTER%'
      ) THEN CASE
        WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
          WHEN SAFE_CAST(
            CASE
              WHEN a.cg_txn_wamt > 0 THEN CAST(
                a.cg_brm_bad_wamt / a.cg_txn_wamt AS STRING
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.10'
          AND a.cg_txn_cnt >= 30 THEN 1
          ELSE 0
        END
        WHEN UPPER(RTRIM(r.rule_mo)) <> 'ATO' THEN CASE
          WHEN SAFE_CAST(
            CASE
              WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                 ,
                    a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.10'
          AND a.match_txn_cnt >= 30 THEN 1
          ELSE 0
        END
        ELSE 0
      END
      WHEN UPPER(r.tags) LIKE '%AUTOSOLUTION%'
      AND UPPER(r.tags) LIKE '%HYPERNET MERCHANT%' THEN CASE
        WHEN CASE
          WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
          ELSE 0
        END = 1 THEN 0
        WHEN excl_accuracy.rule_id IS NOT NULL THEN 0
        WHEN st.is_strategy = 1
        AND (
          UPPER(r.rule_name) LIKE '%FALLBACK%'
          OR UPPER(r.tags) LIKE '%SAFETY%NET%'
        ) THEN CASE
          WHEN r.crs_team IN (
            'gfr_omni_gms'    ,
            'gfr_omni_nextgen',
            'gfr_omni'        ,
            'goal_nextgen'    ,
            'goal_gms'
          ) THEN CASE
            WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
              WHEN a.cg_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a.cg_txn_wamt > 0 THEN CAST(
                    a.cg_brm_bad_wamt / a.cg_txn_wamt AS STRING
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              OR a.cg_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a.decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                         ,
                        a.decled_bad_amt * NUMERIC '1.000' / a.decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10' THEN 1
              ELSE 0
            END
            WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN CASE
              WHEN a.match_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                 ,
                        a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10' THEN 1
              ELSE 0
            END
            WHEN UPPER(RTRIM(r.rule_mo)) = 'CC' THEN CASE
              WHEN a.match_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                 ,
                        a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              OR a.match_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a.decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                         ,
                        a.decled_bad_amt * NUMERIC '1.000' / a.decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10' THEN 1
              ELSE 0
            END
            WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
              WHEN a.match_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                 ,
                        a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10' THEN 1
              ELSE 0
            END
            ELSE 0
          END
          WHEN r.rule_mo IN ('Collusion', 'UBSM')
          AND xoom_team.xoom_ntid IS NULL THEN CASE
            WHEN SAFE_CAST(
              CASE
                WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                 ,
                      a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            AND a.match_txn_cnt >= 30 THEN 1
            ELSE 0
          END
          ELSE CASE
            WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
            AND xoom_team.xoom_ntid IS NULL
            AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
            AND r.rule_id NOT IN (467812)
            AND (
              SAFE_CAST(
                CASE
                  WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                 ,
                        a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              AND a.match_txn_cnt >= 30
            ) THEN 1
            ELSE 0
          END
        END
        ELSE CASE
          WHEN r.crs_team IN (
            'gfr_omni_gms'    ,
            'gfr_omni_nextgen',
            'gfr_omni'        ,
            'goal_nextgen'    ,
            'goal_gms'
          ) THEN CASE
            WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
              WHEN a.cg_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a.cg_txn_wamt > 0 THEN CAST(
                    a.cg_brm_bad_wamt / a.cg_txn_wamt AS STRING
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
              OR a.cg_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a.decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                         ,
                        a.decled_bad_amt * NUMERIC '1.000' / a.decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15' THEN 1
              ELSE 0
            END
            WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN CASE
              WHEN a.match_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                 ,
                        a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15' THEN 1
              ELSE 0
            END
            WHEN UPPER(RTRIM(r.rule_mo)) = 'CC' THEN CASE
              WHEN a.match_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                 ,
                        a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
              OR a.match_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a.decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                         ,
                        a.decled_bad_amt * NUMERIC '1.000' / a.decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15' THEN 1
              ELSE 0
            END
            WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
              WHEN a.match_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                 ,
                        a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15' THEN 1
              ELSE 0
            END
            ELSE 0
          END
          WHEN r.rule_mo IN ('Collusion', 'UBSM')
          AND xoom_team.xoom_ntid IS NULL THEN CASE
            WHEN SAFE_CAST(
              CASE
                WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                 ,
                      a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
            AND a.match_txn_cnt >= 30 THEN 1
            ELSE 0
          END
          ELSE CASE
            WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
            AND xoom_team.xoom_ntid IS NULL
            AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
            AND r.rule_id NOT IN (467812)
            AND (
              SAFE_CAST(
                CASE
                  WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                 ,
                        a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
              AND a.match_txn_cnt >= 30
            ) THEN 1
            ELSE 0
          END
        END
      END
      ELSE 0
    END + CASE
      WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN 0
      WHEN (
        r.crs_team LIKE 'gfr_goal_ato%'
        OR r.crs_team LIKE 'gfr_goal_ach%'
        OR r.crs_team LIKE 'gfr_goal_cc%'
        OR r.crs_team LIKE 'gfr_goal_col%'
        OR r.crs_team LIKE 'gfr_goal_auto%'
        OR LOWER(r.crs_team) LIKE ANY (
          'goal_ato'  ,
          'goal_ach'  ,
          'goal_addfi',
          'goal_cc'   ,
          'goal_col'  ,
          'goal_auto'
        )
      )
      AND (
        UPPER(r.tags) LIKE '%AUTOSOLUTION%'
        AND UPPER(r.tags) LIKE '%HYPERNET%'
      )
      AND (
        UPPER(r.tags) NOT LIKE '%MERCHANT%'
        AND UPPER(r.tags) NOT LIKE '%CLUSTER%'
      ) THEN CASE
        WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
          WHEN CASE
            WHEN e2.rule_id IS NOT NULL
            OR res_dis.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND a.action_type <> 1354
          AND SAFE_CAST(
            CASE
              WHEN a.str_cg_txn_wamt > 0 THEN CAST(
                a.str_cg_brm_bad_wamt / a.str_cg_txn_wamt AS STRING
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.10'
          AND a.str_cg_txn_cnt >= 30 THEN 0
          ELSE 0
        END
        WHEN UPPER(RTRIM(r.rule_mo)) <> 'ATO' THEN CASE
          WHEN CASE
            WHEN e2.rule_id IS NOT NULL
            OR res_dis.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND a.action_type <> 1354
          AND SAFE_CAST(
            CASE
              WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                         ,
                    a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.10'
          AND a.str_match_txn_cnt >= 30 THEN 0
          ELSE 0
        END
        ELSE 0
      END
      WHEN UPPER(r.tags) LIKE '%AUTOSOLUTION%'
      AND UPPER(r.tags) LIKE '%HYPERNET MERCHANT%' THEN CASE
        WHEN CASE
          WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
          ELSE 0
        END = 1 THEN 0
        WHEN excl_accuracy.rule_id IS NOT NULL THEN 0
        WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN 0
        WHEN st.is_strategy = 1
        AND (
          UPPER(r.rule_name) LIKE '%FALLBACK%'
          OR UPPER(r.tags) LIKE '%SAFETY%NET%'
        ) THEN CASE
          WHEN r.crs_team IN (
            'gfr_omni_gms'    ,
            'gfr_omni_nextgen',
            'gfr_omni'        ,
            'goal_nextgen'    ,
            'goal_gms'
          ) THEN CASE
            WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
              WHEN CASE
                WHEN e2.rule_id IS NOT NULL
                OR res_dis.rule_id IS NOT NULL THEN 1
                ELSE 0
              END = 0
              AND a.action_type <> 1354
              AND (
                a.str_cg_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.str_cg_txn_wamt > 0 THEN CAST(
                      a.str_cg_brm_bad_wamt / a.str_cg_txn_wamt AS STRING
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                OR a.str_cg_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                             ,
                          a.incre_str_decled_bad_amt * NUMERIC '1.000' / a.incre_str_decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
              ) THEN 1
              ELSE 0
            END
            WHEN UPPER(RTRIM(r.rule_mo)) = 'CC' THEN CASE
              WHEN CASE
                WHEN e2.rule_id IS NOT NULL
                OR res_dis.rule_id IS NOT NULL THEN 1
                ELSE 0
              END = 0
              AND a.action_type <> 1354
              AND (
                a.str_match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                         ,
                          a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                OR a.str_match_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                             ,
                          a.incre_str_decled_bad_amt * NUMERIC '1.000' / a.incre_str_decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
              ) THEN 1
              ELSE 0
            END
            WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
              WHEN CASE
                WHEN e2.rule_id IS NOT NULL
                OR res_dis.rule_id IS NOT NULL THEN 1
                ELSE 0
              END = 0
              AND a.action_type <> 1354
              AND (
                a.str_match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                         ,
                          a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
              ) THEN 1
              ELSE 0
            END
            ELSE 0
          END
          ELSE CASE
            WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
            AND xoom_team.xoom_ntid IS NULL
            AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
            AND r.rule_id NOT IN (467812)
            AND (
              CASE
                WHEN e2.rule_id IS NOT NULL
                OR res_dis.rule_id IS NOT NULL THEN 1
                ELSE 0
              END = 0
              AND a.action_type <> 1354
              AND SAFE_CAST(
                CASE
                  WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                         ,
                        a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              AND a.str_match_txn_cnt >= 30
            ) THEN 1
            ELSE 0
          END
        END
        ELSE CASE
          WHEN r.crs_team IN (
            'gfr_omni_gms'    ,
            'gfr_omni_nextgen',
            'gfr_omni'        ,
            'goal_nextgen'    ,
            'goal_gms'
          ) THEN CASE
            WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
              WHEN CASE
                WHEN e2.rule_id IS NOT NULL
                OR res_dis.rule_id IS NOT NULL THEN 1
                ELSE 0
              END = 0
              AND a.action_type <> 1354
              AND (
                a.str_cg_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.str_cg_txn_wamt > 0 THEN CAST(
                      a.str_cg_brm_bad_wamt / a.str_cg_txn_wamt AS STRING
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                OR a.str_cg_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                             ,
                          a.incre_str_decled_bad_amt * NUMERIC '1.000' / a.incre_str_decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
              ) THEN 1
              ELSE 0
            END
            WHEN UPPER(RTRIM(r.rule_mo)) = 'CC' THEN CASE
              WHEN CASE
                WHEN e2.rule_id IS NOT NULL
                OR res_dis.rule_id IS NOT NULL THEN 1
                ELSE 0
              END = 0
              AND a.action_type <> 1354
              AND (
                a.str_match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                         ,
                          a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                OR a.str_match_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                             ,
                          a.incre_str_decled_bad_amt * NUMERIC '1.000' / a.incre_str_decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
              ) THEN 1
              ELSE 0
            END
            WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
              WHEN CASE
                WHEN e2.rule_id IS NOT NULL
                OR res_dis.rule_id IS NOT NULL THEN 1
                ELSE 0
              END = 0
              AND a.action_type <> 1354
              AND (
                a.str_match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                         ,
                          a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
              ) THEN 1
              ELSE 0
            END
            ELSE 0
          END
          ELSE CASE
            WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
            AND xoom_team.xoom_ntid IS NULL
            AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
            AND r.rule_id NOT IN (467812)
            AND (
              CASE
                WHEN e2.rule_id IS NOT NULL
                OR res_dis.rule_id IS NOT NULL THEN 1
                ELSE 0
              END = 0
              AND a.action_type <> 1354
              AND SAFE_CAST(
                CASE
                  WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                         ,
                        a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
              AND a.str_match_txn_cnt >= 30
            ) THEN 1
            ELSE 0
          END
        END
      END
      ELSE 0
    END + CASE
      WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN 0
      WHEN (
        r.crs_team LIKE 'gfr_goal_ato%'
        OR r.crs_team LIKE 'gfr_goal_ach%'
        OR r.crs_team LIKE 'gfr_goal_cc%'
        OR r.crs_team LIKE 'gfr_goal_col%'
        OR r.crs_team LIKE 'gfr_goal_auto%'
        OR LOWER(r.crs_team) LIKE ANY (
          'goal_ato'  ,
          'goal_ach'  ,
          'goal_addfi',
          'goal_cc'   ,
          'goal_col'  ,
          'goal_auto'
        )
      )
      AND (
        UPPER(r.tags) LIKE '%AUTOSOLUTION%'
        AND UPPER(r.tags) LIKE '%HYPERNET%'
      )
      AND (
        UPPER(r.tags) NOT LIKE '%MERCHANT%'
        AND UPPER(r.tags) NOT LIKE '%CLUSTER%'
      ) THEN CASE
        WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
          WHEN CASE
            WHEN e2.rule_id IS NOT NULL
            OR res_dis.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND a.action_type <> 1354
          AND SAFE_CAST(
            CASE
              WHEN a.actn_cg_txn_wamt > 0 THEN CAST(
                a.actn_cg_brm_bad_wamt / a.actn_cg_txn_wamt AS STRING
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.10'
          AND a.actn_cg_txn_cnt >= 30 THEN 1
          ELSE 0
        END
        WHEN UPPER(RTRIM(r.rule_mo)) <> 'ATO' THEN CASE
          WHEN CASE
            WHEN e2.rule_id IS NOT NULL
            OR res_dis.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND a.action_type <> 1354
          AND SAFE_CAST(
            CASE
              WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                           ,
                    a.actn_match_bad_wamt_adj * NUMERIC '1.000' / a.actn_match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.10'
          AND a.actn_match_txn_cnt >= 30 THEN 1
          ELSE 0
        END
        ELSE 0
      END
      WHEN UPPER(r.tags) LIKE '%AUTOSOLUTION%'
      AND UPPER(r.tags) LIKE '%HYPERNET MERCHANT%' THEN CASE
        WHEN CASE
          WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
          ELSE 0
        END = 1 THEN 0
        WHEN excl_accuracy.rule_id IS NOT NULL THEN 0
        WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN 0
        WHEN st.is_strategy = 1
        AND (
          UPPER(r.rule_name) LIKE '%FALLBACK%'
          OR UPPER(r.tags) LIKE '%SAFETY%NET%'
        ) THEN CASE
          WHEN r.crs_team IN (
            'gfr_omni_gms'    ,
            'gfr_omni_nextgen',
            'gfr_omni'        ,
            'goal_nextgen'    ,
            'goal_gms'
          ) THEN CASE
            WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
              WHEN CASE
                WHEN e2.rule_id IS NOT NULL
                OR res_dis.rule_id IS NOT NULL THEN 1
                ELSE 0
              END = 0
              AND a.action_type <> 1354
              AND (
                a.actn_cg_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.actn_cg_txn_wamt > 0 THEN CAST(
                      a.actn_cg_brm_bad_wamt / a.actn_cg_txn_wamt AS STRING
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                OR a.actn_cg_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                               ,
                          a.incre_actn_decled_bad_amt * NUMERIC '1.000' / a.incre_actn_decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
              ) THEN 1
              ELSE 0
            END
            WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
              WHEN CASE
                WHEN e2.rule_id IS NOT NULL
                OR res_dis.rule_id IS NOT NULL THEN 1
                ELSE 0
              END = 0
              AND a.action_type <> 1354
              AND (
                a.actn_match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                           ,
                          a.actn_match_bad_wamt_adj * NUMERIC '1.000' / a.actn_match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                OR a.actn_match_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                               ,
                          a.incre_actn_decled_bad_amt * NUMERIC '1.000' / a.incre_actn_decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
              ) THEN 1
              ELSE 0
            END
            ELSE 0
          END
          ELSE CASE
            WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
            AND xoom_team.xoom_ntid IS NULL
            AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
            AND r.rule_id NOT IN (467812)
            AND (
              CASE
                WHEN e2.rule_id IS NOT NULL
                OR res_dis.rule_id IS NOT NULL THEN 1
                ELSE 0
              END = 0
              AND a.action_type <> 1354
              AND SAFE_CAST(
                CASE
                  WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                           ,
                        a.actn_match_bad_wamt_adj * NUMERIC '1.000' / a.actn_match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              AND a.actn_match_txn_cnt >= 30
            ) THEN 1
            ELSE 0
          END
        END
        ELSE CASE
          WHEN r.crs_team IN (
            'gfr_omni_gms'    ,
            'gfr_omni_nextgen',
            'gfr_omni'        ,
            'goal_nextgen'    ,
            'goal_gms'
          ) THEN CASE
            WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
              WHEN CASE
                WHEN e2.rule_id IS NOT NULL
                OR res_dis.rule_id IS NOT NULL THEN 1
                ELSE 0
              END = 0
              AND a.action_type <> 1354
              AND (
                a.actn_cg_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.actn_cg_txn_wamt > 0 THEN CAST(
                      a.actn_cg_brm_bad_wamt / a.actn_cg_txn_wamt AS STRING
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                OR a.actn_cg_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                               ,
                          a.incre_actn_decled_bad_amt * NUMERIC '1.000' / a.incre_actn_decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
              ) THEN 1
              ELSE 0
            END
            WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
              WHEN CASE
                WHEN e2.rule_id IS NOT NULL
                OR res_dis.rule_id IS NOT NULL THEN 1
                ELSE 0
              END = 0
              AND a.action_type <> 1354
              AND (
                a.actn_match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                           ,
                          a.actn_match_bad_wamt_adj * NUMERIC '1.000' / a.actn_match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                OR a.actn_match_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                               ,
                          a.incre_actn_decled_bad_amt * NUMERIC '1.000' / a.incre_actn_decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
              ) THEN 1
              ELSE 0
            END
            ELSE 0
          END
          ELSE CASE
            WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
            AND xoom_team.xoom_ntid IS NULL
            AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
            AND r.rule_id NOT IN (467812)
            AND (
              CASE
                WHEN e2.rule_id IS NOT NULL
                OR res_dis.rule_id IS NOT NULL THEN 1
                ELSE 0
              END = 0
              AND a.action_type <> 1354
              AND SAFE_CAST(
                CASE
                  WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                           ,
                        a.actn_match_bad_wamt_adj * NUMERIC '1.000' / a.actn_match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
              AND a.actn_match_txn_cnt >= 30
            ) THEN 1
            ELSE 0
          END
        END
      END
      ELSE 0
    END > 0 THEN 1
    ELSE 0
  END AS automated_poor_accuracy,
  0 AS automated_low_firing_amt ,
  CASE
    WHEN (
      r.crs_team LIKE 'gfr_goal_ato%'
      OR r.crs_team LIKE 'gfr_goal_ach%'
      OR r.crs_team LIKE 'gfr_goal_cc%'
      OR r.crs_team LIKE 'gfr_goal_col%'
      OR r.crs_team LIKE 'gfr_goal_auto%'
      OR LOWER(r.crs_team) LIKE ANY (
        'goal_ato'  ,
        'goal_ach'  ,
        'goal_addfi',
        'goal_cc'   ,
        'goal_col'  ,
        'goal_auto'
      )
    )
    AND (
      UPPER(r.tags) LIKE '%AUTOSOLUTION%'
      AND UPPER(r.tags) LIKE '%HYPERNET%'
    )
    AND (
      UPPER(r.tags) NOT LIKE '%MERCHANT%'
      AND UPPER(r.tags) NOT LIKE '%CLUSTER%'
    ) THEN 0
    WHEN UPPER(r.tags) LIKE '%AUTOSOLUTION%'
    AND UPPER(r.tags) LIKE '%HYPERNET MERCHANT%' THEN CASE
      WHEN CASE
        WHEN e_ow.rule_id IS NOT NULL THEN 1
        ELSE 0
      END = 0
      AND SAFE_CAST(
        CASE
          WHEN a.fire_vol > 0 THEN REGEXP_REPLACE(
            REGEXP_REPLACE(
              FORMAT(
                '%6.3f'                                    ,
                a.sum_wh_actn * NUMERIC '1.000' / a.fire_vol
              )                   ,
              r'^( *?)(-)?0(\..*)',
              r'\2\3 \1'
            )                     ,
            r'^( *?)(-)?(\d*\..*)',
            r'\2\3\1'
          )
          ELSE 'NA'
        END AS BIGNUMERIC
      ) >= NUMERIC '0.7'
      AND SAFE_CAST(
        CASE
          WHEN a.whed_amt > 0 THEN REGEXP_REPLACE(
            REGEXP_REPLACE(
              FORMAT(
                '%6.3f'                                    ,
                a.bad_amt_adj * NUMERIC '1.000' / a.whed_amt
              )                   ,
              r'^( *?)(-)?0(\..*)',
              r'\2\3 \1'
            )                     ,
            r'^( *?)(-)?(\d*\..*)',
            r'\2\3\1'
          )
          ELSE 'NA'
        END AS BIGNUMERIC
      ) < NUMERIC '0.1'
      AND a.whed_cnt >= 30
      AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%' THEN 1
      ELSE 0
    END
    ELSE 0
  END AS automated_over_whitelisted,
  CASE
    WHEN 0 = 1
    OR CASE
      WHEN (
        r.crs_team LIKE 'gfr_goal_ato%'
        OR r.crs_team LIKE 'gfr_goal_ach%'
        OR r.crs_team LIKE 'gfr_goal_cc%'
        OR r.crs_team LIKE 'gfr_goal_col%'
        OR r.crs_team LIKE 'gfr_goal_auto%'
        OR LOWER(r.crs_team) LIKE ANY (
          'goal_ato'  ,
          'goal_ach'  ,
          'goal_addfi',
          'goal_cc'   ,
          'goal_col'  ,
          'goal_auto'
        )
      )
      AND (
        UPPER(r.tags) LIKE '%AUTOSOLUTION%'
        AND UPPER(r.tags) LIKE '%HYPERNET%'
      )
      AND (
        UPPER(r.tags) NOT LIKE '%MERCHANT%'
        AND UPPER(r.tags) NOT LIKE '%CLUSTER%'
      )
      AND a.incre_str_fire_vol < r.observing_window THEN 1
      WHEN UPPER(r.tags) LIKE '%AUTOSOLUTION%'
      AND UPPER(r.tags) LIKE '%HYPERNET MERCHANT%'
      AND a.incre_str_fire_vol < 1 * r.observing_window THEN 1
      ELSE 0
    END = 1 THEN 1
    ELSE 0
  END AS automated_low_firing,
  CASE
    WHEN UPPER(r.tags) LIKE '%AUTOSOLUTION%'
    AND UPPER(r.tags) LIKE '%HYPERNET MERCHANT%' THEN CASE
      WHEN CASE
        WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
        ELSE 0
      END = 0
      AND SAFE_CAST(b.ov_pct_1 AS BIGNUMERIC) >= NUMERIC '0.8'
      AND CASE
        WHEN e2.rule_id IS NOT NULL
        OR res_dis.rule_id IS NOT NULL THEN 1
        ELSE 0
      END = 0
      AND a.action_type <> 1354 THEN CASE
        WHEN UPPER(r.tags) LIKE '%AUTOSOLUTION%'
        AND UPPER(r.tags) LIKE '%HYPERNET MERCHANT%'
        AND COALESCE(st_ov1.is_strategy_ov1_raw, 0) = 1 THEN 1
        WHEN UPPER(r.tags) LIKE '%AUTOSOLUTION%'
        AND UPPER(r.tags) LIKE '%HYPERNET MERCHANT%'
        AND COALESCE(st_ov1.is_strategy_ov1_raw, 0) = 0 THEN 0
        ELSE 1
      END
      ELSE 0
    END
    ELSE 0
  END AS automated_high_overlap,
  CASE
    WHEN (
      CASE
        WHEN 0 = 1
        OR CASE
          WHEN (
            r.crs_team LIKE 'gfr_goal_ato%'
            OR r.crs_team LIKE 'gfr_goal_ach%'
            OR r.crs_team LIKE 'gfr_goal_cc%'
            OR r.crs_team LIKE 'gfr_goal_col%'
            OR r.crs_team LIKE 'gfr_goal_auto%'
            OR LOWER(r.crs_team) LIKE ANY (
              'goal_ato'  ,
              'goal_ach'  ,
              'goal_addfi',
              'goal_cc'   ,
              'goal_col'  ,
              'goal_auto'
            )
          )
          AND (
            UPPER(r.tags) LIKE '%AUTOSOLUTION%'
            AND UPPER(r.tags) LIKE '%HYPERNET%'
          )
          AND (
            UPPER(r.tags) NOT LIKE '%MERCHANT%'
            AND UPPER(r.tags) NOT LIKE '%CLUSTER%'
          )
          AND a.incre_str_fire_vol < r.observing_window THEN 1
          WHEN UPPER(r.tags) LIKE '%AUTOSOLUTION%'
          AND UPPER(r.tags) LIKE '%HYPERNET MERCHANT%'
          AND a.incre_str_fire_vol < 1 * r.observing_window THEN 1
          ELSE 0
        END = 1 THEN 1
        ELSE 0
      END = 1
      OR CASE
        WHEN CASE
          WHEN (
            r.crs_team LIKE 'gfr_goal_ato%'
            OR r.crs_team LIKE 'gfr_goal_ach%'
            OR r.crs_team LIKE 'gfr_goal_cc%'
            OR r.crs_team LIKE 'gfr_goal_col%'
            OR r.crs_team LIKE 'gfr_goal_auto%'
            OR LOWER(r.crs_team) LIKE ANY (
              'goal_ato'  ,
              'goal_ach'  ,
              'goal_addfi',
              'goal_cc'   ,
              'goal_col'  ,
              'goal_auto'
            )
          )
          AND (
            UPPER(r.tags) LIKE '%AUTOSOLUTION%'
            AND UPPER(r.tags) LIKE '%HYPERNET%'
          )
          AND (
            UPPER(r.tags) NOT LIKE '%MERCHANT%'
            AND UPPER(r.tags) NOT LIKE '%CLUSTER%'
          ) THEN CASE
            WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
              WHEN SAFE_CAST(
                CASE
                  WHEN a.cg_txn_wamt > 0 THEN CAST(
                    a.cg_brm_bad_wamt / a.cg_txn_wamt AS STRING
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              AND a.cg_txn_cnt >= 30 THEN 1
              ELSE 0
            END
            WHEN UPPER(RTRIM(r.rule_mo)) <> 'ATO' THEN CASE
              WHEN SAFE_CAST(
                CASE
                  WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                 ,
                        a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              AND a.match_txn_cnt >= 30 THEN 1
              ELSE 0
            END
            ELSE 0
          END
          WHEN UPPER(r.tags) LIKE '%AUTOSOLUTION%'
          AND UPPER(r.tags) LIKE '%HYPERNET MERCHANT%' THEN CASE
            WHEN CASE
              WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
              ELSE 0
            END = 1 THEN 0
            WHEN excl_accuracy.rule_id IS NOT NULL THEN 0
            WHEN st.is_strategy = 1
            AND (
              UPPER(r.rule_name) LIKE '%FALLBACK%'
              OR UPPER(r.tags) LIKE '%SAFETY%NET%'
            ) THEN CASE
              WHEN r.crs_team IN (
                'gfr_omni_gms'    ,
                'gfr_omni_nextgen',
                'gfr_omni'        ,
                'goal_nextgen'    ,
                'goal_gms'
              ) THEN CASE
                WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
                  WHEN a.cg_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.cg_txn_wamt > 0 THEN CAST(
                        a.cg_brm_bad_wamt / a.cg_txn_wamt AS STRING
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                  OR a.cg_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                         ,
                            a.decled_bad_amt * NUMERIC '1.000' / a.decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10' THEN 1
                  ELSE 0
                END
                WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN CASE
                  WHEN a.match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                 ,
                            a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10' THEN 1
                  ELSE 0
                END
                WHEN UPPER(RTRIM(r.rule_mo)) = 'CC' THEN CASE
                  WHEN a.match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                 ,
                            a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                  OR a.match_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                         ,
                            a.decled_bad_amt * NUMERIC '1.000' / a.decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10' THEN 1
                  ELSE 0
                END
                WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
                  WHEN a.match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                 ,
                            a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10' THEN 1
                  ELSE 0
                END
                ELSE 0
              END
              WHEN r.rule_mo IN ('Collusion', 'UBSM')
              AND xoom_team.xoom_ntid IS NULL THEN CASE
                WHEN SAFE_CAST(
                  CASE
                    WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                 ,
                          a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                AND a.match_txn_cnt >= 30 THEN 1
                ELSE 0
              END
              ELSE CASE
                WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
                AND xoom_team.xoom_ntid IS NULL
                AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
                AND r.rule_id NOT IN (467812)
                AND (
                  SAFE_CAST(
                    CASE
                      WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                 ,
                            a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                  AND a.match_txn_cnt >= 30
                ) THEN 1
                ELSE 0
              END
            END
            ELSE CASE
              WHEN r.crs_team IN (
                'gfr_omni_gms'    ,
                'gfr_omni_nextgen',
                'gfr_omni'        ,
                'goal_nextgen'    ,
                'goal_gms'
              ) THEN CASE
                WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
                  WHEN a.cg_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.cg_txn_wamt > 0 THEN CAST(
                        a.cg_brm_bad_wamt / a.cg_txn_wamt AS STRING
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                  OR a.cg_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                         ,
                            a.decled_bad_amt * NUMERIC '1.000' / a.decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15' THEN 1
                  ELSE 0
                END
                WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN CASE
                  WHEN a.match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                 ,
                            a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15' THEN 1
                  ELSE 0
                END
                WHEN UPPER(RTRIM(r.rule_mo)) = 'CC' THEN CASE
                  WHEN a.match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                 ,
                            a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                  OR a.match_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                         ,
                            a.decled_bad_amt * NUMERIC '1.000' / a.decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15' THEN 1
                  ELSE 0
                END
                WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
                  WHEN a.match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                 ,
                            a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15' THEN 1
                  ELSE 0
                END
                ELSE 0
              END
              WHEN r.rule_mo IN ('Collusion', 'UBSM')
              AND xoom_team.xoom_ntid IS NULL THEN CASE
                WHEN SAFE_CAST(
                  CASE
                    WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                 ,
                          a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                AND a.match_txn_cnt >= 30 THEN 1
                ELSE 0
              END
              ELSE CASE
                WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
                AND xoom_team.xoom_ntid IS NULL
                AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
                AND r.rule_id NOT IN (467812)
                AND (
                  SAFE_CAST(
                    CASE
                      WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                 ,
                            a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                  AND a.match_txn_cnt >= 30
                ) THEN 1
                ELSE 0
              END
            END
          END
          ELSE 0
        END + CASE
          WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN 0
          WHEN (
            r.crs_team LIKE 'gfr_goal_ato%'
            OR r.crs_team LIKE 'gfr_goal_ach%'
            OR r.crs_team LIKE 'gfr_goal_cc%'
            OR r.crs_team LIKE 'gfr_goal_col%'
            OR r.crs_team LIKE 'gfr_goal_auto%'
            OR LOWER(r.crs_team) LIKE ANY (
              'goal_ato'  ,
              'goal_ach'  ,
              'goal_addfi',
              'goal_cc'   ,
              'goal_col'  ,
              'goal_auto'
            )
          )
          AND (
            UPPER(r.tags) LIKE '%AUTOSOLUTION%'
            AND UPPER(r.tags) LIKE '%HYPERNET%'
          )
          AND (
            UPPER(r.tags) NOT LIKE '%MERCHANT%'
            AND UPPER(r.tags) NOT LIKE '%CLUSTER%'
          ) THEN CASE
            WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
              WHEN CASE
                WHEN e2.rule_id IS NOT NULL
                OR res_dis.rule_id IS NOT NULL THEN 1
                ELSE 0
              END = 0
              AND a.action_type <> 1354
              AND SAFE_CAST(
                CASE
                  WHEN a.str_cg_txn_wamt > 0 THEN CAST(
                    a.str_cg_brm_bad_wamt / a.str_cg_txn_wamt AS STRING
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              AND a.str_cg_txn_cnt >= 30 THEN 0
              ELSE 0
            END
            WHEN UPPER(RTRIM(r.rule_mo)) <> 'ATO' THEN CASE
              WHEN CASE
                WHEN e2.rule_id IS NOT NULL
                OR res_dis.rule_id IS NOT NULL THEN 1
                ELSE 0
              END = 0
              AND a.action_type <> 1354
              AND SAFE_CAST(
                CASE
                  WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                         ,
                        a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              AND a.str_match_txn_cnt >= 30 THEN 0
              ELSE 0
            END
            ELSE 0
          END
          WHEN UPPER(r.tags) LIKE '%AUTOSOLUTION%'
          AND UPPER(r.tags) LIKE '%HYPERNET MERCHANT%' THEN CASE
            WHEN CASE
              WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
              ELSE 0
            END = 1 THEN 0
            WHEN excl_accuracy.rule_id IS NOT NULL THEN 0
            WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN 0
            WHEN st.is_strategy = 1
            AND (
              UPPER(r.rule_name) LIKE '%FALLBACK%'
              OR UPPER(r.tags) LIKE '%SAFETY%NET%'
            ) THEN CASE
              WHEN r.crs_team IN (
                'gfr_omni_gms'    ,
                'gfr_omni_nextgen',
                'gfr_omni'        ,
                'goal_nextgen'    ,
                'goal_gms'
              ) THEN CASE
                WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
                  WHEN CASE
                    WHEN e2.rule_id IS NOT NULL
                    OR res_dis.rule_id IS NOT NULL THEN 1
                    ELSE 0
                  END = 0
                  AND a.action_type <> 1354
                  AND (
                    a.str_cg_txn_cnt >= 30
                    AND SAFE_CAST(
                      CASE
                        WHEN a.str_cg_txn_wamt > 0 THEN CAST(
                          a.str_cg_brm_bad_wamt / a.str_cg_txn_wamt AS STRING
                        )
                        ELSE 'NA'
                      END AS BIGNUMERIC
                    ) < NUMERIC '0.10'
                    OR a.str_cg_txn_cnt < 30
                    AND SAFE_CAST(
                      CASE
                        WHEN a.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                          REGEXP_REPLACE(
                            FORMAT(
                              '%6.3f'                                                             ,
                              a.incre_str_decled_bad_amt * NUMERIC '1.000' / a.incre_str_decled_amt
                            )                   ,
                            r'^( *?)(-)?0(\..*)',
                            r'\2\3 \1'
                          )                     ,
                          r'^( *?)(-)?(\d*\..*)',
                          r'\2\3\1'
                        )
                        ELSE 'NA'
                      END AS BIGNUMERIC
                    ) < NUMERIC '0.10'
                  ) THEN 1
                  ELSE 0
                END
                WHEN UPPER(RTRIM(r.rule_mo)) = 'CC' THEN CASE
                  WHEN CASE
                    WHEN e2.rule_id IS NOT NULL
                    OR res_dis.rule_id IS NOT NULL THEN 1
                    ELSE 0
                  END = 0
                  AND a.action_type <> 1354
                  AND (
                    a.str_match_txn_cnt >= 30
                    AND SAFE_CAST(
                      CASE
                        WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                          REGEXP_REPLACE(
                            FORMAT(
                              '%6.3f'                                                         ,
                              a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                            )                   ,
                            r'^( *?)(-)?0(\..*)',
                            r'\2\3 \1'
                          )                     ,
                          r'^( *?)(-)?(\d*\..*)',
                          r'\2\3\1'
                        )
                        ELSE 'NA'
                      END AS BIGNUMERIC
                    ) < NUMERIC '0.10'
                    OR a.str_match_txn_cnt < 30
                    AND SAFE_CAST(
                      CASE
                        WHEN a.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                          REGEXP_REPLACE(
                            FORMAT(
                              '%6.3f'                                                             ,
                              a.incre_str_decled_bad_amt * NUMERIC '1.000' / a.incre_str_decled_amt
                            )                   ,
                            r'^( *?)(-)?0(\..*)',
                            r'\2\3 \1'
                          )                     ,
                          r'^( *?)(-)?(\d*\..*)',
                          r'\2\3\1'
                        )
                        ELSE 'NA'
                      END AS BIGNUMERIC
                    ) < NUMERIC '0.10'
                  ) THEN 1
                  ELSE 0
                END
                WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
                  WHEN CASE
                    WHEN e2.rule_id IS NOT NULL
                    OR res_dis.rule_id IS NOT NULL THEN 1
                    ELSE 0
                  END = 0
                  AND a.action_type <> 1354
                  AND (
                    a.str_match_txn_cnt >= 30
                    AND SAFE_CAST(
                      CASE
                        WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                          REGEXP_REPLACE(
                            FORMAT(
                              '%6.3f'                                                         ,
                              a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                            )                   ,
                            r'^( *?)(-)?0(\..*)',
                            r'\2\3 \1'
                          )                     ,
                          r'^( *?)(-)?(\d*\..*)',
                          r'\2\3\1'
                        )
                        ELSE 'NA'
                      END AS BIGNUMERIC
                    ) < NUMERIC '0.10'
                  ) THEN 1
                  ELSE 0
                END
                ELSE 0
              END
              ELSE CASE
                WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
                AND xoom_team.xoom_ntid IS NULL
                AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
                AND r.rule_id NOT IN (467812)
                AND (
                  CASE
                    WHEN e2.rule_id IS NOT NULL
                    OR res_dis.rule_id IS NOT NULL THEN 1
                    ELSE 0
                  END = 0
                  AND a.action_type <> 1354
                  AND SAFE_CAST(
                    CASE
                      WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                         ,
                            a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                  AND a.str_match_txn_cnt >= 30
                ) THEN 1
                ELSE 0
              END
            END
            ELSE CASE
              WHEN r.crs_team IN (
                'gfr_omni_gms'    ,
                'gfr_omni_nextgen',
                'gfr_omni'        ,
                'goal_nextgen'    ,
                'goal_gms'
              ) THEN CASE
                WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
                  WHEN CASE
                    WHEN e2.rule_id IS NOT NULL
                    OR res_dis.rule_id IS NOT NULL THEN 1
                    ELSE 0
                  END = 0
                  AND a.action_type <> 1354
                  AND (
                    a.str_cg_txn_cnt >= 30
                    AND SAFE_CAST(
                      CASE
                        WHEN a.str_cg_txn_wamt > 0 THEN CAST(
                          a.str_cg_brm_bad_wamt / a.str_cg_txn_wamt AS STRING
                        )
                        ELSE 'NA'
                      END AS BIGNUMERIC
                    ) < NUMERIC '0.15'
                    OR a.str_cg_txn_cnt < 30
                    AND SAFE_CAST(
                      CASE
                        WHEN a.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                          REGEXP_REPLACE(
                            FORMAT(
                              '%6.3f'                                                             ,
                              a.incre_str_decled_bad_amt * NUMERIC '1.000' / a.incre_str_decled_amt
                            )                   ,
                            r'^( *?)(-)?0(\..*)',
                            r'\2\3 \1'
                          )                     ,
                          r'^( *?)(-)?(\d*\..*)',
                          r'\2\3\1'
                        )
                        ELSE 'NA'
                      END AS BIGNUMERIC
                    ) < NUMERIC '0.15'
                  ) THEN 1
                  ELSE 0
                END
                WHEN UPPER(RTRIM(r.rule_mo)) = 'CC' THEN CASE
                  WHEN CASE
                    WHEN e2.rule_id IS NOT NULL
                    OR res_dis.rule_id IS NOT NULL THEN 1
                    ELSE 0
                  END = 0
                  AND a.action_type <> 1354
                  AND (
                    a.str_match_txn_cnt >= 30
                    AND SAFE_CAST(
                      CASE
                        WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                          REGEXP_REPLACE(
                            FORMAT(
                              '%6.3f'                                                         ,
                              a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                            )                   ,
                            r'^( *?)(-)?0(\..*)',
                            r'\2\3 \1'
                          )                     ,
                          r'^( *?)(-)?(\d*\..*)',
                          r'\2\3\1'
                        )
                        ELSE 'NA'
                      END AS BIGNUMERIC
                    ) < NUMERIC '0.15'
                    OR a.str_match_txn_cnt < 30
                    AND SAFE_CAST(
                      CASE
                        WHEN a.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                          REGEXP_REPLACE(
                            FORMAT(
                              '%6.3f'                                                             ,
                              a.incre_str_decled_bad_amt * NUMERIC '1.000' / a.incre_str_decled_amt
                            )                   ,
                            r'^( *?)(-)?0(\..*)',
                            r'\2\3 \1'
                          )                     ,
                          r'^( *?)(-)?(\d*\..*)',
                          r'\2\3\1'
                        )
                        ELSE 'NA'
                      END AS BIGNUMERIC
                    ) < NUMERIC '0.15'
                  ) THEN 1
                  ELSE 0
                END
                WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
                  WHEN CASE
                    WHEN e2.rule_id IS NOT NULL
                    OR res_dis.rule_id IS NOT NULL THEN 1
                    ELSE 0
                  END = 0
                  AND a.action_type <> 1354
                  AND (
                    a.str_match_txn_cnt >= 30
                    AND SAFE_CAST(
                      CASE
                        WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                          REGEXP_REPLACE(
                            FORMAT(
                              '%6.3f'                                                         ,
                              a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                            )                   ,
                            r'^( *?)(-)?0(\..*)',
                            r'\2\3 \1'
                          )                     ,
                          r'^( *?)(-)?(\d*\..*)',
                          r'\2\3\1'
                        )
                        ELSE 'NA'
                      END AS BIGNUMERIC
                    ) < NUMERIC '0.15'
                  ) THEN 1
                  ELSE 0
                END
                ELSE 0
              END
              ELSE CASE
                WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
                AND xoom_team.xoom_ntid IS NULL
                AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
                AND r.rule_id NOT IN (467812)
                AND (
                  CASE
                    WHEN e2.rule_id IS NOT NULL
                    OR res_dis.rule_id IS NOT NULL THEN 1
                    ELSE 0
                  END = 0
                  AND a.action_type <> 1354
                  AND SAFE_CAST(
                    CASE
                      WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                         ,
                            a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                  AND a.str_match_txn_cnt >= 30
                ) THEN 1
                ELSE 0
              END
            END
          END
          ELSE 0
        END + CASE
          WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN 0
          WHEN (
            r.crs_team LIKE 'gfr_goal_ato%'
            OR r.crs_team LIKE 'gfr_goal_ach%'
            OR r.crs_team LIKE 'gfr_goal_cc%'
            OR r.crs_team LIKE 'gfr_goal_col%'
            OR r.crs_team LIKE 'gfr_goal_auto%'
            OR LOWER(r.crs_team) LIKE ANY (
              'goal_ato'  ,
              'goal_ach'  ,
              'goal_addfi',
              'goal_cc'   ,
              'goal_col'  ,
              'goal_auto'
            )
          )
          AND (
            UPPER(r.tags) LIKE '%AUTOSOLUTION%'
            AND UPPER(r.tags) LIKE '%HYPERNET%'
          )
          AND (
            UPPER(r.tags) NOT LIKE '%MERCHANT%'
            AND UPPER(r.tags) NOT LIKE '%CLUSTER%'
          ) THEN CASE
            WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
              WHEN CASE
                WHEN e2.rule_id IS NOT NULL
                OR res_dis.rule_id IS NOT NULL THEN 1
                ELSE 0
              END = 0
              AND a.action_type <> 1354
              AND SAFE_CAST(
                CASE
                  WHEN a.actn_cg_txn_wamt > 0 THEN CAST(
                    a.actn_cg_brm_bad_wamt / a.actn_cg_txn_wamt AS STRING
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              AND a.actn_cg_txn_cnt >= 30 THEN 1
              ELSE 0
            END
            WHEN UPPER(RTRIM(r.rule_mo)) <> 'ATO' THEN CASE
              WHEN CASE
                WHEN e2.rule_id IS NOT NULL
                OR res_dis.rule_id IS NOT NULL THEN 1
                ELSE 0
              END = 0
              AND a.action_type <> 1354
              AND SAFE_CAST(
                CASE
                  WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                           ,
                        a.actn_match_bad_wamt_adj * NUMERIC '1.000' / a.actn_match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              AND a.actn_match_txn_cnt >= 30 THEN 1
              ELSE 0
            END
            ELSE 0
          END
          WHEN UPPER(r.tags) LIKE '%AUTOSOLUTION%'
          AND UPPER(r.tags) LIKE '%HYPERNET MERCHANT%' THEN CASE
            WHEN CASE
              WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
              ELSE 0
            END = 1 THEN 0
            WHEN excl_accuracy.rule_id IS NOT NULL THEN 0
            WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN 0
            WHEN st.is_strategy = 1
            AND (
              UPPER(r.rule_name) LIKE '%FALLBACK%'
              OR UPPER(r.tags) LIKE '%SAFETY%NET%'
            ) THEN CASE
              WHEN r.crs_team IN (
                'gfr_omni_gms'    ,
                'gfr_omni_nextgen',
                'gfr_omni'        ,
                'goal_nextgen'    ,
                'goal_gms'
              ) THEN CASE
                WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
                  WHEN CASE
                    WHEN e2.rule_id IS NOT NULL
                    OR res_dis.rule_id IS NOT NULL THEN 1
                    ELSE 0
                  END = 0
                  AND a.action_type <> 1354
                  AND (
                    a.actn_cg_txn_cnt >= 30
                    AND SAFE_CAST(
                      CASE
                        WHEN a.actn_cg_txn_wamt > 0 THEN CAST(
                          a.actn_cg_brm_bad_wamt / a.actn_cg_txn_wamt AS STRING
                        )
                        ELSE 'NA'
                      END AS BIGNUMERIC
                    ) < NUMERIC '0.10'
                    OR a.actn_cg_txn_cnt < 30
                    AND SAFE_CAST(
                      CASE
                        WHEN a.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                          REGEXP_REPLACE(
                            FORMAT(
                              '%6.3f'                                                               ,
                              a.incre_actn_decled_bad_amt * NUMERIC '1.000' / a.incre_actn_decled_amt
                            )                   ,
                            r'^( *?)(-)?0(\..*)',
                            r'\2\3 \1'
                          )                     ,
                          r'^( *?)(-)?(\d*\..*)',
                          r'\2\3\1'
                        )
                        ELSE 'NA'
                      END AS BIGNUMERIC
                    ) < NUMERIC '0.10'
                  ) THEN 1
                  ELSE 0
                END
                WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
                  WHEN CASE
                    WHEN e2.rule_id IS NOT NULL
                    OR res_dis.rule_id IS NOT NULL THEN 1
                    ELSE 0
                  END = 0
                  AND a.action_type <> 1354
                  AND (
                    a.actn_match_txn_cnt >= 30
                    AND SAFE_CAST(
                      CASE
                        WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                          REGEXP_REPLACE(
                            FORMAT(
                              '%6.3f'                                                           ,
                              a.actn_match_bad_wamt_adj * NUMERIC '1.000' / a.actn_match_txn_wamt
                            )                   ,
                            r'^( *?)(-)?0(\..*)',
                            r'\2\3 \1'
                          )                     ,
                          r'^( *?)(-)?(\d*\..*)',
                          r'\2\3\1'
                        )
                        ELSE 'NA'
                      END AS BIGNUMERIC
                    ) < NUMERIC '0.10'
                    OR a.actn_match_txn_cnt < 30
                    AND SAFE_CAST(
                      CASE
                        WHEN a.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                          REGEXP_REPLACE(
                            FORMAT(
                              '%6.3f'                                                               ,
                              a.incre_actn_decled_bad_amt * NUMERIC '1.000' / a.incre_actn_decled_amt
                            )                   ,
                            r'^( *?)(-)?0(\..*)',
                            r'\2\3 \1'
                          )                     ,
                          r'^( *?)(-)?(\d*\..*)',
                          r'\2\3\1'
                        )
                        ELSE 'NA'
                      END AS BIGNUMERIC
                    ) < NUMERIC '0.10'
                  ) THEN 1
                  ELSE 0
                END
                ELSE 0
              END
              ELSE CASE
                WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
                AND xoom_team.xoom_ntid IS NULL
                AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
                AND r.rule_id NOT IN (467812)
                AND (
                  CASE
                    WHEN e2.rule_id IS NOT NULL
                    OR res_dis.rule_id IS NOT NULL THEN 1
                    ELSE 0
                  END = 0
                  AND a.action_type <> 1354
                  AND SAFE_CAST(
                    CASE
                      WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                           ,
                            a.actn_match_bad_wamt_adj * NUMERIC '1.000' / a.actn_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                  AND a.actn_match_txn_cnt >= 30
                ) THEN 1
                ELSE 0
              END
            END
            ELSE CASE
              WHEN r.crs_team IN (
                'gfr_omni_gms'    ,
                'gfr_omni_nextgen',
                'gfr_omni'        ,
                'goal_nextgen'    ,
                'goal_gms'
              ) THEN CASE
                WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
                  WHEN CASE
                    WHEN e2.rule_id IS NOT NULL
                    OR res_dis.rule_id IS NOT NULL THEN 1
                    ELSE 0
                  END = 0
                  AND a.action_type <> 1354
                  AND (
                    a.actn_cg_txn_cnt >= 30
                    AND SAFE_CAST(
                      CASE
                        WHEN a.actn_cg_txn_wamt > 0 THEN CAST(
                          a.actn_cg_brm_bad_wamt / a.actn_cg_txn_wamt AS STRING
                        )
                        ELSE 'NA'
                      END AS BIGNUMERIC
                    ) < NUMERIC '0.15'
                    OR a.actn_cg_txn_cnt < 30
                    AND SAFE_CAST(
                      CASE
                        WHEN a.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                          REGEXP_REPLACE(
                            FORMAT(
                              '%6.3f'                                                               ,
                              a.incre_actn_decled_bad_amt * NUMERIC '1.000' / a.incre_actn_decled_amt
                            )                   ,
                            r'^( *?)(-)?0(\..*)',
                            r'\2\3 \1'
                          )                     ,
                          r'^( *?)(-)?(\d*\..*)',
                          r'\2\3\1'
                        )
                        ELSE 'NA'
                      END AS BIGNUMERIC
                    ) < NUMERIC '0.15'
                  ) THEN 1
                  ELSE 0
                END
                WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
                  WHEN CASE
                    WHEN e2.rule_id IS NOT NULL
                    OR res_dis.rule_id IS NOT NULL THEN 1
                    ELSE 0
                  END = 0
                  AND a.action_type <> 1354
                  AND (
                    a.actn_match_txn_cnt >= 30
                    AND SAFE_CAST(
                      CASE
                        WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                          REGEXP_REPLACE(
                            FORMAT(
                              '%6.3f'                                                           ,
                              a.actn_match_bad_wamt_adj * NUMERIC '1.000' / a.actn_match_txn_wamt
                            )                   ,
                            r'^( *?)(-)?0(\..*)',
                            r'\2\3 \1'
                          )                     ,
                          r'^( *?)(-)?(\d*\..*)',
                          r'\2\3\1'
                        )
                        ELSE 'NA'
                      END AS BIGNUMERIC
                    ) < NUMERIC '0.15'
                    OR a.actn_match_txn_cnt < 30
                    AND SAFE_CAST(
                      CASE
                        WHEN a.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                          REGEXP_REPLACE(
                            FORMAT(
                              '%6.3f'                                                               ,
                              a.incre_actn_decled_bad_amt * NUMERIC '1.000' / a.incre_actn_decled_amt
                            )                   ,
                            r'^( *?)(-)?0(\..*)',
                            r'\2\3 \1'
                          )                     ,
                          r'^( *?)(-)?(\d*\..*)',
                          r'\2\3\1'
                        )
                        ELSE 'NA'
                      END AS BIGNUMERIC
                    ) < NUMERIC '0.15'
                  ) THEN 1
                  ELSE 0
                END
                ELSE 0
              END
              ELSE CASE
                WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
                AND xoom_team.xoom_ntid IS NULL
                AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
                AND r.rule_id NOT IN (467812)
                AND (
                  CASE
                    WHEN e2.rule_id IS NOT NULL
                    OR res_dis.rule_id IS NOT NULL THEN 1
                    ELSE 0
                  END = 0
                  AND a.action_type <> 1354
                  AND SAFE_CAST(
                    CASE
                      WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                           ,
                            a.actn_match_bad_wamt_adj * NUMERIC '1.000' / a.actn_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                  AND a.actn_match_txn_cnt >= 30
                ) THEN 1
                ELSE 0
              END
            END
          END
          ELSE 0
        END > 0 THEN 1
        ELSE 0
      END = 1
      OR CASE
        WHEN (
          r.crs_team LIKE 'gfr_goal_ato%'
          OR r.crs_team LIKE 'gfr_goal_ach%'
          OR r.crs_team LIKE 'gfr_goal_cc%'
          OR r.crs_team LIKE 'gfr_goal_col%'
          OR r.crs_team LIKE 'gfr_goal_auto%'
          OR LOWER(r.crs_team) LIKE ANY (
            'goal_ato'  ,
            'goal_ach'  ,
            'goal_addfi',
            'goal_cc'   ,
            'goal_col'  ,
            'goal_auto'
          )
        )
        AND (
          UPPER(r.tags) LIKE '%AUTOSOLUTION%'
          AND UPPER(r.tags) LIKE '%HYPERNET%'
        )
        AND (
          UPPER(r.tags) NOT LIKE '%MERCHANT%'
          AND UPPER(r.tags) NOT LIKE '%CLUSTER%'
        ) THEN 0
        WHEN UPPER(r.tags) LIKE '%AUTOSOLUTION%'
        AND UPPER(r.tags) LIKE '%HYPERNET MERCHANT%' THEN CASE
          WHEN CASE
            WHEN e_ow.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND SAFE_CAST(
            CASE
              WHEN a.fire_vol > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                    ,
                    a.sum_wh_actn * NUMERIC '1.000' / a.fire_vol
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) >= NUMERIC '0.7'
          AND SAFE_CAST(
            CASE
              WHEN a.whed_amt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                    ,
                    a.bad_amt_adj * NUMERIC '1.000' / a.whed_amt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.1'
          AND a.whed_cnt >= 30
          AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%' THEN 1
          ELSE 0
        END
        ELSE 0
      END = 1
      OR CASE
        WHEN UPPER(r.tags) LIKE '%AUTOSOLUTION%'
        AND UPPER(r.tags) LIKE '%HYPERNET MERCHANT%' THEN CASE
          WHEN CASE
            WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
            ELSE 0
          END = 0
          AND SAFE_CAST(b.ov_pct_1 AS BIGNUMERIC) >= NUMERIC '0.8'
          AND CASE
            WHEN e2.rule_id IS NOT NULL
            OR res_dis.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND a.action_type <> 1354 THEN CASE
            WHEN UPPER(r.tags) LIKE '%AUTOSOLUTION%'
            AND UPPER(r.tags) LIKE '%HYPERNET MERCHANT%'
            AND COALESCE(st_ov1.is_strategy_ov1_raw, 0) = 1 THEN 1
            WHEN UPPER(r.tags) LIKE '%AUTOSOLUTION%'
            AND UPPER(r.tags) LIKE '%HYPERNET MERCHANT%'
            AND COALESCE(st_ov1.is_strategy_ov1_raw, 0) = 0 THEN 0
            ELSE 1
          END
          ELSE 0
        END
        ELSE 0
      END = 1
    )
    AND (
      UPPER(r.tags) LIKE '%AUTOSOLUTION%'
      AND UPPER(r.tags) LIKE '%HYPERNET MERCHANT%'
    ) THEN 5
    WHEN CASE
      WHEN 0 = 1
      OR CASE
        WHEN (
          r.crs_team LIKE 'gfr_goal_ato%'
          OR r.crs_team LIKE 'gfr_goal_ach%'
          OR r.crs_team LIKE 'gfr_goal_cc%'
          OR r.crs_team LIKE 'gfr_goal_col%'
          OR r.crs_team LIKE 'gfr_goal_auto%'
          OR LOWER(r.crs_team) LIKE ANY (
            'goal_ato'  ,
            'goal_ach'  ,
            'goal_addfi',
            'goal_cc'   ,
            'goal_col'  ,
            'goal_auto'
          )
        )
        AND (
          UPPER(r.tags) LIKE '%AUTOSOLUTION%'
          AND UPPER(r.tags) LIKE '%HYPERNET%'
        )
        AND (
          UPPER(r.tags) NOT LIKE '%MERCHANT%'
          AND UPPER(r.tags) NOT LIKE '%CLUSTER%'
        )
        AND a.incre_str_fire_vol < r.observing_window THEN 1
        WHEN UPPER(r.tags) LIKE '%AUTOSOLUTION%'
        AND UPPER(r.tags) LIKE '%HYPERNET MERCHANT%'
        AND a.incre_str_fire_vol < 1 * r.observing_window THEN 1
        ELSE 0
      END = 1 THEN 1
      ELSE 0
    END = 1
    OR CASE
      WHEN CASE
        WHEN (
          r.crs_team LIKE 'gfr_goal_ato%'
          OR r.crs_team LIKE 'gfr_goal_ach%'
          OR r.crs_team LIKE 'gfr_goal_cc%'
          OR r.crs_team LIKE 'gfr_goal_col%'
          OR r.crs_team LIKE 'gfr_goal_auto%'
          OR LOWER(r.crs_team) LIKE ANY (
            'goal_ato'  ,
            'goal_ach'  ,
            'goal_addfi',
            'goal_cc'   ,
            'goal_col'  ,
            'goal_auto'
          )
        )
        AND (
          UPPER(r.tags) LIKE '%AUTOSOLUTION%'
          AND UPPER(r.tags) LIKE '%HYPERNET%'
        )
        AND (
          UPPER(r.tags) NOT LIKE '%MERCHANT%'
          AND UPPER(r.tags) NOT LIKE '%CLUSTER%'
        ) THEN CASE
          WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
            WHEN SAFE_CAST(
              CASE
                WHEN a.cg_txn_wamt > 0 THEN CAST(
                  a.cg_brm_bad_wamt / a.cg_txn_wamt AS STRING
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            AND a.cg_txn_cnt >= 30 THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r.rule_mo)) <> 'ATO' THEN CASE
            WHEN SAFE_CAST(
              CASE
                WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                 ,
                      a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            AND a.match_txn_cnt >= 30 THEN 1
            ELSE 0
          END
          ELSE 0
        END
        WHEN UPPER(r.tags) LIKE '%AUTOSOLUTION%'
        AND UPPER(r.tags) LIKE '%HYPERNET MERCHANT%' THEN CASE
          WHEN CASE
            WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
            ELSE 0
          END = 1 THEN 0
          WHEN excl_accuracy.rule_id IS NOT NULL THEN 0
          WHEN st.is_strategy = 1
          AND (
            UPPER(r.rule_name) LIKE '%FALLBACK%'
            OR UPPER(r.tags) LIKE '%SAFETY%NET%'
          ) THEN CASE
            WHEN r.crs_team IN (
              'gfr_omni_gms'    ,
              'gfr_omni_nextgen',
              'gfr_omni'        ,
              'goal_nextgen'    ,
              'goal_gms'
            ) THEN CASE
              WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
                WHEN a.cg_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.cg_txn_wamt > 0 THEN CAST(
                      a.cg_brm_bad_wamt / a.cg_txn_wamt AS STRING
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                OR a.cg_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                         ,
                          a.decled_bad_amt * NUMERIC '1.000' / a.decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10' THEN 1
                ELSE 0
              END
              WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN CASE
                WHEN a.match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                 ,
                          a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10' THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r.rule_mo)) = 'CC' THEN CASE
                WHEN a.match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                 ,
                          a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                OR a.match_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                         ,
                          a.decled_bad_amt * NUMERIC '1.000' / a.decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10' THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
                WHEN a.match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                 ,
                          a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10' THEN 1
                ELSE 0
              END
              ELSE 0
            END
            WHEN r.rule_mo IN ('Collusion', 'UBSM')
            AND xoom_team.xoom_ntid IS NULL THEN CASE
              WHEN SAFE_CAST(
                CASE
                  WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                 ,
                        a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              AND a.match_txn_cnt >= 30 THEN 1
              ELSE 0
            END
            ELSE CASE
              WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
              AND xoom_team.xoom_ntid IS NULL
              AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
              AND r.rule_id NOT IN (467812)
              AND (
                SAFE_CAST(
                  CASE
                    WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                 ,
                          a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                AND a.match_txn_cnt >= 30
              ) THEN 1
              ELSE 0
            END
          END
          ELSE CASE
            WHEN r.crs_team IN (
              'gfr_omni_gms'    ,
              'gfr_omni_nextgen',
              'gfr_omni'        ,
              'goal_nextgen'    ,
              'goal_gms'
            ) THEN CASE
              WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
                WHEN a.cg_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.cg_txn_wamt > 0 THEN CAST(
                      a.cg_brm_bad_wamt / a.cg_txn_wamt AS STRING
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                OR a.cg_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                         ,
                          a.decled_bad_amt * NUMERIC '1.000' / a.decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15' THEN 1
                ELSE 0
              END
              WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN CASE
                WHEN a.match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                 ,
                          a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15' THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r.rule_mo)) = 'CC' THEN CASE
                WHEN a.match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                 ,
                          a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                OR a.match_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                         ,
                          a.decled_bad_amt * NUMERIC '1.000' / a.decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15' THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
                WHEN a.match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                 ,
                          a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15' THEN 1
                ELSE 0
              END
              ELSE 0
            END
            WHEN r.rule_mo IN ('Collusion', 'UBSM')
            AND xoom_team.xoom_ntid IS NULL THEN CASE
              WHEN SAFE_CAST(
                CASE
                  WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                 ,
                        a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
              AND a.match_txn_cnt >= 30 THEN 1
              ELSE 0
            END
            ELSE CASE
              WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
              AND xoom_team.xoom_ntid IS NULL
              AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
              AND r.rule_id NOT IN (467812)
              AND (
                SAFE_CAST(
                  CASE
                    WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                 ,
                          a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                AND a.match_txn_cnt >= 30
              ) THEN 1
              ELSE 0
            END
          END
        END
        ELSE 0
      END + CASE
        WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN 0
        WHEN (
          r.crs_team LIKE 'gfr_goal_ato%'
          OR r.crs_team LIKE 'gfr_goal_ach%'
          OR r.crs_team LIKE 'gfr_goal_cc%'
          OR r.crs_team LIKE 'gfr_goal_col%'
          OR r.crs_team LIKE 'gfr_goal_auto%'
          OR LOWER(r.crs_team) LIKE ANY (
            'goal_ato'  ,
            'goal_ach'  ,
            'goal_addfi',
            'goal_cc'   ,
            'goal_col'  ,
            'goal_auto'
          )
        )
        AND (
          UPPER(r.tags) LIKE '%AUTOSOLUTION%'
          AND UPPER(r.tags) LIKE '%HYPERNET%'
        )
        AND (
          UPPER(r.tags) NOT LIKE '%MERCHANT%'
          AND UPPER(r.tags) NOT LIKE '%CLUSTER%'
        ) THEN CASE
          WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
            WHEN CASE
              WHEN e2.rule_id IS NOT NULL
              OR res_dis.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND a.action_type <> 1354
            AND SAFE_CAST(
              CASE
                WHEN a.str_cg_txn_wamt > 0 THEN CAST(
                  a.str_cg_brm_bad_wamt / a.str_cg_txn_wamt AS STRING
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            AND a.str_cg_txn_cnt >= 30 THEN 0
            ELSE 0
          END
          WHEN UPPER(RTRIM(r.rule_mo)) <> 'ATO' THEN CASE
            WHEN CASE
              WHEN e2.rule_id IS NOT NULL
              OR res_dis.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND a.action_type <> 1354
            AND SAFE_CAST(
              CASE
                WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                         ,
                      a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            AND a.str_match_txn_cnt >= 30 THEN 0
            ELSE 0
          END
          ELSE 0
        END
        WHEN UPPER(r.tags) LIKE '%AUTOSOLUTION%'
        AND UPPER(r.tags) LIKE '%HYPERNET MERCHANT%' THEN CASE
          WHEN CASE
            WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
            ELSE 0
          END = 1 THEN 0
          WHEN excl_accuracy.rule_id IS NOT NULL THEN 0
          WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN 0
          WHEN st.is_strategy = 1
          AND (
            UPPER(r.rule_name) LIKE '%FALLBACK%'
            OR UPPER(r.tags) LIKE '%SAFETY%NET%'
          ) THEN CASE
            WHEN r.crs_team IN (
              'gfr_omni_gms'    ,
              'gfr_omni_nextgen',
              'gfr_omni'        ,
              'goal_nextgen'    ,
              'goal_gms'
            ) THEN CASE
              WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
                WHEN CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND (
                  a.str_cg_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.str_cg_txn_wamt > 0 THEN CAST(
                        a.str_cg_brm_bad_wamt / a.str_cg_txn_wamt AS STRING
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                  OR a.str_cg_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                             ,
                            a.incre_str_decled_bad_amt * NUMERIC '1.000' / a.incre_str_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                ) THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r.rule_mo)) = 'CC' THEN CASE
                WHEN CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND (
                  a.str_match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                         ,
                            a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                  OR a.str_match_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                             ,
                            a.incre_str_decled_bad_amt * NUMERIC '1.000' / a.incre_str_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                ) THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
                WHEN CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND (
                  a.str_match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                         ,
                            a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                ) THEN 1
                ELSE 0
              END
              ELSE 0
            END
            ELSE CASE
              WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
              AND xoom_team.xoom_ntid IS NULL
              AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
              AND r.rule_id NOT IN (467812)
              AND (
                CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND SAFE_CAST(
                  CASE
                    WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                         ,
                          a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                AND a.str_match_txn_cnt >= 30
              ) THEN 1
              ELSE 0
            END
          END
          ELSE CASE
            WHEN r.crs_team IN (
              'gfr_omni_gms'    ,
              'gfr_omni_nextgen',
              'gfr_omni'        ,
              'goal_nextgen'    ,
              'goal_gms'
            ) THEN CASE
              WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
                WHEN CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND (
                  a.str_cg_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.str_cg_txn_wamt > 0 THEN CAST(
                        a.str_cg_brm_bad_wamt / a.str_cg_txn_wamt AS STRING
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                  OR a.str_cg_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                             ,
                            a.incre_str_decled_bad_amt * NUMERIC '1.000' / a.incre_str_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                ) THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r.rule_mo)) = 'CC' THEN CASE
                WHEN CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND (
                  a.str_match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                         ,
                            a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                  OR a.str_match_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                             ,
                            a.incre_str_decled_bad_amt * NUMERIC '1.000' / a.incre_str_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                ) THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
                WHEN CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND (
                  a.str_match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                         ,
                            a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                ) THEN 1
                ELSE 0
              END
              ELSE 0
            END
            ELSE CASE
              WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
              AND xoom_team.xoom_ntid IS NULL
              AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
              AND r.rule_id NOT IN (467812)
              AND (
                CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND SAFE_CAST(
                  CASE
                    WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                         ,
                          a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                AND a.str_match_txn_cnt >= 30
              ) THEN 1
              ELSE 0
            END
          END
        END
        ELSE 0
      END + CASE
        WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN 0
        WHEN (
          r.crs_team LIKE 'gfr_goal_ato%'
          OR r.crs_team LIKE 'gfr_goal_ach%'
          OR r.crs_team LIKE 'gfr_goal_cc%'
          OR r.crs_team LIKE 'gfr_goal_col%'
          OR r.crs_team LIKE 'gfr_goal_auto%'
          OR LOWER(r.crs_team) LIKE ANY (
            'goal_ato'  ,
            'goal_ach'  ,
            'goal_addfi',
            'goal_cc'   ,
            'goal_col'  ,
            'goal_auto'
          )
        )
        AND (
          UPPER(r.tags) LIKE '%AUTOSOLUTION%'
          AND UPPER(r.tags) LIKE '%HYPERNET%'
        )
        AND (
          UPPER(r.tags) NOT LIKE '%MERCHANT%'
          AND UPPER(r.tags) NOT LIKE '%CLUSTER%'
        ) THEN CASE
          WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
            WHEN CASE
              WHEN e2.rule_id IS NOT NULL
              OR res_dis.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND a.action_type <> 1354
            AND SAFE_CAST(
              CASE
                WHEN a.actn_cg_txn_wamt > 0 THEN CAST(
                  a.actn_cg_brm_bad_wamt / a.actn_cg_txn_wamt AS STRING
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            AND a.actn_cg_txn_cnt >= 30 THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r.rule_mo)) <> 'ATO' THEN CASE
            WHEN CASE
              WHEN e2.rule_id IS NOT NULL
              OR res_dis.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND a.action_type <> 1354
            AND SAFE_CAST(
              CASE
                WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                           ,
                      a.actn_match_bad_wamt_adj * NUMERIC '1.000' / a.actn_match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            AND a.actn_match_txn_cnt >= 30 THEN 1
            ELSE 0
          END
          ELSE 0
        END
        WHEN UPPER(r.tags) LIKE '%AUTOSOLUTION%'
        AND UPPER(r.tags) LIKE '%HYPERNET MERCHANT%' THEN CASE
          WHEN CASE
            WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
            ELSE 0
          END = 1 THEN 0
          WHEN excl_accuracy.rule_id IS NOT NULL THEN 0
          WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN 0
          WHEN st.is_strategy = 1
          AND (
            UPPER(r.rule_name) LIKE '%FALLBACK%'
            OR UPPER(r.tags) LIKE '%SAFETY%NET%'
          ) THEN CASE
            WHEN r.crs_team IN (
              'gfr_omni_gms'    ,
              'gfr_omni_nextgen',
              'gfr_omni'        ,
              'goal_nextgen'    ,
              'goal_gms'
            ) THEN CASE
              WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
                WHEN CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND (
                  a.actn_cg_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.actn_cg_txn_wamt > 0 THEN CAST(
                        a.actn_cg_brm_bad_wamt / a.actn_cg_txn_wamt AS STRING
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                  OR a.actn_cg_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                               ,
                            a.incre_actn_decled_bad_amt * NUMERIC '1.000' / a.incre_actn_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                ) THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
                WHEN CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND (
                  a.actn_match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                           ,
                            a.actn_match_bad_wamt_adj * NUMERIC '1.000' / a.actn_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                  OR a.actn_match_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                               ,
                            a.incre_actn_decled_bad_amt * NUMERIC '1.000' / a.incre_actn_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                ) THEN 1
                ELSE 0
              END
              ELSE 0
            END
            ELSE CASE
              WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
              AND xoom_team.xoom_ntid IS NULL
              AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
              AND r.rule_id NOT IN (467812)
              AND (
                CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND SAFE_CAST(
                  CASE
                    WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                           ,
                          a.actn_match_bad_wamt_adj * NUMERIC '1.000' / a.actn_match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                AND a.actn_match_txn_cnt >= 30
              ) THEN 1
              ELSE 0
            END
          END
          ELSE CASE
            WHEN r.crs_team IN (
              'gfr_omni_gms'    ,
              'gfr_omni_nextgen',
              'gfr_omni'        ,
              'goal_nextgen'    ,
              'goal_gms'
            ) THEN CASE
              WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
                WHEN CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND (
                  a.actn_cg_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.actn_cg_txn_wamt > 0 THEN CAST(
                        a.actn_cg_brm_bad_wamt / a.actn_cg_txn_wamt AS STRING
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                  OR a.actn_cg_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                               ,
                            a.incre_actn_decled_bad_amt * NUMERIC '1.000' / a.incre_actn_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                ) THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
                WHEN CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND (
                  a.actn_match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                           ,
                            a.actn_match_bad_wamt_adj * NUMERIC '1.000' / a.actn_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                  OR a.actn_match_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                               ,
                            a.incre_actn_decled_bad_amt * NUMERIC '1.000' / a.incre_actn_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                ) THEN 1
                ELSE 0
              END
              ELSE 0
            END
            ELSE CASE
              WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
              AND xoom_team.xoom_ntid IS NULL
              AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
              AND r.rule_id NOT IN (467812)
              AND (
                CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND SAFE_CAST(
                  CASE
                    WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                           ,
                          a.actn_match_bad_wamt_adj * NUMERIC '1.000' / a.actn_match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                AND a.actn_match_txn_cnt >= 30
              ) THEN 1
              ELSE 0
            END
          END
        END
        ELSE 0
      END > 0 THEN 1
      ELSE 0
    END = 1
    OR CASE
      WHEN (
        r.crs_team LIKE 'gfr_goal_ato%'
        OR r.crs_team LIKE 'gfr_goal_ach%'
        OR r.crs_team LIKE 'gfr_goal_cc%'
        OR r.crs_team LIKE 'gfr_goal_col%'
        OR r.crs_team LIKE 'gfr_goal_auto%'
        OR LOWER(r.crs_team) LIKE ANY (
          'goal_ato'  ,
          'goal_ach'  ,
          'goal_addfi',
          'goal_cc'   ,
          'goal_col'  ,
          'goal_auto'
        )
      )
      AND (
        UPPER(r.tags) LIKE '%AUTOSOLUTION%'
        AND UPPER(r.tags) LIKE '%HYPERNET%'
      )
      AND (
        UPPER(r.tags) NOT LIKE '%MERCHANT%'
        AND UPPER(r.tags) NOT LIKE '%CLUSTER%'
      ) THEN 0
      WHEN UPPER(r.tags) LIKE '%AUTOSOLUTION%'
      AND UPPER(r.tags) LIKE '%HYPERNET MERCHANT%' THEN CASE
        WHEN CASE
          WHEN e_ow.rule_id IS NOT NULL THEN 1
          ELSE 0
        END = 0
        AND SAFE_CAST(
          CASE
            WHEN a.fire_vol > 0 THEN REGEXP_REPLACE(
              REGEXP_REPLACE(
                FORMAT(
                  '%6.3f'                                    ,
                  a.sum_wh_actn * NUMERIC '1.000' / a.fire_vol
                )                   ,
                r'^( *?)(-)?0(\..*)',
                r'\2\3 \1'
              )                     ,
              r'^( *?)(-)?(\d*\..*)',
              r'\2\3\1'
            )
            ELSE 'NA'
          END AS BIGNUMERIC
        ) >= NUMERIC '0.7'
        AND SAFE_CAST(
          CASE
            WHEN a.whed_amt > 0 THEN REGEXP_REPLACE(
              REGEXP_REPLACE(
                FORMAT(
                  '%6.3f'                                    ,
                  a.bad_amt_adj * NUMERIC '1.000' / a.whed_amt
                )                   ,
                r'^( *?)(-)?0(\..*)',
                r'\2\3 \1'
              )                     ,
              r'^( *?)(-)?(\d*\..*)',
              r'\2\3\1'
            )
            ELSE 'NA'
          END AS BIGNUMERIC
        ) < NUMERIC '0.1'
        AND a.whed_cnt >= 30
        AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%' THEN 1
        ELSE 0
      END
      ELSE 0
    END = 1
    OR CASE
      WHEN UPPER(r.tags) LIKE '%AUTOSOLUTION%'
      AND UPPER(r.tags) LIKE '%HYPERNET MERCHANT%' THEN CASE
        WHEN CASE
          WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
          ELSE 0
        END = 0
        AND SAFE_CAST(b.ov_pct_1 AS BIGNUMERIC) >= NUMERIC '0.8'
        AND CASE
          WHEN e2.rule_id IS NOT NULL
          OR res_dis.rule_id IS NOT NULL THEN 1
          ELSE 0
        END = 0
        AND a.action_type <> 1354 THEN CASE
          WHEN UPPER(r.tags) LIKE '%AUTOSOLUTION%'
          AND UPPER(r.tags) LIKE '%HYPERNET MERCHANT%'
          AND COALESCE(st_ov1.is_strategy_ov1_raw, 0) = 1 THEN 1
          WHEN UPPER(r.tags) LIKE '%AUTOSOLUTION%'
          AND UPPER(r.tags) LIKE '%HYPERNET MERCHANT%'
          AND COALESCE(st_ov1.is_strategy_ov1_raw, 0) = 0 THEN 0
          ELSE 1
        END
        ELSE 0
      END
      ELSE 0
    END = 1 THEN 5
    WHEN st.is_strategy = 1
    AND (
      CASE
        WHEN CASE
          WHEN CASE
            WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
            ELSE 0
          END = 1 THEN 0
          WHEN excl_accuracy.rule_id IS NOT NULL THEN 0
          WHEN st.is_strategy = 1
          AND (
            UPPER(r.rule_name) LIKE '%FALLBACK%'
            OR UPPER(r.tags) LIKE '%SAFETY%NET%'
          ) THEN CASE
            WHEN r.crs_team IN (
              'gfr_omni_gms'    ,
              'gfr_omni_nextgen',
              'gfr_omni'        ,
              'goal_nextgen'    ,
              'goal_gms'
            ) THEN CASE
              WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
                WHEN a.cg_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.cg_txn_wamt > 0 THEN CAST(
                      a.cg_brm_bad_wamt / a.cg_txn_wamt AS STRING
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                OR a.cg_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                         ,
                          a.decled_bad_amt * NUMERIC '1.000' / a.decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10' THEN 1
                ELSE 0
              END
              WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN CASE
                WHEN a.match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                 ,
                          a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10' THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r.rule_mo)) = 'CC' THEN CASE
                WHEN a.match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                 ,
                          a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                OR a.match_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                         ,
                          a.decled_bad_amt * NUMERIC '1.000' / a.decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10' THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
                WHEN a.match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                 ,
                          a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10' THEN 1
                ELSE 0
              END
              ELSE 0
            END
            WHEN r.rule_mo IN ('Collusion', 'UBSM')
            AND xoom_team.xoom_ntid IS NULL THEN CASE
              WHEN SAFE_CAST(
                CASE
                  WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                 ,
                        a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              AND a.match_txn_cnt >= 30 THEN 1
              ELSE 0
            END
            ELSE CASE
              WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
              AND xoom_team.xoom_ntid IS NULL
              AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
              AND r.rule_id NOT IN (467812)
              AND (
                SAFE_CAST(
                  CASE
                    WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                 ,
                          a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                AND a.match_txn_cnt >= 30
              ) THEN 1
              ELSE 0
            END
          END
          ELSE CASE
            WHEN r.crs_team IN (
              'gfr_omni_gms'    ,
              'gfr_omni_nextgen',
              'gfr_omni'        ,
              'goal_nextgen'    ,
              'goal_gms'
            ) THEN CASE
              WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
                WHEN a.cg_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.cg_txn_wamt > 0 THEN CAST(
                      a.cg_brm_bad_wamt / a.cg_txn_wamt AS STRING
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                OR a.cg_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                         ,
                          a.decled_bad_amt * NUMERIC '1.000' / a.decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15' THEN 1
                ELSE 0
              END
              WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN CASE
                WHEN a.match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                 ,
                          a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15' THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r.rule_mo)) = 'CC' THEN CASE
                WHEN a.match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                 ,
                          a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                OR a.match_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                         ,
                          a.decled_bad_amt * NUMERIC '1.000' / a.decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15' THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
                WHEN a.match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                 ,
                          a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15' THEN 1
                ELSE 0
              END
              ELSE 0
            END
            WHEN r.rule_mo IN ('Collusion', 'UBSM')
            AND xoom_team.xoom_ntid IS NULL THEN CASE
              WHEN SAFE_CAST(
                CASE
                  WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                 ,
                        a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
              AND a.match_txn_cnt >= 30 THEN 1
              ELSE 0
            END
            ELSE CASE
              WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
              AND xoom_team.xoom_ntid IS NULL
              AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
              AND r.rule_id NOT IN (467812)
              AND (
                SAFE_CAST(
                  CASE
                    WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                 ,
                          a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                AND a.match_txn_cnt >= 30
              ) THEN 1
              ELSE 0
            END
          END
        END + CASE
          WHEN CASE
            WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
            ELSE 0
          END = 1 THEN 0
          WHEN excl_accuracy.rule_id IS NOT NULL THEN 0
          WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN 0
          WHEN st.is_strategy = 1
          AND (
            UPPER(r.rule_name) LIKE '%FALLBACK%'
            OR UPPER(r.tags) LIKE '%SAFETY%NET%'
          ) THEN CASE
            WHEN r.crs_team IN (
              'gfr_omni_gms'    ,
              'gfr_omni_nextgen',
              'gfr_omni'        ,
              'goal_nextgen'    ,
              'goal_gms'
            ) THEN CASE
              WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
                WHEN CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND (
                  a.actn_cg_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.actn_cg_txn_wamt > 0 THEN CAST(
                        a.actn_cg_brm_bad_wamt / a.actn_cg_txn_wamt AS STRING
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                  OR a.actn_cg_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                               ,
                            a.incre_actn_decled_bad_amt * NUMERIC '1.000' / a.incre_actn_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                ) THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
                WHEN CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND (
                  a.actn_match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                           ,
                            a.actn_match_bad_wamt_adj * NUMERIC '1.000' / a.actn_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                  OR a.actn_match_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                               ,
                            a.incre_actn_decled_bad_amt * NUMERIC '1.000' / a.incre_actn_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                ) THEN 1
                ELSE 0
              END
              ELSE 0
            END
            ELSE CASE
              WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
              AND xoom_team.xoom_ntid IS NULL
              AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
              AND r.rule_id NOT IN (467812)
              AND (
                CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND SAFE_CAST(
                  CASE
                    WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                           ,
                          a.actn_match_bad_wamt_adj * NUMERIC '1.000' / a.actn_match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                AND a.actn_match_txn_cnt >= 30
              ) THEN 1
              ELSE 0
            END
          END
          ELSE CASE
            WHEN r.crs_team IN (
              'gfr_omni_gms'    ,
              'gfr_omni_nextgen',
              'gfr_omni'        ,
              'goal_nextgen'    ,
              'goal_gms'
            ) THEN CASE
              WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
                WHEN CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND (
                  a.actn_cg_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.actn_cg_txn_wamt > 0 THEN CAST(
                        a.actn_cg_brm_bad_wamt / a.actn_cg_txn_wamt AS STRING
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                  OR a.actn_cg_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                               ,
                            a.incre_actn_decled_bad_amt * NUMERIC '1.000' / a.incre_actn_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                ) THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
                WHEN CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND (
                  a.actn_match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                           ,
                            a.actn_match_bad_wamt_adj * NUMERIC '1.000' / a.actn_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                  OR a.actn_match_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                               ,
                            a.incre_actn_decled_bad_amt * NUMERIC '1.000' / a.incre_actn_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                ) THEN 1
                ELSE 0
              END
              ELSE 0
            END
            ELSE CASE
              WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
              AND xoom_team.xoom_ntid IS NULL
              AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
              AND r.rule_id NOT IN (467812)
              AND (
                CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND SAFE_CAST(
                  CASE
                    WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                           ,
                          a.actn_match_bad_wamt_adj * NUMERIC '1.000' / a.actn_match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                AND a.actn_match_txn_cnt >= 30
              ) THEN 1
              ELSE 0
            END
          END
        END + CASE
          WHEN CASE
            WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
            ELSE 0
          END = 1 THEN 0
          WHEN excl_accuracy.rule_id IS NOT NULL THEN 0
          WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN 0
          WHEN st.is_strategy = 1
          AND (
            UPPER(r.rule_name) LIKE '%FALLBACK%'
            OR UPPER(r.tags) LIKE '%SAFETY%NET%'
          ) THEN CASE
            WHEN r.crs_team IN (
              'gfr_omni_gms'    ,
              'gfr_omni_nextgen',
              'gfr_omni'        ,
              'goal_nextgen'    ,
              'goal_gms'
            ) THEN CASE
              WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
                WHEN CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND (
                  a.str_cg_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.str_cg_txn_wamt > 0 THEN CAST(
                        a.str_cg_brm_bad_wamt / a.str_cg_txn_wamt AS STRING
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                  OR a.str_cg_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                             ,
                            a.incre_str_decled_bad_amt * NUMERIC '1.000' / a.incre_str_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                ) THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r.rule_mo)) = 'CC' THEN CASE
                WHEN CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND (
                  a.str_match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                         ,
                            a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                  OR a.str_match_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                             ,
                            a.incre_str_decled_bad_amt * NUMERIC '1.000' / a.incre_str_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                ) THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
                WHEN CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND (
                  a.str_match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                         ,
                            a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                ) THEN 1
                ELSE 0
              END
              ELSE 0
            END
            ELSE CASE
              WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
              AND xoom_team.xoom_ntid IS NULL
              AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
              AND r.rule_id NOT IN (467812)
              AND (
                CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND SAFE_CAST(
                  CASE
                    WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                         ,
                          a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                AND a.str_match_txn_cnt >= 30
              ) THEN 1
              ELSE 0
            END
          END
          ELSE CASE
            WHEN r.crs_team IN (
              'gfr_omni_gms'    ,
              'gfr_omni_nextgen',
              'gfr_omni'        ,
              'goal_nextgen'    ,
              'goal_gms'
            ) THEN CASE
              WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
                WHEN CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND (
                  a.str_cg_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.str_cg_txn_wamt > 0 THEN CAST(
                        a.str_cg_brm_bad_wamt / a.str_cg_txn_wamt AS STRING
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                  OR a.str_cg_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                             ,
                            a.incre_str_decled_bad_amt * NUMERIC '1.000' / a.incre_str_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                ) THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r.rule_mo)) = 'CC' THEN CASE
                WHEN CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND (
                  a.str_match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                         ,
                            a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                  OR a.str_match_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                             ,
                            a.incre_str_decled_bad_amt * NUMERIC '1.000' / a.incre_str_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                ) THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
                WHEN CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND (
                  a.str_match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                         ,
                            a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                ) THEN 1
                ELSE 0
              END
              ELSE 0
            END
            ELSE CASE
              WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
              AND xoom_team.xoom_ntid IS NULL
              AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
              AND r.rule_id NOT IN (467812)
              AND (
                CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND SAFE_CAST(
                  CASE
                    WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                         ,
                          a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                AND a.str_match_txn_cnt >= 30
              ) THEN 1
              ELSE 0
            END
          END
        END > 0 THEN 1
        ELSE 0
      END = 1
      OR CASE
        WHEN (
          a.fire_vol < 10 * r.observing_window
          AND (
            SAFE_CAST(
              CASE
                WHEN a.cg_txn_wamt > 0 THEN CAST(
                  a.cg_brm_bad_wamt / a.cg_txn_wamt AS STRING
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.1'
            OR RTRIM(
              CASE
                WHEN a.cg_txn_wamt > 0 THEN CAST(
                  a.cg_brm_bad_wamt / a.cg_txn_wamt AS STRING
                )
                ELSE 'NA'
              END
            ) = 'NA'
          )
          OR CASE
            WHEN e2.rule_id IS NOT NULL
            OR res_dis.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND a.action_type <> 1354
          AND a.incre_rule_fire_vol < 5 * r.observing_window
        )
        AND CASE
          WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
          ELSE 0
        END = 0
        AND CASE
          WHEN e.rule_id IS NOT NULL THEN 1
          ELSE 0
        END = 0 THEN 1
        ELSE 0
      END = 1
      OR CASE
        WHEN CASE
          WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
          ELSE 0
        END = 0
        AND SAFE_CAST(b.ov_pct_1 AS BIGNUMERIC) >= NUMERIC '0.8'
        AND CASE
          WHEN e2.rule_id IS NOT NULL
          OR res_dis.rule_id IS NOT NULL THEN 1
          ELSE 0
        END = 0
        AND a.action_type <> 1354 THEN CASE
          WHEN UPPER(r.tags) LIKE '%AUTOSOLUTION%'
          AND UPPER(r.tags) LIKE '%HYPERNET MERCHANT%'
          AND COALESCE(st_ov1.is_strategy_ov1_raw, 0) = 1 THEN 1
          WHEN UPPER(r.tags) LIKE '%AUTOSOLUTION%'
          AND UPPER(r.tags) LIKE '%HYPERNET MERCHANT%'
          AND COALESCE(st_ov1.is_strategy_ov1_raw, 0) = 0 THEN 0
          ELSE 1
        END
        ELSE 0
      END = 1
      OR CASE
        WHEN CASE
          WHEN e_ow.rule_id IS NOT NULL THEN 1
          ELSE 0
        END = 0
        AND SAFE_CAST(
          CASE
            WHEN a.fire_vol > 0 THEN REGEXP_REPLACE(
              REGEXP_REPLACE(
                FORMAT(
                  '%6.3f'                                    ,
                  a.sum_wh_actn * NUMERIC '1.000' / a.fire_vol
                )                   ,
                r'^( *?)(-)?0(\..*)',
                r'\2\3 \1'
              )                     ,
              r'^( *?)(-)?(\d*\..*)',
              r'\2\3\1'
            )
            ELSE 'NA'
          END AS BIGNUMERIC
        ) >= NUMERIC '0.7'
        AND SAFE_CAST(
          CASE
            WHEN a.whed_amt > 0 THEN REGEXP_REPLACE(
              REGEXP_REPLACE(
                FORMAT(
                  '%6.3f'                                    ,
                  a.bad_amt_adj * NUMERIC '1.000' / a.whed_amt
                )                   ,
                r'^( *?)(-)?0(\..*)',
                r'\2\3 \1'
              )                     ,
              r'^( *?)(-)?(\d*\..*)',
              r'\2\3\1'
            )
            ELSE 'NA'
          END AS BIGNUMERIC
        ) < NUMERIC '0.1'
        AND a.whed_cnt >= 30
        AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%' THEN 1
        ELSE 0
      END = 1
    ) THEN 1
    WHEN st.is_strategy = 1
    AND CASE
      WHEN CASE
        WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
        ELSE 0
      END = 1 THEN 0
      ELSE COALESCE(cntct.high_cntct_rate_, 0)
    END = 1 THEN 2
    WHEN st.is_strategy = 0
    AND (
      CASE
        WHEN CASE
          WHEN CASE
            WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
            ELSE 0
          END = 1 THEN 0
          WHEN excl_accuracy.rule_id IS NOT NULL THEN 0
          WHEN st.is_strategy = 1
          AND (
            UPPER(r.rule_name) LIKE '%FALLBACK%'
            OR UPPER(r.tags) LIKE '%SAFETY%NET%'
          ) THEN CASE
            WHEN r.crs_team IN (
              'gfr_omni_gms'    ,
              'gfr_omni_nextgen',
              'gfr_omni'        ,
              'goal_nextgen'    ,
              'goal_gms'
            ) THEN CASE
              WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
                WHEN a.cg_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.cg_txn_wamt > 0 THEN CAST(
                      a.cg_brm_bad_wamt / a.cg_txn_wamt AS STRING
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                OR a.cg_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                         ,
                          a.decled_bad_amt * NUMERIC '1.000' / a.decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10' THEN 1
                ELSE 0
              END
              WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN CASE
                WHEN a.match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                 ,
                          a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10' THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r.rule_mo)) = 'CC' THEN CASE
                WHEN a.match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                 ,
                          a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                OR a.match_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                         ,
                          a.decled_bad_amt * NUMERIC '1.000' / a.decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10' THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
                WHEN a.match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                 ,
                          a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10' THEN 1
                ELSE 0
              END
              ELSE 0
            END
            WHEN r.rule_mo IN ('Collusion', 'UBSM')
            AND xoom_team.xoom_ntid IS NULL THEN CASE
              WHEN SAFE_CAST(
                CASE
                  WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                 ,
                        a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              AND a.match_txn_cnt >= 30 THEN 1
              ELSE 0
            END
            ELSE CASE
              WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
              AND xoom_team.xoom_ntid IS NULL
              AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
              AND r.rule_id NOT IN (467812)
              AND (
                SAFE_CAST(
                  CASE
                    WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                 ,
                          a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                AND a.match_txn_cnt >= 30
              ) THEN 1
              ELSE 0
            END
          END
          ELSE CASE
            WHEN r.crs_team IN (
              'gfr_omni_gms'    ,
              'gfr_omni_nextgen',
              'gfr_omni'        ,
              'goal_nextgen'    ,
              'goal_gms'
            ) THEN CASE
              WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
                WHEN a.cg_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.cg_txn_wamt > 0 THEN CAST(
                      a.cg_brm_bad_wamt / a.cg_txn_wamt AS STRING
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                OR a.cg_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                         ,
                          a.decled_bad_amt * NUMERIC '1.000' / a.decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15' THEN 1
                ELSE 0
              END
              WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN CASE
                WHEN a.match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                 ,
                          a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15' THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r.rule_mo)) = 'CC' THEN CASE
                WHEN a.match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                 ,
                          a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                OR a.match_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                         ,
                          a.decled_bad_amt * NUMERIC '1.000' / a.decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15' THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
                WHEN a.match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                 ,
                          a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15' THEN 1
                ELSE 0
              END
              ELSE 0
            END
            WHEN r.rule_mo IN ('Collusion', 'UBSM')
            AND xoom_team.xoom_ntid IS NULL THEN CASE
              WHEN SAFE_CAST(
                CASE
                  WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                 ,
                        a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
              AND a.match_txn_cnt >= 30 THEN 1
              ELSE 0
            END
            ELSE CASE
              WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
              AND xoom_team.xoom_ntid IS NULL
              AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
              AND r.rule_id NOT IN (467812)
              AND (
                SAFE_CAST(
                  CASE
                    WHEN a.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                 ,
                          a.match_bad_wamt_adj * NUMERIC '1.000' / a.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                AND a.match_txn_cnt >= 30
              ) THEN 1
              ELSE 0
            END
          END
        END + CASE
          WHEN CASE
            WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
            ELSE 0
          END = 1 THEN 0
          WHEN excl_accuracy.rule_id IS NOT NULL THEN 0
          WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN 0
          WHEN st.is_strategy = 1
          AND (
            UPPER(r.rule_name) LIKE '%FALLBACK%'
            OR UPPER(r.tags) LIKE '%SAFETY%NET%'
          ) THEN CASE
            WHEN r.crs_team IN (
              'gfr_omni_gms'    ,
              'gfr_omni_nextgen',
              'gfr_omni'        ,
              'goal_nextgen'    ,
              'goal_gms'
            ) THEN CASE
              WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
                WHEN CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND (
                  a.actn_cg_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.actn_cg_txn_wamt > 0 THEN CAST(
                        a.actn_cg_brm_bad_wamt / a.actn_cg_txn_wamt AS STRING
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                  OR a.actn_cg_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                               ,
                            a.incre_actn_decled_bad_amt * NUMERIC '1.000' / a.incre_actn_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                ) THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
                WHEN CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND (
                  a.actn_match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                           ,
                            a.actn_match_bad_wamt_adj * NUMERIC '1.000' / a.actn_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                  OR a.actn_match_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                               ,
                            a.incre_actn_decled_bad_amt * NUMERIC '1.000' / a.incre_actn_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                ) THEN 1
                ELSE 0
              END
              ELSE 0
            END
            ELSE CASE
              WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
              AND xoom_team.xoom_ntid IS NULL
              AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
              AND r.rule_id NOT IN (467812)
              AND (
                CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND SAFE_CAST(
                  CASE
                    WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                           ,
                          a.actn_match_bad_wamt_adj * NUMERIC '1.000' / a.actn_match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                AND a.actn_match_txn_cnt >= 30
              ) THEN 1
              ELSE 0
            END
          END
          ELSE CASE
            WHEN r.crs_team IN (
              'gfr_omni_gms'    ,
              'gfr_omni_nextgen',
              'gfr_omni'        ,
              'goal_nextgen'    ,
              'goal_gms'
            ) THEN CASE
              WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
                WHEN CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND (
                  a.actn_cg_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.actn_cg_txn_wamt > 0 THEN CAST(
                        a.actn_cg_brm_bad_wamt / a.actn_cg_txn_wamt AS STRING
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                  OR a.actn_cg_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                               ,
                            a.incre_actn_decled_bad_amt * NUMERIC '1.000' / a.incre_actn_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                ) THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
                WHEN CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND (
                  a.actn_match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                           ,
                            a.actn_match_bad_wamt_adj * NUMERIC '1.000' / a.actn_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                  OR a.actn_match_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                               ,
                            a.incre_actn_decled_bad_amt * NUMERIC '1.000' / a.incre_actn_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                ) THEN 1
                ELSE 0
              END
              ELSE 0
            END
            ELSE CASE
              WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
              AND xoom_team.xoom_ntid IS NULL
              AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
              AND r.rule_id NOT IN (467812)
              AND (
                CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND SAFE_CAST(
                  CASE
                    WHEN a.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                           ,
                          a.actn_match_bad_wamt_adj * NUMERIC '1.000' / a.actn_match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                AND a.actn_match_txn_cnt >= 30
              ) THEN 1
              ELSE 0
            END
          END
        END + CASE
          WHEN CASE
            WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
            ELSE 0
          END = 1 THEN 0
          WHEN excl_accuracy.rule_id IS NOT NULL THEN 0
          WHEN r.rule_mo IN ('Collusion', 'UBSM') THEN 0
          WHEN st.is_strategy = 1
          AND (
            UPPER(r.rule_name) LIKE '%FALLBACK%'
            OR UPPER(r.tags) LIKE '%SAFETY%NET%'
          ) THEN CASE
            WHEN r.crs_team IN (
              'gfr_omni_gms'    ,
              'gfr_omni_nextgen',
              'gfr_omni'        ,
              'goal_nextgen'    ,
              'goal_gms'
            ) THEN CASE
              WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
                WHEN CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND (
                  a.str_cg_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.str_cg_txn_wamt > 0 THEN CAST(
                        a.str_cg_brm_bad_wamt / a.str_cg_txn_wamt AS STRING
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                  OR a.str_cg_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                             ,
                            a.incre_str_decled_bad_amt * NUMERIC '1.000' / a.incre_str_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                ) THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r.rule_mo)) = 'CC' THEN CASE
                WHEN CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND (
                  a.str_match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                         ,
                            a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                  OR a.str_match_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                             ,
                            a.incre_str_decled_bad_amt * NUMERIC '1.000' / a.incre_str_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                ) THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
                WHEN CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND (
                  a.str_match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                         ,
                            a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                ) THEN 1
                ELSE 0
              END
              ELSE 0
            END
            ELSE CASE
              WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
              AND xoom_team.xoom_ntid IS NULL
              AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
              AND r.rule_id NOT IN (467812)
              AND (
                CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND SAFE_CAST(
                  CASE
                    WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                         ,
                          a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                AND a.str_match_txn_cnt >= 30
              ) THEN 1
              ELSE 0
            END
          END
          ELSE CASE
            WHEN r.crs_team IN (
              'gfr_omni_gms'    ,
              'gfr_omni_nextgen',
              'gfr_omni'        ,
              'goal_nextgen'    ,
              'goal_gms'
            ) THEN CASE
              WHEN UPPER(RTRIM(r.rule_mo)) = 'ATO' THEN CASE
                WHEN CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND (
                  a.str_cg_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.str_cg_txn_wamt > 0 THEN CAST(
                        a.str_cg_brm_bad_wamt / a.str_cg_txn_wamt AS STRING
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                  OR a.str_cg_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                             ,
                            a.incre_str_decled_bad_amt * NUMERIC '1.000' / a.incre_str_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                ) THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r.rule_mo)) = 'CC' THEN CASE
                WHEN CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND (
                  a.str_match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                         ,
                            a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                  OR a.str_match_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                             ,
                            a.incre_str_decled_bad_amt * NUMERIC '1.000' / a.incre_str_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                ) THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE' THEN CASE
                WHEN CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND (
                  a.str_match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                         ,
                            a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                ) THEN 1
                ELSE 0
              END
              ELSE 0
            END
            ELSE CASE
              WHEN UPPER(RTRIM(r.rule_mo)) <> 'ABUSE'
              AND xoom_team.xoom_ntid IS NULL
              AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
              AND r.rule_id NOT IN (467812)
              AND (
                CASE
                  WHEN e2.rule_id IS NOT NULL
                  OR res_dis.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND a.action_type <> 1354
                AND SAFE_CAST(
                  CASE
                    WHEN a.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                         ,
                          a.str_match_bad_wamt_adj * NUMERIC '1.000' / a.str_match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                AND a.str_match_txn_cnt >= 30
              ) THEN 1
              ELSE 0
            END
          END
        END > 0 THEN 1
        ELSE 0
      END = 1
      OR CASE
        WHEN CASE
          WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
          ELSE 0
        END = 0
        AND SAFE_CAST(b.ov_pct_1 AS BIGNUMERIC) >= NUMERIC '0.8'
        AND CASE
          WHEN e2.rule_id IS NOT NULL
          OR res_dis.rule_id IS NOT NULL THEN 1
          ELSE 0
        END = 0
        AND a.action_type <> 1354 THEN CASE
          WHEN UPPER(r.tags) LIKE '%AUTOSOLUTION%'
          AND UPPER(r.tags) LIKE '%HYPERNET MERCHANT%'
          AND COALESCE(st_ov1.is_strategy_ov1_raw, 0) = 1 THEN 1
          WHEN UPPER(r.tags) LIKE '%AUTOSOLUTION%'
          AND UPPER(r.tags) LIKE '%HYPERNET MERCHANT%'
          AND COALESCE(st_ov1.is_strategy_ov1_raw, 0) = 0 THEN 0
          ELSE 1
        END
        ELSE 0
      END = 1
      OR CASE
        WHEN CASE
          WHEN e_ow.rule_id IS NOT NULL THEN 1
          ELSE 0
        END = 0
        AND SAFE_CAST(
          CASE
            WHEN a.fire_vol > 0 THEN REGEXP_REPLACE(
              REGEXP_REPLACE(
                FORMAT(
                  '%6.3f'                                    ,
                  a.sum_wh_actn * NUMERIC '1.000' / a.fire_vol
                )                   ,
                r'^( *?)(-)?0(\..*)',
                r'\2\3 \1'
              )                     ,
              r'^( *?)(-)?(\d*\..*)',
              r'\2\3\1'
            )
            ELSE 'NA'
          END AS BIGNUMERIC
        ) >= NUMERIC '0.7'
        AND SAFE_CAST(
          CASE
            WHEN a.whed_amt > 0 THEN REGEXP_REPLACE(
              REGEXP_REPLACE(
                FORMAT(
                  '%6.3f'                                    ,
                  a.bad_amt_adj * NUMERIC '1.000' / a.whed_amt
                )                   ,
                r'^( *?)(-)?0(\..*)',
                r'\2\3 \1'
              )                     ,
              r'^( *?)(-)?(\d*\..*)',
              r'\2\3\1'
            )
            ELSE 'NA'
          END AS BIGNUMERIC
        ) < NUMERIC '0.1'
        AND a.whed_cnt >= 30
        AND UPPER(r.tags) NOT LIKE '%%GOOD%%POP%%AF%%' THEN 1
        ELSE 0
      END = 1
      OR COALESCE(
        CASE
          WHEN CASE
            WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
            ELSE 0
          END = 1 THEN 0
          ELSE COALESCE(cntct.high_cntct_rate_, 0)
        END,
        0
      ) = 1
    ) THEN 2
    WHEN st.is_strategy = 0
    AND CASE
      WHEN (
        a.fire_vol < 10 * r.observing_window
        AND (
          SAFE_CAST(
            CASE
              WHEN a.cg_txn_wamt > 0 THEN CAST(
                a.cg_brm_bad_wamt / a.cg_txn_wamt AS STRING
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.1'
          OR RTRIM(
            CASE
              WHEN a.cg_txn_wamt > 0 THEN CAST(
                a.cg_brm_bad_wamt / a.cg_txn_wamt AS STRING
              )
              ELSE 'NA'
            END
          ) = 'NA'
        )
        OR CASE
          WHEN e2.rule_id IS NOT NULL
          OR res_dis.rule_id IS NOT NULL THEN 1
          ELSE 0
        END = 0
        AND a.action_type <> 1354
        AND a.incre_rule_fire_vol < 5 * r.observing_window
      )
      AND CASE
        WHEN UPPER(RTRIM(r.rule_mo)) = 'BUSINESSPOLICY' THEN 1
        ELSE 0
      END = 0
      AND CASE
        WHEN e.rule_id IS NOT NULL THEN 1
        ELSE 0
      END = 0 THEN 1
      ELSE 0
    END = 1 THEN 3
  END AS tier
FROM
  (
    SELECT
      decline_metrics.rule_id                    ,
      decline_metrics.action_type                ,
      decline_metrics.monitoring_window          ,
      decline_metrics.fire_vol                   ,
      decline_metrics.u2u_fire_vol               ,
      decline_metrics.fire_amt                   ,
      decline_metrics.cg_txn_cnt                 ,
      decline_metrics.cg_txn_wcnt                ,
      decline_metrics.cg_txn_amt                 ,
      decline_metrics.cg_txn_wamt                ,
      decline_metrics.cg_brm_bad_wcnt            ,
      decline_metrics.cg_brm_bad_wcnt_adj        ,
      decline_metrics.cg_brm_bad_wamt            ,
      decline_metrics.cg_brm_bad_wamt_adj        ,
      decline_metrics.str_cg_txn_cnt             ,
      decline_metrics.str_cg_txn_wcnt            ,
      decline_metrics.str_cg_txn_amt             ,
      decline_metrics.str_cg_txn_wamt            ,
      decline_metrics.str_cg_brm_bad_wcnt        ,
      decline_metrics.str_cg_brm_bad_wcnt_adj    ,
      decline_metrics.str_cg_brm_bad_wamt        ,
      decline_metrics.str_cg_brm_bad_wamt_adj    ,
      decline_metrics.actn_cg_txn_cnt            ,
      decline_metrics.actn_cg_txn_wcnt           ,
      decline_metrics.actn_cg_txn_amt            ,
      decline_metrics.actn_cg_txn_wamt           ,
      decline_metrics.actn_cg_brm_bad_wcnt       ,
      decline_metrics.actn_cg_brm_bad_wcnt_adj   ,
      decline_metrics.actn_cg_brm_bad_wamt       ,
      decline_metrics.actn_cg_brm_bad_wamt_adj   ,
      decline_metrics.match_txn_cnt              ,
      decline_metrics.match_txn_wcnt             ,
      decline_metrics.match_txn_amt              ,
      decline_metrics.match_txn_wamt             ,
      decline_metrics.match_bad_wcnt             ,
      decline_metrics.match_bad_wcnt_adj         ,
      decline_metrics.match_bad_wamt             ,
      decline_metrics.match_bad_wamt_adj         ,
      decline_metrics.str_match_txn_cnt          ,
      decline_metrics.str_match_txn_wcnt         ,
      decline_metrics.str_match_txn_amt          ,
      decline_metrics.str_match_txn_wamt         ,
      decline_metrics.str_match_bad_wcnt         ,
      decline_metrics.str_match_bad_wcnt_adj     ,
      decline_metrics.str_match_bad_wamt         ,
      decline_metrics.str_match_bad_wamt_adj     ,
      decline_metrics.actn_match_txn_cnt         ,
      decline_metrics.actn_match_txn_wcnt        ,
      decline_metrics.actn_match_txn_amt         ,
      decline_metrics.actn_match_txn_wamt        ,
      decline_metrics.actn_match_bad_wcnt        ,
      decline_metrics.actn_match_bad_wcnt_adj    ,
      decline_metrics.actn_match_bad_wamt        ,
      decline_metrics.actn_match_bad_wamt_adj    ,
      decline_metrics.sum_wh_actn                ,
      decline_metrics.whed_cnt                   ,
      decline_metrics.whed_amt                   ,
      decline_metrics.bad_amt_adj                ,
      decline_metrics.incre_str_fire_vol         ,
      decline_metrics.incre_rule_fire_vol        ,
      decline_metrics.incre_actn_fire_vol        ,
      decline_metrics.catch_bad                  ,
      decline_metrics.incre_str_fire_amt         ,
      decline_metrics.incre_str_non_whed_fire_amt,
      decline_metrics.decled_cnt                 ,
      decline_metrics.decled_amt                 ,
      decline_metrics.decled_bad_cnt             ,
      decline_metrics.decled_bad_amt             ,
      decline_metrics.incre_str_decled_cnt       ,
      decline_metrics.incre_str_decled_amt       ,
      decline_metrics.incre_str_decled_bad_cnt   ,
      decline_metrics.incre_str_decled_bad_amt   ,
      decline_metrics.incre_actn_decled_cnt      ,
      decline_metrics.incre_actn_decled_amt      ,
      decline_metrics.incre_actn_decled_bad_cnt  ,
      decline_metrics.incre_actn_decled_bad_amt
    FROM
      `pypl-edw.${dataset_name_tmp}.decline_metrics` AS decline_metrics
    WHERE
      decline_metrics.rule_id NOT IN (
        SELECT
          rule_id
        FROM
          `pypl-edw.${dataset_name_tmp}.auth_flow_pld_pair`
      )
  ) AS a
  INNER JOIN `pypl-edw.${dataset_name_tmp}.check_action_rule_id` AS r ON a.rule_id = r.rule_id
  LEFT OUTER JOIN (
    SELECT
      UPPER(xoom_team_list.ntid) AS xoom_ntid
    FROM
      `pypl-edw`.pp_risk_crs_core.xoom_team_list
    WHERE
      xoom_team_list.is_effective = 1
    GROUP BY
      1
  ) AS xoom_team ON UPPER(RTRIM(r.owner_ntid)) = UPPER(RTRIM(xoom_team.xoom_ntid))
  LEFT OUTER JOIN `pypl-edw.${dataset_name_tmp}.delta` AS d ON a.rule_id = d.rule_id
  AND a.action_type = d.action_type
  LEFT OUTER JOIN `pypl-edw.${dataset_name_tmp}.overlaprule` AS b ON a.rule_id = b.rule_id
  AND a.action_type = b.action_type
  LEFT OUTER JOIN (
    SELECT
      --  left join ${decline_gms} dg
      --  on a.rule_id=dg.rule_id and a.action_type=dg.action_type
      rule_mo_table.rule_id                          ,
      rule_mo_table.is_strategy AS is_strategy_ov1_raw
    FROM
      `pypl-edw`.pp_risk_crs_core.rule_mo_uni AS rule_mo_table
    QUALIFY
      ROW_NUMBER() OVER (
        PARTITION BY
          rule_mo_table.rule_id
        ORDER BY
          rule_mo_table.baseline DESC
      ) = 1
  ) AS st_ov1 ON --safecast or replace -1. with -1 or instd of int64 use bignumeric/numeric/decimal
  CAST(
    REPLACE(b.ov_rule_1, 'NA', '-1.') AS BIGNUMERIC
  ) = st_ov1.rule_id
  LEFT OUTER JOIN (
    SELECT
      *
    FROM
      `pypl-edw.${dataset_name_tmp}.urm_lite` AS live_metadata_table
    WHERE
      UPPER(live_metadata_table.tags) LIKE '%%RISK%%APPETITE%%'
      OR UPPER(live_metadata_table.tags) LIKE '%%SAFETY%%NET%%'
      OR UPPER(live_metadata_table.tags) LIKE '%%BLACK%%LIST%%'
      OR UPPER(live_metadata_table.tags) LIKE '%%BLOCK%%LIST%%'
      OR UPPER(live_metadata_table.tags) LIKE '%%LATAM%%IPR%%'
      OR UPPER(live_metadata_table.tags) LIKE '%%BUSINESS%%REQUEST%%'
      OR UPPER(live_metadata_table.tags) LIKE '%TESTING%'
      OR UPPER(live_metadata_table.tags) LIKE '%%MARKET%%RAMPING%%'
      OR UPPER(live_metadata_table.tags) LIKE '%VEDA%'
      OR UPPER(live_metadata_table.tags) LIKE '%VSDS%'
      OR UPPER(live_metadata_table.tags) LIKE '%SPEEDSTER%'
      OR UPPER(live_metadata_table.tags) LIKE '%HIGHCONTACTCOST%'
      OR UPPER(live_metadata_table.tags) LIKE '%NNAENABLEMENT%'
      OR UPPER(live_metadata_table.tags) LIKE '%RED%CARPET%'
      OR UPPER(live_metadata_table.tags) LIKE '%STRATEGYFRAMEWORK%'
  ) AS e ON a.rule_id = e.rule_id
  LEFT OUTER JOIN (
    SELECT
      *
    FROM
      `pypl-edw.${dataset_name_tmp}.urm_lite` AS live_metadata_table_0
    WHERE
      UPPER(live_metadata_table_0.tags) LIKE '%UELV_YOUNG_GUEST%'
      OR live_metadata_table_0.rule_id IN (
        607986,
        609172,
        612391,
        625094,
        612323,
        788770,
        805167,
        825037,
        876060,
        803134,
        803094,
        876060,
        877503,
        523010
      )
      OR UPPER(live_metadata_table_0.tags) LIKE '%NEXT OPEN LOOP%'
      OR UPPER(live_metadata_table_0.tags) LIKE '%GMS UCC CBP%'
  ) AS excl_accuracy ON a.rule_id = excl_accuracy.rule_id
  LEFT OUTER JOIN (
    SELECT
      rule_list_stage_2.rule_id            ,
      rule_list_stage_2.priority           ,
      rule_list_stage_2.rule_name          ,
      rule_list_stage_2.control_group      ,
      rule_list_stage_2.crs_team           ,
      rule_list_stage_2.owner_ntid         ,
      rule_list_stage_2.rule_mo            ,
      rule_list_stage_2.tags               ,
      rule_list_stage_2.last_release_length,
      rule_list_stage_2.observing_window
    FROM
      `pypl-edw.${dataset_name_tmp}.check_action_rule_id` AS rule_list_stage_2
    WHERE
      UPPER(rule_list_stage_2.tags) LIKE '%%SCRIPT%%ATTACK%%'
      OR UPPER(rule_list_stage_2.tags) LIKE '%%SUPER%%DECLINE%%'
      OR UPPER(rule_list_stage_2.tags) LIKE '%%BILLING%%AGREEMENT%%'
      OR UPPER(rule_list_stage_2.tags) LIKE '%PURE%MODEL%STRATEGY%'
      AND UPPER(rule_list_stage_2.crs_team) LIKE ANY ('GFR_OMNI_GMS%', 'GOAL_GMS')
      OR UPPER(rule_list_stage_2.tags) LIKE '%%SAFETY%%NET%%'
  ) AS e2 ON a.rule_id = e2.rule_id
  LEFT OUTER JOIN (
    SELECT
      live_metadata_table_1.rule_id
    FROM
      `pypl-edw.${dataset_name_tmp}.urm_lite` AS live_metadata_table_1
    WHERE
      UPPER(live_metadata_table_1.tags) LIKE '%INTERNAL TEST%'
      OR UPPER(live_metadata_table_1.tags) LIKE '%GOOD%POP%AF%'
  ) AS e_ow ON a.rule_id = e_ow.rule_id
  LEFT OUTER JOIN (
    SELECT
      --  2021-07-28: For Yanping's feedback on AF test rules
      rule_mo_table_0.rule_id   ,
      rule_mo_table_0.is_strategy
    FROM
      `pypl-edw`.pp_risk_crs_core.rule_mo_uni AS rule_mo_table_0
    QUALIFY
      ROW_NUMBER() OVER (
        PARTITION BY
          rule_mo_table_0.rule_id
        ORDER BY
          rule_mo_table_0.baseline DESC
      ) = 1
  ) AS st ON CAST(a.rule_id AS INT64) = st.rule_id
  LEFT OUTER JOIN (
    SELECT
      rule_id
    FROM
      `pypl-edw.${dataset_name_tmp}.restrict_disallow_pair`
  ) AS res_dis ON a.rule_id = res_dis.rule_id
  LEFT OUTER JOIN `pypl-edw.${dataset_name_tmp}.contact_metrics` AS cntct ON a.rule_id = cntct.rule_id
  AND a.action_type = cntct.action_type
  CROSS JOIN (
    UNNEST (
      ARRAY[
        CASE
          WHEN a.cg_txn_wcnt > 0 THEN CAST(
            a.cg_brm_bad_wcnt / a.cg_txn_wcnt AS STRING
          )
          ELSE 'NA'
        END
      ]
    )
  ) AS cg_wcnt_bad_rate
UNION ALL
SELECT
  -- -------------------------------------af_pld_pair rules----------------------------------------
  r_0.crs_team           ,
  r_0.owner_ntid         ,
  r_0.rule_id            ,
  2998 AS action_type    ,
  a_0.monitoring_window  ,
  r_0.observing_window   ,
  r_0.rule_mo            ,
  r_0.rule_name          ,
  a_0.fire_vol           ,
  a_0.u2u_fire_vol       ,
  a_0.fire_amt           ,
  a_0.cg_txn_cnt         ,
  a_0.cg_txn_wcnt        ,
  a_0.cg_txn_amt         ,
  a_0.cg_txn_wamt        ,
  a_0.cg_brm_bad_wcnt    ,
  a_0.cg_brm_bad_wcnt_adj,
  a_0.cg_brm_bad_wamt    ,
  a_0.cg_brm_bad_wamt_adj,
  CASE
    WHEN a_0.cg_txn_wcnt > 0 THEN CAST(
      a_0.cg_brm_bad_wcnt / a_0.cg_txn_wcnt AS STRING
    )
    ELSE 'NA'
  END AS cg_wcnt_bad_rate,
  CASE
    WHEN a_0.cg_txn_wamt > 0 THEN CAST(
      a_0.cg_brm_bad_wamt / a_0.cg_txn_wamt AS STRING
    )
    ELSE 'NA'
  END AS cg_wamt_bad_rate,
  CASE
    WHEN a_0.cg_txn_wcnt > 0 THEN CAST(
      a_0.cg_brm_bad_wcnt_adj / a_0.cg_txn_wcnt AS STRING
    )
    ELSE 'NA'
  END AS cg_wcnt_bad_rate_adj,
  CASE
    WHEN a_0.cg_txn_wamt > 0 THEN CAST(
      a_0.cg_brm_bad_wamt_adj / a_0.cg_txn_wamt AS STRING
    )
    ELSE 'NA'
  END AS cg_wamt_bad_rate_adj,
  a_0.str_cg_txn_cnt         ,
  a_0.str_cg_txn_wcnt        ,
  a_0.str_cg_txn_amt         ,
  a_0.str_cg_txn_wamt        ,
  a_0.str_cg_brm_bad_wcnt    ,
  a_0.str_cg_brm_bad_wcnt_adj,
  a_0.str_cg_brm_bad_wamt    ,
  a_0.str_cg_brm_bad_wamt_adj,
  CASE
    WHEN a_0.str_cg_txn_wcnt > 0 THEN CAST(
      a_0.str_cg_brm_bad_wcnt / a_0.str_cg_txn_wcnt AS STRING
    )
    ELSE 'NA'
  END AS str_cg_wcnt_bad_rate,
  CASE
    WHEN a_0.str_cg_txn_wamt > 0 THEN CAST(
      a_0.str_cg_brm_bad_wamt / a_0.str_cg_txn_wamt AS STRING
    )
    ELSE 'NA'
  END AS str_cg_wamt_bad_rate                     ,
  -- ------------used for gms/nextgen--------------
  CASE
    WHEN a_0.str_cg_txn_wcnt > 0 THEN CAST(
      a_0.str_cg_brm_bad_wcnt_adj / a_0.str_cg_txn_wcnt AS STRING
    )
    ELSE 'NA'
  END AS str_cg_wcnt_bad_rate_adj,
  CASE
    WHEN a_0.str_cg_txn_wamt > 0 THEN CAST(
      a_0.str_cg_brm_bad_wamt_adj / a_0.str_cg_txn_wamt AS STRING
    )
    ELSE 'NA'
  END AS str_cg_wamt_bad_rate_adj,
  a_0.actn_cg_txn_cnt            ,
  a_0.actn_cg_txn_wcnt           ,
  a_0.actn_cg_txn_amt            ,
  a_0.actn_cg_txn_wamt           ,
  a_0.actn_cg_brm_bad_wcnt       ,
  a_0.actn_cg_brm_bad_wcnt_adj   ,
  a_0.actn_cg_brm_bad_wamt       ,
  a_0.actn_cg_brm_bad_wamt_adj   ,
  CASE
    WHEN a_0.actn_cg_txn_wcnt > 0 THEN CAST(
      a_0.actn_cg_brm_bad_wcnt / a_0.actn_cg_txn_wcnt AS STRING
    )
    ELSE 'NA'
  END AS actn_cg_wcnt_bad_rate,
  CASE
    WHEN a_0.actn_cg_txn_wamt > 0 THEN CAST(
      a_0.actn_cg_brm_bad_wamt / a_0.actn_cg_txn_wamt AS STRING
    )
    ELSE 'NA'
  END AS actn_cg_wamt_bad_rate                    ,
  -- ------------used for gms/nextgen--------------
  CASE
    WHEN a_0.actn_cg_txn_wcnt > 0 THEN CAST(
      a_0.actn_cg_brm_bad_wcnt_adj / a_0.actn_cg_txn_wcnt AS STRING
    )
    ELSE 'NA'
  END AS actn_cg_wcnt_bad_rate_adj,
  CASE
    WHEN a_0.actn_cg_txn_wamt > 0 THEN CAST(
      a_0.actn_cg_brm_bad_wamt_adj / a_0.actn_cg_txn_wamt AS STRING
    )
    ELSE 'NA'
  END AS actn_cg_wamt_bad_rate_adj,
  a_0.match_txn_cnt               ,
  a_0.match_txn_wcnt              ,
  a_0.match_txn_amt               ,
  a_0.match_txn_wamt              ,
  a_0.match_bad_wcnt              ,
  a_0.match_bad_wcnt_adj          ,
  a_0.match_bad_wamt              ,
  a_0.match_bad_wamt_adj          ,
  CASE
    WHEN a_0.match_txn_wcnt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                                 ,
          a_0.match_bad_wcnt * NUMERIC '1.000' / a_0.match_txn_wcnt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS match_wcnt_bad_rate,
  CASE
    WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                                 ,
          a_0.match_bad_wamt * NUMERIC '1.000' / a_0.match_txn_wamt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS match_wamt_bad_rate,
  CASE
    WHEN a_0.match_txn_wcnt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                                     ,
          a_0.match_bad_wcnt_adj * NUMERIC '1.000' / a_0.match_txn_wcnt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS match_wcnt_bad_rate_adj,
  CASE
    WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                                     ,
          a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS match_wamt_bad_rate_adj                                      ,
  -- ---------------used for collusion and other teams-----------------
  CASE
    WHEN a_0.match_bad_wamt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                                     ,
          a_0.match_txn_wamt * NUMERIC '1.000' / a_0.match_bad_wamt - 1
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS match_wamt_fpr,
  CASE
    WHEN a_0.match_bad_wamt_adj > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                                         ,
          a_0.match_txn_wamt * NUMERIC '1.000' / a_0.match_bad_wamt_adj - 1
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS match_wamt_fpr_adj ,
  a_0.str_match_txn_cnt     ,
  a_0.str_match_txn_wcnt    ,
  a_0.str_match_txn_amt     ,
  a_0.str_match_txn_wamt    ,
  a_0.str_match_bad_wcnt    ,
  a_0.str_match_bad_wcnt_adj,
  a_0.str_match_bad_wamt    ,
  a_0.str_match_bad_wamt_adj,
  CASE
    WHEN a_0.str_match_txn_wcnt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                                         ,
          a_0.str_match_bad_wcnt * NUMERIC '1.000' / a_0.str_match_txn_wcnt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS str_match_wcnt_bad_rate,
  CASE
    WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                                         ,
          a_0.str_match_bad_wamt * NUMERIC '1.000' / a_0.str_match_txn_wamt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS str_match_wamt_bad_rate,
  CASE
    WHEN a_0.str_match_txn_wcnt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                                             ,
          a_0.str_match_bad_wcnt_adj * NUMERIC '1.000' / a_0.str_match_txn_wcnt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS str_match_wcnt_bad_rate_adj,
  CASE
    WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                                             ,
          a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS str_match_wamt_bad_rate_adj                                               ,
  -- ---------------used for non-collusion and non-gms/nextgen teams----------------
  a_0.actn_match_txn_cnt     ,
  a_0.actn_match_txn_wcnt    ,
  a_0.actn_match_txn_amt     ,
  a_0.actn_match_txn_wamt    ,
  a_0.actn_match_bad_wcnt    ,
  a_0.actn_match_bad_wcnt_adj,
  a_0.actn_match_bad_wamt    ,
  a_0.actn_match_bad_wamt_adj,
  CASE
    WHEN a_0.actn_match_txn_wcnt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                                           ,
          a_0.actn_match_bad_wcnt * NUMERIC '1.000' / a_0.actn_match_txn_wcnt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS actn_match_wcnt_bad_rate,
  CASE
    WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                                           ,
          a_0.actn_match_bad_wamt * NUMERIC '1.000' / a_0.actn_match_txn_wamt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS actn_match_wamt_bad_rate,
  CASE
    WHEN a_0.actn_match_txn_wcnt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                                               ,
          a_0.actn_match_bad_wcnt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wcnt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS actn_match_wcnt_bad_rate_adj,
  CASE
    WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                                               ,
          a_0.actn_match_bad_wamt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wamt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS actn_match_wamt_bad_rate_adj                                              ,
  -- ---------------used for non-collusion and non-gms/nextgen teams----------------
  d_0.delta_bad_cnt                    ,
  d_0.delta_txn_cnt                    ,
  d_0.delta_bad_amt                    ,
  d_0.delta_txn_amt                    ,
  d_0.delta_cnt_bad_rate               ,
  d_0.delta_amt_bad_rate               ,
  --  dg.cnt_total as decline_cnt_total,
  --  dg.amt_total as decline_amt_total,
  --  dg.bad_wcnt as decline_bad_wcnt  ,
  --  dg.bad_wamt as decline_bad_wamt  ,
  --  decline_cnt_bad_rate             ,
  --  decline_amt_bad_rate             ,
  a_0.decled_cnt                       ,
  a_0.decled_amt                       ,
  a_0.decled_bad_cnt                   ,
  a_0.decled_bad_amt                   ,
  a_0.incre_str_decled_cnt             ,
  a_0.incre_str_decled_amt             ,
  a_0.incre_str_decled_bad_cnt         ,
  a_0.incre_str_decled_bad_amt         ,
  a_0.incre_actn_decled_cnt            ,
  a_0.incre_actn_decled_amt            ,
  a_0.incre_actn_decled_bad_cnt        ,
  a_0.incre_actn_decled_bad_amt        ,
  CASE
    WHEN a_0.decled_cnt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                             ,
          a_0.decled_bad_cnt * NUMERIC '1.000' / a_0.decled_cnt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS decline_cnt_bad_rate,
  CASE
    WHEN a_0.decled_amt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                             ,
          a_0.decled_bad_amt * NUMERIC '1.000' / a_0.decled_amt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS decline_amt_bad_rate,
  CASE
    WHEN a_0.incre_str_decled_cnt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                                                 ,
          a_0.incre_str_decled_bad_cnt * NUMERIC '1.000' / a_0.incre_str_decled_cnt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS incre_str_decline_cnt_bad_rate,
  CASE
    WHEN a_0.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                                                 ,
          a_0.incre_str_decled_bad_amt * NUMERIC '1.000' / a_0.incre_str_decled_amt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS incre_str_decline_amt_bad_rate,
  CASE
    WHEN a_0.incre_actn_decled_cnt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                                                   ,
          a_0.incre_actn_decled_bad_cnt * NUMERIC '1.000' / a_0.incre_actn_decled_cnt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS incre_actn_decline_cnt_bad_rate,
  CASE
    WHEN a_0.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                                                   ,
          a_0.incre_actn_decled_bad_amt * NUMERIC '1.000' / a_0.incre_actn_decled_amt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS incre_actn_decline_amt_bad_rate   ,
  --'NA' as decline_cnt_bad_rate           ,
  --'NA' as decline_amt_bad_rate           ,
  --'NA' as incre_str_decline_cnt_bad_rate ,
  --'NA' as incre_str_decline_amt_bad_rate ,
  --'NA' as incre_actn_decline_cnt_bad_rate,
  --'NA' as incre_actn_decline_amt_bad_rate,
  a_0.sum_wh_actn                          ,
  CASE
    WHEN a_0.fire_vol > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                        ,
          a_0.sum_wh_actn * NUMERIC '1.000' / a_0.fire_vol
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS whed_pct,
  a_0.whed_cnt   ,
  a_0.whed_amt   ,
  a_0.bad_amt_adj,
  CASE
    WHEN a_0.whed_amt > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                        ,
          a_0.bad_amt_adj * NUMERIC '1.000' / a_0.whed_amt
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS wh_badrate      ,
  a_0.incre_str_fire_vol ,
  a_0.incre_rule_fire_vol,
  a_0.incre_actn_fire_vol,
  CASE
    WHEN a_0.fire_vol > 0 THEN REGEXP_REPLACE(
      REGEXP_REPLACE(
        FORMAT(
          '%6.3f'                                                    ,
          1 - a_0.incre_rule_fire_vol * NUMERIC '1.000' / a_0.fire_vol
        )                   ,
        r'^( *?)(-)?0(\..*)',
        r'\2\3 \1'
      )                     ,
      r'^( *?)(-)?(\d*\..*)',
      r'\2\3\1'
    )
    ELSE 'NA'
  END AS ov_pct                                  ,
  a_0.catch_bad                                  ,
  a_0.incre_str_fire_amt                         ,
  a_0.incre_str_non_whed_fire_amt                ,
  b_0.ov_rule_1                                  ,
  b_0.ov_pct_1                                   ,
  b_0.ov_ntid_1                                  ,
  b_0.ov_rule_2                                  ,
  b_0.ov_pct_2                                   ,
  b_0.ov_ntid_2                                  ,
  b_0.ov_rule_3                                  ,
  b_0.ov_pct_3                                   ,
  b_0.ov_ntid_3                                  ,
  b_0.ov_rule_4                                  ,
  b_0.ov_pct_4                                   ,
  b_0.ov_ntid_4                                  ,
  COALESCE(cntct_0.decline_cnt, 0) AS decline_cnt,
  COALESCE(cntct_0.cntct_cnt, 0) AS cntct_cnt    ,
  COALESCE(
    cntct_0.cntct_rate,
    CAST(0 AS FLOAT64)
  ) AS cntct_rate,
  CASE
    WHEN CASE
      WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
      ELSE 0
    END = 1 THEN 0
    ELSE COALESCE(cntct_0.high_cntct_rate_, 0)
  END AS high_cntct_rate,
  CASE
    WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
    ELSE 0
  END AS business_policy,
  CASE
    WHEN e_0.rule_id IS NOT NULL THEN 1
    ELSE 0
  END AS exclusion_flg,
  CASE
    WHEN e2_0.rule_id IS NOT NULL THEN 1
    ELSE 0
  END AS incre_exempt   ,
  0 AS excl_high_overlap,
  CASE
    WHEN e_ow_0.rule_id IS NOT NULL THEN 1
    ELSE 0
  END AS excl_over_whitelisted,
  CASE
    WHEN st_0.rule_id IS NOT NULL THEN st_0.is_strategy
    ELSE 0
  END AS is_strategy,
  COALESCE(
    st_ov1_0.is_strategy_ov1_raw,
    0
  ) AS is_strategy_ov1,
  CASE
    WHEN (
      a_0.fire_vol < 10 * r_0.observing_window
      AND (
        SAFE_CAST(
          CASE
            WHEN a_0.cg_txn_wamt > 0 THEN CAST(
              a_0.cg_brm_bad_wamt / a_0.cg_txn_wamt AS STRING
            )
            ELSE 'NA'
          END AS BIGNUMERIC
        ) < NUMERIC '0.1'
        OR RTRIM(
          CASE
            WHEN a_0.cg_txn_wamt > 0 THEN CAST(
              a_0.cg_brm_bad_wamt / a_0.cg_txn_wamt AS STRING
            )
            ELSE 'NA'
          END
        ) = 'NA'
      )
      OR CASE
        WHEN e2_0.rule_id IS NOT NULL THEN 1
        ELSE 0
      END = 0
      AND a_0.incre_rule_fire_vol < 5 * r_0.observing_window
    )
    AND CASE
      WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
      ELSE 0
    END = 0
    AND CASE
      WHEN e_0.rule_id IS NOT NULL THEN 1
      ELSE 0
    END = 0 THEN 1
    ELSE 0
  END AS low_firing,
  CASE
    WHEN CASE
      WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
      ELSE 0
    END = 0
    AND SAFE_CAST(b_0.ov_pct_1 AS BIGNUMERIC) >= NUMERIC '0.8'
    AND CASE
      WHEN e2_0.rule_id IS NOT NULL THEN 1
      ELSE 0
    END = 0 THEN CASE
      WHEN r_0.tags LIKE '%AUTOSOLUTION%'
      AND r_0.tags LIKE '%HYPERNET MERCHANT%'
      AND COALESCE(
        st_ov1_0.is_strategy_ov1_raw,
        0
      ) = 1 THEN 1
      WHEN r_0.tags LIKE '%AUTOSOLUTION%'
      AND r_0.tags LIKE '%HYPERNET MERCHANT%'
      AND COALESCE(
        st_ov1_0.is_strategy_ov1_raw,
        0
      ) = 0 THEN 0
      ELSE 1
    END
    ELSE 0
  END AS high_overlap,
  CASE
    WHEN CASE
      WHEN e_ow_0.rule_id IS NOT NULL THEN 1
      ELSE 0
    END = 0
    AND SAFE_CAST(
      CASE
        WHEN a_0.fire_vol > 0 THEN REGEXP_REPLACE(
          REGEXP_REPLACE(
            FORMAT(
              '%6.3f'                                        ,
              a_0.sum_wh_actn * NUMERIC '1.000' / a_0.fire_vol
            )                   ,
            r'^( *?)(-)?0(\..*)',
            r'\2\3 \1'
          )                     ,
          r'^( *?)(-)?(\d*\..*)',
          r'\2\3\1'
        )
        ELSE 'NA'
      END AS BIGNUMERIC
    ) >= NUMERIC '0.7'
    AND SAFE_CAST(
      CASE
        WHEN a_0.whed_amt > 0 THEN REGEXP_REPLACE(
          REGEXP_REPLACE(
            FORMAT(
              '%6.3f'                                        ,
              a_0.bad_amt_adj * NUMERIC '1.000' / a_0.whed_amt
            )                   ,
            r'^( *?)(-)?0(\..*)',
            r'\2\3 \1'
          )                     ,
          r'^( *?)(-)?(\d*\..*)',
          r'\2\3\1'
        )
        ELSE 'NA'
      END AS BIGNUMERIC
    ) < NUMERIC '0.1'
    AND a_0.whed_cnt >= 30 THEN 1
    ELSE 0
  END AS over_whitelisted                                                                                                   ,
  --  CASE WHEN business_policy=0 and r.crs_team IN ('gfr_omni_gms', 'gfr_omni_nextgen', 'gfr_omni') AND r.rule_mo='ATO' Then
  --          case when To_Number(cg_wamt_bad_rate)<0.15 AND cg_txn_cnt>=30 THEN 1 ELSE 0 END
  --       WHEN business_policy=0 and r.rule_mo in ('Collusion' ,'UBSM')  and xoom_team.xoom_ntid is null
  --        then case when To_Number(match_wamt_bad_rate_adj)<0.15 AND match_txn_cnt>=30
  --        THEN 1 ELSE 0 END
  --       ELSE CASE WHEN business_policy=0 and  r.rule_mo<>'Abuse' and xoom_team.xoom_ntid is null  and upper(r.tags) not like '%%good%%pop%%af%%'  and r.rule_id not in (467812) and  (To_Number(match_wamt_bad_rate_adj)<0.15 AND match_txn_cnt>=30)
  --        THEN 1 ELSE 0 END
  --        END AS sub_poor_accuracy_bad_rate,
  CASE
    WHEN CASE
      WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
      ELSE 0
    END = 1 THEN 0
    WHEN excl_accuracy_0.rule_id IS NOT NULL THEN 0
    WHEN st_0.is_strategy = 1
    AND (
      UPPER(r_0.rule_name) LIKE '%FALLBACK%'
      OR UPPER(r_0.tags) LIKE '%SAFETY%NET%'
    ) THEN CASE
      WHEN r_0.crs_team IN (
        'gfr_omni_gms'    ,
        'gfr_omni_nextgen',
        'gfr_omni'        ,
        'goal_nextgen'    ,
        'goal_gms'
      ) THEN CASE
        WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
          WHEN a_0.cg_txn_cnt >= 30
          AND SAFE_CAST(
            CASE
              WHEN a_0.cg_txn_wamt > 0 THEN CAST(
                a_0.cg_brm_bad_wamt / a_0.cg_txn_wamt AS STRING
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.10'
          OR a_0.cg_txn_cnt < 30
          AND SAFE_CAST(
            CASE
              WHEN a_0.decled_amt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                             ,
                    a_0.decled_bad_amt * NUMERIC '1.000' / a_0.decled_amt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.10' THEN 1
          ELSE 0
        END
        WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN CASE
          WHEN a_0.match_txn_cnt >= 30
          AND SAFE_CAST(
            CASE
              WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                     ,
                    a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.10' THEN 1
          ELSE 0
        END
        WHEN UPPER(RTRIM(r_0.rule_mo)) = 'CC' THEN CASE
          WHEN a_0.match_txn_cnt >= 30
          AND SAFE_CAST(
            CASE
              WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                     ,
                    a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.10'
          OR a_0.match_txn_cnt < 30
          AND SAFE_CAST(
            CASE
              WHEN a_0.decled_amt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                             ,
                    a_0.decled_bad_amt * NUMERIC '1.000' / a_0.decled_amt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.10' THEN 1
          ELSE 0
        END
        WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
          WHEN a_0.match_txn_cnt >= 30
          AND SAFE_CAST(
            CASE
              WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                     ,
                    a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.10' THEN 1
          ELSE 0
        END
        ELSE 0
      END
      WHEN r_0.rule_mo IN ('Collusion', 'UBSM')
      AND xoom_team_0.xoom_ntid IS NULL THEN CASE
        WHEN SAFE_CAST(
          CASE
            WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
              REGEXP_REPLACE(
                FORMAT(
                  '%6.3f'                                                     ,
                  a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                )                   ,
                r'^( *?)(-)?0(\..*)',
                r'\2\3 \1'
              )                     ,
              r'^( *?)(-)?(\d*\..*)',
              r'\2\3\1'
            )
            ELSE 'NA'
          END AS BIGNUMERIC
        ) < NUMERIC '0.10'
        AND a_0.match_txn_cnt >= 30 THEN 1
        ELSE 0
      END
      ELSE CASE
        WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
        AND xoom_team_0.xoom_ntid IS NULL
        AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
        AND r_0.rule_id NOT IN (467812)
        AND (
          SAFE_CAST(
            CASE
              WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                     ,
                    a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.10'
          AND a_0.match_txn_cnt >= 30
        ) THEN 1
        ELSE 0
      END
    END
    ELSE CASE
      WHEN r_0.crs_team IN (
        'gfr_omni_gms'    ,
        'gfr_omni_nextgen',
        'gfr_omni'        ,
        'goal_nextgen'    ,
        'goal_gms'
      ) THEN CASE
        WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
          WHEN a_0.cg_txn_cnt >= 30
          AND SAFE_CAST(
            CASE
              WHEN a_0.cg_txn_wamt > 0 THEN CAST(
                a_0.cg_brm_bad_wamt / a_0.cg_txn_wamt AS STRING
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.15'
          OR a_0.cg_txn_cnt < 30
          AND SAFE_CAST(
            CASE
              WHEN a_0.decled_amt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                             ,
                    a_0.decled_bad_amt * NUMERIC '1.000' / a_0.decled_amt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.15' THEN 1
          ELSE 0
        END
        WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN CASE
          WHEN a_0.match_txn_cnt >= 30
          AND SAFE_CAST(
            CASE
              WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                     ,
                    a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.15' THEN 1
          ELSE 0
        END
        WHEN UPPER(RTRIM(r_0.rule_mo)) = 'CC' THEN CASE
          WHEN a_0.match_txn_cnt >= 30
          AND SAFE_CAST(
            CASE
              WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                     ,
                    a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.15'
          OR a_0.match_txn_cnt < 30
          AND SAFE_CAST(
            CASE
              WHEN a_0.decled_amt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                             ,
                    a_0.decled_bad_amt * NUMERIC '1.000' / a_0.decled_amt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.15' THEN 1
          ELSE 0
        END
        WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
          WHEN a_0.match_txn_cnt >= 30
          AND SAFE_CAST(
            CASE
              WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                     ,
                    a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.15' THEN 1
          ELSE 0
        END
        ELSE 0
      END
      WHEN r_0.rule_mo IN ('Collusion', 'UBSM')
      AND xoom_team_0.xoom_ntid IS NULL THEN CASE
        WHEN SAFE_CAST(
          CASE
            WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
              REGEXP_REPLACE(
                FORMAT(
                  '%6.3f'                                                     ,
                  a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                )                   ,
                r'^( *?)(-)?0(\..*)',
                r'\2\3 \1'
              )                     ,
              r'^( *?)(-)?(\d*\..*)',
              r'\2\3\1'
            )
            ELSE 'NA'
          END AS BIGNUMERIC
        ) < NUMERIC '0.15'
        AND a_0.match_txn_cnt >= 30 THEN 1
        ELSE 0
      END
      ELSE CASE
        WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
        AND xoom_team_0.xoom_ntid IS NULL
        AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
        AND r_0.rule_id NOT IN (467812)
        AND (
          SAFE_CAST(
            CASE
              WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                     ,
                    a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.15'
          AND a_0.match_txn_cnt >= 30
        ) THEN 1
        ELSE 0
      END
    END
  END AS sub_poor_accuracy_bad_rate                                                                                    ,
  --  CASE WHEN business_policy=0 and r.crs_team IN ('gfr_omni_gms', 'gfr_omni_nextgen', 'gfr_omni') AND r.rule_mo='ATO'
  --        Then  case when  incre_exempt=0 AND To_Number(str_cg_wamt_bad_rate)<0.15 AND str_cg_txn_cnt>=30
  --        THEN 1 ELSE 0 END
  --    ELSE CASE WHEN business_policy=0 and r.rule_mo not in ('Collusion' ,'UBSM','Abuse')  and xoom_team.xoom_ntid is null and upper(r.tags) not like '%%good%%pop%%af%%'  and r.rule_id not in (467812) and  incre_exempt=0 AND To_Number(str_match_wamt_bad_rate_adj)<0.15 AND str_match_txn_cnt>=30
  --        THEN 1 ELSE 0 END
  --  END AS sub_poor_accuracy_incre_str_bad_rate,
  CASE
    WHEN CASE
      WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
      ELSE 0
    END = 1 THEN 0
    WHEN excl_accuracy_0.rule_id IS NOT NULL THEN 0
    WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN 0
    WHEN st_0.is_strategy = 1
    AND (
      UPPER(r_0.rule_name) LIKE '%FALLBACK%'
      OR UPPER(r_0.tags) LIKE '%SAFETY%NET%'
    ) THEN CASE
      WHEN r_0.crs_team IN (
        'gfr_omni_gms'    ,
        'gfr_omni_nextgen',
        'gfr_omni'        ,
        'goal_nextgen'    ,
        'goal_gms'
      ) THEN CASE
        WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
          WHEN CASE
            WHEN e2_0.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND (
            a_0.str_cg_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.str_cg_txn_wamt > 0 THEN CAST(
                  a_0.str_cg_brm_bad_wamt / a_0.str_cg_txn_wamt AS STRING
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            OR a_0.str_cg_txn_cnt < 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                                 ,
                      a_0.incre_str_decled_bad_amt * NUMERIC '1.000' / a_0.incre_str_decled_amt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
          ) THEN 1
          ELSE 0
        END
        WHEN UPPER(RTRIM(r_0.rule_mo)) = 'CC' THEN CASE
          WHEN CASE
            WHEN e2_0.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND (
            a_0.str_match_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                             ,
                      a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            OR a_0.str_match_txn_cnt < 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                                 ,
                      a_0.incre_str_decled_bad_amt * NUMERIC '1.000' / a_0.incre_str_decled_amt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
          ) THEN 1
          ELSE 0
        END
        WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
          WHEN CASE
            WHEN e2_0.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND (
            a_0.str_match_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                             ,
                      a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
          ) THEN 1
          ELSE 0
        END
        ELSE 0
      END
      ELSE CASE
        WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
        AND xoom_team_0.xoom_ntid IS NULL
        AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
        AND r_0.rule_id NOT IN (467812)
        AND (
          CASE
            WHEN e2_0.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND SAFE_CAST(
            CASE
              WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                             ,
                    a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.10'
          AND a_0.str_match_txn_cnt >= 30
        ) THEN 1
        ELSE 0
      END
    END
    ELSE CASE
      WHEN r_0.crs_team IN (
        'gfr_omni_gms'    ,
        'gfr_omni_nextgen',
        'gfr_omni'        ,
        'goal_nextgen'    ,
        'goal_gms'
      ) THEN CASE
        WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
          WHEN CASE
            WHEN e2_0.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND (
            a_0.str_cg_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.str_cg_txn_wamt > 0 THEN CAST(
                  a_0.str_cg_brm_bad_wamt / a_0.str_cg_txn_wamt AS STRING
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
            OR a_0.str_cg_txn_cnt < 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                                 ,
                      a_0.incre_str_decled_bad_amt * NUMERIC '1.000' / a_0.incre_str_decled_amt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
          ) THEN 1
          ELSE 0
        END
        WHEN UPPER(RTRIM(r_0.rule_mo)) = 'CC' THEN CASE
          WHEN CASE
            WHEN e2_0.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND (
            a_0.str_match_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                             ,
                      a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
            OR a_0.str_match_txn_cnt < 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                                 ,
                      a_0.incre_str_decled_bad_amt * NUMERIC '1.000' / a_0.incre_str_decled_amt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
          ) THEN 1
          ELSE 0
        END
        WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
          WHEN CASE
            WHEN e2_0.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND (
            a_0.str_match_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                             ,
                      a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
          ) THEN 1
          ELSE 0
        END
        ELSE 0
      END
      ELSE CASE
        WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
        AND xoom_team_0.xoom_ntid IS NULL
        AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
        AND r_0.rule_id NOT IN (467812)
        AND (
          CASE
            WHEN e2_0.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND SAFE_CAST(
            CASE
              WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                             ,
                    a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.15'
          AND a_0.str_match_txn_cnt >= 30
        ) THEN 1
        ELSE 0
      END
    END
  END AS sub_poor_accuracy_incre_str_bad_rate                                                                          ,
  --  CASE WHEN business_policy=0 and r.crs_team IN ('gfr_omni_gms', 'gfr_omni_nextgen', 'gfr_omni') AND r.rule_mo='ATO'
  --        Then  case when incre_exempt=0 AND To_Number(actn_cg_wamt_bad_rate)<0.15 AND actn_cg_txn_cnt>=30
  --        THEN 1 ELSE 0 END
  --    ELSE CASE WHEN  business_policy=0 and r.rule_mo not in ('Collusion' ,'UBSM','Abuse')  and xoom_team.xoom_ntid is null and upper(r.tags) not like '%%good%%pop%%af%%'  and r.rule_id not in (467812) and  incre_exempt=0 AND To_Number(actn_match_wamt_bad_rate_adj)<0.15 AND actn_match_txn_cnt>=30
  --        THEN 1 ELSE 0 END
  --  END AS sub_poor_accuracy_incre_actn_bad_rate,
  CASE
    WHEN CASE
      WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
      ELSE 0
    END = 1 THEN 0
    WHEN excl_accuracy_0.rule_id IS NOT NULL THEN 0
    WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN 0
    WHEN st_0.is_strategy = 1
    AND (
      UPPER(r_0.rule_name) LIKE '%FALLBACK%'
      OR UPPER(r_0.tags) LIKE '%SAFETY%NET%'
    ) THEN CASE
      WHEN r_0.crs_team IN (
        'gfr_omni_gms'    ,
        'gfr_omni_nextgen',
        'gfr_omni'        ,
        'goal_nextgen'    ,
        'goal_gms'
      ) THEN CASE
        WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
          WHEN CASE
            WHEN e2_0.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND (
            a_0.actn_cg_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.actn_cg_txn_wamt > 0 THEN CAST(
                  a_0.actn_cg_brm_bad_wamt / a_0.actn_cg_txn_wamt AS STRING
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            OR a_0.actn_cg_txn_cnt < 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                                   ,
                      a_0.incre_actn_decled_bad_amt * NUMERIC '1.000' / a_0.incre_actn_decled_amt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
          ) THEN 1
          ELSE 0
        END
        WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
          WHEN CASE
            WHEN e2_0.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND (
            a_0.actn_match_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                               ,
                      a_0.actn_match_bad_wamt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            OR a_0.actn_match_txn_cnt < 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                                   ,
                      a_0.incre_actn_decled_bad_amt * NUMERIC '1.000' / a_0.incre_actn_decled_amt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
          ) THEN 1
          ELSE 0
        END
        ELSE 0
      END
      ELSE CASE
        WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
        AND xoom_team_0.xoom_ntid IS NULL
        AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
        AND r_0.rule_id NOT IN (467812)
        AND (
          CASE
            WHEN e2_0.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND SAFE_CAST(
            CASE
              WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                               ,
                    a_0.actn_match_bad_wamt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.10'
          AND a_0.actn_match_txn_cnt >= 30
        ) THEN 1
        ELSE 0
      END
    END
    ELSE CASE
      WHEN r_0.crs_team IN (
        'gfr_omni_gms'    ,
        'gfr_omni_nextgen',
        'gfr_omni'        ,
        'goal_nextgen'    ,
        'goal_gms'
      ) THEN CASE
        WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
          WHEN CASE
            WHEN e2_0.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND (
            a_0.actn_cg_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.actn_cg_txn_wamt > 0 THEN CAST(
                  a_0.actn_cg_brm_bad_wamt / a_0.actn_cg_txn_wamt AS STRING
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
            OR a_0.actn_cg_txn_cnt < 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                                   ,
                      a_0.incre_actn_decled_bad_amt * NUMERIC '1.000' / a_0.incre_actn_decled_amt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
          ) THEN 1
          ELSE 0
        END
        WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
          WHEN CASE
            WHEN e2_0.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND (
            a_0.actn_match_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                               ,
                      a_0.actn_match_bad_wamt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
            OR a_0.actn_match_txn_cnt < 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                                   ,
                      a_0.incre_actn_decled_bad_amt * NUMERIC '1.000' / a_0.incre_actn_decled_amt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
          ) THEN 1
          ELSE 0
        END
        ELSE 0
      END
      ELSE CASE
        WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
        AND xoom_team_0.xoom_ntid IS NULL
        AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
        AND r_0.rule_id NOT IN (467812)
        AND (
          CASE
            WHEN e2_0.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND SAFE_CAST(
            CASE
              WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                               ,
                    a_0.actn_match_bad_wamt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.15'
          AND a_0.actn_match_txn_cnt >= 30
        ) THEN 1
        ELSE 0
      END
    END
  END AS sub_poor_accuracy_incre_actn_bad_rate,
  CASE
    WHEN CASE
      WHEN CASE
        WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
        ELSE 0
      END = 1 THEN 0
      WHEN excl_accuracy_0.rule_id IS NOT NULL THEN 0
      WHEN st_0.is_strategy = 1
      AND (
        UPPER(r_0.rule_name) LIKE '%FALLBACK%'
        OR UPPER(r_0.tags) LIKE '%SAFETY%NET%'
      ) THEN CASE
        WHEN LOWER(r_0.crs_team) IN (
          'gfr_omni_gms'    ,
          'gfr_omni_nextgen',
          'gfr_omni'        ,
          'goal_nextgen'    ,
          'goal_gms'
        ) THEN CASE
          WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
            WHEN a_0.cg_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.cg_txn_wamt > 0 THEN CAST(
                  a_0.cg_brm_bad_wamt / a_0.cg_txn_wamt AS STRING
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            OR a_0.cg_txn_cnt < 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.decled_amt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                             ,
                      a_0.decled_bad_amt * NUMERIC '1.000' / a_0.decled_amt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10' THEN 1
            ELSE 0
          END
          WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN CASE
            WHEN a_0.match_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                     ,
                      a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10' THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r_0.rule_mo)) = 'CC' THEN CASE
            WHEN a_0.match_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                     ,
                      a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            OR a_0.match_txn_cnt < 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.decled_amt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                             ,
                      a_0.decled_bad_amt * NUMERIC '1.000' / a_0.decled_amt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10' THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
            WHEN a_0.match_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                     ,
                      a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10' THEN 1
            ELSE 0
          END
          ELSE 0
        END
        WHEN r_0.rule_mo IN ('Collusion', 'UBSM')
        AND xoom_team_0.xoom_ntid IS NULL THEN CASE
          WHEN SAFE_CAST(
            CASE
              WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                     ,
                    a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.10'
          AND a_0.match_txn_cnt >= 30 THEN 1
          ELSE 0
        END
        ELSE CASE
          WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
          AND xoom_team_0.xoom_ntid IS NULL
          AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
          AND r_0.rule_id NOT IN (467812)
          AND (
            SAFE_CAST(
              CASE
                WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                     ,
                      a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            AND a_0.match_txn_cnt >= 30
          ) THEN 1
          ELSE 0
        END
      END
      ELSE CASE
        WHEN LOWER(r_0.crs_team) IN (
          'gfr_omni_gms'    ,
          'gfr_omni_nextgen',
          'gfr_omni'        ,
          'goal_nextgen'    ,
          'goal_gms'
        ) THEN CASE
          WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
            WHEN a_0.cg_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.cg_txn_wamt > 0 THEN CAST(
                  a_0.cg_brm_bad_wamt / a_0.cg_txn_wamt AS STRING
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
            OR a_0.cg_txn_cnt < 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.decled_amt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                             ,
                      a_0.decled_bad_amt * NUMERIC '1.000' / a_0.decled_amt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15' THEN 1
            ELSE 0
          END
          WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN CASE
            WHEN a_0.match_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                     ,
                      a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15' THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r_0.rule_mo)) = 'CC' THEN CASE
            WHEN a_0.match_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                     ,
                      a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
            OR a_0.match_txn_cnt < 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.decled_amt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                             ,
                      a_0.decled_bad_amt * NUMERIC '1.000' / a_0.decled_amt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15' THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
            WHEN a_0.match_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                     ,
                      a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15' THEN 1
            ELSE 0
          END
          ELSE 0
        END
        WHEN r_0.rule_mo IN ('Collusion', 'UBSM')
        AND xoom_team_0.xoom_ntid IS NULL THEN CASE
          WHEN SAFE_CAST(
            CASE
              WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                     ,
                    a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.15'
          AND a_0.match_txn_cnt >= 30 THEN 1
          ELSE 0
        END
        ELSE CASE
          WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
          AND xoom_team_0.xoom_ntid IS NULL
          AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
          AND r_0.rule_id NOT IN (467812)
          AND (
            SAFE_CAST(
              CASE
                WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                     ,
                      a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
            AND a_0.match_txn_cnt >= 30
          ) THEN 1
          ELSE 0
        END
      END
    END + CASE
      WHEN CASE
        WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
        ELSE 0
      END = 1 THEN 0
      WHEN excl_accuracy_0.rule_id IS NOT NULL THEN 0
      WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN 0
      WHEN st_0.is_strategy = 1
      AND (
        UPPER(r_0.rule_name) LIKE '%FALLBACK%'
        OR UPPER(r_0.tags) LIKE '%SAFETY%NET%'
      ) THEN CASE
        WHEN LOWER(r_0.crs_team) IN (
          'gfr_omni_gms'    ,
          'gfr_omni_nextgen',
          'gfr_omni'        ,
          'goal_nextgen'    ,
          'goal_gms'
        ) THEN CASE
          WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
            WHEN CASE
              WHEN e2_0.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND (
              a_0.actn_cg_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.actn_cg_txn_wamt > 0 THEN CAST(
                    a_0.actn_cg_brm_bad_wamt / a_0.actn_cg_txn_wamt AS STRING
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              OR a_0.actn_cg_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                                   ,
                        a_0.incre_actn_decled_bad_amt * NUMERIC '1.000' / a_0.incre_actn_decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
            ) THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
            WHEN CASE
              WHEN e2_0.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND (
              a_0.actn_match_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                               ,
                        a_0.actn_match_bad_wamt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              OR a_0.actn_match_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                                   ,
                        a_0.incre_actn_decled_bad_amt * NUMERIC '1.000' / a_0.incre_actn_decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
            ) THEN 1
            ELSE 0
          END
          ELSE 0
        END
        ELSE CASE
          WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
          AND xoom_team_0.xoom_ntid IS NULL
          AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
          AND r_0.rule_id NOT IN (467812)
          AND (
            CASE
              WHEN e2_0.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND SAFE_CAST(
              CASE
                WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                               ,
                      a_0.actn_match_bad_wamt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            AND a_0.actn_match_txn_cnt >= 30
          ) THEN 1
          ELSE 0
        END
      END
      ELSE CASE
        WHEN LOWER(r_0.crs_team) IN (
          'gfr_omni_gms'    ,
          'gfr_omni_nextgen',
          'gfr_omni'        ,
          'goal_nextgen'    ,
          'goal_gms'
        ) THEN CASE
          WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
            WHEN CASE
              WHEN e2_0.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND (
              a_0.actn_cg_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.actn_cg_txn_wamt > 0 THEN CAST(
                    a_0.actn_cg_brm_bad_wamt / a_0.actn_cg_txn_wamt AS STRING
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
              OR a_0.actn_cg_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                                   ,
                        a_0.incre_actn_decled_bad_amt * NUMERIC '1.000' / a_0.incre_actn_decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
            ) THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
            WHEN CASE
              WHEN e2_0.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND (
              a_0.actn_match_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                               ,
                        a_0.actn_match_bad_wamt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
              OR a_0.actn_match_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                                   ,
                        a_0.incre_actn_decled_bad_amt * NUMERIC '1.000' / a_0.incre_actn_decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
            ) THEN 1
            ELSE 0
          END
          ELSE 0
        END
        ELSE CASE
          WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
          AND xoom_team_0.xoom_ntid IS NULL
          AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
          AND r_0.rule_id NOT IN (467812)
          AND (
            CASE
              WHEN e2_0.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND SAFE_CAST(
              CASE
                WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                               ,
                      a_0.actn_match_bad_wamt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
            AND a_0.actn_match_txn_cnt >= 30
          ) THEN 1
          ELSE 0
        END
      END
    END + CASE
      WHEN CASE
        WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
        ELSE 0
      END = 1 THEN 0
      WHEN excl_accuracy_0.rule_id IS NOT NULL THEN 0
      WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN 0
      WHEN st_0.is_strategy = 1
      AND (
        UPPER(r_0.rule_name) LIKE '%FALLBACK%'
        OR UPPER(r_0.tags) LIKE '%SAFETY%NET%'
      ) THEN CASE
        WHEN LOWER(r_0.crs_team) IN (
          'gfr_omni_gms'    ,
          'gfr_omni_nextgen',
          'gfr_omni'        ,
          'goal_nextgen'    ,
          'goal_gms'
        ) THEN CASE
          WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
            WHEN CASE
              WHEN e2_0.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND (
              a_0.str_cg_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.str_cg_txn_wamt > 0 THEN CAST(
                    a_0.str_cg_brm_bad_wamt / a_0.str_cg_txn_wamt AS STRING
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              OR a_0.str_cg_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                                 ,
                        a_0.incre_str_decled_bad_amt * NUMERIC '1.000' / a_0.incre_str_decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
            ) THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r_0.rule_mo)) = 'CC' THEN CASE
            WHEN CASE
              WHEN e2_0.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND (
              a_0.str_match_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                             ,
                        a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              OR a_0.str_match_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                                 ,
                        a_0.incre_str_decled_bad_amt * NUMERIC '1.000' / a_0.incre_str_decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
            ) THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
            WHEN CASE
              WHEN e2_0.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND (
              a_0.str_match_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                             ,
                        a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
            ) THEN 1
            ELSE 0
          END
          ELSE 0
        END
        ELSE CASE
          WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
          AND xoom_team_0.xoom_ntid IS NULL
          AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
          AND r_0.rule_id NOT IN (467812)
          AND (
            CASE
              WHEN e2_0.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND SAFE_CAST(
              CASE
                WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                             ,
                      a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            AND a_0.str_match_txn_cnt >= 30
          ) THEN 1
          ELSE 0
        END
      END
      ELSE CASE
        WHEN LOWER(r_0.crs_team) IN (
          'gfr_omni_gms'    ,
          'gfr_omni_nextgen',
          'gfr_omni'        ,
          'goal_nextgen'    ,
          'goal_gms'
        ) THEN CASE
          WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
            WHEN CASE
              WHEN e2_0.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND (
              a_0.str_cg_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.str_cg_txn_wamt > 0 THEN CAST(
                    a_0.str_cg_brm_bad_wamt / a_0.str_cg_txn_wamt AS STRING
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
              OR a_0.str_cg_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                                 ,
                        a_0.incre_str_decled_bad_amt * NUMERIC '1.000' / a_0.incre_str_decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
            ) THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r_0.rule_mo)) = 'CC' THEN CASE
            WHEN CASE
              WHEN e2_0.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND (
              a_0.str_match_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                             ,
                        a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
              OR a_0.str_match_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                                 ,
                        a_0.incre_str_decled_bad_amt * NUMERIC '1.000' / a_0.incre_str_decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
            ) THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
            WHEN CASE
              WHEN e2_0.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND (
              a_0.str_match_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                             ,
                        a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
            ) THEN 1
            ELSE 0
          END
          ELSE 0
        END
        ELSE CASE
          WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
          AND xoom_team_0.xoom_ntid IS NULL
          AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
          AND r_0.rule_id NOT IN (467812)
          AND (
            CASE
              WHEN e2_0.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND SAFE_CAST(
              CASE
                WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                             ,
                      a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
            AND a_0.str_match_txn_cnt >= 30
          ) THEN 1
          ELSE 0
        END
      END
    END > 0 THEN 1
    ELSE 0
  END AS poor_accuracy,
  CASE
    WHEN a_0.fire_vol < 10 * r_0.observing_window
    AND (
      SAFE_CAST(
        CASE
          WHEN a_0.cg_txn_wamt > 0 THEN CAST(
            a_0.cg_brm_bad_wamt / a_0.cg_txn_wamt AS STRING
          )
          ELSE 'NA'
        END AS BIGNUMERIC
      ) < NUMERIC '0.1'
      OR RTRIM(
        CASE
          WHEN a_0.cg_txn_wamt > 0 THEN CAST(
            a_0.cg_brm_bad_wamt / a_0.cg_txn_wamt AS STRING
          )
          ELSE 'NA'
        END
      ) = 'NA'
    )
    AND CASE
      WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
      ELSE 0
    END = 0
    AND CASE
      WHEN e_0.rule_id IS NOT NULL THEN 1
      ELSE 0
    END = 0 THEN 1
    ELSE 0
  END AS sub_low_firing_all_firing,
  CASE
    WHEN CASE
      WHEN e2_0.rule_id IS NOT NULL THEN 1
      ELSE 0
    END = 0
    AND a_0.incre_rule_fire_vol < 5 * r_0.observing_window
    AND CASE
      WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
      ELSE 0
    END = 0
    AND CASE
      WHEN e_0.rule_id IS NOT NULL THEN 1
      ELSE 0
    END = 0 THEN 1
    ELSE 0
  END AS sub_low_firing_incre_firing ,
  --  add automated alerting criterias
  CASE
    WHEN (
      r_0.crs_team LIKE 'gfr_goal_ato%'
      OR r_0.crs_team LIKE 'gfr_goal_ach%'
      OR r_0.crs_team LIKE 'gfr_goal_cc%'
      OR r_0.crs_team LIKE 'gfr_goal_col%'
      OR r_0.crs_team LIKE 'gfr_goal_auto%'
      OR LOWER(r_0.crs_team) LIKE ANY (
        'goal_ato'  ,
        'goal_ach'  ,
        'goal_addfi',
        'goal_cc'   ,
        'goal_col'  ,
        'goal_auto'
      )
    )
    AND (
      r_0.tags LIKE '%AUTOSOLUTION%'
      AND r_0.tags LIKE '%HYPERNET%'
    )
    AND (
      r_0.tags NOT LIKE '%MERCHANT%'
      AND r_0.tags NOT LIKE '%CLUSTER%'
    )
    AND a_0.incre_str_fire_vol < r_0.observing_window THEN 1
    WHEN r_0.tags LIKE '%AUTOSOLUTION%'
    AND r_0.tags LIKE '%HYPERNET MERCHANT%'
    AND a_0.incre_str_fire_vol < 1 * r_0.observing_window THEN 1
    ELSE 0
  END AS automated_low_firing_cnt,
  CASE
    WHEN (
      r_0.crs_team LIKE 'gfr_goal_ato%'
      OR r_0.crs_team LIKE 'gfr_goal_ach%'
      OR r_0.crs_team LIKE 'gfr_goal_cc%'
      OR r_0.crs_team LIKE 'gfr_goal_col%'
      OR r_0.crs_team LIKE 'gfr_goal_auto%'
      OR LOWER(r_0.crs_team) LIKE ANY (
        'goal_ato'  ,
        'goal_ach'  ,
        'goal_addfi',
        'goal_cc'   ,
        'goal_col'  ,
        'goal_auto'
      )
    )
    AND (
      r_0.tags LIKE '%AUTOSOLUTION%'
      AND r_0.tags LIKE '%HYPERNET%'
    )
    AND (
      r_0.tags NOT LIKE '%MERCHANT%'
      AND r_0.tags NOT LIKE '%CLUSTER%'
    ) THEN CASE
      WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
        WHEN SAFE_CAST(
          CASE
            WHEN a_0.cg_txn_wamt > 0 THEN CAST(
              a_0.cg_brm_bad_wamt / a_0.cg_txn_wamt AS STRING
            )
            ELSE 'NA'
          END AS BIGNUMERIC
        ) < NUMERIC '0.10'
        AND a_0.cg_txn_cnt >= 30 THEN 1
        ELSE 0
      END
      WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ATO' THEN CASE
        WHEN SAFE_CAST(
          CASE
            WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
              REGEXP_REPLACE(
                FORMAT(
                  '%6.3f'                                                     ,
                  a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                )                   ,
                r'^( *?)(-)?0(\..*)',
                r'\2\3 \1'
              )                     ,
              r'^( *?)(-)?(\d*\..*)',
              r'\2\3\1'
            )
            ELSE 'NA'
          END AS BIGNUMERIC
        ) < NUMERIC '0.10'
        AND a_0.match_txn_cnt >= 30 THEN 1
        ELSE 0
      END
      ELSE 0
    END
    WHEN r_0.tags LIKE '%AUTOSOLUTION%'
    AND r_0.tags LIKE '%HYPERNET MERCHANT%' THEN CASE
      WHEN CASE
        WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
        ELSE 0
      END = 1 THEN 0
      WHEN excl_accuracy_0.rule_id IS NOT NULL THEN 0
      WHEN st_0.is_strategy = 1
      AND (
        UPPER(r_0.rule_name) LIKE '%FALLBACK%'
        OR UPPER(r_0.tags) LIKE '%SAFETY%NET%'
      ) THEN CASE
        WHEN LOWER(r_0.crs_team) IN (
          'gfr_omni_gms'    ,
          'gfr_omni_nextgen',
          'gfr_omni'        ,
          'goal_nextgen'    ,
          'goal_gms'
        ) THEN CASE
          WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
            WHEN a_0.cg_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.cg_txn_wamt > 0 THEN CAST(
                  a_0.cg_brm_bad_wamt / a_0.cg_txn_wamt AS STRING
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            OR a_0.cg_txn_cnt < 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.decled_amt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                             ,
                      a_0.decled_bad_amt * NUMERIC '1.000' / a_0.decled_amt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10' THEN 1
            ELSE 0
          END
          WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN CASE
            WHEN a_0.match_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                     ,
                      a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10' THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r_0.rule_mo)) = 'CC' THEN CASE
            WHEN a_0.match_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                     ,
                      a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            OR a_0.match_txn_cnt < 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.decled_amt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                             ,
                      a_0.decled_bad_amt * NUMERIC '1.000' / a_0.decled_amt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10' THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
            WHEN a_0.match_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                     ,
                      a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10' THEN 1
            ELSE 0
          END
          ELSE 0
        END
        WHEN r_0.rule_mo IN ('Collusion', 'UBSM')
        AND xoom_team_0.xoom_ntid IS NULL THEN CASE
          WHEN SAFE_CAST(
            CASE
              WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                     ,
                    a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.10'
          AND a_0.match_txn_cnt >= 30 THEN 1
          ELSE 0
        END
        ELSE CASE
          WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
          AND xoom_team_0.xoom_ntid IS NULL
          AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
          AND r_0.rule_id NOT IN (467812)
          AND (
            SAFE_CAST(
              CASE
                WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                     ,
                      a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            AND a_0.match_txn_cnt >= 30
          ) THEN 1
          ELSE 0
        END
      END
      ELSE CASE
        WHEN LOWER(r_0.crs_team) IN (
          'gfr_omni_gms'    ,
          'gfr_omni_nextgen',
          'gfr_omni'        ,
          'goal_nextgen'    ,
          'goal_gms'
        ) THEN CASE
          WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
            WHEN a_0.cg_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.cg_txn_wamt > 0 THEN CAST(
                  a_0.cg_brm_bad_wamt / a_0.cg_txn_wamt AS STRING
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
            OR a_0.cg_txn_cnt < 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.decled_amt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                             ,
                      a_0.decled_bad_amt * NUMERIC '1.000' / a_0.decled_amt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15' THEN 1
            ELSE 0
          END
          WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN CASE
            WHEN a_0.match_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                     ,
                      a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15' THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r_0.rule_mo)) = 'CC' THEN CASE
            WHEN a_0.match_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                     ,
                      a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
            OR a_0.match_txn_cnt < 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.decled_amt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                             ,
                      a_0.decled_bad_amt * NUMERIC '1.000' / a_0.decled_amt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15' THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
            WHEN a_0.match_txn_cnt >= 30
            AND SAFE_CAST(
              CASE
                WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                     ,
                      a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15' THEN 1
            ELSE 0
          END
          ELSE 0
        END
        WHEN r_0.rule_mo IN ('Collusion', 'UBSM')
        AND xoom_team_0.xoom_ntid IS NULL THEN CASE
          WHEN SAFE_CAST(
            CASE
              WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                     ,
                    a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.15'
          AND a_0.match_txn_cnt >= 30 THEN 1
          ELSE 0
        END
        ELSE CASE
          WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
          AND xoom_team_0.xoom_ntid IS NULL
          AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
          AND r_0.rule_id NOT IN (467812)
          AND (
            SAFE_CAST(
              CASE
                WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                     ,
                      a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
            AND a_0.match_txn_cnt >= 30
          ) THEN 1
          ELSE 0
        END
      END
    END
    ELSE 0
  END AS sub_automated_poor_accuracy_bad_rate,
  CASE
    WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN 0
    WHEN (
      r_0.crs_team LIKE 'gfr_goal_ato%'
      OR r_0.crs_team LIKE 'gfr_goal_ach%'
      OR r_0.crs_team LIKE 'gfr_goal_cc%'
      OR r_0.crs_team LIKE 'gfr_goal_col%'
      OR r_0.crs_team LIKE 'gfr_goal_auto%'
      OR LOWER(r_0.crs_team) LIKE ANY (
        'goal_ato'  ,
        'goal_ach'  ,
        'goal_addfi',
        'goal_cc'   ,
        'goal_col'  ,
        'goal_auto'
      )
    )
    AND (
      r_0.tags LIKE '%AUTOSOLUTION%'
      AND r_0.tags LIKE '%HYPERNET%'
    )
    AND (
      r_0.tags NOT LIKE '%MERCHANT%'
      AND r_0.tags NOT LIKE '%CLUSTER%'
    ) THEN CASE
      WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
        WHEN CASE
          WHEN e2_0.rule_id IS NOT NULL THEN 1
          ELSE 0
        END = 0
        AND SAFE_CAST(
          CASE
            WHEN a_0.str_cg_txn_wamt > 0 THEN CAST(
              a_0.str_cg_brm_bad_wamt / a_0.str_cg_txn_wamt AS STRING
            )
            ELSE 'NA'
          END AS BIGNUMERIC
        ) < NUMERIC '0.10'
        AND a_0.str_cg_txn_cnt >= 30 THEN 0
        ELSE 0
      END
      WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ATO' THEN CASE
        WHEN CASE
          WHEN e2_0.rule_id IS NOT NULL THEN 1
          ELSE 0
        END = 0
        AND SAFE_CAST(
          CASE
            WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
              REGEXP_REPLACE(
                FORMAT(
                  '%6.3f'                                                             ,
                  a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                )                   ,
                r'^( *?)(-)?0(\..*)',
                r'\2\3 \1'
              )                     ,
              r'^( *?)(-)?(\d*\..*)',
              r'\2\3\1'
            )
            ELSE 'NA'
          END AS BIGNUMERIC
        ) < NUMERIC '0.10'
        AND a_0.str_match_txn_cnt >= 30 THEN 0
        ELSE 0
      END
      ELSE 0
    END
    WHEN r_0.tags LIKE '%AUTOSOLUTION%'
    AND r_0.tags LIKE '%HYPERNET MERCHANT%' THEN CASE
      WHEN CASE
        WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
        ELSE 0
      END = 1 THEN 0
      WHEN excl_accuracy_0.rule_id IS NOT NULL THEN 0
      WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN 0
      WHEN st_0.is_strategy = 1
      AND (
        UPPER(r_0.rule_name) LIKE '%FALLBACK%'
        OR UPPER(r_0.tags) LIKE '%SAFETY%NET%'
      ) THEN CASE
        WHEN LOWER(r_0.crs_team) IN (
          'gfr_omni_gms'    ,
          'gfr_omni_nextgen',
          'gfr_omni'        ,
          'goal_nextgen'    ,
          'goal_gms'
        ) THEN CASE
          WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
            WHEN CASE
              WHEN e2_0.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND (
              a_0.str_cg_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.str_cg_txn_wamt > 0 THEN CAST(
                    a_0.str_cg_brm_bad_wamt / a_0.str_cg_txn_wamt AS STRING
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              OR a_0.str_cg_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                                 ,
                        a_0.incre_str_decled_bad_amt * NUMERIC '1.000' / a_0.incre_str_decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
            ) THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r_0.rule_mo)) = 'CC' THEN CASE
            WHEN CASE
              WHEN e2_0.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND (
              a_0.str_match_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                             ,
                        a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              OR a_0.str_match_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                                 ,
                        a_0.incre_str_decled_bad_amt * NUMERIC '1.000' / a_0.incre_str_decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
            ) THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
            WHEN CASE
              WHEN e2_0.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND (
              a_0.str_match_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                             ,
                        a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
            ) THEN 1
            ELSE 0
          END
          ELSE 0
        END
        ELSE CASE
          WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
          AND xoom_team_0.xoom_ntid IS NULL
          AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
          AND r_0.rule_id NOT IN (467812)
          AND (
            CASE
              WHEN e2_0.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND SAFE_CAST(
              CASE
                WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                             ,
                      a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            AND a_0.str_match_txn_cnt >= 30
          ) THEN 1
          ELSE 0
        END
      END
      ELSE CASE
        WHEN LOWER(r_0.crs_team) IN (
          'gfr_omni_gms'    ,
          'gfr_omni_nextgen',
          'gfr_omni'        ,
          'goal_nextgen'    ,
          'goal_gms'
        ) THEN CASE
          WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
            WHEN CASE
              WHEN e2_0.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND (
              a_0.str_cg_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.str_cg_txn_wamt > 0 THEN CAST(
                    a_0.str_cg_brm_bad_wamt / a_0.str_cg_txn_wamt AS STRING
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
              OR a_0.str_cg_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                                 ,
                        a_0.incre_str_decled_bad_amt * NUMERIC '1.000' / a_0.incre_str_decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
            ) THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r_0.rule_mo)) = 'CC' THEN CASE
            WHEN CASE
              WHEN e2_0.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND (
              a_0.str_match_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                             ,
                        a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
              OR a_0.str_match_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                                 ,
                        a_0.incre_str_decled_bad_amt * NUMERIC '1.000' / a_0.incre_str_decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
            ) THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
            WHEN CASE
              WHEN e2_0.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND (
              a_0.str_match_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                             ,
                        a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
            ) THEN 1
            ELSE 0
          END
          ELSE 0
        END
        ELSE CASE
          WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
          AND xoom_team_0.xoom_ntid IS NULL
          AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
          AND r_0.rule_id NOT IN (467812)
          AND (
            CASE
              WHEN e2_0.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND SAFE_CAST(
              CASE
                WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                             ,
                      a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
            AND a_0.str_match_txn_cnt >= 30
          ) THEN 1
          ELSE 0
        END
      END
    END
    ELSE 0
  END AS sub_automated_poor_accuracy_incre_str_bad_rate,
  CASE
    WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN 0
    WHEN (
      r_0.crs_team LIKE 'gfr_goal_ato%'
      OR r_0.crs_team LIKE 'gfr_goal_ach%'
      OR r_0.crs_team LIKE 'gfr_goal_cc%'
      OR r_0.crs_team LIKE 'gfr_goal_col%'
      OR r_0.crs_team LIKE 'gfr_goal_auto%'
      OR LOWER(r_0.crs_team) LIKE ANY (
        'goal_ato'  ,
        'goal_ach'  ,
        'goal_addfi',
        'goal_cc'   ,
        'goal_col'  ,
        'goal_auto'
      )
    )
    AND (
      r_0.tags LIKE '%AUTOSOLUTION%'
      AND r_0.tags LIKE '%HYPERNET%'
    )
    AND (
      r_0.tags NOT LIKE '%MERCHANT%'
      AND r_0.tags NOT LIKE '%CLUSTER%'
    ) THEN CASE
      WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
        WHEN CASE
          WHEN e2_0.rule_id IS NOT NULL THEN 1
          ELSE 0
        END = 0
        AND SAFE_CAST(
          CASE
            WHEN a_0.actn_cg_txn_wamt > 0 THEN CAST(
              a_0.actn_cg_brm_bad_wamt / a_0.actn_cg_txn_wamt AS STRING
            )
            ELSE 'NA'
          END AS BIGNUMERIC
        ) < NUMERIC '0.10'
        AND a_0.actn_cg_txn_cnt >= 30 THEN 1
        ELSE 0
      END
      WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ATO' THEN CASE
        WHEN CASE
          WHEN e2_0.rule_id IS NOT NULL THEN 1
          ELSE 0
        END = 0
        AND SAFE_CAST(
          CASE
            WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
              REGEXP_REPLACE(
                FORMAT(
                  '%6.3f'                                                               ,
                  a_0.actn_match_bad_wamt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wamt
                )                   ,
                r'^( *?)(-)?0(\..*)',
                r'\2\3 \1'
              )                     ,
              r'^( *?)(-)?(\d*\..*)',
              r'\2\3\1'
            )
            ELSE 'NA'
          END AS BIGNUMERIC
        ) < NUMERIC '0.10'
        AND a_0.actn_match_txn_cnt >= 30 THEN 1
        ELSE 0
      END
      ELSE 0
    END
    WHEN r_0.tags LIKE '%AUTOSOLUTION%'
    AND r_0.tags LIKE '%HYPERNET MERCHANT%' THEN CASE
      WHEN CASE
        WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
        ELSE 0
      END = 1 THEN 0
      WHEN excl_accuracy_0.rule_id IS NOT NULL THEN 0
      WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN 0
      WHEN st_0.is_strategy = 1
      AND (
        UPPER(r_0.rule_name) LIKE '%FALLBACK%'
        OR UPPER(r_0.tags) LIKE '%SAFETY%NET%'
      ) THEN CASE
        WHEN LOWER(r_0.crs_team) IN (
          'gfr_omni_gms'    ,
          'gfr_omni_nextgen',
          'gfr_omni'        ,
          'goal_nextgen'    ,
          'goal_gms'
        ) THEN CASE
          WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
            WHEN CASE
              WHEN e2_0.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND (
              a_0.actn_cg_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.actn_cg_txn_wamt > 0 THEN CAST(
                    a_0.actn_cg_brm_bad_wamt / a_0.actn_cg_txn_wamt AS STRING
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              OR a_0.actn_cg_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                                   ,
                        a_0.incre_actn_decled_bad_amt * NUMERIC '1.000' / a_0.incre_actn_decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
            ) THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
            WHEN CASE
              WHEN e2_0.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND (
              a_0.actn_match_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                               ,
                        a_0.actn_match_bad_wamt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              OR a_0.actn_match_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                                   ,
                        a_0.incre_actn_decled_bad_amt * NUMERIC '1.000' / a_0.incre_actn_decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
            ) THEN 1
            ELSE 0
          END
          ELSE 0
        END
        ELSE CASE
          WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
          AND xoom_team_0.xoom_ntid IS NULL
          AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
          AND r_0.rule_id NOT IN (467812)
          AND (
            CASE
              WHEN e2_0.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND SAFE_CAST(
              CASE
                WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                               ,
                      a_0.actn_match_bad_wamt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            AND a_0.actn_match_txn_cnt >= 30
          ) THEN 1
          ELSE 0
        END
      END
      ELSE CASE
        WHEN LOWER(r_0.crs_team) IN (
          'gfr_omni_gms'    ,
          'gfr_omni_nextgen',
          'gfr_omni'        ,
          'goal_nextgen'    ,
          'goal_gms'
        ) THEN CASE
          WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
            WHEN CASE
              WHEN e2_0.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND (
              a_0.actn_cg_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.actn_cg_txn_wamt > 0 THEN CAST(
                    a_0.actn_cg_brm_bad_wamt / a_0.actn_cg_txn_wamt AS STRING
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
              OR a_0.actn_cg_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                                   ,
                        a_0.incre_actn_decled_bad_amt * NUMERIC '1.000' / a_0.incre_actn_decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
            ) THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
            WHEN CASE
              WHEN e2_0.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND (
              a_0.actn_match_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                               ,
                        a_0.actn_match_bad_wamt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
              OR a_0.actn_match_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                                   ,
                        a_0.incre_actn_decled_bad_amt * NUMERIC '1.000' / a_0.incre_actn_decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
            ) THEN 1
            ELSE 0
          END
          ELSE 0
        END
        ELSE CASE
          WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
          AND xoom_team_0.xoom_ntid IS NULL
          AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
          AND r_0.rule_id NOT IN (467812)
          AND (
            CASE
              WHEN e2_0.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND SAFE_CAST(
              CASE
                WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                               ,
                      a_0.actn_match_bad_wamt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
            AND a_0.actn_match_txn_cnt >= 30
          ) THEN 1
          ELSE 0
        END
      END
    END
    ELSE 0
  END AS sub_automated_poor_accuracy_incre_actn_bad_rate,
  CASE
    WHEN CASE
      WHEN (
        r_0.crs_team LIKE 'gfr_goal_ato%'
        OR r_0.crs_team LIKE 'gfr_goal_ach%'
        OR r_0.crs_team LIKE 'gfr_goal_cc%'
        OR r_0.crs_team LIKE 'gfr_goal_col%'
        OR r_0.crs_team LIKE 'gfr_goal_auto%'
        OR LOWER(r_0.crs_team) LIKE ANY (
          'goal_ato'  ,
          'goal_ach'  ,
          'goal_addfi',
          'goal_cc'   ,
          'goal_col'  ,
          'goal_auto'
        )
      )
      AND (
        r_0.tags LIKE '%AUTOSOLUTION%'
        AND r_0.tags LIKE '%HYPERNET%'
      )
      AND (
        r_0.tags NOT LIKE '%MERCHANT%'
        AND r_0.tags NOT LIKE '%CLUSTER%'
      ) THEN CASE
        WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
          WHEN SAFE_CAST(
            CASE
              WHEN a_0.cg_txn_wamt > 0 THEN CAST(
                a_0.cg_brm_bad_wamt / a_0.cg_txn_wamt AS STRING
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.10'
          AND a_0.cg_txn_cnt >= 30 THEN 1
          ELSE 0
        END
        WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ATO' THEN CASE
          WHEN SAFE_CAST(
            CASE
              WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                     ,
                    a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.10'
          AND a_0.match_txn_cnt >= 30 THEN 1
          ELSE 0
        END
        ELSE 0
      END
      WHEN r_0.tags LIKE '%AUTOSOLUTION%'
      AND r_0.tags LIKE '%HYPERNET MERCHANT%' THEN CASE
        WHEN CASE
          WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
          ELSE 0
        END = 1 THEN 0
        WHEN excl_accuracy_0.rule_id IS NOT NULL THEN 0
        WHEN st_0.is_strategy = 1
        AND (
          UPPER(r_0.rule_name) LIKE '%FALLBACK%'
          OR UPPER(r_0.tags) LIKE '%SAFETY%NET%'
        ) THEN CASE
          WHEN r_0.crs_team IN (
            'gfr_omni_gms'    ,
            'gfr_omni_nextgen',
            'gfr_omni'        ,
            'goal_nextgen'    ,
            'goal_gms'
          ) THEN CASE
            WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
              WHEN a_0.cg_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.cg_txn_wamt > 0 THEN CAST(
                    a_0.cg_brm_bad_wamt / a_0.cg_txn_wamt AS STRING
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              OR a_0.cg_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                             ,
                        a_0.decled_bad_amt * NUMERIC '1.000' / a_0.decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10' THEN 1
              ELSE 0
            END
            WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN CASE
              WHEN a_0.match_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                     ,
                        a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10' THEN 1
              ELSE 0
            END
            WHEN UPPER(RTRIM(r_0.rule_mo)) = 'CC' THEN CASE
              WHEN a_0.match_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                     ,
                        a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              OR a_0.match_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                             ,
                        a_0.decled_bad_amt * NUMERIC '1.000' / a_0.decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10' THEN 1
              ELSE 0
            END
            WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
              WHEN a_0.match_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                     ,
                        a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10' THEN 1
              ELSE 0
            END
            ELSE 0
          END
          WHEN r_0.rule_mo IN ('Collusion', 'UBSM')
          AND xoom_team_0.xoom_ntid IS NULL THEN CASE
            WHEN SAFE_CAST(
              CASE
                WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                     ,
                      a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            AND a_0.match_txn_cnt >= 30 THEN 1
            ELSE 0
          END
          ELSE CASE
            WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
            AND xoom_team_0.xoom_ntid IS NULL
            AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
            AND r_0.rule_id NOT IN (467812)
            AND (
              SAFE_CAST(
                CASE
                  WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                     ,
                        a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              AND a_0.match_txn_cnt >= 30
            ) THEN 1
            ELSE 0
          END
        END
        ELSE CASE
          WHEN r_0.crs_team IN (
            'gfr_omni_gms'    ,
            'gfr_omni_nextgen',
            'gfr_omni'        ,
            'goal_nextgen'    ,
            'goal_gms'
          ) THEN CASE
            WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
              WHEN a_0.cg_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.cg_txn_wamt > 0 THEN CAST(
                    a_0.cg_brm_bad_wamt / a_0.cg_txn_wamt AS STRING
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
              OR a_0.cg_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                             ,
                        a_0.decled_bad_amt * NUMERIC '1.000' / a_0.decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15' THEN 1
              ELSE 0
            END
            WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN CASE
              WHEN a_0.match_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                     ,
                        a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15' THEN 1
              ELSE 0
            END
            WHEN UPPER(RTRIM(r_0.rule_mo)) = 'CC' THEN CASE
              WHEN a_0.match_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                     ,
                        a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
              OR a_0.match_txn_cnt < 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.decled_amt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                             ,
                        a_0.decled_bad_amt * NUMERIC '1.000' / a_0.decled_amt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15' THEN 1
              ELSE 0
            END
            WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
              WHEN a_0.match_txn_cnt >= 30
              AND SAFE_CAST(
                CASE
                  WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                     ,
                        a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15' THEN 1
              ELSE 0
            END
            ELSE 0
          END
          WHEN r_0.rule_mo IN ('Collusion', 'UBSM')
          AND xoom_team_0.xoom_ntid IS NULL THEN CASE
            WHEN SAFE_CAST(
              CASE
                WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                     ,
                      a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.15'
            AND a_0.match_txn_cnt >= 30 THEN 1
            ELSE 0
          END
          ELSE CASE
            WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
            AND xoom_team_0.xoom_ntid IS NULL
            AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
            AND r_0.rule_id NOT IN (467812)
            AND (
              SAFE_CAST(
                CASE
                  WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                     ,
                        a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
              AND a_0.match_txn_cnt >= 30
            ) THEN 1
            ELSE 0
          END
        END
      END
      ELSE 0
    END + CASE
      WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN 0
      WHEN (
        r_0.crs_team LIKE 'gfr_goal_ato%'
        OR r_0.crs_team LIKE 'gfr_goal_ach%'
        OR r_0.crs_team LIKE 'gfr_goal_cc%'
        OR r_0.crs_team LIKE 'gfr_goal_col%'
        OR r_0.crs_team LIKE 'gfr_goal_auto%'
        OR LOWER(r_0.crs_team) LIKE ANY (
          'goal_ato'  ,
          'goal_ach'  ,
          'goal_addfi',
          'goal_cc'   ,
          'goal_col'  ,
          'goal_auto'
        )
      )
      AND (
        r_0.tags LIKE '%AUTOSOLUTION%'
        AND r_0.tags LIKE '%HYPERNET%'
      )
      AND (
        r_0.tags NOT LIKE '%MERCHANT%'
        AND r_0.tags NOT LIKE '%CLUSTER%'
      ) THEN CASE
        WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
          WHEN CASE
            WHEN e2_0.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND SAFE_CAST(
            CASE
              WHEN a_0.str_cg_txn_wamt > 0 THEN CAST(
                a_0.str_cg_brm_bad_wamt / a_0.str_cg_txn_wamt AS STRING
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.10'
          AND a_0.str_cg_txn_cnt >= 30 THEN 0
          ELSE 0
        END
        WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ATO' THEN CASE
          WHEN CASE
            WHEN e2_0.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND SAFE_CAST(
            CASE
              WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                             ,
                    a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.10'
          AND a_0.str_match_txn_cnt >= 30 THEN 0
          ELSE 0
        END
        ELSE 0
      END
      WHEN r_0.tags LIKE '%AUTOSOLUTION%'
      AND r_0.tags LIKE '%HYPERNET MERCHANT%' THEN CASE
        WHEN CASE
          WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
          ELSE 0
        END = 1 THEN 0
        WHEN excl_accuracy_0.rule_id IS NOT NULL THEN 0
        WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN 0
        WHEN st_0.is_strategy = 1
        AND (
          UPPER(r_0.rule_name) LIKE '%FALLBACK%'
          OR UPPER(r_0.tags) LIKE '%SAFETY%NET%'
        ) THEN CASE
          WHEN r_0.crs_team IN (
            'gfr_omni_gms'    ,
            'gfr_omni_nextgen',
            'gfr_omni'        ,
            'goal_nextgen'    ,
            'goal_gms'
          ) THEN CASE
            WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
              WHEN CASE
                WHEN e2_0.rule_id IS NOT NULL THEN 1
                ELSE 0
              END = 0
              AND (
                a_0.str_cg_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.str_cg_txn_wamt > 0 THEN CAST(
                      a_0.str_cg_brm_bad_wamt / a_0.str_cg_txn_wamt AS STRING
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                OR a_0.str_cg_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                                 ,
                          a_0.incre_str_decled_bad_amt * NUMERIC '1.000' / a_0.incre_str_decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
              ) THEN 1
              ELSE 0
            END
            WHEN UPPER(RTRIM(r_0.rule_mo)) = 'CC' THEN CASE
              WHEN CASE
                WHEN e2_0.rule_id IS NOT NULL THEN 1
                ELSE 0
              END = 0
              AND (
                a_0.str_match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                             ,
                          a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                OR a_0.str_match_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                                 ,
                          a_0.incre_str_decled_bad_amt * NUMERIC '1.000' / a_0.incre_str_decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
              ) THEN 1
              ELSE 0
            END
            WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
              WHEN CASE
                WHEN e2_0.rule_id IS NOT NULL THEN 1
                ELSE 0
              END = 0
              AND (
                a_0.str_match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                             ,
                          a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
              ) THEN 1
              ELSE 0
            END
            ELSE 0
          END
          ELSE CASE
            WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
            AND xoom_team_0.xoom_ntid IS NULL
            AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
            AND r_0.rule_id NOT IN (467812)
            AND (
              CASE
                WHEN e2_0.rule_id IS NOT NULL THEN 1
                ELSE 0
              END = 0
              AND SAFE_CAST(
                CASE
                  WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                             ,
                        a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              AND a_0.str_match_txn_cnt >= 30
            ) THEN 1
            ELSE 0
          END
        END
        ELSE CASE
          WHEN r_0.crs_team IN (
            'gfr_omni_gms'    ,
            'gfr_omni_nextgen',
            'gfr_omni'        ,
            'goal_nextgen'    ,
            'goal_gms'
          ) THEN CASE
            WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
              WHEN CASE
                WHEN e2_0.rule_id IS NOT NULL THEN 1
                ELSE 0
              END = 0
              AND (
                a_0.str_cg_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.str_cg_txn_wamt > 0 THEN CAST(
                      a_0.str_cg_brm_bad_wamt / a_0.str_cg_txn_wamt AS STRING
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                OR a_0.str_cg_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                                 ,
                          a_0.incre_str_decled_bad_amt * NUMERIC '1.000' / a_0.incre_str_decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
              ) THEN 1
              ELSE 0
            END
            WHEN UPPER(RTRIM(r_0.rule_mo)) = 'CC' THEN CASE
              WHEN CASE
                WHEN e2_0.rule_id IS NOT NULL THEN 1
                ELSE 0
              END = 0
              AND (
                a_0.str_match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                             ,
                          a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                OR a_0.str_match_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                                 ,
                          a_0.incre_str_decled_bad_amt * NUMERIC '1.000' / a_0.incre_str_decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
              ) THEN 1
              ELSE 0
            END
            WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
              WHEN CASE
                WHEN e2_0.rule_id IS NOT NULL THEN 1
                ELSE 0
              END = 0
              AND (
                a_0.str_match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                             ,
                          a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
              ) THEN 1
              ELSE 0
            END
            ELSE 0
          END
          ELSE CASE
            WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
            AND xoom_team_0.xoom_ntid IS NULL
            AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
            AND r_0.rule_id NOT IN (467812)
            AND (
              CASE
                WHEN e2_0.rule_id IS NOT NULL THEN 1
                ELSE 0
              END = 0
              AND SAFE_CAST(
                CASE
                  WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                             ,
                        a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
              AND a_0.str_match_txn_cnt >= 30
            ) THEN 1
            ELSE 0
          END
        END
      END
      ELSE 0
    END + CASE
      WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN 0
      WHEN (
        r_0.crs_team LIKE 'gfr_goal_ato%'
        OR r_0.crs_team LIKE 'gfr_goal_ach%'
        OR r_0.crs_team LIKE 'gfr_goal_cc%'
        OR r_0.crs_team LIKE 'gfr_goal_col%'
        OR r_0.crs_team LIKE 'gfr_goal_auto%'
        OR LOWER(r_0.crs_team) LIKE ANY (
          'goal_ato'  ,
          'goal_ach'  ,
          'goal_addfi',
          'goal_cc'   ,
          'goal_col'  ,
          'goal_auto'
        )
      )
      AND (
        r_0.tags LIKE '%AUTOSOLUTION%'
        AND r_0.tags LIKE '%HYPERNET%'
      )
      AND (
        r_0.tags NOT LIKE '%MERCHANT%'
        AND r_0.tags NOT LIKE '%CLUSTER%'
      ) THEN CASE
        WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
          WHEN CASE
            WHEN e2_0.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND SAFE_CAST(
            CASE
              WHEN a_0.actn_cg_txn_wamt > 0 THEN CAST(
                a_0.actn_cg_brm_bad_wamt / a_0.actn_cg_txn_wamt AS STRING
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.10'
          AND a_0.actn_cg_txn_cnt >= 30 THEN 1
          ELSE 0
        END
        WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ATO' THEN CASE
          WHEN CASE
            WHEN e2_0.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND SAFE_CAST(
            CASE
              WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                                               ,
                    a_0.actn_match_bad_wamt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wamt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.10'
          AND a_0.actn_match_txn_cnt >= 30 THEN 1
          ELSE 0
        END
        ELSE 0
      END
      WHEN r_0.tags LIKE '%AUTOSOLUTION%'
      AND r_0.tags LIKE '%HYPERNET MERCHANT%' THEN CASE
        WHEN CASE
          WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
          ELSE 0
        END = 1 THEN 0
        WHEN excl_accuracy_0.rule_id IS NOT NULL THEN 0
        WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN 0
        WHEN st_0.is_strategy = 1
        AND (
          UPPER(r_0.rule_name) LIKE '%FALLBACK%'
          OR UPPER(r_0.tags) LIKE '%SAFETY%NET%'
        ) THEN CASE
          WHEN r_0.crs_team IN (
            'gfr_omni_gms'    ,
            'gfr_omni_nextgen',
            'gfr_omni'        ,
            'goal_nextgen'    ,
            'goal_gms'
          ) THEN CASE
            WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
              WHEN CASE
                WHEN e2_0.rule_id IS NOT NULL THEN 1
                ELSE 0
              END = 0
              AND (
                a_0.actn_cg_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.actn_cg_txn_wamt > 0 THEN CAST(
                      a_0.actn_cg_brm_bad_wamt / a_0.actn_cg_txn_wamt AS STRING
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                OR a_0.actn_cg_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                                   ,
                          a_0.incre_actn_decled_bad_amt * NUMERIC '1.000' / a_0.incre_actn_decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
              ) THEN 1
              ELSE 0
            END
            WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
              WHEN CASE
                WHEN e2_0.rule_id IS NOT NULL THEN 1
                ELSE 0
              END = 0
              AND (
                a_0.actn_match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                               ,
                          a_0.actn_match_bad_wamt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                OR a_0.actn_match_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                                   ,
                          a_0.incre_actn_decled_bad_amt * NUMERIC '1.000' / a_0.incre_actn_decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
              ) THEN 1
              ELSE 0
            END
            ELSE 0
          END
          ELSE CASE
            WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
            AND xoom_team_0.xoom_ntid IS NULL
            AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
            AND r_0.rule_id NOT IN (467812)
            AND (
              CASE
                WHEN e2_0.rule_id IS NOT NULL THEN 1
                ELSE 0
              END = 0
              AND SAFE_CAST(
                CASE
                  WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                               ,
                        a_0.actn_match_bad_wamt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              AND a_0.actn_match_txn_cnt >= 30
            ) THEN 1
            ELSE 0
          END
        END
        ELSE CASE
          WHEN r_0.crs_team IN (
            'gfr_omni_gms'    ,
            'gfr_omni_nextgen',
            'gfr_omni'        ,
            'goal_nextgen'    ,
            'goal_gms'
          ) THEN CASE
            WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
              WHEN CASE
                WHEN e2_0.rule_id IS NOT NULL THEN 1
                ELSE 0
              END = 0
              AND (
                a_0.actn_cg_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.actn_cg_txn_wamt > 0 THEN CAST(
                      a_0.actn_cg_brm_bad_wamt / a_0.actn_cg_txn_wamt AS STRING
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                OR a_0.actn_cg_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                                   ,
                          a_0.incre_actn_decled_bad_amt * NUMERIC '1.000' / a_0.incre_actn_decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
              ) THEN 1
              ELSE 0
            END
            WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
              WHEN CASE
                WHEN e2_0.rule_id IS NOT NULL THEN 1
                ELSE 0
              END = 0
              AND (
                a_0.actn_match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                               ,
                          a_0.actn_match_bad_wamt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                OR a_0.actn_match_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                                   ,
                          a_0.incre_actn_decled_bad_amt * NUMERIC '1.000' / a_0.incre_actn_decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
              ) THEN 1
              ELSE 0
            END
            ELSE 0
          END
          ELSE CASE
            WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
            AND xoom_team_0.xoom_ntid IS NULL
            AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
            AND r_0.rule_id NOT IN (467812)
            AND (
              CASE
                WHEN e2_0.rule_id IS NOT NULL THEN 1
                ELSE 0
              END = 0
              AND SAFE_CAST(
                CASE
                  WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                               ,
                        a_0.actn_match_bad_wamt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
              AND a_0.actn_match_txn_cnt >= 30
            ) THEN 1
            ELSE 0
          END
        END
      END
      ELSE 0
    END > 0 THEN 1
    ELSE 0
  END AS automated_poor_accuracy,
  0 AS automated_low_firing_amt ,
  CASE
    WHEN (
      r_0.crs_team LIKE 'gfr_goal_ato%'
      OR r_0.crs_team LIKE 'gfr_goal_ach%'
      OR r_0.crs_team LIKE 'gfr_goal_cc%'
      OR r_0.crs_team LIKE 'gfr_goal_col%'
      OR r_0.crs_team LIKE 'gfr_goal_auto%'
      OR LOWER(r_0.crs_team) LIKE ANY (
        'goal_ato'  ,
        'goal_ach'  ,
        'goal_addfi',
        'goal_cc'   ,
        'goal_col'  ,
        'goal_auto'
      )
    )
    AND (
      r_0.tags LIKE '%AUTOSOLUTION%'
      AND r_0.tags LIKE '%HYPERNET%'
    )
    AND (
      r_0.tags NOT LIKE '%MERCHANT%'
      AND r_0.tags NOT LIKE '%CLUSTER%'
    ) THEN 0
    WHEN r_0.tags LIKE '%AUTOSOLUTION%'
    AND r_0.tags LIKE '%HYPERNET MERCHANT%' THEN CASE
      WHEN CASE
        WHEN e_ow_0.rule_id IS NOT NULL THEN 1
        ELSE 0
      END = 0
      AND SAFE_CAST(
        CASE
          WHEN a_0.fire_vol > 0 THEN REGEXP_REPLACE(
            REGEXP_REPLACE(
              FORMAT(
                '%6.3f'                                        ,
                a_0.sum_wh_actn * NUMERIC '1.000' / a_0.fire_vol
              )                   ,
              r'^( *?)(-)?0(\..*)',
              r'\2\3 \1'
            )                     ,
            r'^( *?)(-)?(\d*\..*)',
            r'\2\3\1'
          )
          ELSE 'NA'
        END AS BIGNUMERIC
      ) >= NUMERIC '0.7'
      AND SAFE_CAST(
        CASE
          WHEN a_0.whed_amt > 0 THEN REGEXP_REPLACE(
            REGEXP_REPLACE(
              FORMAT(
                '%6.3f'                                        ,
                a_0.bad_amt_adj * NUMERIC '1.000' / a_0.whed_amt
              )                   ,
              r'^( *?)(-)?0(\..*)',
              r'\2\3 \1'
            )                     ,
            r'^( *?)(-)?(\d*\..*)',
            r'\2\3\1'
          )
          ELSE 'NA'
        END AS BIGNUMERIC
      ) < NUMERIC '0.1'
      AND a_0.whed_cnt >= 30 THEN 1
      ELSE 0
    END
    ELSE 0
  END AS automated_over_whitelisted,
  CASE
    WHEN 0 = 1
    OR CASE
      WHEN (
        r_0.crs_team LIKE 'gfr_goal_ato%'
        OR r_0.crs_team LIKE 'gfr_goal_ach%'
        OR r_0.crs_team LIKE 'gfr_goal_cc%'
        OR r_0.crs_team LIKE 'gfr_goal_col%'
        OR r_0.crs_team LIKE 'gfr_goal_auto%'
        OR LOWER(r_0.crs_team) LIKE ANY (
          'goal_ato'  ,
          'goal_ach'  ,
          'goal_addfi',
          'goal_cc'   ,
          'goal_col'  ,
          'goal_auto'
        )
      )
      AND (
        r_0.tags LIKE '%AUTOSOLUTION%'
        AND r_0.tags LIKE '%HYPERNET%'
      )
      AND (
        r_0.tags NOT LIKE '%MERCHANT%'
        AND r_0.tags NOT LIKE '%CLUSTER%'
      )
      AND a_0.incre_str_fire_vol < r_0.observing_window THEN 1
      WHEN r_0.tags LIKE '%AUTOSOLUTION%'
      AND r_0.tags LIKE '%HYPERNET MERCHANT%'
      AND a_0.incre_str_fire_vol < 1 * r_0.observing_window THEN 1
      ELSE 0
    END = 1 THEN 1
    ELSE 0
  END AS automated_low_firing,
  CASE
    WHEN r_0.tags LIKE '%AUTOSOLUTION%'
    AND r_0.tags LIKE '%HYPERNET MERCHANT%' THEN CASE
      WHEN CASE
        WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
        ELSE 0
      END = 0
      AND SAFE_CAST(b_0.ov_pct_1 AS BIGNUMERIC) >= NUMERIC '0.8'
      AND CASE
        WHEN e2_0.rule_id IS NOT NULL THEN 1
        ELSE 0
      END = 0 THEN CASE
        WHEN r_0.tags LIKE '%AUTOSOLUTION%'
        AND r_0.tags LIKE '%HYPERNET MERCHANT%'
        AND COALESCE(
          st_ov1_0.is_strategy_ov1_raw,
          0
        ) = 1 THEN 1
        WHEN r_0.tags LIKE '%AUTOSOLUTION%'
        AND r_0.tags LIKE '%HYPERNET MERCHANT%'
        AND COALESCE(
          st_ov1_0.is_strategy_ov1_raw,
          0
        ) = 0 THEN 0
        ELSE 1
      END
      ELSE 0
    END
    ELSE 0
  END AS automated_high_overlap,
  CASE
    WHEN (
      CASE
        WHEN 0 = 1
        OR CASE
          WHEN (
            r_0.crs_team LIKE 'gfr_goal_ato%'
            OR r_0.crs_team LIKE 'gfr_goal_ach%'
            OR r_0.crs_team LIKE 'gfr_goal_cc%'
            OR r_0.crs_team LIKE 'gfr_goal_col%'
            OR r_0.crs_team LIKE 'gfr_goal_auto%'
            OR LOWER(r_0.crs_team) LIKE ANY (
              'goal_ato'  ,
              'goal_ach'  ,
              'goal_addfi',
              'goal_cc'   ,
              'goal_col'  ,
              'goal_auto'
            )
          )
          AND (
            r_0.tags LIKE '%AUTOSOLUTION%'
            AND r_0.tags LIKE '%HYPERNET%'
          )
          AND (
            r_0.tags NOT LIKE '%MERCHANT%'
            AND r_0.tags NOT LIKE '%CLUSTER%'
          )
          AND a_0.incre_str_fire_vol < r_0.observing_window THEN 1
          WHEN r_0.tags LIKE '%AUTOSOLUTION%'
          AND r_0.tags LIKE '%HYPERNET MERCHANT%'
          AND a_0.incre_str_fire_vol < 1 * r_0.observing_window THEN 1
          ELSE 0
        END = 1 THEN 1
        ELSE 0
      END = 1
      OR CASE
        WHEN CASE
          WHEN (
            r_0.crs_team LIKE 'gfr_goal_ato%'
            OR r_0.crs_team LIKE 'gfr_goal_ach%'
            OR r_0.crs_team LIKE 'gfr_goal_cc%'
            OR r_0.crs_team LIKE 'gfr_goal_col%'
            OR r_0.crs_team LIKE 'gfr_goal_auto%'
            OR LOWER(r_0.crs_team) LIKE ANY (
              'goal_ato'  ,
              'goal_ach'  ,
              'goal_addfi',
              'goal_cc'   ,
              'goal_col'  ,
              'goal_auto'
            )
          )
          AND (
            r_0.tags LIKE '%AUTOSOLUTION%'
            AND r_0.tags LIKE '%HYPERNET%'
          )
          AND (
            r_0.tags NOT LIKE '%MERCHANT%'
            AND r_0.tags NOT LIKE '%CLUSTER%'
          ) THEN CASE
            WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
              WHEN SAFE_CAST(
                CASE
                  WHEN a_0.cg_txn_wamt > 0 THEN CAST(
                    a_0.cg_brm_bad_wamt / a_0.cg_txn_wamt AS STRING
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              AND a_0.cg_txn_cnt >= 30 THEN 1
              ELSE 0
            END
            WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ATO' THEN CASE
              WHEN SAFE_CAST(
                CASE
                  WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                     ,
                        a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              AND a_0.match_txn_cnt >= 30 THEN 1
              ELSE 0
            END
            ELSE 0
          END
          WHEN r_0.tags LIKE '%AUTOSOLUTION%'
          AND r_0.tags LIKE '%HYPERNET MERCHANT%' THEN CASE
            WHEN CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
              ELSE 0
            END = 1 THEN 0
            WHEN excl_accuracy_0.rule_id IS NOT NULL THEN 0
            WHEN st_0.is_strategy = 1
            AND (
              UPPER(r_0.rule_name) LIKE '%FALLBACK%'
              OR UPPER(r_0.tags) LIKE '%SAFETY%NET%'
            ) THEN CASE
              WHEN r_0.crs_team IN (
                'gfr_omni_gms'    ,
                'gfr_omni_nextgen',
                'gfr_omni'        ,
                'goal_nextgen'    ,
                'goal_gms'
              ) THEN CASE
                WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
                  WHEN a_0.cg_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.cg_txn_wamt > 0 THEN CAST(
                        a_0.cg_brm_bad_wamt / a_0.cg_txn_wamt AS STRING
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                  OR a_0.cg_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                             ,
                            a_0.decled_bad_amt * NUMERIC '1.000' / a_0.decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10' THEN 1
                  ELSE 0
                END
                WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN CASE
                  WHEN a_0.match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                     ,
                            a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10' THEN 1
                  ELSE 0
                END
                WHEN UPPER(RTRIM(r_0.rule_mo)) = 'CC' THEN CASE
                  WHEN a_0.match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                     ,
                            a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                  OR a_0.match_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                             ,
                            a_0.decled_bad_amt * NUMERIC '1.000' / a_0.decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10' THEN 1
                  ELSE 0
                END
                WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
                  WHEN a_0.match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                     ,
                            a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10' THEN 1
                  ELSE 0
                END
                ELSE 0
              END
              WHEN r_0.rule_mo IN ('Collusion', 'UBSM')
              AND xoom_team_0.xoom_ntid IS NULL THEN CASE
                WHEN SAFE_CAST(
                  CASE
                    WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                     ,
                          a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                AND a_0.match_txn_cnt >= 30 THEN 1
                ELSE 0
              END
              ELSE CASE
                WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
                AND xoom_team_0.xoom_ntid IS NULL
                AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
                AND r_0.rule_id NOT IN (467812)
                AND (
                  SAFE_CAST(
                    CASE
                      WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                     ,
                            a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                  AND a_0.match_txn_cnt >= 30
                ) THEN 1
                ELSE 0
              END
            END
            ELSE CASE
              WHEN r_0.crs_team IN (
                'gfr_omni_gms'    ,
                'gfr_omni_nextgen',
                'gfr_omni'        ,
                'goal_nextgen'    ,
                'goal_gms'
              ) THEN CASE
                WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
                  WHEN a_0.cg_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.cg_txn_wamt > 0 THEN CAST(
                        a_0.cg_brm_bad_wamt / a_0.cg_txn_wamt AS STRING
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                  OR a_0.cg_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                             ,
                            a_0.decled_bad_amt * NUMERIC '1.000' / a_0.decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15' THEN 1
                  ELSE 0
                END
                WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN CASE
                  WHEN a_0.match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                     ,
                            a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15' THEN 1
                  ELSE 0
                END
                WHEN UPPER(RTRIM(r_0.rule_mo)) = 'CC' THEN CASE
                  WHEN a_0.match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                     ,
                            a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                  OR a_0.match_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                             ,
                            a_0.decled_bad_amt * NUMERIC '1.000' / a_0.decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15' THEN 1
                  ELSE 0
                END
                WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
                  WHEN a_0.match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                     ,
                            a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15' THEN 1
                  ELSE 0
                END
                ELSE 0
              END
              WHEN r_0.rule_mo IN ('Collusion', 'UBSM')
              AND xoom_team_0.xoom_ntid IS NULL THEN CASE
                WHEN SAFE_CAST(
                  CASE
                    WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                     ,
                          a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                AND a_0.match_txn_cnt >= 30 THEN 1
                ELSE 0
              END
              ELSE CASE
                WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
                AND xoom_team_0.xoom_ntid IS NULL
                AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
                AND r_0.rule_id NOT IN (467812)
                AND (
                  SAFE_CAST(
                    CASE
                      WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                     ,
                            a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                  AND a_0.match_txn_cnt >= 30
                ) THEN 1
                ELSE 0
              END
            END
          END
          ELSE 0
        END + CASE
          WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN 0
          WHEN (
            r_0.crs_team LIKE 'gfr_goal_ato%'
            OR r_0.crs_team LIKE 'gfr_goal_ach%'
            OR r_0.crs_team LIKE 'gfr_goal_cc%'
            OR r_0.crs_team LIKE 'gfr_goal_col%'
            OR r_0.crs_team LIKE 'gfr_goal_auto%'
            OR LOWER(r_0.crs_team) LIKE ANY (
              'goal_ato'  ,
              'goal_ach'  ,
              'goal_addfi',
              'goal_cc'   ,
              'goal_col'  ,
              'goal_auto'
            )
          )
          AND (
            r_0.tags LIKE '%AUTOSOLUTION%'
            AND r_0.tags LIKE '%HYPERNET%'
          )
          AND (
            r_0.tags NOT LIKE '%MERCHANT%'
            AND r_0.tags NOT LIKE '%CLUSTER%'
          ) THEN CASE
            WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
              WHEN CASE
                WHEN e2_0.rule_id IS NOT NULL THEN 1
                ELSE 0
              END = 0
              AND SAFE_CAST(
                CASE
                  WHEN a_0.str_cg_txn_wamt > 0 THEN CAST(
                    a_0.str_cg_brm_bad_wamt / a_0.str_cg_txn_wamt AS STRING
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              AND a_0.str_cg_txn_cnt >= 30 THEN 0
              ELSE 0
            END
            WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ATO' THEN CASE
              WHEN CASE
                WHEN e2_0.rule_id IS NOT NULL THEN 1
                ELSE 0
              END = 0
              AND SAFE_CAST(
                CASE
                  WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                             ,
                        a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              AND a_0.str_match_txn_cnt >= 30 THEN 0
              ELSE 0
            END
            ELSE 0
          END
          WHEN r_0.tags LIKE '%AUTOSOLUTION%'
          AND r_0.tags LIKE '%HYPERNET MERCHANT%' THEN CASE
            WHEN CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
              ELSE 0
            END = 1 THEN 0
            WHEN excl_accuracy_0.rule_id IS NOT NULL THEN 0
            WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN 0
            WHEN st_0.is_strategy = 1
            AND (
              UPPER(r_0.rule_name) LIKE '%FALLBACK%'
              OR UPPER(r_0.tags) LIKE '%SAFETY%NET%'
            ) THEN CASE
              WHEN r_0.crs_team IN (
                'gfr_omni_gms'    ,
                'gfr_omni_nextgen',
                'gfr_omni'        ,
                'goal_nextgen'    ,
                'goal_gms'
              ) THEN CASE
                WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
                  WHEN CASE
                    WHEN e2_0.rule_id IS NOT NULL THEN 1
                    ELSE 0
                  END = 0
                  AND (
                    a_0.str_cg_txn_cnt >= 30
                    AND SAFE_CAST(
                      CASE
                        WHEN a_0.str_cg_txn_wamt > 0 THEN CAST(
                          a_0.str_cg_brm_bad_wamt / a_0.str_cg_txn_wamt AS STRING
                        )
                        ELSE 'NA'
                      END AS BIGNUMERIC
                    ) < NUMERIC '0.10'
                    OR a_0.str_cg_txn_cnt < 30
                    AND SAFE_CAST(
                      CASE
                        WHEN a_0.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                          REGEXP_REPLACE(
                            FORMAT(
                              '%6.3f'                                                                 ,
                              a_0.incre_str_decled_bad_amt * NUMERIC '1.000' / a_0.incre_str_decled_amt
                            )                   ,
                            r'^( *?)(-)?0(\..*)',
                            r'\2\3 \1'
                          )                     ,
                          r'^( *?)(-)?(\d*\..*)',
                          r'\2\3\1'
                        )
                        ELSE 'NA'
                      END AS BIGNUMERIC
                    ) < NUMERIC '0.10'
                  ) THEN 1
                  ELSE 0
                END
                WHEN UPPER(RTRIM(r_0.rule_mo)) = 'CC' THEN CASE
                  WHEN CASE
                    WHEN e2_0.rule_id IS NOT NULL THEN 1
                    ELSE 0
                  END = 0
                  AND (
                    a_0.str_match_txn_cnt >= 30
                    AND SAFE_CAST(
                      CASE
                        WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                          REGEXP_REPLACE(
                            FORMAT(
                              '%6.3f'                                                             ,
                              a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                            )                   ,
                            r'^( *?)(-)?0(\..*)',
                            r'\2\3 \1'
                          )                     ,
                          r'^( *?)(-)?(\d*\..*)',
                          r'\2\3\1'
                        )
                        ELSE 'NA'
                      END AS BIGNUMERIC
                    ) < NUMERIC '0.10'
                    OR a_0.str_match_txn_cnt < 30
                    AND SAFE_CAST(
                      CASE
                        WHEN a_0.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                          REGEXP_REPLACE(
                            FORMAT(
                              '%6.3f'                                                                 ,
                              a_0.incre_str_decled_bad_amt * NUMERIC '1.000' / a_0.incre_str_decled_amt
                            )                   ,
                            r'^( *?)(-)?0(\..*)',
                            r'\2\3 \1'
                          )                     ,
                          r'^( *?)(-)?(\d*\..*)',
                          r'\2\3\1'
                        )
                        ELSE 'NA'
                      END AS BIGNUMERIC
                    ) < NUMERIC '0.10'
                  ) THEN 1
                  ELSE 0
                END
                WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
                  WHEN CASE
                    WHEN e2_0.rule_id IS NOT NULL THEN 1
                    ELSE 0
                  END = 0
                  AND (
                    a_0.str_match_txn_cnt >= 30
                    AND SAFE_CAST(
                      CASE
                        WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                          REGEXP_REPLACE(
                            FORMAT(
                              '%6.3f'                                                             ,
                              a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                            )                   ,
                            r'^( *?)(-)?0(\..*)',
                            r'\2\3 \1'
                          )                     ,
                          r'^( *?)(-)?(\d*\..*)',
                          r'\2\3\1'
                        )
                        ELSE 'NA'
                      END AS BIGNUMERIC
                    ) < NUMERIC '0.10'
                  ) THEN 1
                  ELSE 0
                END
                ELSE 0
              END
              ELSE CASE
                WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
                AND xoom_team_0.xoom_ntid IS NULL
                AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
                AND r_0.rule_id NOT IN (467812)
                AND (
                  CASE
                    WHEN e2_0.rule_id IS NOT NULL THEN 1
                    ELSE 0
                  END = 0
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                             ,
                            a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                  AND a_0.str_match_txn_cnt >= 30
                ) THEN 1
                ELSE 0
              END
            END
            ELSE CASE
              WHEN r_0.crs_team IN (
                'gfr_omni_gms'    ,
                'gfr_omni_nextgen',
                'gfr_omni'        ,
                'goal_nextgen'    ,
                'goal_gms'
              ) THEN CASE
                WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
                  WHEN CASE
                    WHEN e2_0.rule_id IS NOT NULL THEN 1
                    ELSE 0
                  END = 0
                  AND (
                    a_0.str_cg_txn_cnt >= 30
                    AND SAFE_CAST(
                      CASE
                        WHEN a_0.str_cg_txn_wamt > 0 THEN CAST(
                          a_0.str_cg_brm_bad_wamt / a_0.str_cg_txn_wamt AS STRING
                        )
                        ELSE 'NA'
                      END AS BIGNUMERIC
                    ) < NUMERIC '0.15'
                    OR a_0.str_cg_txn_cnt < 30
                    AND SAFE_CAST(
                      CASE
                        WHEN a_0.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                          REGEXP_REPLACE(
                            FORMAT(
                              '%6.3f'                                                                 ,
                              a_0.incre_str_decled_bad_amt * NUMERIC '1.000' / a_0.incre_str_decled_amt
                            )                   ,
                            r'^( *?)(-)?0(\..*)',
                            r'\2\3 \1'
                          )                     ,
                          r'^( *?)(-)?(\d*\..*)',
                          r'\2\3\1'
                        )
                        ELSE 'NA'
                      END AS BIGNUMERIC
                    ) < NUMERIC '0.15'
                  ) THEN 1
                  ELSE 0
                END
                WHEN UPPER(RTRIM(r_0.rule_mo)) = 'CC' THEN CASE
                  WHEN CASE
                    WHEN e2_0.rule_id IS NOT NULL THEN 1
                    ELSE 0
                  END = 0
                  AND (
                    a_0.str_match_txn_cnt >= 30
                    AND SAFE_CAST(
                      CASE
                        WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                          REGEXP_REPLACE(
                            FORMAT(
                              '%6.3f'                                                             ,
                              a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                            )                   ,
                            r'^( *?)(-)?0(\..*)',
                            r'\2\3 \1'
                          )                     ,
                          r'^( *?)(-)?(\d*\..*)',
                          r'\2\3\1'
                        )
                        ELSE 'NA'
                      END AS BIGNUMERIC
                    ) < NUMERIC '0.15'
                    OR a_0.str_match_txn_cnt < 30
                    AND SAFE_CAST(
                      CASE
                        WHEN a_0.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                          REGEXP_REPLACE(
                            FORMAT(
                              '%6.3f'                                                                 ,
                              a_0.incre_str_decled_bad_amt * NUMERIC '1.000' / a_0.incre_str_decled_amt
                            )                   ,
                            r'^( *?)(-)?0(\..*)',
                            r'\2\3 \1'
                          )                     ,
                          r'^( *?)(-)?(\d*\..*)',
                          r'\2\3\1'
                        )
                        ELSE 'NA'
                      END AS BIGNUMERIC
                    ) < NUMERIC '0.15'
                  ) THEN 1
                  ELSE 0
                END
                WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
                  WHEN CASE
                    WHEN e2_0.rule_id IS NOT NULL THEN 1
                    ELSE 0
                  END = 0
                  AND (
                    a_0.str_match_txn_cnt >= 30
                    AND SAFE_CAST(
                      CASE
                        WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                          REGEXP_REPLACE(
                            FORMAT(
                              '%6.3f'                                                             ,
                              a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                            )                   ,
                            r'^( *?)(-)?0(\..*)',
                            r'\2\3 \1'
                          )                     ,
                          r'^( *?)(-)?(\d*\..*)',
                          r'\2\3\1'
                        )
                        ELSE 'NA'
                      END AS BIGNUMERIC
                    ) < NUMERIC '0.15'
                  ) THEN 1
                  ELSE 0
                END
                ELSE 0
              END
              ELSE CASE
                WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
                AND xoom_team_0.xoom_ntid IS NULL
                AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
                AND r_0.rule_id NOT IN (467812)
                AND (
                  CASE
                    WHEN e2_0.rule_id IS NOT NULL THEN 1
                    ELSE 0
                  END = 0
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                             ,
                            a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                  AND a_0.str_match_txn_cnt >= 30
                ) THEN 1
                ELSE 0
              END
            END
          END
          ELSE 0
        END + CASE
          WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN 0
          WHEN (
            r_0.crs_team LIKE 'gfr_goal_ato%'
            OR r_0.crs_team LIKE 'gfr_goal_ach%'
            OR r_0.crs_team LIKE 'gfr_goal_cc%'
            OR r_0.crs_team LIKE 'gfr_goal_col%'
            OR r_0.crs_team LIKE 'gfr_goal_auto%'
            OR LOWER(r_0.crs_team) LIKE ANY (
              'goal_ato'  ,
              'goal_ach'  ,
              'goal_addfi',
              'goal_cc'   ,
              'goal_col'  ,
              'goal_auto'
            )
          )
          AND (
            r_0.tags LIKE '%AUTOSOLUTION%'
            AND r_0.tags LIKE '%HYPERNET%'
          )
          AND (
            r_0.tags NOT LIKE '%MERCHANT%'
            AND r_0.tags NOT LIKE '%CLUSTER%'
          ) THEN CASE
            WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
              WHEN CASE
                WHEN e2_0.rule_id IS NOT NULL THEN 1
                ELSE 0
              END = 0
              AND SAFE_CAST(
                CASE
                  WHEN a_0.actn_cg_txn_wamt > 0 THEN CAST(
                    a_0.actn_cg_brm_bad_wamt / a_0.actn_cg_txn_wamt AS STRING
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              AND a_0.actn_cg_txn_cnt >= 30 THEN 1
              ELSE 0
            END
            WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ATO' THEN CASE
              WHEN CASE
                WHEN e2_0.rule_id IS NOT NULL THEN 1
                ELSE 0
              END = 0
              AND SAFE_CAST(
                CASE
                  WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                               ,
                        a_0.actn_match_bad_wamt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              AND a_0.actn_match_txn_cnt >= 30 THEN 1
              ELSE 0
            END
            ELSE 0
          END
          WHEN r_0.tags LIKE '%AUTOSOLUTION%'
          AND r_0.tags LIKE '%HYPERNET MERCHANT%' THEN CASE
            WHEN CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
              ELSE 0
            END = 1 THEN 0
            WHEN excl_accuracy_0.rule_id IS NOT NULL THEN 0
            WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN 0
            WHEN st_0.is_strategy = 1
            AND (
              UPPER(r_0.rule_name) LIKE '%FALLBACK%'
              OR UPPER(r_0.tags) LIKE '%SAFETY%NET%'
            ) THEN CASE
              WHEN r_0.crs_team IN (
                'gfr_omni_gms'    ,
                'gfr_omni_nextgen',
                'gfr_omni'        ,
                'goal_nextgen'    ,
                'goal_gms'
              ) THEN CASE
                WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
                  WHEN CASE
                    WHEN e2_0.rule_id IS NOT NULL THEN 1
                    ELSE 0
                  END = 0
                  AND (
                    a_0.actn_cg_txn_cnt >= 30
                    AND SAFE_CAST(
                      CASE
                        WHEN a_0.actn_cg_txn_wamt > 0 THEN CAST(
                          a_0.actn_cg_brm_bad_wamt / a_0.actn_cg_txn_wamt AS STRING
                        )
                        ELSE 'NA'
                      END AS BIGNUMERIC
                    ) < NUMERIC '0.10'
                    OR a_0.actn_cg_txn_cnt < 30
                    AND SAFE_CAST(
                      CASE
                        WHEN a_0.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                          REGEXP_REPLACE(
                            FORMAT(
                              '%6.3f'                                                                   ,
                              a_0.incre_actn_decled_bad_amt * NUMERIC '1.000' / a_0.incre_actn_decled_amt
                            )                   ,
                            r'^( *?)(-)?0(\..*)',
                            r'\2\3 \1'
                          )                     ,
                          r'^( *?)(-)?(\d*\..*)',
                          r'\2\3\1'
                        )
                        ELSE 'NA'
                      END AS BIGNUMERIC
                    ) < NUMERIC '0.10'
                  ) THEN 1
                  ELSE 0
                END
                WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
                  WHEN CASE
                    WHEN e2_0.rule_id IS NOT NULL THEN 1
                    ELSE 0
                  END = 0
                  AND (
                    a_0.actn_match_txn_cnt >= 30
                    AND SAFE_CAST(
                      CASE
                        WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                          REGEXP_REPLACE(
                            FORMAT(
                              '%6.3f'                                                               ,
                              a_0.actn_match_bad_wamt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wamt
                            )                   ,
                            r'^( *?)(-)?0(\..*)',
                            r'\2\3 \1'
                          )                     ,
                          r'^( *?)(-)?(\d*\..*)',
                          r'\2\3\1'
                        )
                        ELSE 'NA'
                      END AS BIGNUMERIC
                    ) < NUMERIC '0.10'
                    OR a_0.actn_match_txn_cnt < 30
                    AND SAFE_CAST(
                      CASE
                        WHEN a_0.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                          REGEXP_REPLACE(
                            FORMAT(
                              '%6.3f'                                                                   ,
                              a_0.incre_actn_decled_bad_amt * NUMERIC '1.000' / a_0.incre_actn_decled_amt
                            )                   ,
                            r'^( *?)(-)?0(\..*)',
                            r'\2\3 \1'
                          )                     ,
                          r'^( *?)(-)?(\d*\..*)',
                          r'\2\3\1'
                        )
                        ELSE 'NA'
                      END AS BIGNUMERIC
                    ) < NUMERIC '0.10'
                  ) THEN 1
                  ELSE 0
                END
                ELSE 0
              END
              ELSE CASE
                WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
                AND xoom_team_0.xoom_ntid IS NULL
                AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
                AND r_0.rule_id NOT IN (467812)
                AND (
                  CASE
                    WHEN e2_0.rule_id IS NOT NULL THEN 1
                    ELSE 0
                  END = 0
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                               ,
                            a_0.actn_match_bad_wamt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                  AND a_0.actn_match_txn_cnt >= 30
                ) THEN 1
                ELSE 0
              END
            END
            ELSE CASE
              WHEN r_0.crs_team IN (
                'gfr_omni_gms'    ,
                'gfr_omni_nextgen',
                'gfr_omni'        ,
                'goal_nextgen'    ,
                'goal_gms'
              ) THEN CASE
                WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
                  WHEN CASE
                    WHEN e2_0.rule_id IS NOT NULL THEN 1
                    ELSE 0
                  END = 0
                  AND (
                    a_0.actn_cg_txn_cnt >= 30
                    AND SAFE_CAST(
                      CASE
                        WHEN a_0.actn_cg_txn_wamt > 0 THEN CAST(
                          a_0.actn_cg_brm_bad_wamt / a_0.actn_cg_txn_wamt AS STRING
                        )
                        ELSE 'NA'
                      END AS BIGNUMERIC
                    ) < NUMERIC '0.15'
                    OR a_0.actn_cg_txn_cnt < 30
                    AND SAFE_CAST(
                      CASE
                        WHEN a_0.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                          REGEXP_REPLACE(
                            FORMAT(
                              '%6.3f'                                                                   ,
                              a_0.incre_actn_decled_bad_amt * NUMERIC '1.000' / a_0.incre_actn_decled_amt
                            )                   ,
                            r'^( *?)(-)?0(\..*)',
                            r'\2\3 \1'
                          )                     ,
                          r'^( *?)(-)?(\d*\..*)',
                          r'\2\3\1'
                        )
                        ELSE 'NA'
                      END AS BIGNUMERIC
                    ) < NUMERIC '0.15'
                  ) THEN 1
                  ELSE 0
                END
                WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
                  WHEN CASE
                    WHEN e2_0.rule_id IS NOT NULL THEN 1
                    ELSE 0
                  END = 0
                  AND (
                    a_0.actn_match_txn_cnt >= 30
                    AND SAFE_CAST(
                      CASE
                        WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                          REGEXP_REPLACE(
                            FORMAT(
                              '%6.3f'                                                               ,
                              a_0.actn_match_bad_wamt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wamt
                            )                   ,
                            r'^( *?)(-)?0(\..*)',
                            r'\2\3 \1'
                          )                     ,
                          r'^( *?)(-)?(\d*\..*)',
                          r'\2\3\1'
                        )
                        ELSE 'NA'
                      END AS BIGNUMERIC
                    ) < NUMERIC '0.15'
                    OR a_0.actn_match_txn_cnt < 30
                    AND SAFE_CAST(
                      CASE
                        WHEN a_0.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                          REGEXP_REPLACE(
                            FORMAT(
                              '%6.3f'                                                                   ,
                              a_0.incre_actn_decled_bad_amt * NUMERIC '1.000' / a_0.incre_actn_decled_amt
                            )                   ,
                            r'^( *?)(-)?0(\..*)',
                            r'\2\3 \1'
                          )                     ,
                          r'^( *?)(-)?(\d*\..*)',
                          r'\2\3\1'
                        )
                        ELSE 'NA'
                      END AS BIGNUMERIC
                    ) < NUMERIC '0.15'
                  ) THEN 1
                  ELSE 0
                END
                ELSE 0
              END
              ELSE CASE
                WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
                AND xoom_team_0.xoom_ntid IS NULL
                AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
                AND r_0.rule_id NOT IN (467812)
                AND (
                  CASE
                    WHEN e2_0.rule_id IS NOT NULL THEN 1
                    ELSE 0
                  END = 0
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                               ,
                            a_0.actn_match_bad_wamt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                  AND a_0.actn_match_txn_cnt >= 30
                ) THEN 1
                ELSE 0
              END
            END
          END
          ELSE 0
        END > 0 THEN 1
        ELSE 0
      END = 1
      OR CASE
        WHEN (
          r_0.crs_team LIKE 'gfr_goal_ato%'
          OR r_0.crs_team LIKE 'gfr_goal_ach%'
          OR r_0.crs_team LIKE 'gfr_goal_cc%'
          OR r_0.crs_team LIKE 'gfr_goal_col%'
          OR r_0.crs_team LIKE 'gfr_goal_auto%'
          OR LOWER(r_0.crs_team) LIKE ANY (
            'goal_ato'  ,
            'goal_ach'  ,
            'goal_addfi',
            'goal_cc'   ,
            'goal_col'  ,
            'goal_auto'
          )
        )
        AND (
          r_0.tags LIKE '%AUTOSOLUTION%'
          AND r_0.tags LIKE '%HYPERNET%'
        )
        AND (
          r_0.tags NOT LIKE '%MERCHANT%'
          AND r_0.tags NOT LIKE '%CLUSTER%'
        ) THEN 0
        WHEN r_0.tags LIKE '%AUTOSOLUTION%'
        AND r_0.tags LIKE '%HYPERNET MERCHANT%' THEN CASE
          WHEN CASE
            WHEN e_ow_0.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND SAFE_CAST(
            CASE
              WHEN a_0.fire_vol > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                        ,
                    a_0.sum_wh_actn * NUMERIC '1.000' / a_0.fire_vol
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) >= NUMERIC '0.7'
          AND SAFE_CAST(
            CASE
              WHEN a_0.whed_amt > 0 THEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                  FORMAT(
                    '%6.3f'                                        ,
                    a_0.bad_amt_adj * NUMERIC '1.000' / a_0.whed_amt
                  )                   ,
                  r'^( *?)(-)?0(\..*)',
                  r'\2\3 \1'
                )                     ,
                r'^( *?)(-)?(\d*\..*)',
                r'\2\3\1'
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.1'
          AND a_0.whed_cnt >= 30 THEN 1
          ELSE 0
        END
        ELSE 0
      END = 1
      OR CASE
        WHEN r_0.tags LIKE '%AUTOSOLUTION%'
        AND r_0.tags LIKE '%HYPERNET MERCHANT%' THEN CASE
          WHEN CASE
            WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
            ELSE 0
          END = 0
          AND SAFE_CAST(b_0.ov_pct_1 AS BIGNUMERIC) >= NUMERIC '0.8'
          AND CASE
            WHEN e2_0.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0 THEN CASE
            WHEN r_0.tags LIKE '%AUTOSOLUTION%'
            AND r_0.tags LIKE '%HYPERNET MERCHANT%'
            AND COALESCE(
              st_ov1_0.is_strategy_ov1_raw,
              0
            ) = 1 THEN 1
            WHEN r_0.tags LIKE '%AUTOSOLUTION%'
            AND r_0.tags LIKE '%HYPERNET MERCHANT%'
            AND COALESCE(
              st_ov1_0.is_strategy_ov1_raw,
              0
            ) = 0 THEN 0
            ELSE 1
          END
          ELSE 0
        END
        ELSE 0
      END = 1
    )
    AND (
      r_0.tags LIKE '%AUTOSOLUTION%'
      AND r_0.tags LIKE '%HYPERNET MERCHANT%'
    ) THEN 5
    WHEN CASE
      WHEN 0 = 1
      OR CASE
        WHEN (
          r_0.crs_team LIKE 'gfr_goal_ato%'
          OR r_0.crs_team LIKE 'gfr_goal_ach%'
          OR r_0.crs_team LIKE 'gfr_goal_cc%'
          OR r_0.crs_team LIKE 'gfr_goal_col%'
          OR r_0.crs_team LIKE 'gfr_goal_auto%'
          OR LOWER(r_0.crs_team) LIKE ANY (
            'goal_ato'  ,
            'goal_ach'  ,
            'goal_addfi',
            'goal_cc'   ,
            'goal_col'  ,
            'goal_auto'
          )
        )
        AND (
          r_0.tags LIKE '%AUTOSOLUTION%'
          AND r_0.tags LIKE '%HYPERNET%'
        )
        AND (
          r_0.tags NOT LIKE '%MERCHANT%'
          AND r_0.tags NOT LIKE '%CLUSTER%'
        )
        AND a_0.incre_str_fire_vol < r_0.observing_window THEN 1
        WHEN r_0.tags LIKE '%AUTOSOLUTION%'
        AND r_0.tags LIKE '%HYPERNET MERCHANT%'
        AND a_0.incre_str_fire_vol < 1 * r_0.observing_window THEN 1
        ELSE 0
      END = 1 THEN 1
      ELSE 0
    END = 1
    OR CASE
      WHEN CASE
        WHEN (
          r_0.crs_team LIKE 'gfr_goal_ato%'
          OR r_0.crs_team LIKE 'gfr_goal_ach%'
          OR r_0.crs_team LIKE 'gfr_goal_cc%'
          OR r_0.crs_team LIKE 'gfr_goal_col%'
          OR r_0.crs_team LIKE 'gfr_goal_auto%'
          OR LOWER(r_0.crs_team) LIKE ANY (
            'goal_ato'  ,
            'goal_ach'  ,
            'goal_addfi',
            'goal_cc'   ,
            'goal_col'  ,
            'goal_auto'
          )
        )
        AND (
          r_0.tags LIKE '%AUTOSOLUTION%'
          AND r_0.tags LIKE '%HYPERNET%'
        )
        AND (
          r_0.tags NOT LIKE '%MERCHANT%'
          AND r_0.tags NOT LIKE '%CLUSTER%'
        ) THEN CASE
          WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
            WHEN SAFE_CAST(
              CASE
                WHEN a_0.cg_txn_wamt > 0 THEN CAST(
                  a_0.cg_brm_bad_wamt / a_0.cg_txn_wamt AS STRING
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            AND a_0.cg_txn_cnt >= 30 THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ATO' THEN CASE
            WHEN SAFE_CAST(
              CASE
                WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                     ,
                      a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            AND a_0.match_txn_cnt >= 30 THEN 1
            ELSE 0
          END
          ELSE 0
        END
        WHEN r_0.tags LIKE '%AUTOSOLUTION%'
        AND r_0.tags LIKE '%HYPERNET MERCHANT%' THEN CASE
          WHEN CASE
            WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
            ELSE 0
          END = 1 THEN 0
          WHEN excl_accuracy_0.rule_id IS NOT NULL THEN 0
          WHEN st_0.is_strategy = 1
          AND (
            UPPER(r_0.rule_name) LIKE '%FALLBACK%'
            OR UPPER(r_0.tags) LIKE '%SAFETY%NET%'
          ) THEN CASE
            WHEN r_0.crs_team IN (
              'gfr_omni_gms'    ,
              'gfr_omni_nextgen',
              'gfr_omni'        ,
              'goal_nextgen'    ,
              'goal_gms'
            ) THEN CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
                WHEN a_0.cg_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.cg_txn_wamt > 0 THEN CAST(
                      a_0.cg_brm_bad_wamt / a_0.cg_txn_wamt AS STRING
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                OR a_0.cg_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                             ,
                          a_0.decled_bad_amt * NUMERIC '1.000' / a_0.decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10' THEN 1
                ELSE 0
              END
              WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN CASE
                WHEN a_0.match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                     ,
                          a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10' THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r_0.rule_mo)) = 'CC' THEN CASE
                WHEN a_0.match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                     ,
                          a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                OR a_0.match_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                             ,
                          a_0.decled_bad_amt * NUMERIC '1.000' / a_0.decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10' THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
                WHEN a_0.match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                     ,
                          a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10' THEN 1
                ELSE 0
              END
              ELSE 0
            END
            WHEN r_0.rule_mo IN ('Collusion', 'UBSM')
            AND xoom_team_0.xoom_ntid IS NULL THEN CASE
              WHEN SAFE_CAST(
                CASE
                  WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                     ,
                        a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              AND a_0.match_txn_cnt >= 30 THEN 1
              ELSE 0
            END
            ELSE CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
              AND xoom_team_0.xoom_ntid IS NULL
              AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
              AND r_0.rule_id NOT IN (467812)
              AND (
                SAFE_CAST(
                  CASE
                    WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                     ,
                          a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                AND a_0.match_txn_cnt >= 30
              ) THEN 1
              ELSE 0
            END
          END
          ELSE CASE
            WHEN r_0.crs_team IN (
              'gfr_omni_gms'    ,
              'gfr_omni_nextgen',
              'gfr_omni'        ,
              'goal_nextgen'    ,
              'goal_gms'
            ) THEN CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
                WHEN a_0.cg_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.cg_txn_wamt > 0 THEN CAST(
                      a_0.cg_brm_bad_wamt / a_0.cg_txn_wamt AS STRING
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                OR a_0.cg_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                             ,
                          a_0.decled_bad_amt * NUMERIC '1.000' / a_0.decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15' THEN 1
                ELSE 0
              END
              WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN CASE
                WHEN a_0.match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                     ,
                          a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15' THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r_0.rule_mo)) = 'CC' THEN CASE
                WHEN a_0.match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                     ,
                          a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                OR a_0.match_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                             ,
                          a_0.decled_bad_amt * NUMERIC '1.000' / a_0.decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15' THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
                WHEN a_0.match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                     ,
                          a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15' THEN 1
                ELSE 0
              END
              ELSE 0
            END
            WHEN r_0.rule_mo IN ('Collusion', 'UBSM')
            AND xoom_team_0.xoom_ntid IS NULL THEN CASE
              WHEN SAFE_CAST(
                CASE
                  WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                     ,
                        a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
              AND a_0.match_txn_cnt >= 30 THEN 1
              ELSE 0
            END
            ELSE CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
              AND xoom_team_0.xoom_ntid IS NULL
              AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
              AND r_0.rule_id NOT IN (467812)
              AND (
                SAFE_CAST(
                  CASE
                    WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                     ,
                          a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                AND a_0.match_txn_cnt >= 30
              ) THEN 1
              ELSE 0
            END
          END
        END
        ELSE 0
      END + CASE
        WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN 0
        WHEN (
          r_0.crs_team LIKE 'gfr_goal_ato%'
          OR r_0.crs_team LIKE 'gfr_goal_ach%'
          OR r_0.crs_team LIKE 'gfr_goal_cc%'
          OR r_0.crs_team LIKE 'gfr_goal_col%'
          OR r_0.crs_team LIKE 'gfr_goal_auto%'
          OR LOWER(r_0.crs_team) LIKE ANY (
            'goal_ato'  ,
            'goal_ach'  ,
            'goal_addfi',
            'goal_cc'   ,
            'goal_col'  ,
            'goal_auto'
          )
        )
        AND (
          r_0.tags LIKE '%AUTOSOLUTION%'
          AND r_0.tags LIKE '%HYPERNET%'
        )
        AND (
          r_0.tags NOT LIKE '%MERCHANT%'
          AND r_0.tags NOT LIKE '%CLUSTER%'
        ) THEN CASE
          WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
            WHEN CASE
              WHEN e2_0.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND SAFE_CAST(
              CASE
                WHEN a_0.str_cg_txn_wamt > 0 THEN CAST(
                  a_0.str_cg_brm_bad_wamt / a_0.str_cg_txn_wamt AS STRING
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            AND a_0.str_cg_txn_cnt >= 30 THEN 0
            ELSE 0
          END
          WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ATO' THEN CASE
            WHEN CASE
              WHEN e2_0.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND SAFE_CAST(
              CASE
                WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                             ,
                      a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            AND a_0.str_match_txn_cnt >= 30 THEN 0
            ELSE 0
          END
          ELSE 0
        END
        WHEN r_0.tags LIKE '%AUTOSOLUTION%'
        AND r_0.tags LIKE '%HYPERNET MERCHANT%' THEN CASE
          WHEN CASE
            WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
            ELSE 0
          END = 1 THEN 0
          WHEN excl_accuracy_0.rule_id IS NOT NULL THEN 0
          WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN 0
          WHEN st_0.is_strategy = 1
          AND (
            UPPER(r_0.rule_name) LIKE '%FALLBACK%'
            OR UPPER(r_0.tags) LIKE '%SAFETY%NET%'
          ) THEN CASE
            WHEN r_0.crs_team IN (
              'gfr_omni_gms'    ,
              'gfr_omni_nextgen',
              'gfr_omni'        ,
              'goal_nextgen'    ,
              'goal_gms'
            ) THEN CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
                WHEN CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND (
                  a_0.str_cg_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.str_cg_txn_wamt > 0 THEN CAST(
                        a_0.str_cg_brm_bad_wamt / a_0.str_cg_txn_wamt AS STRING
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                  OR a_0.str_cg_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                                 ,
                            a_0.incre_str_decled_bad_amt * NUMERIC '1.000' / a_0.incre_str_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                ) THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r_0.rule_mo)) = 'CC' THEN CASE
                WHEN CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND (
                  a_0.str_match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                             ,
                            a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                  OR a_0.str_match_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                                 ,
                            a_0.incre_str_decled_bad_amt * NUMERIC '1.000' / a_0.incre_str_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                ) THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
                WHEN CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND (
                  a_0.str_match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                             ,
                            a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                ) THEN 1
                ELSE 0
              END
              ELSE 0
            END
            ELSE CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
              AND xoom_team_0.xoom_ntid IS NULL
              AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
              AND r_0.rule_id NOT IN (467812)
              AND (
                CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                             ,
                          a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                AND a_0.str_match_txn_cnt >= 30
              ) THEN 1
              ELSE 0
            END
          END
          ELSE CASE
            WHEN r_0.crs_team IN (
              'gfr_omni_gms'    ,
              'gfr_omni_nextgen',
              'gfr_omni'        ,
              'goal_nextgen'    ,
              'goal_gms'
            ) THEN CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
                WHEN CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND (
                  a_0.str_cg_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.str_cg_txn_wamt > 0 THEN CAST(
                        a_0.str_cg_brm_bad_wamt / a_0.str_cg_txn_wamt AS STRING
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                  OR a_0.str_cg_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                                 ,
                            a_0.incre_str_decled_bad_amt * NUMERIC '1.000' / a_0.incre_str_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                ) THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r_0.rule_mo)) = 'CC' THEN CASE
                WHEN CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND (
                  a_0.str_match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                             ,
                            a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                  OR a_0.str_match_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                                 ,
                            a_0.incre_str_decled_bad_amt * NUMERIC '1.000' / a_0.incre_str_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                ) THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
                WHEN CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND (
                  a_0.str_match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                             ,
                            a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                ) THEN 1
                ELSE 0
              END
              ELSE 0
            END
            ELSE CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
              AND xoom_team_0.xoom_ntid IS NULL
              AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
              AND r_0.rule_id NOT IN (467812)
              AND (
                CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                             ,
                          a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                AND a_0.str_match_txn_cnt >= 30
              ) THEN 1
              ELSE 0
            END
          END
        END
        ELSE 0
      END + CASE
        WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN 0
        WHEN (
          r_0.crs_team LIKE 'gfr_goal_ato%'
          OR r_0.crs_team LIKE 'gfr_goal_ach%'
          OR r_0.crs_team LIKE 'gfr_goal_cc%'
          OR r_0.crs_team LIKE 'gfr_goal_col%'
          OR r_0.crs_team LIKE 'gfr_goal_auto%'
          OR LOWER(r_0.crs_team) LIKE ANY (
            'goal_ato'  ,
            'goal_ach'  ,
            'goal_addfi',
            'goal_cc'   ,
            'goal_col'  ,
            'goal_auto'
          )
        )
        AND (
          r_0.tags LIKE '%AUTOSOLUTION%'
          AND r_0.tags LIKE '%HYPERNET%'
        )
        AND (
          r_0.tags NOT LIKE '%MERCHANT%'
          AND r_0.tags NOT LIKE '%CLUSTER%'
        ) THEN CASE
          WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
            WHEN CASE
              WHEN e2_0.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND SAFE_CAST(
              CASE
                WHEN a_0.actn_cg_txn_wamt > 0 THEN CAST(
                  a_0.actn_cg_brm_bad_wamt / a_0.actn_cg_txn_wamt AS STRING
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            AND a_0.actn_cg_txn_cnt >= 30 THEN 1
            ELSE 0
          END
          WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ATO' THEN CASE
            WHEN CASE
              WHEN e2_0.rule_id IS NOT NULL THEN 1
              ELSE 0
            END = 0
            AND SAFE_CAST(
              CASE
                WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    FORMAT(
                      '%6.3f'                                                               ,
                      a_0.actn_match_bad_wamt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wamt
                    )                   ,
                    r'^( *?)(-)?0(\..*)',
                    r'\2\3 \1'
                  )                     ,
                  r'^( *?)(-)?(\d*\..*)',
                  r'\2\3\1'
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.10'
            AND a_0.actn_match_txn_cnt >= 30 THEN 1
            ELSE 0
          END
          ELSE 0
        END
        WHEN r_0.tags LIKE '%AUTOSOLUTION%'
        AND r_0.tags LIKE '%HYPERNET MERCHANT%' THEN CASE
          WHEN CASE
            WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
            ELSE 0
          END = 1 THEN 0
          WHEN excl_accuracy_0.rule_id IS NOT NULL THEN 0
          WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN 0
          WHEN st_0.is_strategy = 1
          AND (
            UPPER(r_0.rule_name) LIKE '%FALLBACK%'
            OR UPPER(r_0.tags) LIKE '%SAFETY%NET%'
          ) THEN CASE
            WHEN r_0.crs_team IN (
              'gfr_omni_gms'    ,
              'gfr_omni_nextgen',
              'gfr_omni'        ,
              'goal_nextgen'    ,
              'goal_gms'
            ) THEN CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
                WHEN CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND (
                  a_0.actn_cg_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.actn_cg_txn_wamt > 0 THEN CAST(
                        a_0.actn_cg_brm_bad_wamt / a_0.actn_cg_txn_wamt AS STRING
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                  OR a_0.actn_cg_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                                   ,
                            a_0.incre_actn_decled_bad_amt * NUMERIC '1.000' / a_0.incre_actn_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                ) THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
                WHEN CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND (
                  a_0.actn_match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                               ,
                            a_0.actn_match_bad_wamt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                  OR a_0.actn_match_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                                   ,
                            a_0.incre_actn_decled_bad_amt * NUMERIC '1.000' / a_0.incre_actn_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                ) THEN 1
                ELSE 0
              END
              ELSE 0
            END
            ELSE CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
              AND xoom_team_0.xoom_ntid IS NULL
              AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
              AND r_0.rule_id NOT IN (467812)
              AND (
                CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                               ,
                          a_0.actn_match_bad_wamt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                AND a_0.actn_match_txn_cnt >= 30
              ) THEN 1
              ELSE 0
            END
          END
          ELSE CASE
            WHEN r_0.crs_team IN (
              'gfr_omni_gms'    ,
              'gfr_omni_nextgen',
              'gfr_omni'        ,
              'goal_nextgen'    ,
              'goal_gms'
            ) THEN CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
                WHEN CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND (
                  a_0.actn_cg_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.actn_cg_txn_wamt > 0 THEN CAST(
                        a_0.actn_cg_brm_bad_wamt / a_0.actn_cg_txn_wamt AS STRING
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                  OR a_0.actn_cg_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                                   ,
                            a_0.incre_actn_decled_bad_amt * NUMERIC '1.000' / a_0.incre_actn_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                ) THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
                WHEN CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND (
                  a_0.actn_match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                               ,
                            a_0.actn_match_bad_wamt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                  OR a_0.actn_match_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                                   ,
                            a_0.incre_actn_decled_bad_amt * NUMERIC '1.000' / a_0.incre_actn_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                ) THEN 1
                ELSE 0
              END
              ELSE 0
            END
            ELSE CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
              AND xoom_team_0.xoom_ntid IS NULL
              AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
              AND r_0.rule_id NOT IN (467812)
              AND (
                CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                               ,
                          a_0.actn_match_bad_wamt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                AND a_0.actn_match_txn_cnt >= 30
              ) THEN 1
              ELSE 0
            END
          END
        END
        ELSE 0
      END > 0 THEN 1
      ELSE 0
    END = 1
    OR CASE
      WHEN (
        r_0.crs_team LIKE 'gfr_goal_ato%'
        OR r_0.crs_team LIKE 'gfr_goal_ach%'
        OR r_0.crs_team LIKE 'gfr_goal_cc%'
        OR r_0.crs_team LIKE 'gfr_goal_col%'
        OR r_0.crs_team LIKE 'gfr_goal_auto%'
        OR LOWER(r_0.crs_team) LIKE ANY (
          'goal_ato'  ,
          'goal_ach'  ,
          'goal_addfi',
          'goal_cc'   ,
          'goal_col'  ,
          'goal_auto'
        )
      )
      AND (
        r_0.tags LIKE '%AUTOSOLUTION%'
        AND r_0.tags LIKE '%HYPERNET%'
      )
      AND (
        r_0.tags NOT LIKE '%MERCHANT%'
        AND r_0.tags NOT LIKE '%CLUSTER%'
      ) THEN 0
      WHEN r_0.tags LIKE '%AUTOSOLUTION%'
      AND r_0.tags LIKE '%HYPERNET MERCHANT%' THEN CASE
        WHEN CASE
          WHEN e_ow_0.rule_id IS NOT NULL THEN 1
          ELSE 0
        END = 0
        AND SAFE_CAST(
          CASE
            WHEN a_0.fire_vol > 0 THEN REGEXP_REPLACE(
              REGEXP_REPLACE(
                FORMAT(
                  '%6.3f'                                        ,
                  a_0.sum_wh_actn * NUMERIC '1.000' / a_0.fire_vol
                )                   ,
                r'^( *?)(-)?0(\..*)',
                r'\2\3 \1'
              )                     ,
              r'^( *?)(-)?(\d*\..*)',
              r'\2\3\1'
            )
            ELSE 'NA'
          END AS BIGNUMERIC
        ) >= NUMERIC '0.7'
        AND SAFE_CAST(
          CASE
            WHEN a_0.whed_amt > 0 THEN REGEXP_REPLACE(
              REGEXP_REPLACE(
                FORMAT(
                  '%6.3f'                                        ,
                  a_0.bad_amt_adj * NUMERIC '1.000' / a_0.whed_amt
                )                   ,
                r'^( *?)(-)?0(\..*)',
                r'\2\3 \1'
              )                     ,
              r'^( *?)(-)?(\d*\..*)',
              r'\2\3\1'
            )
            ELSE 'NA'
          END AS BIGNUMERIC
        ) < NUMERIC '0.1'
        AND a_0.whed_cnt >= 30 THEN 1
        ELSE 0
      END
      ELSE 0
    END = 1
    OR CASE
      WHEN r_0.tags LIKE '%AUTOSOLUTION%'
      AND r_0.tags LIKE '%HYPERNET MERCHANT%' THEN CASE
        WHEN CASE
          WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
          ELSE 0
        END = 0
        AND SAFE_CAST(b_0.ov_pct_1 AS BIGNUMERIC) >= NUMERIC '0.8'
        AND CASE
          WHEN e2_0.rule_id IS NOT NULL THEN 1
          ELSE 0
        END = 0 THEN CASE
          WHEN r_0.tags LIKE '%AUTOSOLUTION%'
          AND r_0.tags LIKE '%HYPERNET MERCHANT%'
          AND COALESCE(
            st_ov1_0.is_strategy_ov1_raw,
            0
          ) = 1 THEN 1
          WHEN r_0.tags LIKE '%AUTOSOLUTION%'
          AND r_0.tags LIKE '%HYPERNET MERCHANT%'
          AND COALESCE(
            st_ov1_0.is_strategy_ov1_raw,
            0
          ) = 0 THEN 0
          ELSE 1
        END
        ELSE 0
      END
      ELSE 0
    END = 1 THEN 5
    WHEN st_0.is_strategy = 1
    AND (
      CASE
        WHEN CASE
          WHEN CASE
            WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
            ELSE 0
          END = 1 THEN 0
          WHEN excl_accuracy_0.rule_id IS NOT NULL THEN 0
          WHEN st_0.is_strategy = 1
          AND (
            UPPER(r_0.rule_name) LIKE '%FALLBACK%'
            OR UPPER(r_0.tags) LIKE '%SAFETY%NET%'
          ) THEN CASE
            WHEN r_0.crs_team IN (
              'gfr_omni_gms'    ,
              'gfr_omni_nextgen',
              'gfr_omni'        ,
              'goal_nextgen'    ,
              'goal_gms'
            ) THEN CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
                WHEN a_0.cg_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.cg_txn_wamt > 0 THEN CAST(
                      a_0.cg_brm_bad_wamt / a_0.cg_txn_wamt AS STRING
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                OR a_0.cg_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                             ,
                          a_0.decled_bad_amt * NUMERIC '1.000' / a_0.decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10' THEN 1
                ELSE 0
              END
              WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN CASE
                WHEN a_0.match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                     ,
                          a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10' THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r_0.rule_mo)) = 'CC' THEN CASE
                WHEN a_0.match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                     ,
                          a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                OR a_0.match_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                             ,
                          a_0.decled_bad_amt * NUMERIC '1.000' / a_0.decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10' THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
                WHEN a_0.match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                     ,
                          a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10' THEN 1
                ELSE 0
              END
              ELSE 0
            END
            WHEN r_0.rule_mo IN ('Collusion', 'UBSM')
            AND xoom_team_0.xoom_ntid IS NULL THEN CASE
              WHEN SAFE_CAST(
                CASE
                  WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                     ,
                        a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              AND a_0.match_txn_cnt >= 30 THEN 1
              ELSE 0
            END
            ELSE CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
              AND xoom_team_0.xoom_ntid IS NULL
              AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
              AND r_0.rule_id NOT IN (467812)
              AND (
                SAFE_CAST(
                  CASE
                    WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                     ,
                          a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                AND a_0.match_txn_cnt >= 30
              ) THEN 1
              ELSE 0
            END
          END
          ELSE CASE
            WHEN r_0.crs_team IN (
              'gfr_omni_gms'    ,
              'gfr_omni_nextgen',
              'gfr_omni'        ,
              'goal_nextgen'    ,
              'goal_gms'
            ) THEN CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
                WHEN a_0.cg_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.cg_txn_wamt > 0 THEN CAST(
                      a_0.cg_brm_bad_wamt / a_0.cg_txn_wamt AS STRING
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                OR a_0.cg_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                             ,
                          a_0.decled_bad_amt * NUMERIC '1.000' / a_0.decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15' THEN 1
                ELSE 0
              END
              WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN CASE
                WHEN a_0.match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                     ,
                          a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15' THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r_0.rule_mo)) = 'CC' THEN CASE
                WHEN a_0.match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                     ,
                          a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                OR a_0.match_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                             ,
                          a_0.decled_bad_amt * NUMERIC '1.000' / a_0.decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15' THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
                WHEN a_0.match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                     ,
                          a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15' THEN 1
                ELSE 0
              END
              ELSE 0
            END
            WHEN r_0.rule_mo IN ('Collusion', 'UBSM')
            AND xoom_team_0.xoom_ntid IS NULL THEN CASE
              WHEN SAFE_CAST(
                CASE
                  WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                     ,
                        a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
              AND a_0.match_txn_cnt >= 30 THEN 1
              ELSE 0
            END
            ELSE CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
              AND xoom_team_0.xoom_ntid IS NULL
              AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
              AND r_0.rule_id NOT IN (467812)
              AND (
                SAFE_CAST(
                  CASE
                    WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                     ,
                          a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                AND a_0.match_txn_cnt >= 30
              ) THEN 1
              ELSE 0
            END
          END
        END + CASE
          WHEN CASE
            WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
            ELSE 0
          END = 1 THEN 0
          WHEN excl_accuracy_0.rule_id IS NOT NULL THEN 0
          WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN 0
          WHEN st_0.is_strategy = 1
          AND (
            UPPER(r_0.rule_name) LIKE '%FALLBACK%'
            OR UPPER(r_0.tags) LIKE '%SAFETY%NET%'
          ) THEN CASE
            WHEN r_0.crs_team IN (
              'gfr_omni_gms'    ,
              'gfr_omni_nextgen',
              'gfr_omni'        ,
              'goal_nextgen'    ,
              'goal_gms'
            ) THEN CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
                WHEN CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND (
                  a_0.actn_cg_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.actn_cg_txn_wamt > 0 THEN CAST(
                        a_0.actn_cg_brm_bad_wamt / a_0.actn_cg_txn_wamt AS STRING
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                  OR a_0.actn_cg_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                                   ,
                            a_0.incre_actn_decled_bad_amt * NUMERIC '1.000' / a_0.incre_actn_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                ) THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
                WHEN CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND (
                  a_0.actn_match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                               ,
                            a_0.actn_match_bad_wamt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                  OR a_0.actn_match_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                                   ,
                            a_0.incre_actn_decled_bad_amt * NUMERIC '1.000' / a_0.incre_actn_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                ) THEN 1
                ELSE 0
              END
              ELSE 0
            END
            ELSE CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
              AND xoom_team_0.xoom_ntid IS NULL
              AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
              AND r_0.rule_id NOT IN (467812)
              AND (
                CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                               ,
                          a_0.actn_match_bad_wamt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                AND a_0.actn_match_txn_cnt >= 30
              ) THEN 1
              ELSE 0
            END
          END
          ELSE CASE
            WHEN r_0.crs_team IN (
              'gfr_omni_gms'    ,
              'gfr_omni_nextgen',
              'gfr_omni'        ,
              'goal_nextgen'    ,
              'goal_gms'
            ) THEN CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
                WHEN CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND (
                  a_0.actn_cg_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.actn_cg_txn_wamt > 0 THEN CAST(
                        a_0.actn_cg_brm_bad_wamt / a_0.actn_cg_txn_wamt AS STRING
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                  OR a_0.actn_cg_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                                   ,
                            a_0.incre_actn_decled_bad_amt * NUMERIC '1.000' / a_0.incre_actn_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                ) THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
                WHEN CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND (
                  a_0.actn_match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                               ,
                            a_0.actn_match_bad_wamt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                  OR a_0.actn_match_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                                   ,
                            a_0.incre_actn_decled_bad_amt * NUMERIC '1.000' / a_0.incre_actn_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                ) THEN 1
                ELSE 0
              END
              ELSE 0
            END
            ELSE CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
              AND xoom_team_0.xoom_ntid IS NULL
              AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
              AND r_0.rule_id NOT IN (467812)
              AND (
                CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                               ,
                          a_0.actn_match_bad_wamt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                AND a_0.actn_match_txn_cnt >= 30
              ) THEN 1
              ELSE 0
            END
          END
        END + CASE
          WHEN CASE
            WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
            ELSE 0
          END = 1 THEN 0
          WHEN excl_accuracy_0.rule_id IS NOT NULL THEN 0
          WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN 0
          WHEN st_0.is_strategy = 1
          AND (
            UPPER(r_0.rule_name) LIKE '%FALLBACK%'
            OR UPPER(r_0.tags) LIKE '%SAFETY%NET%'
          ) THEN CASE
            WHEN r_0.crs_team IN (
              'gfr_omni_gms'    ,
              'gfr_omni_nextgen',
              'gfr_omni'        ,
              'goal_nextgen'    ,
              'goal_gms'
            ) THEN CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
                WHEN CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND (
                  a_0.str_cg_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.str_cg_txn_wamt > 0 THEN CAST(
                        a_0.str_cg_brm_bad_wamt / a_0.str_cg_txn_wamt AS STRING
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                  OR a_0.str_cg_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                                 ,
                            a_0.incre_str_decled_bad_amt * NUMERIC '1.000' / a_0.incre_str_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                ) THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r_0.rule_mo)) = 'CC' THEN CASE
                WHEN CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND (
                  a_0.str_match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                             ,
                            a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                  OR a_0.str_match_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                                 ,
                            a_0.incre_str_decled_bad_amt * NUMERIC '1.000' / a_0.incre_str_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                ) THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
                WHEN CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND (
                  a_0.str_match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                             ,
                            a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                ) THEN 1
                ELSE 0
              END
              ELSE 0
            END
            ELSE CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
              AND xoom_team_0.xoom_ntid IS NULL
              AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
              AND r_0.rule_id NOT IN (467812)
              AND (
                CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                             ,
                          a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                AND a_0.str_match_txn_cnt >= 30
              ) THEN 1
              ELSE 0
            END
          END
          ELSE CASE
            WHEN r_0.crs_team IN (
              'gfr_omni_gms'    ,
              'gfr_omni_nextgen',
              'gfr_omni'        ,
              'goal_nextgen'    ,
              'goal_gms'
            ) THEN CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
                WHEN CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND (
                  a_0.str_cg_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.str_cg_txn_wamt > 0 THEN CAST(
                        a_0.str_cg_brm_bad_wamt / a_0.str_cg_txn_wamt AS STRING
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                  OR a_0.str_cg_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                                 ,
                            a_0.incre_str_decled_bad_amt * NUMERIC '1.000' / a_0.incre_str_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                ) THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r_0.rule_mo)) = 'CC' THEN CASE
                WHEN CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND (
                  a_0.str_match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                             ,
                            a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                  OR a_0.str_match_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                                 ,
                            a_0.incre_str_decled_bad_amt * NUMERIC '1.000' / a_0.incre_str_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                ) THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
                WHEN CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND (
                  a_0.str_match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                             ,
                            a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                ) THEN 1
                ELSE 0
              END
              ELSE 0
            END
            ELSE CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
              AND xoom_team_0.xoom_ntid IS NULL
              AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
              AND r_0.rule_id NOT IN (467812)
              AND (
                CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                             ,
                          a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                AND a_0.str_match_txn_cnt >= 30
              ) THEN 1
              ELSE 0
            END
          END
        END > 0 THEN 1
        ELSE 0
      END = 1
      OR CASE
        WHEN (
          a_0.fire_vol < 10 * r_0.observing_window
          AND (
            SAFE_CAST(
              CASE
                WHEN a_0.cg_txn_wamt > 0 THEN CAST(
                  a_0.cg_brm_bad_wamt / a_0.cg_txn_wamt AS STRING
                )
                ELSE 'NA'
              END AS BIGNUMERIC
            ) < NUMERIC '0.1'
            OR RTRIM(
              CASE
                WHEN a_0.cg_txn_wamt > 0 THEN CAST(
                  a_0.cg_brm_bad_wamt / a_0.cg_txn_wamt AS STRING
                )
                ELSE 'NA'
              END
            ) = 'NA'
          )
          OR CASE
            WHEN e2_0.rule_id IS NOT NULL THEN 1
            ELSE 0
          END = 0
          AND a_0.incre_rule_fire_vol < 5 * r_0.observing_window
        )
        AND CASE
          WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
          ELSE 0
        END = 0
        AND CASE
          WHEN e_0.rule_id IS NOT NULL THEN 1
          ELSE 0
        END = 0 THEN 1
        ELSE 0
      END = 1
      OR CASE
        WHEN CASE
          WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
          ELSE 0
        END = 0
        AND SAFE_CAST(b_0.ov_pct_1 AS BIGNUMERIC) >= NUMERIC '0.8'
        AND CASE
          WHEN e2_0.rule_id IS NOT NULL THEN 1
          ELSE 0
        END = 0 THEN CASE
          WHEN r_0.tags LIKE '%AUTOSOLUTION%'
          AND r_0.tags LIKE '%HYPERNET MERCHANT%'
          AND COALESCE(
            st_ov1_0.is_strategy_ov1_raw,
            0
          ) = 1 THEN 1
          WHEN r_0.tags LIKE '%AUTOSOLUTION%'
          AND r_0.tags LIKE '%HYPERNET MERCHANT%'
          AND COALESCE(
            st_ov1_0.is_strategy_ov1_raw,
            0
          ) = 0 THEN 0
          ELSE 1
        END
        ELSE 0
      END = 1
      OR CASE
        WHEN CASE
          WHEN e_ow_0.rule_id IS NOT NULL THEN 1
          ELSE 0
        END = 0
        AND SAFE_CAST(
          CASE
            WHEN a_0.fire_vol > 0 THEN REGEXP_REPLACE(
              REGEXP_REPLACE(
                FORMAT(
                  '%6.3f'                                        ,
                  a_0.sum_wh_actn * NUMERIC '1.000' / a_0.fire_vol
                )                   ,
                r'^( *?)(-)?0(\..*)',
                r'\2\3 \1'
              )                     ,
              r'^( *?)(-)?(\d*\..*)',
              r'\2\3\1'
            )
            ELSE 'NA'
          END AS BIGNUMERIC
        ) >= NUMERIC '0.7'
        AND SAFE_CAST(
          CASE
            WHEN a_0.whed_amt > 0 THEN REGEXP_REPLACE(
              REGEXP_REPLACE(
                FORMAT(
                  '%6.3f'                                        ,
                  a_0.bad_amt_adj * NUMERIC '1.000' / a_0.whed_amt
                )                   ,
                r'^( *?)(-)?0(\..*)',
                r'\2\3 \1'
              )                     ,
              r'^( *?)(-)?(\d*\..*)',
              r'\2\3\1'
            )
            ELSE 'NA'
          END AS BIGNUMERIC
        ) < NUMERIC '0.1'
        AND a_0.whed_cnt >= 30 THEN 1
        ELSE 0
      END = 1
    ) THEN 1
    WHEN st_0.is_strategy = 1
    AND CASE
      WHEN CASE
        WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
        ELSE 0
      END = 1 THEN 0
      ELSE COALESCE(cntct_0.high_cntct_rate_, 0)
    END = 1 THEN 2
    WHEN st_0.is_strategy = 0
    AND (
      CASE
        WHEN CASE
          WHEN CASE
            WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
            ELSE 0
          END = 1 THEN 0
          WHEN excl_accuracy_0.rule_id IS NOT NULL THEN 0
          WHEN st_0.is_strategy = 1
          AND (
            UPPER(r_0.rule_name) LIKE '%FALLBACK%'
            OR UPPER(r_0.tags) LIKE '%SAFETY%NET%'
          ) THEN CASE
            WHEN r_0.crs_team IN (
              'gfr_omni_gms'    ,
              'gfr_omni_nextgen',
              'gfr_omni'        ,
              'goal_nextgen'    ,
              'goal_gms'
            ) THEN CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
                WHEN a_0.cg_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.cg_txn_wamt > 0 THEN CAST(
                      a_0.cg_brm_bad_wamt / a_0.cg_txn_wamt AS STRING
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                OR a_0.cg_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                             ,
                          a_0.decled_bad_amt * NUMERIC '1.000' / a_0.decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10' THEN 1
                ELSE 0
              END
              WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN CASE
                WHEN a_0.match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                     ,
                          a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10' THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r_0.rule_mo)) = 'CC' THEN CASE
                WHEN a_0.match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                     ,
                          a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                OR a_0.match_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                             ,
                          a_0.decled_bad_amt * NUMERIC '1.000' / a_0.decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10' THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
                WHEN a_0.match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                     ,
                          a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10' THEN 1
                ELSE 0
              END
              ELSE 0
            END
            WHEN r_0.rule_mo IN ('Collusion', 'UBSM')
            AND xoom_team_0.xoom_ntid IS NULL THEN CASE
              WHEN SAFE_CAST(
                CASE
                  WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                     ,
                        a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.10'
              AND a_0.match_txn_cnt >= 30 THEN 1
              ELSE 0
            END
            ELSE CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
              AND xoom_team_0.xoom_ntid IS NULL
              AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
              AND r_0.rule_id NOT IN (467812)
              AND (
                SAFE_CAST(
                  CASE
                    WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                     ,
                          a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                AND a_0.match_txn_cnt >= 30
              ) THEN 1
              ELSE 0
            END
          END
          ELSE CASE
            WHEN r_0.crs_team IN (
              'gfr_omni_gms'    ,
              'gfr_omni_nextgen',
              'gfr_omni'        ,
              'goal_nextgen'    ,
              'goal_gms'
            ) THEN CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
                WHEN a_0.cg_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.cg_txn_wamt > 0 THEN CAST(
                      a_0.cg_brm_bad_wamt / a_0.cg_txn_wamt AS STRING
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                OR a_0.cg_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                             ,
                          a_0.decled_bad_amt * NUMERIC '1.000' / a_0.decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15' THEN 1
                ELSE 0
              END
              WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN CASE
                WHEN a_0.match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                     ,
                          a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15' THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r_0.rule_mo)) = 'CC' THEN CASE
                WHEN a_0.match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                     ,
                          a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                OR a_0.match_txn_cnt < 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.decled_amt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                             ,
                          a_0.decled_bad_amt * NUMERIC '1.000' / a_0.decled_amt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15' THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
                WHEN a_0.match_txn_cnt >= 30
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                     ,
                          a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15' THEN 1
                ELSE 0
              END
              ELSE 0
            END
            WHEN r_0.rule_mo IN ('Collusion', 'UBSM')
            AND xoom_team_0.xoom_ntid IS NULL THEN CASE
              WHEN SAFE_CAST(
                CASE
                  WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      FORMAT(
                        '%6.3f'                                                     ,
                        a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                      )                   ,
                      r'^( *?)(-)?0(\..*)',
                      r'\2\3 \1'
                    )                     ,
                    r'^( *?)(-)?(\d*\..*)',
                    r'\2\3\1'
                  )
                  ELSE 'NA'
                END AS BIGNUMERIC
              ) < NUMERIC '0.15'
              AND a_0.match_txn_cnt >= 30 THEN 1
              ELSE 0
            END
            ELSE CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
              AND xoom_team_0.xoom_ntid IS NULL
              AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
              AND r_0.rule_id NOT IN (467812)
              AND (
                SAFE_CAST(
                  CASE
                    WHEN a_0.match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                     ,
                          a_0.match_bad_wamt_adj * NUMERIC '1.000' / a_0.match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                AND a_0.match_txn_cnt >= 30
              ) THEN 1
              ELSE 0
            END
          END
        END + CASE
          WHEN CASE
            WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
            ELSE 0
          END = 1 THEN 0
          WHEN excl_accuracy_0.rule_id IS NOT NULL THEN 0
          WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN 0
          WHEN st_0.is_strategy = 1
          AND (
            UPPER(r_0.rule_name) LIKE '%FALLBACK%'
            OR UPPER(r_0.tags) LIKE '%SAFETY%NET%'
          ) THEN CASE
            WHEN r_0.crs_team IN (
              'gfr_omni_gms'    ,
              'gfr_omni_nextgen',
              'gfr_omni'        ,
              'goal_nextgen'    ,
              'goal_gms'
            ) THEN CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
                WHEN CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND (
                  a_0.actn_cg_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.actn_cg_txn_wamt > 0 THEN CAST(
                        a_0.actn_cg_brm_bad_wamt / a_0.actn_cg_txn_wamt AS STRING
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                  OR a_0.actn_cg_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                                   ,
                            a_0.incre_actn_decled_bad_amt * NUMERIC '1.000' / a_0.incre_actn_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                ) THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
                WHEN CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND (
                  a_0.actn_match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                               ,
                            a_0.actn_match_bad_wamt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                  OR a_0.actn_match_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                                   ,
                            a_0.incre_actn_decled_bad_amt * NUMERIC '1.000' / a_0.incre_actn_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                ) THEN 1
                ELSE 0
              END
              ELSE 0
            END
            ELSE CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
              AND xoom_team_0.xoom_ntid IS NULL
              AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
              AND r_0.rule_id NOT IN (467812)
              AND (
                CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                               ,
                          a_0.actn_match_bad_wamt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                AND a_0.actn_match_txn_cnt >= 30
              ) THEN 1
              ELSE 0
            END
          END
          ELSE CASE
            WHEN r_0.crs_team IN (
              'gfr_omni_gms'    ,
              'gfr_omni_nextgen',
              'gfr_omni'        ,
              'goal_nextgen'    ,
              'goal_gms'
            ) THEN CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
                WHEN CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND (
                  a_0.actn_cg_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.actn_cg_txn_wamt > 0 THEN CAST(
                        a_0.actn_cg_brm_bad_wamt / a_0.actn_cg_txn_wamt AS STRING
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                  OR a_0.actn_cg_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                                   ,
                            a_0.incre_actn_decled_bad_amt * NUMERIC '1.000' / a_0.incre_actn_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                ) THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
                WHEN CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND (
                  a_0.actn_match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                               ,
                            a_0.actn_match_bad_wamt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                  OR a_0.actn_match_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.incre_actn_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                                   ,
                            a_0.incre_actn_decled_bad_amt * NUMERIC '1.000' / a_0.incre_actn_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                ) THEN 1
                ELSE 0
              END
              ELSE 0
            END
            ELSE CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
              AND xoom_team_0.xoom_ntid IS NULL
              AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
              AND r_0.rule_id NOT IN (467812)
              AND (
                CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.actn_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                               ,
                          a_0.actn_match_bad_wamt_adj * NUMERIC '1.000' / a_0.actn_match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                AND a_0.actn_match_txn_cnt >= 30
              ) THEN 1
              ELSE 0
            END
          END
        END + CASE
          WHEN CASE
            WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
            ELSE 0
          END = 1 THEN 0
          WHEN excl_accuracy_0.rule_id IS NOT NULL THEN 0
          WHEN r_0.rule_mo IN ('Collusion', 'UBSM') THEN 0
          WHEN st_0.is_strategy = 1
          AND (
            UPPER(r_0.rule_name) LIKE '%FALLBACK%'
            OR UPPER(r_0.tags) LIKE '%SAFETY%NET%'
          ) THEN CASE
            WHEN r_0.crs_team IN (
              'gfr_omni_gms'    ,
              'gfr_omni_nextgen',
              'gfr_omni'        ,
              'goal_nextgen'    ,
              'goal_gms'
            ) THEN CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
                WHEN CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND (
                  a_0.str_cg_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.str_cg_txn_wamt > 0 THEN CAST(
                        a_0.str_cg_brm_bad_wamt / a_0.str_cg_txn_wamt AS STRING
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                  OR a_0.str_cg_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                                 ,
                            a_0.incre_str_decled_bad_amt * NUMERIC '1.000' / a_0.incre_str_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                ) THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r_0.rule_mo)) = 'CC' THEN CASE
                WHEN CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND (
                  a_0.str_match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                             ,
                            a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                  OR a_0.str_match_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                                 ,
                            a_0.incre_str_decled_bad_amt * NUMERIC '1.000' / a_0.incre_str_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                ) THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
                WHEN CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND (
                  a_0.str_match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                             ,
                            a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.10'
                ) THEN 1
                ELSE 0
              END
              ELSE 0
            END
            ELSE CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
              AND xoom_team_0.xoom_ntid IS NULL
              AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
              AND r_0.rule_id NOT IN (467812)
              AND (
                CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                             ,
                          a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.10'
                AND a_0.str_match_txn_cnt >= 30
              ) THEN 1
              ELSE 0
            END
          END
          ELSE CASE
            WHEN r_0.crs_team IN (
              'gfr_omni_gms'    ,
              'gfr_omni_nextgen',
              'gfr_omni'        ,
              'goal_nextgen'    ,
              'goal_gms'
            ) THEN CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) = 'ATO' THEN CASE
                WHEN CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND (
                  a_0.str_cg_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.str_cg_txn_wamt > 0 THEN CAST(
                        a_0.str_cg_brm_bad_wamt / a_0.str_cg_txn_wamt AS STRING
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                  OR a_0.str_cg_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                                 ,
                            a_0.incre_str_decled_bad_amt * NUMERIC '1.000' / a_0.incre_str_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                ) THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r_0.rule_mo)) = 'CC' THEN CASE
                WHEN CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND (
                  a_0.str_match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                             ,
                            a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                  OR a_0.str_match_txn_cnt < 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.incre_str_decled_amt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                                 ,
                            a_0.incre_str_decled_bad_amt * NUMERIC '1.000' / a_0.incre_str_decled_amt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                ) THEN 1
                ELSE 0
              END
              WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE' THEN CASE
                WHEN CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND (
                  a_0.str_match_txn_cnt >= 30
                  AND SAFE_CAST(
                    CASE
                      WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          FORMAT(
                            '%6.3f'                                                             ,
                            a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                          )                   ,
                          r'^( *?)(-)?0(\..*)',
                          r'\2\3 \1'
                        )                     ,
                        r'^( *?)(-)?(\d*\..*)',
                        r'\2\3\1'
                      )
                      ELSE 'NA'
                    END AS BIGNUMERIC
                  ) < NUMERIC '0.15'
                ) THEN 1
                ELSE 0
              END
              ELSE 0
            END
            ELSE CASE
              WHEN UPPER(RTRIM(r_0.rule_mo)) <> 'ABUSE'
              AND xoom_team_0.xoom_ntid IS NULL
              AND UPPER(r_0.tags) NOT LIKE '%%GOOD%%POP%%AF%%'
              AND r_0.rule_id NOT IN (467812)
              AND (
                CASE
                  WHEN e2_0.rule_id IS NOT NULL THEN 1
                  ELSE 0
                END = 0
                AND SAFE_CAST(
                  CASE
                    WHEN a_0.str_match_txn_wamt > 0 THEN REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        FORMAT(
                          '%6.3f'                                                             ,
                          a_0.str_match_bad_wamt_adj * NUMERIC '1.000' / a_0.str_match_txn_wamt
                        )                   ,
                        r'^( *?)(-)?0(\..*)',
                        r'\2\3 \1'
                      )                     ,
                      r'^( *?)(-)?(\d*\..*)',
                      r'\2\3\1'
                    )
                    ELSE 'NA'
                  END AS BIGNUMERIC
                ) < NUMERIC '0.15'
                AND a_0.str_match_txn_cnt >= 30
              ) THEN 1
              ELSE 0
            END
          END
        END > 0 THEN 1
        ELSE 0
      END = 1
      OR CASE
        WHEN CASE
          WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
          ELSE 0
        END = 0
        AND SAFE_CAST(b_0.ov_pct_1 AS BIGNUMERIC) >= NUMERIC '0.8'
        AND CASE
          WHEN e2_0.rule_id IS NOT NULL THEN 1
          ELSE 0
        END = 0 THEN CASE
          WHEN r_0.tags LIKE '%AUTOSOLUTION%'
          AND r_0.tags LIKE '%HYPERNET MERCHANT%'
          AND COALESCE(
            st_ov1_0.is_strategy_ov1_raw,
            0
          ) = 1 THEN 1
          WHEN r_0.tags LIKE '%AUTOSOLUTION%'
          AND r_0.tags LIKE '%HYPERNET MERCHANT%'
          AND COALESCE(
            st_ov1_0.is_strategy_ov1_raw,
            0
          ) = 0 THEN 0
          ELSE 1
        END
        ELSE 0
      END = 1
      OR CASE
        WHEN CASE
          WHEN e_ow_0.rule_id IS NOT NULL THEN 1
          ELSE 0
        END = 0
        AND SAFE_CAST(
          CASE
            WHEN a_0.fire_vol > 0 THEN REGEXP_REPLACE(
              REGEXP_REPLACE(
                FORMAT(
                  '%6.3f'                                        ,
                  a_0.sum_wh_actn * NUMERIC '1.000' / a_0.fire_vol
                )                   ,
                r'^( *?)(-)?0(\..*)',
                r'\2\3 \1'
              )                     ,
              r'^( *?)(-)?(\d*\..*)',
              r'\2\3\1'
            )
            ELSE 'NA'
          END AS BIGNUMERIC
        ) >= NUMERIC '0.7'
        AND SAFE_CAST(
          CASE
            WHEN a_0.whed_amt > 0 THEN REGEXP_REPLACE(
              REGEXP_REPLACE(
                FORMAT(
                  '%6.3f'                                        ,
                  a_0.bad_amt_adj * NUMERIC '1.000' / a_0.whed_amt
                )                   ,
                r'^( *?)(-)?0(\..*)',
                r'\2\3 \1'
              )                     ,
              r'^( *?)(-)?(\d*\..*)',
              r'\2\3\1'
            )
            ELSE 'NA'
          END AS BIGNUMERIC
        ) < NUMERIC '0.1'
        AND a_0.whed_cnt >= 30 THEN 1
        ELSE 0
      END = 1
      OR COALESCE(
        CASE
          WHEN CASE
            WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
            ELSE 0
          END = 1 THEN 0
          ELSE COALESCE(cntct_0.high_cntct_rate_, 0)
        END,
        0
      ) = 1
    ) THEN 2
    WHEN st_0.is_strategy = 0
    AND CASE
      WHEN (
        a_0.fire_vol < 10 * r_0.observing_window
        AND (
          SAFE_CAST(
            CASE
              WHEN a_0.cg_txn_wamt > 0 THEN CAST(
                a_0.cg_brm_bad_wamt / a_0.cg_txn_wamt AS STRING
              )
              ELSE 'NA'
            END AS BIGNUMERIC
          ) < NUMERIC '0.1'
          OR RTRIM(
            CASE
              WHEN a_0.cg_txn_wamt > 0 THEN CAST(
                a_0.cg_brm_bad_wamt / a_0.cg_txn_wamt AS STRING
              )
              ELSE 'NA'
            END
          ) = 'NA'
        )
        OR CASE
          WHEN e2_0.rule_id IS NOT NULL THEN 1
          ELSE 0
        END = 0
        AND a_0.incre_rule_fire_vol < 5 * r_0.observing_window
      )
      AND CASE
        WHEN UPPER(RTRIM(r_0.rule_mo)) = 'BUSINESSPOLICY' THEN 1
        ELSE 0
      END = 0
      AND CASE
        WHEN e_0.rule_id IS NOT NULL THEN 1
        ELSE 0
      END = 0 THEN 1
      ELSE 0
    END = 1 THEN 3
  END AS tier
FROM
  `pypl-edw.${dataset_name_tmp}.decline_metrics_auth_flow_pld` AS a_0
  INNER JOIN `pypl-edw.${dataset_name_tmp}.check_action_rule_id` AS r_0 ON a_0.rule_id = r_0.rule_id
  LEFT OUTER JOIN (
    SELECT
      UPPER(xoom_team_list_0.ntid) AS xoom_ntid
    FROM
      `pypl-edw`.pp_risk_crs_core.xoom_team_list AS xoom_team_list_0
    WHERE
      xoom_team_list_0.is_effective = 1
    GROUP BY
      1
  ) AS xoom_team_0 ON UPPER(RTRIM(r_0.owner_ntid)) = UPPER(RTRIM(xoom_team_0.xoom_ntid))
  LEFT OUTER JOIN `pypl-edw.${dataset_name_tmp}.delta_auth_flow_pld` AS d_0 ON a_0.rule_id = d_0.rule_id
  LEFT OUTER JOIN `pypl-edw.${dataset_name_tmp}.overlaprule_auth_flow_pld` AS b_0 ON a_0.rule_id = b_0.rule_id
  LEFT OUTER JOIN (
    SELECT
      --  left join ${decline_gms_auth_flow_pld} dg
      --  on a.rule_id=dg.rule_id
      rule_mo_table_1.rule_id                          ,
      rule_mo_table_1.is_strategy AS is_strategy_ov1_raw
    FROM
      `pypl-edw`.pp_risk_crs_core.rule_mo_uni AS rule_mo_table_1
    QUALIFY
      ROW_NUMBER() OVER (
        PARTITION BY
          rule_mo_table_1.rule_id
        ORDER BY
          rule_mo_table_1.baseline DESC
      ) = 1
  ) AS st_ov1_0 ON CAST(
    REPLACE(b_0.ov_rule_1, 'NA', '-1.') AS BIGNUMERIC
  ) = st_ov1_0.rule_id
  LEFT OUTER JOIN (
    SELECT
      live_metadata_table_2.chkpnt_delimited_str,
      live_metadata_table_2.action_raw_str      ,
      live_metadata_table_2.tags                ,
      live_metadata_table_2.rule_id
    FROM
      `pypl-edw.${dataset_name_tmp}.urm_lite` AS live_metadata_table_2
    WHERE
      UPPER(live_metadata_table_2.tags) LIKE '%%RISK%%APPETITE%%'
      OR UPPER(live_metadata_table_2.tags) LIKE '%%SAFETY%%NET%%'
      OR UPPER(live_metadata_table_2.tags) LIKE '%%BLACK%%LIST%%'
      OR UPPER(live_metadata_table_2.tags) LIKE '%%BLOCK%%LIST%%'
      OR UPPER(live_metadata_table_2.tags) LIKE '%%LATAM%%IPR%%'
      OR UPPER(live_metadata_table_2.tags) LIKE '%%BUSINESS%%REQUEST%%'
      OR UPPER(live_metadata_table_2.tags) LIKE '%TESTING%'
      OR UPPER(live_metadata_table_2.tags) LIKE '%%MARKET%%RAMPING%%'
      OR UPPER(live_metadata_table_2.tags) LIKE '%VEDA%'
      OR UPPER(live_metadata_table_2.tags) LIKE '%VSDS%'
      OR UPPER(live_metadata_table_2.tags) LIKE '%SPEEDSTER%'
      OR UPPER(live_metadata_table_2.tags) LIKE '%HIGHCONTACTCOST%'
      OR UPPER(live_metadata_table_2.tags) LIKE '%NNAENABLEMENT%'
      OR UPPER(live_metadata_table_2.tags) LIKE '%RED%CARPET%'
      OR UPPER(live_metadata_table_2.tags) LIKE '%STRATEGYFRAMEWORK%'
  ) AS e_0 ON a_0.rule_id = e_0.rule_id
  LEFT OUTER JOIN (
    SELECT
      live_metadata_table_3.chkpnt_delimited_str,
      live_metadata_table_3.action_raw_str      ,
      live_metadata_table_3.tags                ,
      live_metadata_table_3.rule_id
    FROM
      `pypl-edw.${dataset_name_tmp}.urm_lite` AS live_metadata_table_3
    WHERE
      UPPER(live_metadata_table_3.tags) LIKE '%UELV_YOUNG_GUEST%'
      OR live_metadata_table_3.rule_id IN (
        607986,
        609172,
        612391,
        625094,
        612323,
        788770,
        805167,
        825037,
        876060,
        803134,
        803094,
        876060,
        877503,
        523010
      )
      OR UPPER(live_metadata_table_3.tags) LIKE '%NEXT OPEN LOOP%'
      OR UPPER(live_metadata_table_3.tags) LIKE '%GMS UCC CBP%'
  ) AS excl_accuracy_0 ON a_0.rule_id = excl_accuracy_0.rule_id
  LEFT OUTER JOIN (
    SELECT
      rule_list_stage_2_0.rule_id            ,
      rule_list_stage_2_0.priority           ,
      rule_list_stage_2_0.rule_name          ,
      rule_list_stage_2_0.control_group      ,
      rule_list_stage_2_0.crs_team           ,
      rule_list_stage_2_0.owner_ntid         ,
      rule_list_stage_2_0.rule_mo            ,
      rule_list_stage_2_0.tags               ,
      rule_list_stage_2_0.last_release_length,
      rule_list_stage_2_0.observing_window
    FROM
      `pypl-edw.${dataset_name_tmp}.check_action_rule_id` AS rule_list_stage_2_0
    WHERE
      UPPER(rule_list_stage_2_0.tags) LIKE '%%SCRIPT%%ATTACK%%'
      OR UPPER(rule_list_stage_2_0.tags) LIKE '%%SUPER%%DECLINE%%'
      OR UPPER(rule_list_stage_2_0.tags) LIKE '%%BILLING%%AGREEMENT%%'
      OR UPPER(rule_list_stage_2_0.tags) LIKE '%PURE%MODEL%STRATEGY%'
      AND UPPER(rule_list_stage_2_0.crs_team) LIKE ANY ('GFR_OMNI_GMS%', 'GOAL_GMS')
      OR UPPER(rule_list_stage_2_0.tags) LIKE '%%SAFETY%%NET%%'
  ) AS e2_0 ON a_0.rule_id = e2_0.rule_id
  LEFT OUTER JOIN (
    SELECT
      live_metadata_table_4.rule_id
    FROM
      `pypl-edw.${dataset_name_tmp}.urm_lite` AS live_metadata_table_4
    WHERE
      UPPER(live_metadata_table_4.tags) LIKE '%INTERNAL TEST%'
      OR UPPER(live_metadata_table_4.tags) LIKE '%GOOD%POP%AF%'
  ) AS e_ow_0 ON a_0.rule_id = e_ow_0.rule_id
  LEFT OUTER JOIN (
    SELECT
      --  2021-07-28: For Yanping's feedback on AF test rules
      rule_mo_table_2.rule_id   ,
      rule_mo_table_2.is_strategy
    FROM
      `pypl-edw`.pp_risk_crs_core.rule_mo_uni AS rule_mo_table_2
    QUALIFY
      ROW_NUMBER() OVER (
        PARTITION BY
          rule_mo_table_2.rule_id
        ORDER BY
          rule_mo_table_2.baseline DESC
      ) = 1
  ) AS st_0 ON CAST(a_0.rule_id AS INT64) = st_0.rule_id
  LEFT OUTER JOIN `pypl-edw.${dataset_name_tmp}.contact_metrics_auth_flow_pld` AS cntct_0 ON a_0.rule_id = cntct_0.rule_id
UNION ALL
SELECT
  cw_ss.crs_team                                ,
  cw_ss.owner_ntid                              ,
  cw_ss.rule_id                                 ,
  cw_ss.action_type                             ,
  cw_ss.monitoring_window                       ,
  cw_ss.observing_window                        ,
  cw_ss.rule_mo                                 ,
  cw_ss.rule_name                               ,
  cw_ss.fire_vol                                ,
  cw_ss.u2u_fire_vol                            ,
  CAST(cw_ss.fire_amt AS BIGNUMERIC) AS fire_amt,
  cw_ss.cg_txn_cnt                              ,
  CAST(
    cw_ss.cg_txn_wcnt AS BIGNUMERIC
  ) AS cg_txn_wcnt                                  ,
  CAST(cw_ss.cg_txn_amt AS BIGNUMERIC) AS cg_txn_amt,
  CAST(
    cw_ss.cg_txn_wamt AS BIGNUMERIC
  ) AS cg_txn_wamt,
  CAST(
    cw_ss.cg_brm_bad_wcnt AS BIGNUMERIC
  ) AS cg_brm_bad_wcnt,
  CAST(
    cw_ss.cg_brm_bad_wcnt_adj AS BIGNUMERIC
  ) AS cg_brm_bad_wcnt_adj,
  CAST(
    cw_ss.cg_brm_bad_wamt AS BIGNUMERIC
  ) AS cg_brm_bad_wamt,
  CAST(
    cw_ss.cg_brm_bad_wamt_adj AS BIGNUMERIC
  ) AS cg_brm_bad_wamt_adj  ,
  cw_ss.cg_wcnt_bad_rate    ,
  cw_ss.cg_wamt_bad_rate    ,
  cw_ss.cg_wcnt_bad_rate_adj,
  cw_ss.cg_wamt_bad_rate_adj,
  cw_ss.str_cg_txn_cnt      ,
  CAST(
    cw_ss.str_cg_txn_wcnt AS BIGNUMERIC
  ) AS str_cg_txn_wcnt,
  CAST(
    cw_ss.str_cg_txn_amt AS BIGNUMERIC
  ) AS str_cg_txn_amt,
  CAST(
    cw_ss.str_cg_txn_wamt AS BIGNUMERIC
  ) AS str_cg_txn_wamt,
  CAST(
    cw_ss.str_cg_brm_bad_wcnt AS BIGNUMERIC
  ) AS str_cg_brm_bad_wcnt,
  CAST(
    cw_ss.str_cg_brm_bad_wcnt_adj AS BIGNUMERIC
  ) AS str_cg_brm_bad_wcnt_adj,
  CAST(
    cw_ss.str_cg_brm_bad_wamt AS BIGNUMERIC
  ) AS str_cg_brm_bad_wamt,
  CAST(
    cw_ss.str_cg_brm_bad_wamt_adj AS BIGNUMERIC
  ) AS str_cg_brm_bad_wamt_adj  ,
  cw_ss.str_cg_wcnt_bad_rate    ,
  cw_ss.str_cg_wamt_bad_rate    ,
  cw_ss.str_cg_wcnt_bad_rate_adj,
  cw_ss.str_cg_wamt_bad_rate_adj,
  cw_ss.actn_cg_txn_cnt         ,
  CAST(
    cw_ss.actn_cg_txn_wcnt AS BIGNUMERIC
  ) AS actn_cg_txn_wcnt,
  CAST(
    cw_ss.actn_cg_txn_amt AS BIGNUMERIC
  ) AS actn_cg_txn_amt,
  CAST(
    cw_ss.actn_cg_txn_wamt AS BIGNUMERIC
  ) AS actn_cg_txn_wamt,
  CAST(
    cw_ss.actn_cg_brm_bad_wcnt AS BIGNUMERIC
  ) AS actn_cg_brm_bad_wcnt,
  CAST(
    cw_ss.actn_cg_brm_bad_wcnt_adj AS BIGNUMERIC
  ) AS actn_cg_brm_bad_wcnt_adj,
  CAST(
    cw_ss.actn_cg_brm_bad_wamt AS BIGNUMERIC
  ) AS actn_cg_brm_bad_wamt,
  CAST(
    cw_ss.actn_cg_brm_bad_wamt_adj AS BIGNUMERIC
  ) AS actn_cg_brm_bad_wamt_adj  ,
  cw_ss.actn_cg_wcnt_bad_rate    ,
  cw_ss.actn_cg_wamt_bad_rate    ,
  cw_ss.actn_cg_wcnt_bad_rate_adj,
  cw_ss.actn_cg_wamt_bad_rate_adj,
  CAST(
    cw_ss.match_txn_cnt AS BIGNUMERIC
  ) AS match_txn_cnt,
  CAST(
    cw_ss.match_txn_wcnt AS BIGNUMERIC
  ) AS match_txn_wcnt,
  CAST(
    cw_ss.match_txn_amt AS BIGNUMERIC
  ) AS match_txn_amt,
  CAST(
    cw_ss.match_txn_wamt AS BIGNUMERIC
  ) AS match_txn_wamt,
  CAST(
    cw_ss.match_bad_wcnt AS BIGNUMERIC
  ) AS match_bad_wcnt,
  CAST(
    cw_ss.match_bad_wcnt_adj AS BIGNUMERIC
  ) AS match_bad_wcnt_adj,
  CAST(
    cw_ss.match_bad_wamt AS BIGNUMERIC
  ) AS match_bad_wamt,
  CAST(
    cw_ss.match_bad_wamt_adj AS BIGNUMERIC
  ) AS match_bad_wamt_adj      ,
  cw_ss.match_wcnt_bad_rate    ,
  cw_ss.match_wamt_bad_rate    ,
  cw_ss.match_wcnt_bad_rate_adj,
  cw_ss.match_wamt_bad_rate_adj,
  cw_ss.match_wamt_fpr         ,
  cw_ss.match_wamt_fpr_adj     ,
  CAST(
    cw_ss.str_match_txn_cnt AS BIGNUMERIC
  ) AS str_match_txn_cnt,
  CAST(
    cw_ss.str_match_txn_wcnt AS BIGNUMERIC
  ) AS str_match_txn_wcnt,
  CAST(
    cw_ss.str_match_txn_amt AS BIGNUMERIC
  ) AS str_match_txn_amt,
  CAST(
    cw_ss.str_match_txn_wamt AS BIGNUMERIC
  ) AS str_match_txn_wamt,
  CAST(
    cw_ss.str_match_bad_wcnt AS BIGNUMERIC
  ) AS str_match_bad_wcnt,
  CAST(
    cw_ss.str_match_bad_wcnt_adj AS BIGNUMERIC
  ) AS str_match_bad_wcnt_adj,
  CAST(
    cw_ss.str_match_bad_wamt AS BIGNUMERIC
  ) AS str_match_bad_wamt,
  CAST(
    cw_ss.str_match_bad_wamt_adj AS BIGNUMERIC
  ) AS str_match_bad_wamt_adj      ,
  cw_ss.str_match_wcnt_bad_rate    ,
  cw_ss.str_match_wamt_bad_rate    ,
  cw_ss.str_match_wcnt_bad_rate_adj,
  cw_ss.str_match_wamt_bad_rate_adj,
  CAST(
    cw_ss.actn_match_txn_cnt AS BIGNUMERIC
  ) AS actn_match_txn_cnt,
  CAST(
    cw_ss.actn_match_txn_wcnt AS BIGNUMERIC
  ) AS actn_match_txn_wcnt,
  CAST(
    cw_ss.actn_match_txn_amt AS BIGNUMERIC
  ) AS actn_match_txn_amt,
  CAST(
    cw_ss.actn_match_txn_wamt AS BIGNUMERIC
  ) AS actn_match_txn_wamt,
  CAST(
    cw_ss.actn_match_bad_wcnt AS BIGNUMERIC
  ) AS actn_match_bad_wcnt,
  CAST(
    cw_ss.actn_match_bad_wcnt_adj AS BIGNUMERIC
  ) AS actn_match_bad_wcnt_adj,
  CAST(
    cw_ss.actn_match_bad_wamt AS BIGNUMERIC
  ) AS actn_match_bad_wamt,
  CAST(
    cw_ss.actn_match_bad_wamt_adj AS BIGNUMERIC
  ) AS actn_match_bad_wamt_adj      ,
  cw_ss.actn_match_wcnt_bad_rate    ,
  cw_ss.actn_match_wamt_bad_rate    ,
  cw_ss.actn_match_wcnt_bad_rate_adj,
  cw_ss.actn_match_wamt_bad_rate_adj,
  cw_ss.delta_bad_cnt               ,
  cw_ss.delta_txn_cnt               ,
  CAST(
    cw_ss.delta_bad_amt AS BIGNUMERIC
  ) AS delta_bad_amt,
  CAST(
    cw_ss.delta_txn_amt AS BIGNUMERIC
  ) AS delta_txn_amt                                ,
  cw_ss.delta_cnt_bad_rate                          ,
  cw_ss.delta_amt_bad_rate                          ,
  CAST(cw_ss.decled_cnt AS BIGNUMERIC) AS decled_cnt,
  CAST(cw_ss.decled_amt AS BIGNUMERIC) AS decled_amt,
  CAST(
    cw_ss.decled_bad_cnt AS BIGNUMERIC
  ) AS decled_bad_cnt,
  CAST(
    cw_ss.decled_bad_amt AS BIGNUMERIC
  ) AS decled_bad_amt,
  CAST(
    cw_ss.incre_str_decled_cnt AS BIGNUMERIC
  ) AS incre_str_decled_cnt,
  CAST(
    cw_ss.incre_str_decled_amt AS BIGNUMERIC
  ) AS incre_str_decled_amt,
  CAST(
    cw_ss.incre_str_decled_bad_cnt AS BIGNUMERIC
  ) AS incre_str_decled_bad_cnt,
  CAST(
    cw_ss.incre_str_decled_bad_amt AS BIGNUMERIC
  ) AS incre_str_decled_bad_amt,
  CAST(
    cw_ss.incre_actn_decled_cnt AS BIGNUMERIC
  ) AS incre_actn_decled_cnt,
  CAST(
    cw_ss.incre_actn_decled_amt AS BIGNUMERIC
  ) AS incre_actn_decled_amt,
  CAST(
    cw_ss.incre_actn_decled_bad_cnt AS BIGNUMERIC
  ) AS incre_actn_decled_bad_cnt,
  CAST(
    cw_ss.incre_actn_decled_bad_amt AS BIGNUMERIC
  ) AS incre_actn_decled_bad_amt                ,
  cw_ss.decline_cnt_bad_rate                    ,
  cw_ss.decline_amt_bad_rate                    ,
  cw_ss.incre_str_decline_cnt_bad_rate          ,
  cw_ss.incre_str_decline_amt_bad_rate          ,
  cw_ss.incre_actn_decline_cnt_bad_rate         ,
  cw_ss.incre_actn_decline_amt_bad_rate         ,
  cw_ss.sum_wh_actn                             ,
  cw_ss.whed_pct                                ,
  cw_ss.whed_cnt                                ,
  CAST(cw_ss.whed_amt AS BIGNUMERIC) AS whed_amt,
  CAST(
    cw_ss.bad_amt_adj AS BIGNUMERIC
  ) AS bad_amt_adj                                ,
  cw_ss.wh_badrate                                ,
  cw_ss.incre_str_fire_vol                        ,
  cw_ss.incre_rule_fire_vol                       ,
  cw_ss.incre_actn_fire_vol                       ,
  cw_ss.ov_pct                                    ,
  CAST(cw_ss.catch_bad AS BIGNUMERIC) AS catch_bad,
  CAST(
    cw_ss.incre_str_fire_amt AS BIGNUMERIC
  ) AS incre_str_fire_amt,
  CAST(
    cw_ss.incre_str_non_whed_fire_amt AS BIGNUMERIC
  ) AS incre_str_non_whed_fire_amt                     ,
  SUBSTR(cw_ss.ov_rule_1, 1, 10) AS ov_rule_1          ,
  cw_ss.ov_pct_1                                       ,
  cw_ss.ov_ntid_1                                      ,
  SUBSTR(cw_ss.ov_rule_2, 1, 10) AS ov_rule_2          ,
  cw_ss.ov_pct_2                                       ,
  cw_ss.ov_ntid_2                                      ,
  SUBSTR(cw_ss.ov_rule_3, 1, 10) AS ov_rule_3          ,
  cw_ss.ov_pct_3                                       ,
  cw_ss.ov_ntid_3                                      ,
  SUBSTR(cw_ss.ov_rule_4, 1, 10) AS ov_rule_4          ,
  cw_ss.ov_pct_4                                       ,
  cw_ss.ov_ntid_4                                      ,
  cw_ss.decline_cnt                                    ,
  cw_ss.cntct_cnt                                      ,
  CAST(cw_ss.cntct_rate AS FLOAT64) AS cntct_rate      ,
  cw_ss.high_cntct_rate                                ,
  cw_ss.business_policy                                ,
  cw_ss.exclusion_flg                                  ,
  cw_ss.incre_exempt                                   ,
  cw_ss.excl_high_overlap                              ,
  cw_ss.excl_over_whitelisted                          ,
  cw_ss.is_strategy                                    ,
  cw_ss.is_strategy_ov1                                ,
  cw_ss.low_firing                                     ,
  cw_ss.high_overlap                                   ,
  cw_ss.over_whitelisted                               ,
  cw_ss.sub_poor_accuracy_bad_rate                     ,
  cw_ss.sub_poor_accuracy_incre_str_bad_rate           ,
  cw_ss.sub_poor_accuracy_incre_actn_bad_rate          ,
  cw_ss.poor_accuracy                                  ,
  cw_ss.sub_low_firing_all_firing                      ,
  cw_ss.sub_low_firing_incre_firing                    ,
  cw_ss.automated_low_firing_cnt                       ,
  cw_ss.sub_automated_poor_accuracy_bad_rate           ,
  cw_ss.sub_automated_poor_accuracy_incre_str_bad_rate ,
  cw_ss.sub_automated_poor_accuracy_incre_actn_bad_rate,
  cw_ss.automated_poor_accuracy                        ,
  cw_ss.automated_low_firing_amt                       ,
  cw_ss.automated_over_whitelisted                     ,
  cw_ss.automated_low_firing                           ,
  cw_ss.automated_high_overlap                         ,
  cw_ss.tier
FROM
  (
    SELECT
      -- ----------------------------------zero firing rules---------------------------------
      sss.crs_team                        ,
      sss.owner_ntid                      ,
      sss.rule_id                         ,
      0 AS action_type                    ,
      -- ---to update with action type name
      --date_diff(date_sub(current_date('America/Los_Angeles'), interval 7 DAY), extract(date from b.action_start_date), DAY)
      CONCAT(
        CAST(
          CURRENT_DATE('America/Los_Angeles') - 90 AS STRING
        )     ,
        ' to ',
        CAST(
          CURRENT_DATE('America/Los_Angeles') - 7 AS STRING
        )
      ) AS monitoring_window                 ,
      sss.observing_window                   ,
      sss.rule_mo                            ,
      sss.rule_name                          ,
      0 AS fire_vol                          ,
      0 AS u2u_fire_vol                      ,
      0 AS fire_amt                          ,
      0 AS cg_txn_cnt                        ,
      0 AS cg_txn_wcnt                       ,
      0 AS cg_txn_amt                        ,
      0 AS cg_txn_wamt                       ,
      0 AS cg_brm_bad_wcnt                   ,
      0 AS cg_brm_bad_wcnt_adj               ,
      0 AS cg_brm_bad_wamt                   ,
      0 AS cg_brm_bad_wamt_adj               ,
      'NA' AS cg_wcnt_bad_rate               ,
      'NA' AS cg_wamt_bad_rate               ,
      'NA' AS cg_wcnt_bad_rate_adj           ,
      'NA' AS cg_wamt_bad_rate_adj           ,
      0 AS str_cg_txn_cnt                    ,
      0 AS str_cg_txn_wcnt                   ,
      0 AS str_cg_txn_amt                    ,
      0 AS str_cg_txn_wamt                   ,
      0 AS str_cg_brm_bad_wcnt               ,
      0 AS str_cg_brm_bad_wcnt_adj           ,
      0 AS str_cg_brm_bad_wamt               ,
      0 AS str_cg_brm_bad_wamt_adj           ,
      'NA' AS str_cg_wcnt_bad_rate           ,
      'NA' AS str_cg_wamt_bad_rate           ,
      'NA' AS str_cg_wcnt_bad_rate_adj       ,
      'NA' AS str_cg_wamt_bad_rate_adj       ,
      0 AS actn_cg_txn_cnt                   ,
      0 AS actn_cg_txn_wcnt                  ,
      0 AS actn_cg_txn_amt                   ,
      0 AS actn_cg_txn_wamt                  ,
      0 AS actn_cg_brm_bad_wcnt              ,
      0 AS actn_cg_brm_bad_wcnt_adj          ,
      0 AS actn_cg_brm_bad_wamt              ,
      0 AS actn_cg_brm_bad_wamt_adj          ,
      'NA' AS actn_cg_wcnt_bad_rate          ,
      'NA' AS actn_cg_wamt_bad_rate          ,
      'NA' AS actn_cg_wcnt_bad_rate_adj      ,
      'NA' AS actn_cg_wamt_bad_rate_adj      ,
      0 AS match_txn_cnt                     ,
      0 AS match_txn_wcnt                    ,
      0 AS match_txn_amt                     ,
      0 AS match_txn_wamt                    ,
      0 AS match_bad_wcnt                    ,
      0 AS match_bad_wcnt_adj                ,
      0 AS match_bad_wamt                    ,
      0 AS match_bad_wamt_adj                ,
      'NA' AS match_wcnt_bad_rate            ,
      'NA' AS match_wamt_bad_rate            ,
      'NA' AS match_wcnt_bad_rate_adj        ,
      'NA' AS match_wamt_bad_rate_adj        ,
      'NA' AS match_wamt_fpr                 ,
      'NA' AS match_wamt_fpr_adj             ,
      0 AS str_match_txn_cnt                 ,
      0 AS str_match_txn_wcnt                ,
      0 AS str_match_txn_amt                 ,
      0 AS str_match_txn_wamt                ,
      0 AS str_match_bad_wcnt                ,
      0 AS str_match_bad_wcnt_adj            ,
      0 AS str_match_bad_wamt                ,
      0 AS str_match_bad_wamt_adj            ,
      'NA' AS str_match_wcnt_bad_rate        ,
      'NA' AS str_match_wamt_bad_rate        ,
      'NA' AS str_match_wcnt_bad_rate_adj    ,
      'NA' AS str_match_wamt_bad_rate_adj    ,
      0 AS actn_match_txn_cnt                ,
      0 AS actn_match_txn_wcnt               ,
      0 AS actn_match_txn_amt                ,
      0 AS actn_match_txn_wamt               ,
      0 AS actn_match_bad_wcnt               ,
      0 AS actn_match_bad_wcnt_adj           ,
      0 AS actn_match_bad_wamt               ,
      0 AS actn_match_bad_wamt_adj           ,
      'NA' AS actn_match_wcnt_bad_rate       ,
      'NA' AS actn_match_wamt_bad_rate       ,
      'NA' AS actn_match_wcnt_bad_rate_adj   ,
      'NA' AS actn_match_wamt_bad_rate_adj   ,
      0 AS delta_bad_cnt                     ,
      0 AS delta_txn_cnt                     ,
      0 AS delta_bad_amt                     ,
      0 AS delta_txn_amt                     ,
      'NA' AS delta_cnt_bad_rate             ,
      'NA' AS delta_amt_bad_rate             ,
      --  0 as decline_cnt_total             ,
      --  0 as decline_amt_total             ,
      --  0 as decline_bad_wcnt              ,
      --  0 as decline_bad_wamt              ,
      --  'NA' AS decline_cnt_bad_rate       ,
      --  'NA' AS decline_amt_bad_rate       ,
      0 AS decled_cnt                        ,
      0 AS decled_amt                        ,
      0 AS decled_bad_cnt                    ,
      0 AS decled_bad_amt                    ,
      0 AS incre_str_decled_cnt              ,
      0 AS incre_str_decled_amt              ,
      0 AS incre_str_decled_bad_cnt          ,
      0 AS incre_str_decled_bad_amt          ,
      0 AS incre_actn_decled_cnt             ,
      0 AS incre_actn_decled_amt             ,
      0 AS incre_actn_decled_bad_cnt         ,
      0 AS incre_actn_decled_bad_amt         ,
      'NA' AS decline_cnt_bad_rate           ,
      'NA' AS decline_amt_bad_rate           ,
      'NA' AS incre_str_decline_cnt_bad_rate ,
      'NA' AS incre_str_decline_amt_bad_rate ,
      'NA' AS incre_actn_decline_cnt_bad_rate,
      'NA' AS incre_actn_decline_amt_bad_rate,
      0 AS sum_wh_actn                       ,
      'NA' AS whed_pct                       ,
      0 AS whed_cnt                          ,
      0 AS whed_amt                          ,
      0 AS bad_amt_adj                       ,
      'NA' AS wh_badrate                     ,
      0 AS incre_str_fire_vol                ,
      0 AS incre_rule_fire_vol               ,
      0 AS incre_actn_fire_vol               ,
      'NA' AS ov_pct                         ,
      0 AS catch_bad                         ,
      0 AS incre_str_fire_amt                ,
      0 AS incre_str_non_whed_fire_amt       ,
      'NA' AS ov_rule_1                      ,
      'NA' AS ov_pct_1                       ,
      'NA' AS ov_ntid_1                      ,
      'NA' AS ov_rule_2                      ,
      'NA' AS ov_pct_2                       ,
      'NA' AS ov_ntid_2                      ,
      'NA' AS ov_rule_3                      ,
      'NA' AS ov_pct_3                       ,
      'NA' AS ov_ntid_3                      ,
      'NA' AS ov_rule_4                      ,
      'NA' AS ov_pct_4                       ,
      'NA' AS ov_ntid_4                      ,
      0 AS decline_cnt                       ,
      0 AS cntct_cnt                         ,
      0 AS cntct_rate                        ,
      0 AS high_cntct_rate                   ,
      CASE
        WHEN UPPER(RTRIM(sss.rule_mo)) = 'BUSINESSPOLICY' THEN 1
        ELSE 0
      END AS business_policy,
      CASE
        WHEN e_1.rule_id IS NOT NULL THEN 1
        ELSE 0
      END AS exclusion_flg,
      CASE
        WHEN e2_1.rule_id IS NOT NULL THEN 1
        ELSE 0
      END AS incre_exempt       ,
      0 AS excl_high_overlap    ,
      0 AS excl_over_whitelisted,
      CASE
        WHEN st_1.rule_id IS NOT NULL THEN st_1.is_strategy
        ELSE 0
      END AS is_strategy    ,
      0 AS is_strategy_ov1  ,
      --  0 as poor_accuracy,
      CASE
        WHEN CASE
          WHEN UPPER(RTRIM(sss.rule_mo)) = 'BUSINESSPOLICY' THEN 1
          ELSE 0
        END = 0
        AND CASE
          WHEN e_1.rule_id IS NOT NULL THEN 1
          ELSE 0
        END = 0 THEN 1
        ELSE 0
      END AS low_firing                         ,
      0 AS high_overlap                         ,
      0 AS over_whitelisted                     ,
      0 AS sub_poor_accuracy_bad_rate           ,
      0 AS sub_poor_accuracy_incre_str_bad_rate ,
      0 AS sub_poor_accuracy_incre_actn_bad_rate,
      CASE
        WHEN 0 + 0 + 0 > 0 THEN 1
        ELSE 0
      END AS poor_accuracy,
      CASE
        WHEN CASE
          WHEN UPPER(RTRIM(sss.rule_mo)) = 'BUSINESSPOLICY' THEN 1
          ELSE 0
        END = 0
        AND CASE
          WHEN e_1.rule_id IS NOT NULL THEN 1
          ELSE 0
        END = 0 THEN 1
        ELSE 0
      END AS sub_low_firing_all_firing   ,
      0 AS sub_low_firing_incre_firing   ,
      --  add automated alerting criterias
      CASE
        WHEN (
          sss.crs_team LIKE 'gfr_goal_ato%'
          OR sss.crs_team LIKE 'gfr_goal_ach%'
          OR sss.crs_team LIKE 'gfr_goal_cc%'
          OR sss.crs_team LIKE 'gfr_goal_col%'
          OR sss.crs_team LIKE 'gfr_goal_auto%'
          OR LOWER(sss.crs_team) LIKE ANY (
            'goal_ato'  ,
            'goal_ach'  ,
            'goal_addfi',
            'goal_cc'   ,
            'goal_col'  ,
            'goal_auto'
          )
        )
        AND (
          sss.tags LIKE '%AUTOSOLUTION%'
          AND sss.tags LIKE '%HYPERNET%'
        )
        AND (
          sss.tags NOT LIKE '%MERCHANT%'
          AND sss.tags NOT LIKE '%CLUSTER%'
        )
        AND 0 < sss.observing_window THEN 1
        WHEN sss.tags LIKE '%AUTOSOLUTION%'
        AND sss.tags LIKE '%HYPERNET MERCHANT%'
        AND 0 < 1 * sss.observing_window THEN 1
        ELSE 0
      END AS automated_low_firing_cnt                     ,
      0 AS sub_automated_poor_accuracy_bad_rate           ,
      0 AS sub_automated_poor_accuracy_incre_str_bad_rate ,
      0 AS sub_automated_poor_accuracy_incre_actn_bad_rate,
      CASE
        WHEN 0 + 0 + 0 > 0 THEN 1
        ELSE 0
      END AS automated_poor_accuracy ,
      0 AS automated_low_firing_amt  ,
      0 AS automated_over_whitelisted,
      CASE
        WHEN 0 = 1
        OR CASE
          WHEN (
            sss.crs_team LIKE 'gfr_goal_ato%'
            OR sss.crs_team LIKE 'gfr_goal_ach%'
            OR sss.crs_team LIKE 'gfr_goal_cc%'
            OR sss.crs_team LIKE 'gfr_goal_col%'
            OR sss.crs_team LIKE 'gfr_goal_auto%'
            OR LOWER(sss.crs_team) LIKE ANY (
              'goal_ato'  ,
              'goal_ach'  ,
              'goal_addfi',
              'goal_cc'   ,
              'goal_col'  ,
              'goal_auto'
            )
          )
          AND (
            sss.tags LIKE '%AUTOSOLUTION%'
            AND sss.tags LIKE '%HYPERNET%'
          )
          AND (
            sss.tags NOT LIKE '%MERCHANT%'
            AND sss.tags NOT LIKE '%CLUSTER%'
          )
          AND 0 < sss.observing_window THEN 1
          WHEN sss.tags LIKE '%AUTOSOLUTION%'
          AND sss.tags LIKE '%HYPERNET MERCHANT%'
          AND 0 < 1 * sss.observing_window THEN 1
          ELSE 0
        END = 1 THEN 1
        ELSE 0
      END AS automated_low_firing,
      0 AS automated_high_overlap,
      CASE
        WHEN (
          CASE
            WHEN 0 = 1
            OR CASE
              WHEN (
                sss.crs_team LIKE 'gfr_goal_ato%'
                OR sss.crs_team LIKE 'gfr_goal_ach%'
                OR sss.crs_team LIKE 'gfr_goal_cc%'
                OR sss.crs_team LIKE 'gfr_goal_col%'
                OR sss.crs_team LIKE 'gfr_goal_auto%'
                OR LOWER(sss.crs_team) LIKE ANY (
                  'goal_ato'  ,
                  'goal_ach'  ,
                  'goal_addfi',
                  'goal_cc'   ,
                  'goal_col'  ,
                  'goal_auto'
                )
              )
              AND (
                sss.tags LIKE '%AUTOSOLUTION%'
                AND sss.tags LIKE '%HYPERNET%'
              )
              AND (
                sss.tags NOT LIKE '%MERCHANT%'
                AND sss.tags NOT LIKE '%CLUSTER%'
              )
              AND 0 < sss.observing_window THEN 1
              WHEN sss.tags LIKE '%AUTOSOLUTION%'
              AND sss.tags LIKE '%HYPERNET MERCHANT%'
              AND 0 < 1 * sss.observing_window THEN 1
              ELSE 0
            END = 1 THEN 1
            ELSE 0
          END = 1
          OR CASE
            WHEN 0 + 0 + 0 > 0 THEN 1
            ELSE 0
          END = 1
          OR 0 = 1
          OR 0 = 1
        )
        AND (
          sss.tags LIKE '%AUTOSOLUTION%'
          AND sss.tags LIKE '%HYPERNET MERCHANT%'
        ) THEN 5
        WHEN CASE
          WHEN 0 = 1
          OR CASE
            WHEN (
              sss.crs_team LIKE 'gfr_goal_ato%'
              OR sss.crs_team LIKE 'gfr_goal_ach%'
              OR sss.crs_team LIKE 'gfr_goal_cc%'
              OR sss.crs_team LIKE 'gfr_goal_col%'
              OR sss.crs_team LIKE 'gfr_goal_auto%'
              OR LOWER(sss.crs_team) LIKE ANY (
                'goal_ato'  ,
                'goal_ach'  ,
                'goal_addfi',
                'goal_cc'   ,
                'goal_col'  ,
                'goal_auto'
              )
            )
            AND (
              sss.tags LIKE '%AUTOSOLUTION%'
              AND sss.tags LIKE '%HYPERNET%'
            )
            AND (
              sss.tags NOT LIKE '%MERCHANT%'
              AND sss.tags NOT LIKE '%CLUSTER%'
            )
            AND 0 < sss.observing_window THEN 1
            WHEN sss.tags LIKE '%AUTOSOLUTION%'
            AND sss.tags LIKE '%HYPERNET MERCHANT%'
            AND 0 < 1 * sss.observing_window THEN 1
            ELSE 0
          END = 1 THEN 1
          ELSE 0
        END = 1
        OR CASE
          WHEN 0 + 0 + 0 > 0 THEN 1
          ELSE 0
        END = 1
        OR 0 = 1
        OR 0 = 1 THEN 5
        WHEN CASE
          WHEN UPPER(RTRIM(sss.rule_mo)) = 'BUSINESSPOLICY' THEN 1
          ELSE 0
        END = 0
        AND CASE
          WHEN e_1.rule_id IS NOT NULL THEN 1
          ELSE 0
        END = 0 THEN 3
      END AS tier
    FROM
      (
        SELECT
          rule_list_stage_2_1.rule_id            ,
          rule_list_stage_2_1.priority           ,
          rule_list_stage_2_1.rule_name          ,
          rule_list_stage_2_1.control_group      ,
          rule_list_stage_2_1.crs_team           ,
          rule_list_stage_2_1.owner_ntid         ,
          rule_list_stage_2_1.rule_mo            ,
          rule_list_stage_2_1.tags               ,
          rule_list_stage_2_1.last_release_length,
          rule_list_stage_2_1.observing_window
        FROM
          `pypl-edw.${dataset_name_tmp}.check_action_rule_id` AS rule_list_stage_2_1
        WHERE
          rule_list_stage_2_1.rule_id NOT IN (
            SELECT
              decline_metrics_0.rule_id
            FROM
              `pypl-edw.${dataset_name_tmp}.decline_metrics` AS decline_metrics_0
            GROUP BY
              1
          )
      ) AS sss
      LEFT OUTER JOIN (
        SELECT
          live_metadata_table_5.chkpnt_delimited_str,
          live_metadata_table_5.action_raw_str      ,
          live_metadata_table_5.tags                ,
          live_metadata_table_5.rule_id
        FROM
          `pypl-edw.${dataset_name_tmp}.urm_lite` AS live_metadata_table_5
        WHERE
          UPPER(live_metadata_table_5.tags) LIKE '%%RISK%%APPETITE%%'
          OR UPPER(live_metadata_table_5.tags) LIKE '%%SAFETY%%NET%%'
          OR UPPER(live_metadata_table_5.tags) LIKE '%%BLACK%%LIST%%'
          OR UPPER(live_metadata_table_5.tags) LIKE '%%BLOCK%%LIST%%'
          OR UPPER(live_metadata_table_5.tags) LIKE '%%LATAM%%IPR%%'
          OR UPPER(live_metadata_table_5.tags) LIKE '%%BUSINESS%%REQUEST%%'
          OR UPPER(live_metadata_table_5.tags) LIKE '%TESTING%'
          OR UPPER(live_metadata_table_5.tags) LIKE '%%MARKET%%RAMPING%%'
          OR UPPER(live_metadata_table_5.tags) LIKE '%VEDA%'
          OR UPPER(live_metadata_table_5.tags) LIKE '%VSDS%'
          OR UPPER(live_metadata_table_5.tags) LIKE '%SPEEDSTER%'
          OR UPPER(live_metadata_table_5.tags) LIKE '%HIGHCONTACTCOST%'
          OR UPPER(live_metadata_table_5.tags) LIKE '%NNAENABLEMENT%'
          OR UPPER(live_metadata_table_5.tags) LIKE '%RED%CARPET%'
          OR UPPER(live_metadata_table_5.tags) LIKE '%STRATEGYFRAMEWORK%'
      ) AS e_1 ON sss.rule_id = e_1.rule_id
      LEFT OUTER JOIN (
        SELECT
          live_metadata_table_6.chkpnt_delimited_str,
          live_metadata_table_6.action_raw_str      ,
          live_metadata_table_6.tags                ,
          live_metadata_table_6.rule_id
        FROM
          `pypl-edw.${dataset_name_tmp}.urm_lite` AS live_metadata_table_6
        WHERE
          UPPER(live_metadata_table_6.tags) LIKE '%%SCRIPT%%ATTACK%%'
          OR UPPER(live_metadata_table_6.tags) LIKE '%%SUPER%%DECLINE%%'
      ) AS e2_1 ON sss.rule_id = e2_1.rule_id
      LEFT OUTER JOIN (
        SELECT
          rule_mo_table_3.rule_id   ,
          rule_mo_table_3.is_strategy
        FROM
          `pypl-edw`.pp_risk_crs_core.rule_mo_uni AS rule_mo_table_3
        QUALIFY
          ROW_NUMBER() OVER (
            PARTITION BY
              rule_mo_table_3.rule_id
            ORDER BY
              rule_mo_table_3.baseline DESC
          ) = 1
      ) AS st_1 ON sss.rule_id = st_1.rule_id
  ) AS cw_ss;


--remove alert indicators for rules that are excluded from all alerting criteria
--  below code for auditing use
CALL
  `pypl-edw`.pp_monitor.drop_table (
    '${dataset_name_tmp}.funding_decline_alert_v0305_audit'
  );


CREATE TABLE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305_audit`
CLUSTER BY
  action_type,
  rule_id AS
SELECT
  output_table.crs_team                                       ,
  output_table.owner_ntid                                     ,
  output_table.rule_id                                        ,
  output_table.action_type                                    ,
  output_table.monitoring_window                              ,
  output_table.observing_window                               ,
  output_table.rule_mo                                        ,
  output_table.rule_name                                      ,
  output_table.fire_vol                                       ,
  output_table.u2u_fire_vol                                   ,
  output_table.fire_amt                                       ,
  output_table.cg_txn_cnt                                     ,
  output_table.cg_txn_wcnt                                    ,
  output_table.cg_txn_amt                                     ,
  output_table.cg_txn_wamt                                    ,
  output_table.cg_brm_bad_wcnt                                ,
  output_table.cg_brm_bad_wcnt_adj                            ,
  output_table.cg_brm_bad_wamt                                ,
  output_table.cg_brm_bad_wamt_adj                            ,
  output_table.cg_wcnt_bad_rate                               ,
  output_table.cg_wamt_bad_rate                               ,
  output_table.cg_wcnt_bad_rate_adj                           ,
  output_table.cg_wamt_bad_rate_adj                           ,
  output_table.str_cg_txn_cnt                                 ,
  output_table.str_cg_txn_wcnt                                ,
  output_table.str_cg_txn_amt                                 ,
  output_table.str_cg_txn_wamt                                ,
  output_table.str_cg_brm_bad_wcnt                            ,
  output_table.str_cg_brm_bad_wcnt_adj                        ,
  output_table.str_cg_brm_bad_wamt                            ,
  output_table.str_cg_brm_bad_wamt_adj                        ,
  output_table.str_cg_wcnt_bad_rate                           ,
  output_table.str_cg_wamt_bad_rate                           ,
  output_table.str_cg_wcnt_bad_rate_adj                       ,
  output_table.str_cg_wamt_bad_rate_adj                       ,
  output_table.actn_cg_txn_cnt                                ,
  output_table.actn_cg_txn_wcnt                               ,
  output_table.actn_cg_txn_amt                                ,
  output_table.actn_cg_txn_wamt                               ,
  output_table.actn_cg_brm_bad_wcnt                           ,
  output_table.actn_cg_brm_bad_wcnt_adj                       ,
  output_table.actn_cg_brm_bad_wamt                           ,
  output_table.actn_cg_brm_bad_wamt_adj                       ,
  output_table.actn_cg_wcnt_bad_rate                          ,
  output_table.actn_cg_wamt_bad_rate                          ,
  output_table.actn_cg_wcnt_bad_rate_adj                      ,
  output_table.actn_cg_wamt_bad_rate_adj                      ,
  output_table.match_txn_cnt                                  ,
  output_table.match_txn_wcnt                                 ,
  output_table.match_txn_amt                                  ,
  output_table.match_txn_wamt                                 ,
  output_table.match_bad_wcnt                                 ,
  output_table.match_bad_wcnt_adj                             ,
  output_table.match_bad_wamt                                 ,
  output_table.match_bad_wamt_adj                             ,
  output_table.match_wcnt_bad_rate                            ,
  output_table.match_wamt_bad_rate                            ,
  output_table.match_wcnt_bad_rate_adj                        ,
  output_table.match_wamt_bad_rate_adj                        ,
  output_table.match_wamt_fpr                                 ,
  output_table.match_wamt_fpr_adj                             ,
  output_table.str_match_txn_cnt                              ,
  output_table.str_match_txn_wcnt                             ,
  output_table.str_match_txn_amt                              ,
  output_table.str_match_txn_wamt                             ,
  output_table.str_match_bad_wcnt                             ,
  output_table.str_match_bad_wcnt_adj                         ,
  output_table.str_match_bad_wamt                             ,
  output_table.str_match_bad_wamt_adj                         ,
  output_table.str_match_wcnt_bad_rate                        ,
  output_table.str_match_wamt_bad_rate                        ,
  output_table.str_match_wcnt_bad_rate_adj                    ,
  output_table.str_match_wamt_bad_rate_adj                    ,
  output_table.actn_match_txn_cnt                             ,
  output_table.actn_match_txn_wcnt                            ,
  output_table.actn_match_txn_amt                             ,
  output_table.actn_match_txn_wamt                            ,
  output_table.actn_match_bad_wcnt                            ,
  output_table.actn_match_bad_wcnt_adj                        ,
  output_table.actn_match_bad_wamt                            ,
  output_table.actn_match_bad_wamt_adj                        ,
  output_table.actn_match_wcnt_bad_rate                       ,
  output_table.actn_match_wamt_bad_rate                       ,
  output_table.actn_match_wcnt_bad_rate_adj                   ,
  output_table.actn_match_wamt_bad_rate_adj                   ,
  output_table.delta_bad_cnt                                  ,
  output_table.delta_txn_cnt                                  ,
  output_table.delta_bad_amt                                  ,
  output_table.delta_txn_amt                                  ,
  output_table.delta_cnt_bad_rate                             ,
  output_table.delta_amt_bad_rate                             ,
  output_table.decled_cnt                                     ,
  output_table.decled_amt                                     ,
  output_table.decled_bad_cnt                                 ,
  output_table.decled_bad_amt                                 ,
  output_table.incre_str_decled_cnt                           ,
  output_table.incre_str_decled_amt                           ,
  output_table.incre_str_decled_bad_cnt                       ,
  output_table.incre_str_decled_bad_amt                       ,
  output_table.incre_actn_decled_cnt                          ,
  output_table.incre_actn_decled_amt                          ,
  output_table.incre_actn_decled_bad_cnt                      ,
  output_table.incre_actn_decled_bad_amt                      ,
  output_table.decline_cnt_bad_rate                           ,
  output_table.decline_amt_bad_rate                           ,
  output_table.incre_str_decline_cnt_bad_rate                 ,
  output_table.incre_str_decline_amt_bad_rate                 ,
  output_table.incre_actn_decline_cnt_bad_rate                ,
  output_table.incre_actn_decline_amt_bad_rate                ,
  output_table.sum_wh_actn                                    ,
  output_table.whed_pct                                       ,
  output_table.whed_cnt                                       ,
  output_table.whed_amt                                       ,
  output_table.bad_amt_adj                                    ,
  output_table.wh_badrate                                     ,
  output_table.incre_str_fire_vol                             ,
  output_table.incre_rule_fire_vol                            ,
  output_table.incre_actn_fire_vol                            ,
  output_table.ov_pct                                         ,
  output_table.catch_bad                                      ,
  output_table.incre_str_fire_amt                             ,
  output_table.incre_str_non_whed_fire_amt                    ,
  output_table.ov_rule_1                                      ,
  output_table.ov_pct_1                                       ,
  output_table.ov_ntid_1                                      ,
  output_table.ov_rule_2                                      ,
  output_table.ov_pct_2                                       ,
  output_table.ov_ntid_2                                      ,
  output_table.ov_rule_3                                      ,
  output_table.ov_pct_3                                       ,
  output_table.ov_ntid_3                                      ,
  output_table.ov_rule_4                                      ,
  output_table.ov_pct_4                                       ,
  output_table.ov_ntid_4                                      ,
  output_table.decline_cnt                                    ,
  output_table.cntct_cnt                                      ,
  output_table.cntct_rate                                     ,
  output_table.high_cntct_rate                                ,
  output_table.business_policy                                ,
  output_table.exclusion_flg                                  ,
  output_table.incre_exempt                                   ,
  output_table.excl_high_overlap                              ,
  output_table.excl_over_whitelisted                          ,
  output_table.is_strategy                                    ,
  output_table.is_strategy_ov1                                ,
  output_table.low_firing                                     ,
  output_table.high_overlap                                   ,
  output_table.over_whitelisted                               ,
  output_table.sub_poor_accuracy_bad_rate                     ,
  output_table.sub_poor_accuracy_incre_str_bad_rate           ,
  output_table.sub_poor_accuracy_incre_actn_bad_rate          ,
  output_table.poor_accuracy                                  ,
  output_table.sub_low_firing_all_firing                      ,
  output_table.sub_low_firing_incre_firing                    ,
  output_table.automated_low_firing_cnt                       ,
  output_table.sub_automated_poor_accuracy_bad_rate           ,
  output_table.sub_automated_poor_accuracy_incre_str_bad_rate ,
  output_table.sub_automated_poor_accuracy_incre_actn_bad_rate,
  output_table.automated_poor_accuracy                        ,
  output_table.automated_low_firing_amt                       ,
  output_table.automated_over_whitelisted                     ,
  output_table.automated_low_firing                           ,
  output_table.automated_high_overlap                         ,
  output_table.tier
FROM
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS output_table;


--  below code used to check credit rule performance
UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS a
SET
  poor_accuracy = 0                                  ,
  sub_poor_accuracy_bad_rate = 0                     ,
  sub_poor_accuracy_incre_str_bad_rate = 0           ,
  sub_poor_accuracy_incre_actn_bad_rate = 0          ,
  over_whitelisted = 0                               ,
  automated_poor_accuracy = 0                        ,
  sub_automated_poor_accuracy_bad_rate = 0           ,
  sub_automated_poor_accuracy_incre_str_bad_rate = 0 ,
  sub_automated_poor_accuracy_incre_actn_bad_rate = 0,
  automated_over_whitelisted = 0                     ,
  automated_low_firing = 0                           ,
  automated_low_firing_amt = 0                       ,
  automated_low_firing_cnt = 0                       ,
  automated_high_overlap = 0
FROM
  `pypl-edw.${dataset_name_tmp}.check_action_rule_id` AS b
WHERE
  a.rule_id = b.rule_id
  AND a.crs_team IN (
    --  , low_firing=0
    --  , sub_low_firing_all_firing=0
    --  , sub_low_firing_incre_firing=0
    --  , high_overlap=0
    'gfr_credit',
    'goal_credit'
  );


--  below code to do the update
-- 2023-04-26 update contact rate criteria
UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS a
SET
  high_cntct_rate = CASE
    WHEN a.rule_mo IN ('ATO') THEN CASE
      WHEN a.cntct_cnt >= 30
      AND a.cntct_rate >= NUMERIC '0.02'
      AND a.match_txn_cnt >= 30
      AND SAFE_CAST(
        a.match_wamt_bad_rate_adj AS BIGNUMERIC
      ) < NUMERIC '0.2'
      AND a.crs_team NOT IN (
        'gfr_omni_gms'    ,
        'gfr_omni_nextgen',
        'gfr_omni'        ,
        'goal_gms'        ,
        'goal_nextgen'
      )
      OR a.cntct_cnt >= 30
      AND a.cntct_rate >= NUMERIC '0.02'
      AND (
        a.cg_txn_cnt >= 30
        AND SAFE_CAST(
          a.cg_wamt_bad_rate AS BIGNUMERIC
        ) < NUMERIC '0.2'
        OR a.cg_txn_cnt < 30
        AND a.decled_cnt >= 30
        AND SAFE_CAST(
          a.decline_amt_bad_rate AS BIGNUMERIC
        ) < NUMERIC '0.2'
      )
      OR a.cntct_cnt >= 30
      AND a.cntct_rate >= NUMERIC '0.02'
      AND a.match_txn_cnt < 30
      OR a.cntct_rate >= NUMERIC '0.03'
      AND a.cntct_cnt >= 150 THEN 1
      ELSE 0
    END
    WHEN a.rule_mo IN (
      --  not omni
      --  omni
      -- CG <30
      -- criteria c
      'Collusion'
    ) THEN CASE
      WHEN a.cntct_cnt >= 30
      AND a.cntct_rate >= NUMERIC '0.02'
      AND a.match_txn_cnt >= 30
      AND SAFE_CAST(
        a.match_wamt_bad_rate_adj AS BIGNUMERIC
      ) < NUMERIC '0.2'
      AND a.crs_team NOT IN (
        'gfr_omni_gms'    ,
        'gfr_omni_nextgen',
        'gfr_omni'        ,
        'goal_gms'        ,
        'goal_nextgen'
      )
      OR a.cntct_cnt >= 30
      AND a.cntct_rate >= NUMERIC '0.02'
      AND (
        a.match_txn_cnt >= 30
        AND SAFE_CAST(
          a.match_wamt_bad_rate_adj AS BIGNUMERIC
        ) < NUMERIC '0.2'
      )
      OR a.cntct_cnt >= 30
      AND a.cntct_rate >= NUMERIC '0.02'
      AND a.match_txn_cnt < 30
      OR a.cntct_rate >= NUMERIC '0.03'
      AND a.cntct_cnt >= 150 THEN 1
      ELSE 0
    END
    WHEN a.rule_mo IN (
      --  not omni
      --  omni
      -- CG <30
      -- criteria c
      'CC'
    ) THEN CASE
      WHEN a.cntct_cnt >= 30
      AND a.cntct_rate >= NUMERIC '0.015'
      AND a.match_txn_cnt >= 30
      AND SAFE_CAST(
        a.match_wamt_bad_rate_adj AS BIGNUMERIC
      ) < NUMERIC '0.2'
      AND a.crs_team NOT IN (
        'gfr_omni_gms'    ,
        'gfr_omni_nextgen',
        'gfr_omni'        ,
        'goal_gms'        ,
        'goal_nextgen'
      )
      OR a.cntct_cnt >= 30
      AND a.cntct_rate >= NUMERIC '0.015'
      AND (
        a.match_txn_cnt >= 30
        AND SAFE_CAST(
          a.match_wamt_bad_rate_adj AS BIGNUMERIC
        ) < NUMERIC '0.2'
        OR a.match_txn_cnt < 30
        AND a.decled_cnt >= 30
        AND SAFE_CAST(
          a.decline_amt_bad_rate AS BIGNUMERIC
        ) < NUMERIC '0.2'
      )
      OR a.cntct_cnt >= 30
      AND a.cntct_rate >= NUMERIC '0.015'
      AND a.match_txn_cnt < 30
      OR a.cntct_rate >= NUMERIC '0.025'
      AND a.cntct_cnt >= 150 THEN 1
      ELSE 0
    END
    WHEN a.rule_mo IN (
      --  not omni
      --  omni
      -- CG <30
      -- criteria c
      'ACH'
    ) THEN CASE
      WHEN a.cntct_cnt >= 30
      AND a.cntct_rate >= NUMERIC '0.015'
      AND a.match_txn_cnt >= 30
      AND SAFE_CAST(
        a.match_wamt_bad_rate_adj AS BIGNUMERIC
      ) < NUMERIC '0.2'
      AND a.crs_team NOT IN (
        'gfr_omni_gms'    ,
        'gfr_omni_nextgen',
        'gfr_omni'        ,
        'goal_gms'        ,
        'goal_nextgen'
      )
      OR a.cntct_cnt >= 30
      AND a.cntct_rate >= NUMERIC '0.015'
      AND (
        a.match_txn_cnt >= 30
        AND SAFE_CAST(
          a.match_wamt_bad_rate_adj AS BIGNUMERIC
        ) < NUMERIC '0.2'
      )
      OR a.cntct_cnt >= 30
      AND a.cntct_rate >= NUMERIC '0.015'
      AND a.match_txn_cnt < 30
      OR a.cntct_rate >= NUMERIC '0.025'
      AND a.cntct_cnt >= 150 THEN 1
      ELSE 0
    END
    WHEN a.rule_mo IN (
      --  not omni
      --  omni
      -- CG <30
      -- criteria c
      'UBSM'
    ) THEN CASE
      WHEN a.cntct_cnt >= 30
      AND a.cntct_rate >= NUMERIC '0.015'
      AND a.match_txn_cnt >= 30
      AND SAFE_CAST(
        a.match_wamt_bad_rate_adj AS BIGNUMERIC
      ) < NUMERIC '0.2'
      AND a.crs_team NOT IN (
        'gfr_omni_gms'    ,
        'gfr_omni_nextgen',
        'gfr_omni'        ,
        'goal_gms'        ,
        'goal_nextgen'
      )
      OR a.cntct_cnt >= 30
      AND a.cntct_rate >= NUMERIC '0.015'
      AND (
        a.match_txn_cnt >= 30
        AND SAFE_CAST(
          a.match_wamt_bad_rate_adj AS BIGNUMERIC
        ) < NUMERIC '0.2'
      )
      OR a.cntct_cnt >= 30
      AND a.cntct_rate >= NUMERIC '0.015'
      AND a.match_txn_cnt < 30
      OR a.cntct_rate >= NUMERIC '0.025'
      AND a.cntct_cnt >= 150 THEN 1
      ELSE 0
    END
    ELSE 0
  END
WHERE
  TRUE;


--  not omni
--  omni
-- CG <30
-- criteria c
-- REMOVE ALERT FOR NEWLY CREATED/UPDATED RULES 
UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS a
SET
  tier = NULL                                        ,
  poor_accuracy = 0                                  ,
  sub_poor_accuracy_bad_rate = 0                     ,
  sub_poor_accuracy_incre_str_bad_rate = 0           ,
  sub_poor_accuracy_incre_actn_bad_rate = 0          ,
  low_firing = 0                                     ,
  sub_low_firing_all_firing = 0                      ,
  sub_low_firing_incre_firing = 0                    ,
  high_overlap = 0                                   ,
  over_whitelisted = 0                               ,
  automated_poor_accuracy = 0                        ,
  sub_automated_poor_accuracy_bad_rate = 0           ,
  sub_automated_poor_accuracy_incre_str_bad_rate = 0 ,
  sub_automated_poor_accuracy_incre_actn_bad_rate = 0,
  automated_over_whitelisted = 0                     ,
  automated_low_firing = 0                           ,
  automated_low_firing_amt = 0                       ,
  automated_low_firing_cnt = 0                       ,
  automated_high_overlap = 0                         ,
  high_cntct_rate = 0
FROM
  `pypl-edw.${dataset_name_tmp}.check_action_rule_id` AS b
WHERE
  a.rule_id = b.rule_id
  AND (
    (
      (
        b.last_release_length <= 49
        OR a.observing_window <= 23
      )
      AND NOT (
        a.observing_window BETWEEN 7 AND 23
      )
    ) -- OR upper(b.tags) LIKE '%%BILLING%%AGREEMENT%%'
    OR a.crs_team IN (
      --  or a.crs_team in ('gfr_credit', 'gfr_e2e')
      'gfr_e2e'
    )
    OR a.rule_id IN (
      SELECT
        restrict_pld_pair.rule_id
      FROM
        `pypl-edw.${dataset_name_tmp}.restrict_pld_pair` AS restrict_pld_pair
    )
  );


-- Update on 2024.02.20
-- Only exclude poor accuracy alerts for BA rules
UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS a
SET
  poor_accuracy = 0                                 ,
  sub_poor_accuracy_bad_rate = 0                    ,
  sub_poor_accuracy_incre_str_bad_rate = 0          ,
  sub_poor_accuracy_incre_actn_bad_rate = 0         ,
  automated_poor_accuracy = 0                       ,
  sub_automated_poor_accuracy_bad_rate = 0          ,
  sub_automated_poor_accuracy_incre_str_bad_rate = 0,
  sub_automated_poor_accuracy_incre_actn_bad_rate = 0
FROM
  `pypl-edw.${dataset_name_tmp}.check_action_rule_id` AS b
WHERE
  a.rule_id = b.rule_id
  AND UPPER(b.tags) LIKE '%%BILLING%%AGREEMENT%%';


UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS a
SET
  poor_accuracy = 0                                  ,
  sub_poor_accuracy_bad_rate = 0                     ,
  sub_poor_accuracy_incre_str_bad_rate = 0           ,
  sub_poor_accuracy_incre_actn_bad_rate = 0          ,
  low_firing = 0                                     ,
  sub_low_firing_all_firing = 0                      ,
  sub_low_firing_incre_firing = 0                    ,
  high_overlap = 0                                   ,
  over_whitelisted = 0                               ,
  automated_poor_accuracy = 0                        ,
  sub_automated_poor_accuracy_bad_rate = 0           ,
  sub_automated_poor_accuracy_incre_str_bad_rate = 0 ,
  sub_automated_poor_accuracy_incre_actn_bad_rate = 0,
  automated_over_whitelisted = 0                     ,
  automated_low_firing = 0                           ,
  automated_low_firing_amt = 0                       ,
  automated_low_firing_cnt = 0                       ,
  automated_high_overlap = 0                         ,
  high_cntct_rate = 0
WHERE
  UPPER(RTRIM(a.rule_mo)) = 'BUSINESSPOLICY';


--20240110: derive early stage indicator
ALTER TABLE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305`
ADD COLUMN
  is_early_stage INT64;


UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305`
SET
  is_early_stage = 0
WHERE
  1 = 1;


UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS a
SET
  is_early_stage = 1
FROM
  `pypl-edw.${dataset_name_tmp}.check_action_rule_id` AS b
WHERE
  a.rule_id = b.rule_id
  AND (a.observing_window <= 23);


UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305`
SET
  low_firing = 0                                    ,
  sub_low_firing_all_firing = 0                     ,
  sub_low_firing_incre_firing = 0                   ,
  high_overlap = 0                                  ,
  automated_low_firing = 0                          ,
  automated_low_firing_amt = 0                      ,
  automated_low_firing_cnt = 0                      ,
  automated_high_overlap = 0                        ,
  sub_poor_accuracy_incre_actn_bad_rate = 0         ,
  sub_automated_poor_accuracy_incre_actn_bad_rate = 0
WHERE
  is_early_stage = 1;


UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305`
SET
  poor_accuracy = CASE
    WHEN SUB_POOR_ACCURACY_BAD_RATE + SUB_POOR_ACCURACY_INCRE_STR_BAD_RATE + SUB_POOR_ACCURACY_INCRE_ACTN_BAD_RATE = 0 THEN 0
    ELSE 1
  END                          ,
  automated_poor_accuracy = CASE
    WHEN sub_automated_poor_accuracy_bad_rate + sub_automated_poor_accuracy_incre_str_bad_rate + sub_automated_poor_accuracy_incre_actn_bad_rate = 0 THEN 0
    ELSE 1
  END
WHERE
  is_early_stage = 1;


-- update rules regarding notaggingtrend on 2023/10/25
UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS a
SET
  poor_accuracy = 0                                 ,
  sub_poor_accuracy_bad_rate = 0                    ,
  sub_poor_accuracy_incre_str_bad_rate = 0          ,
  sub_poor_accuracy_incre_actn_bad_rate = 0         ,
  automated_poor_accuracy = 0                       ,
  sub_automated_poor_accuracy_bad_rate = 0          ,
  sub_automated_poor_accuracy_incre_str_bad_rate = 0,
  sub_automated_poor_accuracy_incre_actn_bad_rate = 0
FROM
  `pypl-edw.${dataset_name_tmp}.urm_lite` AS b
WHERE
  a.rule_id = b.rule_id
  AND UPPER(b.tags) LIKE '%%NOTAGGINGTREND%%';


UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS output_table
SET
  sub_poor_accuracy_bad_rate = 0
WHERE
  output_table.sub_poor_accuracy_bad_rate = 1
  AND output_table.decled_cnt < 30
  AND (
    output_table.cg_txn_cnt < 30
    OR output_table.match_txn_cnt < 30
  )
  AND UPPER(output_table.crs_team) LIKE ANY (
    'GFR_OMNI%'   ,
    'GOAL_NEXTGEN',
    'GOAL_GMS'
  );


UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS output_table
SET
  sub_poor_accuracy_incre_str_bad_rate = 0
WHERE
  output_table.sub_poor_accuracy_incre_str_bad_rate = 1
  AND output_table.incre_str_decled_cnt < 30
  AND (
    output_table.str_cg_txn_cnt < 30
    OR output_table.str_match_txn_cnt < 30
  )
  AND UPPER(output_table.crs_team) LIKE ANY (
    'GFR_OMNI%'   ,
    'GOAL_NEXTGEN',
    'GOAL_GMS'
  );


UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS output_table
SET
  sub_poor_accuracy_incre_actn_bad_rate = 0
WHERE
  output_table.sub_poor_accuracy_incre_actn_bad_rate = 1
  AND output_table.incre_actn_decled_cnt < 30
  AND (
    output_table.actn_cg_txn_cnt < 30
    OR output_table.actn_match_txn_cnt < 30
  )
  AND UPPER(output_table.crs_team) LIKE ANY (
    'GFR_OMNI%'   ,
    'GOAL_NEXTGEN',
    'GOAL_GMS'
  );


UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS output_table
SET
  poor_accuracy = 0
WHERE
  output_table.sub_poor_accuracy_bad_rate = 0
  AND output_table.sub_poor_accuracy_incre_str_bad_rate = 0
  AND output_table.sub_poor_accuracy_incre_actn_bad_rate = 0
  AND output_table.poor_accuracy = 1;


UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS output_table
SET
  sub_automated_poor_accuracy_bad_rate = 0
WHERE
  output_table.sub_automated_poor_accuracy_bad_rate = 1
  AND output_table.decled_cnt < 30
  AND (
    output_table.cg_txn_cnt < 30
    OR output_table.match_txn_cnt < 30
  )
  AND UPPER(output_table.crs_team) LIKE ANY (
    'GFR_OMNI%'   ,
    'GOAL_NEXTGEN',
    'GOAL_GMS'
  );


UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS output_table
SET
  sub_automated_poor_accuracy_incre_str_bad_rate = 0
WHERE
  output_table.sub_automated_poor_accuracy_incre_str_bad_rate = 1
  AND output_table.incre_str_decled_cnt < 30
  AND (
    output_table.str_cg_txn_cnt < 30
    OR output_table.str_match_txn_cnt < 30
  )
  AND UPPER(output_table.crs_team) LIKE ANY (
    'GFR_OMNI%'   ,
    'GOAL_NEXTGEN',
    'GOAL_GMS'
  );


UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS output_table
SET
  sub_automated_poor_accuracy_incre_actn_bad_rate = 0
WHERE
  output_table.sub_automated_poor_accuracy_incre_actn_bad_rate = 1
  AND output_table.incre_actn_decled_cnt < 30
  AND (
    output_table.actn_cg_txn_cnt < 30
    OR output_table.actn_match_txn_cnt < 30
  )
  AND UPPER(output_table.crs_team) LIKE ANY (
    'GFR_OMNI%'   ,
    'GOAL_NEXTGEN',
    'GOAL_GMS'
  );


UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS output_table
SET
  automated_poor_accuracy = 0
WHERE
  output_table.sub_automated_poor_accuracy_bad_rate = 0
  AND output_table.sub_automated_poor_accuracy_incre_str_bad_rate = 0
  AND output_table.sub_automated_poor_accuracy_incre_actn_bad_rate = 0
  AND output_table.automated_poor_accuracy = 1;


UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS output_table
SET
  high_overlap = 0
WHERE
  output_table.action_type = 2998
  AND output_table.high_overlap = 1;


--  if two rules overlap with each other and both alerted for high overlap, alert the one with higher overlap ratio
UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS a
SET
  high_overlap = 0
FROM
  (
    SELECT
      output_table.rule_id    ,
      output_table.action_type,
      output_table.ov_rule_1  ,
      output_table.ov_pct_1   ,
      output_table.high_overlap
    FROM
      `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS output_table
  ) AS b
WHERE
  a.rule_id = `pypl-edw`.td_sysfnlib.to_number (b.ov_rule_1)
  AND `pypl-edw`.td_sysfnlib.to_number (a.ov_rule_1) = b.rule_id
  AND a.action_type = b.action_type
  AND a.high_overlap = 1
  AND a.ov_pct_1 < b.ov_pct_1
  AND b.high_overlap = 1;


--SET high_overlap = 0
--WHERE a.rule_id = `pypl-edw`td_sysfnlibto_numberb.ov_rule_1
-- AND `pypl-edw`td_sysfnlibto_numbera.ov_rule_1 = b.rule_id
-- AND a.action_type = b.action_type
-- AND a.high_overlap = 1
-- AND SAFE_CAST(a.ov_pct_1 AS BIGNUMERIC) < SAFE_CAST(b.ov_pct_1 AS BIGNUMERIC)
-- AND b.high_overlap = 1;
-- Update on 2024.01.15
-- Exclude rule 467776 from incremental alert 
UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305`
SET
  sub_poor_accuracy_incre_str_bad_rate = 0                      ,
  sub_poor_accuracy_incre_actn_bad_rate = 0                     ,
  poor_accuracy = sub_poor_accuracy_bad_rate                    ,
  sub_automated_poor_accuracy_incre_str_bad_rate = 0            ,
  sub_automated_poor_accuracy_incre_actn_bad_rate = 0           ,
  automated_poor_accuracy = sub_automated_poor_accuracy_bad_rate,
  sub_low_firing_incre_firing = 0                               ,
  low_firing = sub_low_firing_all_firing
WHERE
  rule_id = 467776;


-- Update on 2024.01.15
-- 6 month poor_accuracy only period for rule 310696, 1276516, 1280169
UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305`
SET
  sub_poor_accuracy_bad_rate = 0                    ,
  sub_poor_accuracy_incre_str_bad_rate = 0          ,
  sub_poor_accuracy_incre_actn_bad_rate = 0         ,
  poor_accuracy = 0                                 ,
  automated_poor_accuracy = 0                       ,
  sub_automated_poor_accuracy_bad_rate = 0          ,
  sub_automated_poor_accuracy_incre_str_bad_rate = 0,
  sub_automated_poor_accuracy_incre_actn_bad_rate = 0
WHERE
  rule_id IN (310696, 1276516, 1280169)
  AND CURRENT_DATE('America/Los_Angeles') <= '2024-05-15';


-- Update on 2024.01.22
-- poor_accuracy alert exception for rules with merch_col tag
UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS a
SET
  sub_poor_accuracy_bad_rate = 0                    ,
  sub_poor_accuracy_incre_str_bad_rate = 0          ,
  sub_poor_accuracy_incre_actn_bad_rate = 0         ,
  poor_accuracy = 0                                 ,
  automated_poor_accuracy = 0                       ,
  sub_automated_poor_accuracy_bad_rate = 0          ,
  sub_automated_poor_accuracy_incre_str_bad_rate = 0,
  sub_automated_poor_accuracy_incre_actn_bad_rate = 0
FROM
  `pypl-edw.${dataset_name_tmp}.check_action_rule_id` AS b
WHERE
  a.rule_id = b.rule_id
  AND LOWER(b.tags) LIKE '%merch_col%';


--re-derive alert tier after alert indicator updates
UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS output_table
SET
  tier = NULL
WHERE
  output_table.poor_accuracy = 0
  AND output_table.low_firing = 0
  AND output_table.high_overlap = 0
  AND output_table.over_whitelisted = 0
  AND output_table.automated_poor_accuracy = 0
  AND output_table.automated_over_whitelisted = 0
  AND output_table.automated_low_firing = 0
  AND output_table.high_cntct_rate = 0
  AND output_table.tier IS NOT NULL;


UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS output_table
SET
  tier = CASE
    WHEN output_table.automated_low_firing = 1
    OR output_table.automated_poor_accuracy = 1
    OR output_table.automated_over_whitelisted = 1
    OR output_table.automated_high_overlap = 1 THEN 5
    WHEN output_table.is_strategy = 1
    AND (
      output_table.poor_accuracy = 1
      OR output_table.low_firing = 1
      OR output_table.high_overlap = 1
      OR output_table.over_whitelisted = 1
    ) THEN 1
    WHEN output_table.is_strategy = 1
    AND output_table.high_cntct_rate = 1 THEN 2
    WHEN output_table.is_strategy = 0
    AND (
      output_table.poor_accuracy = 1
      OR output_table.high_overlap = 1
      OR output_table.over_whitelisted = 1
      OR COALESCE(
        output_table.high_cntct_rate,
        0
      ) = 1
    ) THEN 2
    WHEN output_table.is_strategy = 0
    AND output_table.low_firing = 1 THEN 3
  END
WHERE
  TRUE;


--  update tier for Branded Experience Crypto rules
UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS a
SET
  tier = 3
FROM
  `pypl-edw.${dataset_name_tmp}.check_action_rule_id` AS b
WHERE
  a.rule_id = b.rule_id
  AND UPPER(b.tags) LIKE '%BRDEXPCRYPTO%'
  AND a.poor_accuracy = 1;


--  update for rule 1169796 (safetynet rule, owner request exemption from high contact and poor accuracy alert)
UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS a
SET
  high_cntct_rate = 0                      ,
  sub_poor_accuracy_bad_rate = 0           ,
  sub_poor_accuracy_incre_str_bad_rate = 0 ,
  sub_poor_accuracy_incre_actn_bad_rate = 0,
  poor_accuracy = 0
WHERE
  a.rule_id IN (1169796)
  AND CURRENT_DATE('America/Los_Angeles') <= '2022-12-21';


UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS a
SET
  tier = CASE
    WHEN a.is_strategy = 0
    AND (
      a.poor_accuracy = 1
      OR a.high_overlap = 1
      OR a.over_whitelisted = 1
      OR COALESCE(a.high_cntct_rate, 0) = 1
    ) THEN 2
    WHEN a.is_strategy = 0
    AND a.low_firing = 1 THEN 3
  END
WHERE
  a.rule_id IN (1169796)
  AND CURRENT_DATE('America/Los_Angeles') <= '2022-12-21';


--  update for rules with good accuracy per manual review
UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS a
SET
  high_cntct_rate = 0                      ,
  sub_poor_accuracy_bad_rate = 0           ,
  sub_poor_accuracy_incre_str_bad_rate = 0 ,
  sub_poor_accuracy_incre_actn_bad_rate = 0,
  poor_accuracy = 0
WHERE
  a.rule_id IN (1353568, 1448313)
  AND CURRENT_DATE('America/Los_Angeles') <= '2023-09-30'
  OR a.rule_id IN (
    1208940,
    1386391,
    1366808,
    1425120
  )
  AND CURRENT_DATE('America/Los_Angeles') <= '2023-10-31';


UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS a
SET
  tier = CASE
    WHEN a.is_strategy = 0
    AND (
      a.poor_accuracy = 1
      OR a.high_overlap = 1
      OR a.over_whitelisted = 1
      OR COALESCE(a.high_cntct_rate, 0) = 1
    ) THEN 2
    WHEN a.is_strategy = 0
    AND a.low_firing = 1 THEN 3
  END
WHERE
  a.rule_id IN (1353568, 1448313)
  AND CURRENT_DATE('America/Los_Angeles') <= '2023-09-30'
  OR a.rule_id IN (
    1208940,
    1386391,
    1366808,
    1425120
  )
  AND CURRENT_DATE('America/Los_Angeles') <= '2023-10-31';


UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS a
SET
  sub_low_firing_incre_firing = 0,
  sub_low_firing_all_firing = 0  ,
  low_firing = 0
WHERE
  a.rule_id IN (
    SELECT
      live_metadata_table.rule_id
    FROM
      `pypl-edw.${dataset_name_tmp}.urm_lite` AS live_metadata_table
    WHERE
      UPPER(
        live_metadata_table.action_raw_str
      ) LIKE '%DECLINE_PMT%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) LIKE '%IPR_HOLD_TRANSACTION%'
  )
  AND COALESCE(a.low_firing, 0) <> 0;


UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS a
SET
  tier = (
    CASE
      WHEN a.poor_accuracy = 0
      AND a.high_overlap = 0
      AND a.over_whitelisted = 0
      AND a.high_cntct_rate = 0
      AND a.low_firing = 0 THEN CAST(NULL AS INT64)
      ELSE a.tier
    END
  )
WHERE
  a.rule_id IN (
    SELECT
      live_metadata_table.rule_id
    FROM
      `pypl-edw.${dataset_name_tmp}.urm_lite` AS live_metadata_table
    WHERE
      UPPER(
        live_metadata_table.action_raw_str
      ) LIKE '%DECLINE_PMT%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) LIKE '%IPR_HOLD_TRANSACTION%'
  )
  AND a.tier IS NOT NULL
  AND a.tier > 0
  AND a.low_firing = 0;


CALL
  `pypl-edw`.pp_monitor.drop_table (
    '${dataset_name_tmp}.funding_actn_base'
  );


CALL
  `pypl-edw`.pp_monitor.drop_table (
    '${dataset_name_tmp}.delta_temp'
  );


-- Update on 2024.03.08: Exclude seller rules in output table
DELETE FROM
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305`
WHERE
  (
    crs_team LIKE '%seller%'
    OR crs_team LIKE 'better_experiences'
    OR crs_team LIKE 'goal_ppcn'
  );


-- Update on 2024.06.26: tags exemption for alerts
-- Low firing exemption
UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS a
SET
  sub_low_firing_incre_firing = 0,
  sub_low_firing_all_firing = 0  ,
  low_firing = 0
FROM
  (
    SELECT
      --  how many days has the rule been running since the last update
      rule_id,
      tags
    FROM
      `pypl-edw.${dataset_name_tmp}.urm_lite` AS live_metadata_table
    WHERE
      UPPER(
        live_metadata_table.chkpnt_delimited_str
      ) LIKE '%CONSOLIDATEDFUNDING%'
      AND (
        UPPER(
          live_metadata_table.action_raw_str
        ) LIKE '%DECLINE_PMT%'
        OR UPPER(
          live_metadata_table.action_raw_str
        ) LIKE '%AUTH_FLOW%'
        OR UPPER(
          live_metadata_table.action_raw_str
        ) LIKE '%DISALLOW%'
      )
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%WHITE_LIST%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%WHITELIST%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%ADDLMTNINPUTMAP%SETISDISALLOW%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%SET_IS_DISALLOW%'
  ) AS st
WHERE
  a.rule_id = st.rule_id
  AND (
    UPPER(st.tags) LIKE '%%RISK%%APPETITE%%'
    OR UPPER(st.tags) LIKE '%%SAFETY%%NET%%'
    OR UPPER(st.tags) LIKE '%%BLACK%%LIST%%'
    OR UPPER(st.tags) LIKE '%%BLOCK%%LIST%%'
    OR UPPER(st.tags) LIKE '%%LATAM%%IPR%%'
    OR UPPER(st.tags) LIKE '%%BUSINESS%%REQUEST%%'
    OR UPPER(st.tags) LIKE '%TESTING%'
    OR UPPER(st.tags) LIKE '%%MARKET%%RAMPING%%'
    OR UPPER(st.tags) LIKE '%VEDA%'
    OR UPPER(st.tags) LIKE '%VSDS%'
    OR UPPER(st.tags) LIKE '%SPEEDSTER%'
    OR UPPER(st.tags) LIKE '%HIGHCONTACTCOST%'
    OR UPPER(st.tags) LIKE '%NNAENABLEMENT%'
    OR UPPER(st.tags) LIKE '%RED%CARPET%'
    OR UPPER(st.tags) LIKE '%STRATEGYFRAMEWORK%'
    OR UPPER(st.tags) LIKE '%FRAMEWORK%TECH%CHECK%'
    OR UPPER(st.tags) LIKE '%LOW%VOLUME%DISPUTE%FLOW%'
  );


UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS a
SET
  sub_low_firing_incre_firing = 0,
  sub_low_firing_all_firing = 0
FROM
  (
    SELECT
      --  how many days has the rule been running since the last update
      rule_id,
      tags
    FROM
      `pypl-edw.${dataset_name_tmp}.urm_lite` AS live_metadata_table
    WHERE
      UPPER(
        live_metadata_table.chkpnt_delimited_str
      ) LIKE '%CONSOLIDATEDFUNDING%'
      AND (
        UPPER(
          live_metadata_table.action_raw_str
        ) LIKE '%DECLINE_PMT%'
        OR UPPER(
          live_metadata_table.action_raw_str
        ) LIKE '%AUTH_FLOW%'
        OR UPPER(
          live_metadata_table.action_raw_str
        ) LIKE '%DISALLOW%'
      )
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%WHITE_LIST%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%WHITELIST%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%ADDLMTNINPUTMAP%SETISDISALLOW%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%SET_IS_DISALLOW%'
  ) AS st
WHERE
  a.rule_id = st.rule_id
  AND (
    UPPER(st.tags) LIKE '%%SCRIPT%%ATTACK%%'
    OR UPPER(st.tags) LIKE '%%SUPER%%DECLINE%%'
  );


UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS a
SET
  low_firing = CASE
    WHEN (
      sub_low_firing_incre_firing + sub_low_firing_all_firing
    ) > 0 THEN 1
    ELSE 0
  END
WHERE
  TRUE;


-- Poor accuracy alert exemption
UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS a
SET
  sub_poor_accuracy_bad_rate = 0           ,
  sub_poor_accuracy_incre_str_bad_rate = 0 ,
  sub_poor_accuracy_incre_actn_bad_rate = 0,
  poor_accuracy = 0
FROM
  (
    SELECT
      --  how many days has the rule been running since the last update
      rule_id,
      tags
    FROM
      `pypl-edw.${dataset_name_tmp}.urm_lite` AS live_metadata_table
    WHERE
      UPPER(
        live_metadata_table.chkpnt_delimited_str
      ) LIKE '%CONSOLIDATEDFUNDING%'
      AND (
        UPPER(
          live_metadata_table.action_raw_str
        ) LIKE '%DECLINE_PMT%'
        OR UPPER(
          live_metadata_table.action_raw_str
        ) LIKE '%AUTH_FLOW%'
        OR UPPER(
          live_metadata_table.action_raw_str
        ) LIKE '%DISALLOW%'
      )
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%WHITE_LIST%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%WHITELIST%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%ADDLMTNINPUTMAP%SETISDISALLOW%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%SET_IS_DISALLOW%'
  ) AS st
WHERE
  a.rule_id = st.rule_id
  AND (
    UPPER(st.tags) LIKE '%BUSINESSREQUESTALLOWLIST%'
    OR UPPER(st.tags) LIKE '%INTERNAL%TEST%'
    OR UPPER(st.tags) LIKE '%BILLING%AGREEMENT%'
    OR UPPER(st.tags) LIKE '%UELV%YOUNG%'
    OR (
      UPPER(st.tags) LIKE '%NEXT%OPEN%LOOP%'
      AND crs_team = 'goal_nextgen'
    )
    OR UPPER(st.tags) LIKE '%NO%TAGGING%TREND%'
  );


UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS a
SET
  sub_poor_accuracy_incre_str_bad_rate = 0,
  sub_poor_accuracy_incre_actn_bad_rate = 0
FROM
  (
    SELECT
      --  how many days has the rule been running since the last update
      rule_id,
      tags
    FROM
      `pypl-edw.${dataset_name_tmp}.urm_lite` AS live_metadata_table
    WHERE
      UPPER(
        live_metadata_table.chkpnt_delimited_str
      ) LIKE '%CONSOLIDATEDFUNDING%'
      AND (
        UPPER(
          live_metadata_table.action_raw_str
        ) LIKE '%DECLINE_PMT%'
        OR UPPER(
          live_metadata_table.action_raw_str
        ) LIKE '%AUTH_FLOW%'
        OR UPPER(
          live_metadata_table.action_raw_str
        ) LIKE '%DISALLOW%'
      )
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%WHITE_LIST%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%WHITELIST%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%ADDLMTNINPUTMAP%SETISDISALLOW%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%SET_IS_DISALLOW%'
  ) AS st
WHERE
  a.rule_id = st.rule_id
  AND (
    UPPER(st.tags) LIKE '%%SCRIPT%%ATTACK%%'
    OR UPPER(st.tags) LIKE '%%SUPER%%DECLINE%%'
  );


-- Update on 2024.07.31: Removal of decline bad accuracy alert for unbranded/cbp rules with CG cnt <= 30
UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS a
SET
  sub_poor_accuracy_bad_rate = 0
FROM
  (
    SELECT
      --  how many days has the rule been running since the last update
      rule_id,
      tags
    FROM
      `pypl-edw.${dataset_name_tmp}.urm_lite` AS live_metadata_table
    WHERE
      UPPER(
        live_metadata_table.chkpnt_delimited_str
      ) LIKE '%CONSOLIDATEDFUNDING%'
      AND (
        UPPER(
          live_metadata_table.action_raw_str
        ) LIKE '%DECLINE_PMT%'
        OR UPPER(
          live_metadata_table.action_raw_str
        ) LIKE '%AUTH_FLOW%'
        OR UPPER(
          live_metadata_table.action_raw_str
        ) LIKE '%DISALLOW%'
      )
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%WHITE_LIST%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%WHITELIST%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%ADDLMTNINPUTMAP%SETISDISALLOW%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%SET_IS_DISALLOW%'
  ) AS st
WHERE
  a.rule_id = st.rule_id
  AND (
    UPPER(st.tags) LIKE '%UNBRANDED%'
    OR UPPER(st.tags) LIKE '%CBP%'
  )
  AND match_txn_cnt < 30;


UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS a
SET
  sub_poor_accuracy_incre_str_bad_rate = 0
FROM
  (
    SELECT
      --  how many days has the rule been running since the last update
      rule_id,
      tags
    FROM
      `pypl-edw.${dataset_name_tmp}.urm_lite` AS live_metadata_table
    WHERE
      UPPER(
        live_metadata_table.chkpnt_delimited_str
      ) LIKE '%CONSOLIDATEDFUNDING%'
      AND (
        UPPER(
          live_metadata_table.action_raw_str
        ) LIKE '%DECLINE_PMT%'
        OR UPPER(
          live_metadata_table.action_raw_str
        ) LIKE '%AUTH_FLOW%'
        OR UPPER(
          live_metadata_table.action_raw_str
        ) LIKE '%DISALLOW%'
      )
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%WHITE_LIST%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%WHITELIST%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%ADDLMTNINPUTMAP%SETISDISALLOW%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%SET_IS_DISALLOW%'
  ) AS st
WHERE
  a.rule_id = st.rule_id
  AND (
    UPPER(st.tags) LIKE '%UNBRANDED%'
    OR UPPER(st.tags) LIKE '%CBP%'
  )
  AND str_match_txn_cnt < 30;


UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS a
SET
  sub_poor_accuracy_incre_actn_bad_rate = 0
FROM
  (
    SELECT
      --  how many days has the rule been running since the last update
      rule_id,
      tags
    FROM
      `pypl-edw.${dataset_name_tmp}.urm_lite` AS live_metadata_table
    WHERE
      UPPER(
        live_metadata_table.chkpnt_delimited_str
      ) LIKE '%CONSOLIDATEDFUNDING%'
      AND (
        UPPER(
          live_metadata_table.action_raw_str
        ) LIKE '%DECLINE_PMT%'
        OR UPPER(
          live_metadata_table.action_raw_str
        ) LIKE '%AUTH_FLOW%'
        OR UPPER(
          live_metadata_table.action_raw_str
        ) LIKE '%DISALLOW%'
      )
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%WHITE_LIST%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%WHITELIST%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%ADDLMTNINPUTMAP%SETISDISALLOW%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%SET_IS_DISALLOW%'
  ) AS st
WHERE
  a.rule_id = st.rule_id
  AND (
    UPPER(st.tags) LIKE '%UNBRANDED%'
    OR UPPER(st.tags) LIKE '%CBP%'
  )
  AND actn_match_txn_cnt < 30;


UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS a
SET
  poor_accuracy = CASE
    WHEN (
      sub_poor_accuracy_bad_rate + sub_poor_accuracy_incre_str_bad_rate + sub_poor_accuracy_incre_actn_bad_rate
    ) > 0 THEN 1
    ELSE 0
  END
WHERE
  TRUE;


-- Over allowlisted alert exemption
UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS a
SET
  over_whitelisted = 0
FROM
  (
    SELECT
      --  how many days has the rule been running since the last update
      rule_id,
      tags
    FROM
      `pypl-edw.${dataset_name_tmp}.urm_lite` AS live_metadata_table
    WHERE
      UPPER(
        live_metadata_table.chkpnt_delimited_str
      ) LIKE '%CONSOLIDATEDFUNDING%'
      AND (
        UPPER(
          live_metadata_table.action_raw_str
        ) LIKE '%DECLINE_PMT%'
        OR UPPER(
          live_metadata_table.action_raw_str
        ) LIKE '%AUTH_FLOW%'
        OR UPPER(
          live_metadata_table.action_raw_str
        ) LIKE '%DISALLOW%'
      )
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%WHITE_LIST%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%WHITELIST%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%ADDLMTNINPUTMAP%SETISDISALLOW%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%SET_IS_DISALLOW%'
  ) AS st
WHERE
  a.rule_id = st.rule_id
  AND (
    UPPER(st.tags) LIKE '%GOOD%POP%AF%'
    OR UPPER(st.tags) LIKE '%INTERNAL%TEST%'
  );


-- High contact rate alert exmeption
UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS a
SET
  high_cntct_rate = 0
FROM
  (
    SELECT
      --  how many days has the rule been running since the last update
      rule_id,
      tags
    FROM
      `pypl-edw.${dataset_name_tmp}.urm_lite` AS live_metadata_table
    WHERE
      UPPER(
        live_metadata_table.chkpnt_delimited_str
      ) LIKE '%CONSOLIDATEDFUNDING%'
      AND (
        UPPER(
          live_metadata_table.action_raw_str
        ) LIKE '%DECLINE_PMT%'
        OR UPPER(
          live_metadata_table.action_raw_str
        ) LIKE '%AUTH_FLOW%'
        OR UPPER(
          live_metadata_table.action_raw_str
        ) LIKE '%DISALLOW%'
      )
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%WHITE_LIST%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%WHITELIST%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%ADDLMTNINPUTMAP%SETISDISALLOW%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%SET_IS_DISALLOW%'
  ) AS st
WHERE
  a.rule_id = st.rule_id
  AND UPPER(st.tags) LIKE '%%BLOCK%%LIST%%';


-- High overlap alert exmeption
UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS a
SET
  high_overlap = 0
FROM
  (
    SELECT
      --  how many days has the rule been running since the last update
      rule_id,
      tags
    FROM
      `pypl-edw.${dataset_name_tmp}.urm_lite` AS live_metadata_table
    WHERE
      UPPER(
        live_metadata_table.chkpnt_delimited_str
      ) LIKE '%CONSOLIDATEDFUNDING%'
      AND (
        UPPER(
          live_metadata_table.action_raw_str
        ) LIKE '%DECLINE_PMT%'
        OR UPPER(
          live_metadata_table.action_raw_str
        ) LIKE '%AUTH_FLOW%'
        OR UPPER(
          live_metadata_table.action_raw_str
        ) LIKE '%DISALLOW%'
      )
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%WHITE_LIST%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%WHITELIST%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%ADDLMTNINPUTMAP%SETISDISALLOW%'
      AND UPPER(
        live_metadata_table.action_raw_str
      ) NOT LIKE '%SET_IS_DISALLOW%'
  ) AS st
WHERE
  a.rule_id = st.rule_id
  AND (
    UPPER(st.tags) LIKE '%%SCRIPT%%ATTACK%%'
    OR UPPER(st.tags) LIKE '%%VELOCITY%%'
  );


-- Previous temp exclusion logic in sx102
UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS a
SET
  sub_poor_accuracy_bad_rate = 0           ,
  sub_poor_accuracy_incre_str_bad_rate = 0 ,
  sub_poor_accuracy_incre_actn_bad_rate = 0,
  poor_accuracy = 0
WHERE
  rule_mo IN ('ffa', 'stolenid');


UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS a
SET
  sub_low_firing_incre_firing = 0                    ,
  sub_low_firing_all_firing = 0                      ,
  low_firing = 0                                     ,
  over_whitelisted = 0                               ,
  high_cntct_rate = 0                                ,
  high_overlap = 0                                   ,
  sub_poor_accuracy_bad_rate = 0                     ,
  sub_poor_accuracy_incre_str_bad_rate = 0           ,
  sub_poor_accuracy_incre_actn_bad_rate = 0          ,
  poor_accuracy = 0                                  ,
  automated_poor_accuracy = 0                        ,
  sub_automated_poor_accuracy_bad_rate = 0           ,
  sub_automated_poor_accuracy_incre_str_bad_rate = 0 ,
  sub_automated_poor_accuracy_incre_actn_bad_rate = 0,
  automated_low_firing = 0                           ,
  automated_low_firing_cnt = 0                       ,
  automated_low_firing_amt = 0                       ,
  automated_over_whitelisted = 0
WHERE
  crs_team NOT LIKE 'gfr%'
  AND crs_team NOT LIKE 'goal%'
  AND crs_team NOT LIKE 'better%'
  AND crs_team NOT LIKE 'ats%'
  AND tier IS NOT NULL
  AND (low_firing + high_overlap >= 1);


-- Derive tier
UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS a
SET
  tier = CASE
    WHEN (
      automated_low_firing = 1
      OR automated_poor_accuracy = 1
      OR automated_over_whitelisted = 1
      OR automated_high_overlap = 1
    ) THEN 5
    WHEN is_strategy = 1
    AND (
      poor_accuracy = 1
      OR low_firing = 1
      OR high_overlap = 1
      OR over_whitelisted = 1
    ) THEN 1
    WHEN is_strategy = 1
    AND (high_cntct_rate = 1) THEN 2
    WHEN is_strategy = 0
    AND (
      poor_accuracy = 1
      OR high_overlap = 1
      OR over_whitelisted = 1
      OR COALESCE(high_cntct_rate, 0) = 1
    ) THEN 2
    WHEN is_strategy = 0
    AND low_firing = 1 THEN 3
  END
WHERE
  TRUE;


UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305` AS a
SET
  automated_poor_accuracy = 0                                ,
  automated_low_firing = 0                                   ,
  automated_over_whitelisted = 0 -- , automated_high_overlap=0
            ,
  tier = CASE
    WHEN low_firing = 1
    AND poor_accuracy + over_whitelisted + high_overlap + high_cntct_rate = 0 THEN 3
    WHEN poor_accuracy + over_whitelisted + high_overlap + high_cntct_rate > 0 THEN 2
    WHEN low_firing + poor_accuracy + over_whitelisted + high_overlap + high_cntct_rate = 0 THEN NULL
    ELSE 100
  END
WHERE
  tier = 5;


UPDATE
  `pypl-edw.${dataset_name_tmp}.funding_decline_alert_v0305`
SET
  monitoring_window = REPLACE(monitoring_window, '-', '/')
WHERE
  1 = 1;


--SELECT count(*) FROM `pypl-edw`pp_aura_tablesoutput_tabl;--TRUNCATE TABLE `pypl-edw`pp_aura_tablesoutput_tabl;--SELECT    count(*)  FROM    `pypl-edw`pp_aura_tablesoutput_tabl;
