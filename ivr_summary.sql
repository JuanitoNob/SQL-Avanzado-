CREATE OR REPLACE TABLE keepcoding.ivr_summary AS


WITH tma 
AS (SELECT 
        dt.calls_ivr_id AS ivr_id,
        dt.calls_phone_number AS phone_number,
        dt.calls_ivr_results AS ivr_result,
        dt.calls_start_date AS start_date,
        dt.calls_end_date AS end_date,
        dt.calls_total_duration AS total_duration,
        dt.calls_customer_segment AS customer_segment,
        dt.calls_ivr_language AS ivr_language,
        dt.calls_steps_module AS steps_module,
        dt.calls_module_aggregation AS module_aggregation,
        LAG(dt.calls_start_date) OVER(PARTITION BY dt.calls_phone_number ORDER BY dt.calls_phone_number, dt.calls_start_date) AS llamada_anterior,
        LEAD(dt.calls_start_date) OVER(PARTITION BY dt.calls_phone_number ORDER BY dt.calls_phone_number, dt.calls_start_date) AS llamada_posterior,
    FROM keepcoding.ivr_detail AS dt
    GROUP BY ivr_id, phone_number, ivr_result, start_date, end_date, total_duration, customer_segment, ivr_language, steps_module, module_aggregation
    ORDER BY dt.calls_phone_number
),


pasos 
AS (SELECT calls_ivr_id,
      COALESCE(NULLIF(dt.document_type, 'NULL'), NULLIF(st.document_type, 'NULL')) AS document_type,
      COALESCE(NULLIF(dt.document_identification, 'NULL'), NULLIF(st.document_identification, 'NULL')) AS document_identification,
      COALESCE(NULLIF(dt.customer_phone, 'NULL'), NULLIF(st.customer_phone, 'NULL')) AS customer_phone,
      COALESCE(NULLIF(dt.billing_account_id, 'NULL'), NULLIF(st.billing_account_id, 'NULL')) AS billing_account_id
    FROM keepcoding.ivr_detail AS dt
    LEFT
    JOIN keepcoding.ivr_steps AS st
      ON dt.calls_ivr_id = st.ivr_id
    GROUP BY dt.calls_ivr_id, document_type, document_identification, customer_phone, billing_account_id
    QUALIFY ROW_NUMBER() OVER(PARTITION BY CAST(dt.calls_ivr_id AS STRING) ORDER BY dt.calls_ivr_id, document_type, document_identification, billing_account_id DESC) = 1
)


SELECT 
      tma.ivr_id,
      tma.phone_number,
      tma.ivr_result,
      CASE WHEN dt.calls_vdn_label LIKE 'ATC%' THEN'FRONT'
           WHEN dt.calls_vdn_label LIKE 'TECH%' THEN 'TECH'
           WHEN dt.calls_vdn_label = 'ABSORPTION' THEN dt.calls_vdn_label
           ELSE 'RESTO'
      END AS vdn_aggregation,
      tma.start_date,
      tma.end_date,
      tma.total_duration,
      tma.customer_segment,
      tma.ivr_language,
      tma.steps_module,
      tma.module_aggregation,
      pasos.document_type,
      pasos.document_identification,
      pasos.customer_phone,
      pasos.billing_account_id,
      IF(CONTAINS_SUBSTR(dt.calls_module_aggregation, 'AVERIA_MASIVA'), 1, 0) AS masiva_lg,
      CASE WHEN SUM(IF((dt.step_name = 'CUSTOMERINFOBYPHONE.TX') AND (dt.step_description_error = 'NULL'), 1, 0)) > 0 THEN 1 ELSE 0
      END AS info_by_phone_lg,
      CASE WHEN SUM(IF((dt.step_name = 'CUSTOMERINFOBYDNI.TX') AND (dt.step_description_error = 'NULL'), 1, 0)) > 0 THEN 1 ELSE 0
      END AS info_by_dni_lg,
      IF(DATETIME_DIFF(dt.calls_start_date, llamada_anterior, HOUR)< 24, 1, 0) AS repeated_phone_24H,
      IF(DATETIME_DIFF(llamada_posterior, dt.calls_end_date, HOUR) < 24, 1,0) AS cause_recall_phone_24H,
    FROM tma
    LEFT
    JOIN pasos
      ON tma.ivr_id = pasos.calls_ivr_id
    LEFT
    JOIN keepcoding.ivr_detail AS dt
      ON tma.ivr_id = dt.calls_ivr_id
    LEFT
    JOIN keepcoding.ivr_steps AS st
      ON tma.ivr_id = st.ivr_id
    GROUP BY 
        tma.ivr_id,
        tma.phone_number,
        tma.ivr_result,
        vdn_aggregation,
        tma.start_date,
        tma.end_date,
        tma.total_duration,
        tma.customer_segment,
        tma.ivr_language,
        tma.steps_module,
        tma.module_aggregation,
        pasos.document_type,
        pasos.document_identification,
        pasos.customer_phone,
        pasos.billing_account_id,
        masiva_lg,
        repeated_phone_24H,
        cause_recall_phone_24H
    ORDER BY tma.phone_number, tma.ivr_id;



