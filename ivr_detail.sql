
CREATE OR REPLACE TABLE keepcoding.ivr_detail AS


    SELECT ic.ivr_id AS calls_ivr_id,
          ic.phone_number AS calls_phone_number,
          ic.ivr_result AS calls_ivr_results,
          ic.vdn_label AS calls_vdn_label,
          ic.start_date AS calls_start_date,
          FORMAT_TIMESTAMP('%Y%m%d', TIMESTAMP(start_date)) AS calls_start_date_id,
          ic.end_date AS calls_end_date,
          FORMAT_TIMESTAMP('%Y%m%d', TIMESTAMP(end_date)) AS calls_end_date_id,
          ic.total_duration AS calls_total_duration,
          ic.customer_segment AS calls_customer_segment,
          ic.ivr_language AS calls_ivr_language,
          ic.steps_module AS calls_steps_module,
          ic.module_aggregation AS calls_module_aggregation,
          im.module_sequece,
          im.module_name,
          im.module_duration,
          im.module_result,
          ist.step_sequence,
          ist.step_name,
          ist.step_result,
          ist.step_description_error,
          ist.document_type,
          ist.document_identification,
          ist.customer_phone,
          ist.billing_account_id
        FROM `keepcoding.ivr_steps` AS ist
        LEFT
        JOIN `keepcoding.ivr_modules` AS im
          ON ist.ivr_id = im.ivr_id
        AND ist.module_sequece = im.module_sequece
        LEFT 
        JOIN keepcoding.ivr_calls AS ic
          ON im.ivr_id = ic.ivr_id;