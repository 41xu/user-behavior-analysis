DROP TABLE IF EXISTS rawdata.event_export;
CREATE TABLE rawdata.event_export (
  event_id INT,
  month_id INT,
  week_id INT,
  user_id BIGINT,
  distinct_id STRING,
  time TIMESTAMP,
  day INT,
  event_bucket INT,
  _offset BIGINT,
  p__app_version STRING,
  p__browser STRING,
  p__browser_version STRING,
  p__carrier STRING,
  p__city STRING,
  p__country STRING,
  p__device_id STRING,
  p__element_content STRING,
  p__element_id STRING,
  p__element_position STRING,
  p__element_type STRING,
  p__event_duration BIGINT,
  p__ip STRING,
  p__is_first_day BIGINT,
  p__is_first_time BIGINT,
  p__is_login_id BIGINT,
  p__kafka_offset BIGINT,
  p__lib STRING,
  p__lib_version STRING,
  p__manufacturer STRING,
  p__model STRING,
  p__network_type STRING,
  p__os STRING,
  p__os_version STRING,
  p__province STRING,
  p__referrer STRING,
  p__resume_from_background BIGINT,
  p__screen_height BIGINT,
  p__screen_name STRING,
  p__screen_width BIGINT,
  p__title STRING,
  p__track_signup_original_id STRING,
  p__url STRING,
  p__wifi BIGINT,
  p_productname STRING,
  p_productprice BIGINT,
  p_buttonname STRING,
  p_companyname STRING,
  p_count_i BIGINT,
  p_customname STRING,
  p_customtype STRING,
  p_data_flag STRING,
  p_entrance STRING,
  p_grade BIGINT,
  p_is_check BIGINT,
  p_is_first_day BIGINT,
  p_is_first_day_start BIGINT,
  p_is_first_time BIGINT,
  p_is_first_time_start BIGINT,
  p_is_success BIGINT,
  p_linkclassify STRING,
  p_linkname STRING,
  p_logintyle STRING,
  p_memberid BIGINT,
  p_mobile STRING,
  p_msg_id STRING,
  p_msg_title STRING,
  p_operationobject STRING,
  p_operationtype STRING,
  p_other_property STRING,
  p_programauthority STRING,
  p_programid BIGINT,
  p_programname STRING,
  p_programtype STRING,
  p_promotioncode STRING,
  p_realname STRING,
  p_referrer STRING,
  p_referrer_host STRING,
  p_resume_from_background BIGINT,
  p_schemaclassify1 STRING,
  p_schemaclassify2 STRING,
  p_school STRING,
  p_superkey STRING,
  p_superkey2 STRING,
  p_teamidentify STRING,
  p_teamname STRING,
  p_teamname_member STRING,
  p_test_boolean1 BIGINT,
  p_test_boolean2 BIGINT,
  p_test_boolean3 BIGINT,
  p_test_count BIGINT,
  p_test_number1 BIGINT,
  p_test_number2 BIGINT,
  p_test_number3 BIGINT,
  p_test_text1 STRING,
  p_test_text2 STRING,
  p_test_text3 STRING,
  p_title STRING,
  p_url STRING,
  p_url_path STRING,
  p_utm_campaign STRING,
  p_utm_content STRING,
  p_utm_medium STRING,
  p_utm_source STRING,
  p_utm_term STRING
)
STORED AS TEXTFILE
LOCATION '/user/impala/data/event_export'

