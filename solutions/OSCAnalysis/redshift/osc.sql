
CREATE EXTERNAL SCHEMA osc_staging_db
FROM DATA CATALOG
DATABASE 'osc_staging_db'
IAM_ROLE 'arn:aws:iam::889515947644:role/OscAnalysis3-Redshift-RedshiftRole-106DGXNXIREL8';

-- select * from svv_external_schemas;
-- select * from svv_external_tables;

SELECT
  year,
  month,
  day,
  feature,
  count(feature) AS count
FROM osc_staging_db.osc_changes
GROUP BY year, month, day, feature;
