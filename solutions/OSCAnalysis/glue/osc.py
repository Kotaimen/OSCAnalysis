import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

glueContext = GlueContext(SparkContext.getOrCreate())

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'source_database',
    'target_database',
])

# catalog: database and table names
source_db = args['source_database']
target_db = args['target_database']
tbl_osc_change = 'osc/changes'

# output:
tbl_osc_change_parquet = 's3://oscanalysis3-datalake-stagingbucket-12s55zt9zlkxs/osc/changes/'

osc_change = glueContext.create_dynamic_frame.from_catalog(
    database=args['source_database'],
    table_name=tbl_osc_change)

glueContext.write_dynamic_frame.from_options(
    frame=osc_change,
    connection_type="s3",
    connection_options={"path": tbl_osc_change_parquet},
    format="parquet")
