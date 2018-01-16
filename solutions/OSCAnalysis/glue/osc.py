import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(
    sys.argv,
    [
        'JOB_NAME',
        'SOURCE_DATABASE',
        'SOURCE_TABLE',
        'TARGET_DATABASE',
        'TARGET_TABLE',
        'TARGET_TABLE_LOCATION'
    ]
);

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "oscanalysis3-curated-database", table_name = "osc/changes", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database=args['SOURCE_DATABASE'],
                                                            table_name=args['SOURCE_TABLE'],
                                                            transformation_ctx="datasource0")
## @type: ApplyMapping
## @args: [mapping = [("change", "string", "change", "string"), ("feature", "string", "feature", "string"), ("id", "long", "id", "long"), ("version", "int", "version", "int"), ("timestamp", "string", "timestamp", "string"), ("uid", "int", "uid", "int"), ("user", "string", "user", "string"), ("changeset", "int", "changeset", "int"), ("lon", "double", "lon", "double"), ("lat", "double", "lat", "double")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame=datasource0,
                                   mappings=[("change", "string", "change", "string"),
                                             ("feature", "string", "feature", "string"),
                                             ("id", "long", "id", "long"),
                                             ("version", "int", "version", "int"),
                                             ("timestamp", "string", "timestamp", "string"),
                                             ("uid", "int", "uid", "int"),
                                             ("user", "string", "user", "string"),
                                             ("changeset", "int", "changeset", "int"),
                                             ("lon", "double", "lon", "double"),
                                             ("lat", "double", "lat", "double"),
                                             ("partition_0", "string", "year", "string"),
                                             ("partition_1", "string", "month", "string"),
                                             ("partition_2", "string", "day", "string"),
                                             ("partition_3", "string", "hour", "string")
                                             ],
                                   transformation_ctx="applymapping1")
## @type: SelectFields
## @args: [paths = ["change", "feature", "id", "version", "timestamp", "uid", "user", "changeset", "lon", "lat"], transformation_ctx = "selectfields2"]
## @return: selectfields2
## @inputs: [frame = applymapping1]
selectfields2 = SelectFields.apply(frame=applymapping1,
                                   paths=["change", "feature", "id", "version", "timestamp", "uid", "user", "changeset",
                                          "lon", "lat", "year", "month", "day", "hour"], transformation_ctx="selectfields2")
## @type: ResolveChoice
## @args: [choice = "MATCH_CATALOG", database = "oscanalysis3-staging-database", table_name = "osc/changes", transformation_ctx = "resolvechoice3"]
## @return: resolvechoice3
## @inputs: [frame = selectfields2]
resolvechoice3 = ResolveChoice.apply(frame=selectfields2, choice="MATCH_CATALOG",
                                     database=args["TARGET_DATABASE"], table_name=args["TARGET_TABLE"],
                                     transformation_ctx="resolvechoice3")
## @type: ResolveChoice
## @args: [choice = "make_struct", transformation_ctx = "resolvechoice4"]
## @return: resolvechoice4
## @inputs: [frame = resolvechoice3]
resolvechoice4 = ResolveChoice.apply(frame=resolvechoice3, choice="make_struct", transformation_ctx="resolvechoice4")
## @type: DataSink
## @args: [database = "oscanalysis3-staging-database", table_name = "osc/changes", transformation_ctx = "datasink5"]
## @return: datasink5
## @inputs: [frame = resolvechoice4]
# datasink5 = glueContext.write_dynamic_frame.from_catalog(frame=resolvechoice4, database=args["TARGET_DATABASE"],
#                                                          table_name=args["TARGET_TABLE"], transformation_ctx="datasink5")

resolvechoice4.toDF().write.parquet(args['TARGET_TABLE_LOCATION'],
                               partitionBy=['year', 'month', 'day', 'hour']);

job.commit()
