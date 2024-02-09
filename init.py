import sys
#from awsglue.transforms import *
#from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
#from awsglue.context import GlueContext
#from awsglue.job import Job
#from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
source_data = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://test-etl-bucket-erp/athletes.csv"],
        "recurse": True,
    },
    transformation_ctx="source_data",
)

# Script generated for node Drop Duplicates
transform_data = DynamicFrame.fromDF(
    source_data.toDF().dropDuplicates(),
    glueContext,
    "transform_data",
)

# Script generated for node Amazon S3
write_to_s3 = glueContext.write_dynamic_frame.from_options(
    frame=transform_data,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://transformed-test-etl-bucket-erp",
        "partitionKeys": [],
    },
    # format_options={"compression": "snappy"},
    transformation_ctx="write_to_s3",
)