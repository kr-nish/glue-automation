import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Example ETL transformation
source_df = spark.read.csv("s3://nishant-glue-bucket-234/input/employees.csv")
transformed_df = source_df.select("column1", "column2")
transformed_df.write.parquet("s3://nishant-glue-bucket-234/output/")

job.commit()