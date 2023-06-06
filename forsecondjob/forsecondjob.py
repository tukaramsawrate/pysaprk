import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import *
from pyspark.sql.functions import *
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)                                          

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "forcrawlerdb", table_name = "input", transformation_ctx = "datasource0")
df1=datasource0.toDF()
df1=df1.where(df1["age"]>=30)
#DynamicFrame.fromDF(df1, glueContext, "test_nest")
#datasink4 = glueContext.write_dynamic_frame.from_options(frame = test_nest, connection_type = "s3", 
#connection_options = {"path": "s3://tstukaram/output1/"}, format = "parquet", transformation_ctx = "datasink4")
df1.write.mode("overwrite").format("csv").option("header","true").save("s3://tstukaram/output2")