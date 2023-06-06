import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import *
from pyspark.sql.functions import *
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
data="s3://tstukaram/input1/asl.csv"

df1=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)

df1=df1.where(col("age")>=30)

df1.write.format("csv").option("header","true").save("s3://tstukaram/output3/")

