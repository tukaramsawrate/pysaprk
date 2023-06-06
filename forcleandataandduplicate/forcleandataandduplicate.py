import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
args = getResolvedOptions(sys.argv, ["buck","file"])
file_name=args['file']
bucket_name=args['buck']
print("Bucket Name" , bucket_name)
print("File Name" , file_name)
input_file_path="s3a://{}/{}".format(bucket_name,file_name)
#print("Input File Path : ",input_file_path);
output="s3a://{}/{}".format(bucket_name,"/output/aslres")

df1=spark.read.format("csv").option("header","true").option("inferSchema","true").load(input_file_path)
cols=[re.sub("[^a-zA-Z]","", x) for x in df1.columns]
df1=df1.toDF(*cols)
df1=df1.dropna()
df1=df1.drop_duplicates()
df1.coalesce(1).write.format("csv").option("header","true").save(output)
