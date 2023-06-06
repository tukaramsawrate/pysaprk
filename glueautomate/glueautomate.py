import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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
#output="s3a://{}/{}".format(bucket_name,"/output/aslres")
#print("Input File Path : ",input_file_path);

def check_file_format(fpath):
    data_path=fpath
    f_format=fpath.split('.')[-1]
    #print(f_format)
    if f_format=="csv":
        csv_data_load(data_path)
        
    elif f_format=="json":
        #print("json here")
        json_data_load(data_path)
    else:
        raw_df=spark.read.format(f_format).load(data_path)
        raw_df.write.mode("overwrite").format(f_format).save("s3a://errobuck/land/")

check_file_format(input_file_path)



def csv_data_load(data):
    raw_df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
    raw_df.write.mode("overwrite").format("csv").option("header","true").save("s3a://rawbucketts/land/")
    

def json_data_load(data):
    raw_df = spark.read.format("json").option("header","true").option("inferSchema","true").load(data)
    
    raw_df.write.mode("overwrite").format("csv").option("header","true").save("s3a://rawbucketts/land/")



#df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(input_file_path)

#df.show()

#df=df.withColumn("dr",dense_rank().over(Window.partitionBy(col("state")))).withColumn("rank",rank().over(Window.partitionBy(col("state")))).withColumn("rn",row_number().over(Window.partitionBy(col("state"))))

#df=df.where(col("dr")==1)

#df.write.format("csv").option("header","true").save(output)
