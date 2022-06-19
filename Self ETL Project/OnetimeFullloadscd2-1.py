import sys
#Onetime Script Full Load
import pyspark
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('One time Full Load SCD2 emp 061922').master("local[*]").getOrCreate()

print("Started")
inputfile_name = sys.argv[1]
##inputfile_name = "hdfs://localhost:9000/ravi/spark/hdfsloc"
#"dbfs:/FileStore/shared_uploads/nivedika.thalla@gmail.com/emp.csv"

outputdir_name = sys.argv[2]
#hdfs://localhost:9000/ravi/spark/hdfsdest
#"dbfs:/FileStore/shared_uploads/nivedika.thalla@gmail.com/empmaindir"


schema = StructType() \
      .add("emp_id",IntegerType(),True) \
      .add("ename",StringType(),True) \
      .add("job",StringType(),True) \
      .add("mgr",IntegerType(),True) \
      .add("doj",StringType(),True) \
      .add("sal",IntegerType(),True) \
      .add("bonus",IntegerType(),True) \
      .add("dept_id",IntegerType(),True) 

df1 = spark.read.format("csv").schema(schema).load(inputfile_name)

#below is only one time only 
dfscd2main = df1.withColumn("Start_dt",lit("1900-01-01")).withColumn("End_dt",lit(None).cast('string')).withColumn("Status",lit("Active"))
#dfscd2main.show()
dfscd2main.write.mode('overwrite').options(header='True', delimiter=',').csv(outputdir_name)

print("Completed")