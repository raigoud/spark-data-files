from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import *
schema = StructType() \
      .add("emp_id",IntegerType(),True) \
      .add("ename",StringType(),True) \
      .add("job",StringType(),True) \
      .add("mgr",IntegerType(),True) \
      .add("doj",StringType(),True) \
      .add("sal",IntegerType(),True) \
      .add("bonus",IntegerType(),True) \
      .add("dept_id",IntegerType(),True) 


dfmain = spark.read.format("csv").option("header",True).load("dbfs:/FileStore/shared_uploads/nivedika.thalla@gmail.com/empmaindir")
#dfmain.show()

dfmain.createOrReplaceTempView("empmain")
dfmain.printSchema()
df2 = spark.read.format("csv").schema(schema).load("dbfs:/FileStore/shared_uploads/nivedika.thalla@gmail.com/empday3.csv")
df2.createOrReplaceTempView("empstg")

#no change includes unchanged common data and data which is not received in staging but exisiting in main
dfnochg = spark.sql("select e.emp_id,e.ename,e.job,e.mgr,e.doj,e.sal,e.bonus,e.dept_id,e.Start_dt, e.End_dt,e.Status from empmain e left outer join empstg s  on e.emp_id =s.emp_id where (e.emp_id =s.emp_id and s.ename = e.ename and s.job = e.job and e.sal = s.sal and e.dept_id = s.dept_id) or (s.emp_id is null) ")
#dfnochg.show()

#end date the cahnged records
dfenddt = spark.sql("select e.emp_id,e.ename,e.job,e.mgr,e.doj,e.sal,e.bonus,e.dept_id,e.Start_dt,current_date() as  End_dt,'Inactive' as Status from empstg s left outer join empmain e on e.emp_id =s.emp_id where e.emp_id =s.emp_id and (s.ename <> e.ename or s.job <> e.job or e.sal <> s.sal or e.dept_id <> s.dept_id )")
#dfenddt.show()
#new set of records for changed records
dfupdt = spark.sql("select s.*,current_date() as Start_dt, NULL as  End_dt,'Active' as Status from empstg s left outer join empmain e on e.emp_id =s.emp_id where e.emp_id =s.emp_id and (s.ename <> e.ename or s.job <> e.job or e.sal <> s.sal or e.dept_id <> s.dept_id )")
#dfupdt.show()

#newly received data
dfnew = spark.sql("select s.*,current_date() as Start_dt, NULL as  End_dt,'Active' as Status from empstg s left outer join empmain e on s.emp_id =e.emp_id where e.emp_id is null ")
#dfnew.show()

dfscd2day2 = dfnochg.union(dfenddt).union(dfupdt).union(dfnew)
dfsincr = dfscd2day2.sort(dfscd2day2.emp_id.asc_nulls_last())

dfsincr.write.mode('overwrite').options(header='True', delimiter=',').csv("dbfs:/FileStore/shared_uploads/nivedika.thalla@gmail.com/empmaindir")
