# https://sparkbyexamples.com/pyspark/convert-pyspark-rdd-to-dataframe/
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
dept = [("Finance",10),("Marketing",20),("Sales",30),("IT",40)]
rdd = spark.sparkContext.parallelize(dept)
### way 1, DataFrame from rdd.toDF
df = rdd.toDF()
df.printSchema()
#root
# |-- _1: string (nullable = true)
# |-- _2: long (nullable = true)
df.show(truncate=False)
#+---------+---+
#|       _1| _2|
#+---------+---+
#|  Finance| 10|
#|Marketing| 20|
#|    Sales| 30|
#|       IT| 40|
#+---------+---+
# DataFrame from rdd.toDF with column names
deptColumns = ["dept_name","dept_id"]
df2 = rdd.toDF(deptColumns)
df2.printSchema()
#root
# |-- dept_name: string (nullable = true)
# |-- dept_id: long (nullable = true)
df2.show(truncate=False)
#+---------+-------+
#|dept_name|dept_id|
#+---------+-------+
#|  Finance|     10|
#|Marketing|     20|
#|    Sales|     30|
#|       IT|     40|
#+---------+-------+
### way 2, DataFrame with createDataFrame method
deptDF = spark.createDataFrame(rdd, schema = deptColumns)
deptDF.printSchema()
#root
# |-- dept_name: string (nullable = true)
# |-- dept_id: long (nullable = true)
deptDF.show(truncate=False)
#+---------+-------+
#|dept_name|dept_id|
#+---------+-------+
#|  Finance|     10|
#|Marketing|     20|
#|    Sales|     30|
#|       IT|     40|
#+---------+-------+
### way 3, DataFrame with createDataFrame and StructType schema
from pyspark.sql.types import StructType,StructField, StringType

deptSchema = StructType([       
    StructField('dept_name', StringType(), True),
    StructField('dept_id', StringType(), True)
])

deptDF1 = spark.createDataFrame(rdd, schema = deptSchema)
deptDF1.printSchema()
#root
# |-- dept_name: string (nullable = true)
# |-- dept_id: string (nullable = true)
deptDF1.show(truncate=False)
#+---------+-------+
#|dept_name|dept_id|
#+---------+-------+
#|  Finance|     10|
#|Marketing|     20|
#|    Sales|     30|
#|       IT|     40|
#+---------+-------+