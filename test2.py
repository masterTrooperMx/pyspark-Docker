# https://sparkbyexamples.com/pyspark/different-ways-to-create-dataframe-in-pyspark/
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples').getOrCreate()

columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]

rdd = spark.sparkContext.parallelize(data)
# 1 way to create a DataFrame with spark from session
dfFromRDD1 = rdd.toDF()
dfFromRDD1.printSchema()
#root
# |-- _1: string (nullable = true)
# |-- _2: string (nullable = true)
dfFromRDD1 = rdd.toDF(columns)
dfFromRDD1.printSchema()
#root
# |-- language: string (nullable = true)
# |-- users_count: string (nullable = true)
# 2 way to create a DataFrame with spark from rdd
dfFromRDD2 = spark.createDataFrame(rdd).toDF(*columns)
dfFromRDD2.printSchema()
#root
# |-- language: string (nullable = true)
# |-- users_count: string (nullable = true)
# create DataFrame from list collection
dfFromData2 = spark.createDataFrame(data).toDF(*columns)
dfFromData2.printSchema()
#root
# |-- language: string (nullable = true)
# |-- users_count: string (nullable = true)
# create DataFrame with row type
rowData = map(lambda x: Row(*x), data) 
dfFromData3 = spark.createDataFrame(rowData,columns)
# create DataFrame from schema
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
data2 = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
  ])
 
df = spark.createDataFrame(data=data2,schema=schema)
df.printSchema()
#root
# |-- firstname: string (nullable = true)
# |-- middlename: string (nullable = true)
# |-- lastname: string (nullable = true)
# |-- id: string (nullable = true)
# |-- gender: string (nullable = true)
# |-- salary: integer (nullable = true)
df.show(truncate=False)
#+---------+----------+--------+-----+------+------+
#|firstname|middlename|lastname|id   |gender|salary|
#+---------+----------+--------+-----+------+------+
#|James    |          |Smith   |36636|M     |3000  |
#|Michael  |Rose      |        |40288|M     |4000  |
#|Robert   |          |Williams|42114|M     |4000  |
#|Maria    |Anne      |Jones   |39192|F     |4000  |
#|Jen      |Mary      |Brown   |     |F     |-1    |
#+---------+----------+--------+-----+------+------+