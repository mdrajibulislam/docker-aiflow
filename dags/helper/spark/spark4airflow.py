import sys
from datetime import datetime
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, asc, desc, month, year, lpad

spark = SparkSession \
        .builder \
        .appName("spark demo") \
        .getOrCreate()

def run_spark_code(awsAccessKeyId, awsSecretAccessKey, top_n, s3_input_path, s3_output_path):
    # please use your credential at bellow line
    sc=spark.sparkContext
    sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId",awsAccessKeyId)
    sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", awsSecretAccessKey)

    # customers table
    # chang bucket name
    customers_df = spark.read.csv(s3_input_path, header=True)
    customers_df.createOrReplaceTempView("customers_v2")
    # spark.sql("select * from customers_v2").show(10,False)query_df = """
    sql_query=""" select state, count(distinct customer_id) no_of_distinct_customer
    from customers_v2 group by state order by 2 desc limit {top_n}
    """.format(top_n=top_n)
    print('Query : ===============>>>',sql_query)
    output_df=spark.sql(sql_query)

    output_df.write.mode('overwrite') \
    .json(s3_output_path)

    spark.stop()

#usage: sample command python3 simple.py '2020-06-23'
def main(argv):
   awsAccessKeyId = argv[1]
   awsSecretAccessKey = argv[2]
   top_n = argv[3]
   s3_input_path = argv[4]
   s3_output_path = argv[5]
   run_spark_code(awsAccessKeyId,awsSecretAccessKey,top_n,s3_input_path,s3_output_path)

if __name__ == "__main__":
   main(sys.argv)
