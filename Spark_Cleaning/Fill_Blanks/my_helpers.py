import findspark
findspark.init("/opt/spark")

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F


class MyHelpers:

    def get_spark_session(self, session_params: dict) -> SparkSession:
        ## Create spark session and return it.
        spark = SparkSession.builder \
            .appName(session_params["appName"]) \
            .master(session_params["master"]) \
            .config("spark.jars", "/opt/spark/jars/postgresql-42.3.5.jar") \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()

        return spark


    def get_data(self, spark_session: SparkSession, read_params: dict) -> DataFrame:
        ## Read the data
        df = spark_session.read \
            .format(read_params["format"]) \
            .option("header", read_params["header"]) \
            .option("sep", read_params["sep"]) \
            .option("inferSchema", read_params["inferSchema"]) \
            .load(read_params["path"])

        return df


    def format_dates(self, date_cols: list, input_df: DataFrame) -> DataFrame:

        
        
        return 

    def make_nulls_to_unknown(self, input_df: DataFrame) -> DataFrame:
        

        return 

    def trim_str_cols(self, input_df) -> DataFrame:
        

        return

    def write_to_postgresql(self, input_df: DataFrame):
        # write to postgresql test1 database cars_cleaned table
        input_df = input_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgresql:5432/mydb") \
            .option("dbtable", "clean3") \
            .option("user", "myuser") \
            .option("password", "mypassword") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        
        pass
