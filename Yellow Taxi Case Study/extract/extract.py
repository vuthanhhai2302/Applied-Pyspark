from utils import spark_session

def convert_csv_to_parquet(spark, input_file, output_file):
    #load csv data to dataframe
    df_temp = spark.read.format("csv")\
        .options(delimiter=",",
                 header="true",
                 inferSchema="true",
                 mode="DROPMALFORMED",
                 path=input_file)\
        .load()
    #load dataframe to parquet
    df_temp.write.mode("overwrite").parquet(output_file)
