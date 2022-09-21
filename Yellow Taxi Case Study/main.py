from utils import spark_session
from extract import extract
from transform import transform




if __name__ == '__main__':
    #check_location_jfk(-73.968163, 40.80008)
    spark = spark_session.init_spark_session("YellowTaxiCaseStudy's pipline")
    
    #path file section
    readPath = "file:///YellowTaxiCaseStudy//input_data//yellow_tripdata_2009-12.csv"
    writePath = "file:///YellowTaxiCaseStudy/output_data//yellow_tripdata_2009-12.parquet"
    
    #calling function to extract data
    extract.extract_csv_to_parquet(spark, readPath, writePath)
    
    #calling function to transform data
    df_transformed = transform.transform_data(spark, writePath)
    df_transformed.show()

    #check_location_jfk(-73.968163, 40.80008)
