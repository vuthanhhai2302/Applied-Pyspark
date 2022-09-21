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

"""
#if __name__ == '__main__':
    #create spark session
    

    #file paths
    

    #read parquet file

    
    for i in len(df_sql):
        if check_location_jfk(df_sql["End_Lon"][i], df_sql["End_Lat"][i]) == "INSIDE":
            
    
    #extract CSV file to df
    #convert_csv_to_parquet(spark, readPath, writePath)
    df_yellow_tripdata = spark.read.parquet(writePath)
    
    df_yellow_tripdata.createTempView("yellow_tripdata")
    sql = "SELECT " \
          "       count(*) as total" \
          " FROM yellow_tripdata " \
          "WHERE End_Lon = -73.780968 and  End_Lat = 40.641766"
    df_sql = spark.sql(sql)
    df_sql.show()
    jfk_geo = spark.read.format("csv").options(header="true", path=taxi_zones).load()
    jfk_geo.createTempView("jfk_polygon")

    jfk_sql = "SELECT the_geom FROM jfk_polygon WHERE OBJECTID = 132"
    jfk_polygon = spark.sql(jfk_sql)

    jfk_polygon.show()
    shpfilePoints = []
    for shape in jfk_polygon:
        shpfilePoints = shape.points

    polygon = shpfilePoints
    #poly = Polygon(polygon)
    print(polygon)
    #print(p1.within(jfk_shape))
    #spark.close()
    """



