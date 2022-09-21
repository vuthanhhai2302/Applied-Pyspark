import shapefile
from shapely.geometry import Polygon, Point, MultiPolygon
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as f
from pyspark.sql import udf
from utils import spark_session

"""
idea is:
    1. find drop off location.
    2. form the jfk airport zone
    3. check the drop off location is in the jfk airport zone or not
return value: 1 (yes) and 0 (no)
"""

def check_location_jfk(long, lat):
    #create drop off point
    point = Point(long, lat)

    polygon = shapefile.Reader("..//YellowTaxiCaseStudy//input_data//taxi_zones.sph")
    #shape number 132 = jfk airport
    polygon = polygon.shapes()[132]
    #form the airport polygon
    jfk_shape = Polygon(polygon.points)
    #check function
    if jfk_shape.contains(point):
        return 1
    else:
        return 0

def transform_data(spark, pathToTransformFile):
    #read data from file
    df_yellow_tripdata = spark.read.parquet(pathToTransformFile)
    df_yellow_tripdata.createTempView("yellow_tripdata")
    sql = "SELECT " \
          "       *" \
          " FROM yellow_tripdata "
    df_sql = spark.sql(sql)

    #transform the function into spark function
    udf_check_loc_jfk = f.udf(lambda x, y: check_location_jfk(x, y))
    #create condition using spark function
    condition = (udf_check_loc_jfk(df_sql["End_Lon"], df_sql["End_Lat"]) == 1)
    #add a column
    final_df = df_sql.withColumn("Is_to_JFK_airport", f.when(condition, "YES").otherwise("NO"))

    return final_df
