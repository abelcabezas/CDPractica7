from __future__ import print_function

import sys
from random import random
from operator import add
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row, SparkSession

if __name__ == "__main__":
    spark_session = SparkSession \
        .builder \
        .appName("CarAccidents") \
        .getOrCreate()
    sc = spark_session._sc
    car_accidents_file = "/user/practica7/preprocessed_car_accidents.csv"
    '''	
    sqlContext = SQLContext(sc)
    accidents_df = sqlContext.read.csv(car_accidents_file)
    accidents_df.show()
    accidents_df.printSchema()
    #Stop spark session
       spark_session.stop()
    '''

    car_accidents = sc.textFile(car_accidents_file)
    # severity_not_nulls = car_accidents.map(lambda s: s.split(",")).filter(lambda s: len(s.split(",")) == 5)
    severity = car_accidents.map(lambda s: s.split(",")[0])
    print("Numero de registros:" + str(severity.count()))
    count = severity.map(lambda severidad: (severidad, 1)).reduceByKey(add)
    severity_columns = count.map(
        lambda p: Row(severity=p[0], cuenta=int(p[1])))
    sqlContext = SQLContext(sc)
    schemaSeverity = sqlContext.createDataFrame(severity_columns)
    schemaSeverity.registerTempTable("ocurrencias")
    print("Los diferentes tipos de severidad son:")
    sqlContext.sql(
        "SELECT severity, cuenta FROM ocurrencias order by cuenta 		DESC").show()
    sqlContext.sql(
        "SELECT severity, cuenta FROM ocurrencias order by cuenta DESC limit 1").show()

    print("La severidad mas comun es: ")
    sqlContext.sql("SELECT severity FROM ocurrencias").show()

    # for result in max_severities:
    # print("Severidad: "+str(result.severity)+" Numero de 	ocurrencias: "+str(result.cuenta.value))
    spark_session.stop()
