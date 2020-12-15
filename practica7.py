from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row, SparkSession

def get_most_common_severity():
    spark_session = SparkSession \
        .builder \
        .appName("CarAccidents_Spark_1") \
        .getOrCreate()
    sc = spark_session._sc
    car_accidents_file = "/user/practica7/preprocessed_car_accidents.csv"
    car_accidents = sc.textFile(car_accidents_file)
    severity = car_accidents.map(lambda s: s.split(",")[0])
    count = severity.map(lambda severidad: (severidad, 1)).reduceByKey(add)
    severity_columns = count.map(
        lambda p: Row(severidad=p[0], ocurrencias=int(p[1])))
    sqlContext = SQLContext(sc)
    schemaSeverity = sqlContext.createDataFrame(severity_columns)
    schemaSeverity.registerTempTable("severidades")
    '''
    print("Los diferentes tipos de severidad son:")
    sqlContext.sql(
        "SELECT severidad, ocurrencias FROM severidades order by cuenta 		DESC").show()
    '''
    print("La severidad mas comun es: ")
    sqlContext.sql("SELECT severidad, ocurrencias FROM severidades order by ocurrencias DESC limit 1").show()

    # for result in max_severities:
    # print("Severidad: "+str(result.severity)+" Numero de 	ocurrencias: "+str(result.cuenta.value))
    spark_session.stop()

def get_medium_distance():
    spark_session = SparkSession \
        .builder \
        .appName("CarAccidents_Spark_02") \
        .getOrCreate()
    sc = spark_session.__sc
    car_accidents_file = "/user/practica7/preprocessed_car_accidents.csv"
    car_accidents = sc.textFile(car_accidents_file)
    severity = car_accidents.map(lambda s: s.split(",")[1])
    count = severity.map(lambda severidad: ("media", 1)).reduceByKey(add)
    severity_columns = count.map(
        lambda p: Row(media=p[0], cuenta=int(p[1])))
    sqlContext = SQLContext(sc)
    schemaMedian = sqlContext.createDataFrame(severity_columns)
    schemaMedian.registerTempTable("ocurrencias")
    print("La distancia media en la que ocurren los accidentes es: ")
    sqlContext.sql(
        "SELECT media, cuenta FROM ocurrencias order by cuenta DESC limit 1").show()


def get_most_common_side():
    spark_session = SparkSession \
        .builder \
        .appName("CarAccidents_Spark_3") \
        .getOrCreate()
    sc = spark_session._sc
    car_accidents_file = "/user/practica7/preprocessed_car_accidents.csv"
    car_accidents = sc.textFile(car_accidents_file)
    side = car_accidents.map(lambda s: s.split(",")[0])
    count = side.map(lambda lado: (lado, 1)).reduceByKey(add)
    side_columns = count.map(
        lambda p: Row(lado=p[0], ocurrencias=int(p[1])))
    sqlContext = SQLContext(sc)
    schemaSide = sqlContext.createDataFrame(side_columns)
    schemaSide.registerTempTable("lados")
    '''
    print("Los diferentes tipos de severidad son:")
    sqlContext.sql(
        "SELECT severidad, ocurrencias FROM severidades order by cuenta 		DESC").show()
    '''
    print("El lado de la calle mas comun es: ")
    sqlContext.sql(
        "SELECT lado, ocurrencias FROM lados order by ocurrencias DESC limit 1").show()

    # for result in max_severities:
    # print("Severidad: "+str(result.severity)+" Numero de 	ocurrencias: "+str(result.cuenta.value))
    spark_session.stop()

if __name__  == "__main__":
    if len(sys.argv) != 1 and len(sys.argv) !=2:
        print("Numero de argumentos no valido\n el programa toma 1 o 2 argumentos")
        sys.exit(-1)
    if sys.argv[1]:
        if sys.argv[1] == "1":
            get_most_common_severity()
        elif sys.argv[1] == "2":
            get_medium_distance()
        elif sys.argv[1] == "3":
            pass
        elif sys.argv[1] == "4":
            pass
        elif sys.argv[1] == "5":
            pass
    else:
        print("Error no arguments provided")
