from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row, SparkSession
from timeit import default_timer as timer


def get_most_common_severity():
    '''
    Prints to stdout the most common type of severity from the car accidents dataset
    '''
    spark_session = SparkSession \
        .builder \
        .appName("CarAccidents_Spark_1") \
        .getOrCreate()
    sc = spark_session._sc
    car_accidents_file = "/user/practica7/preprocessed_car_accidents.csv"
    car_accidents = sc.textFile(car_accidents_file)
    start = timer()
    severity = car_accidents.map(lambda s: s.split(",")[0])
    count = severity.map(lambda severidad: (severidad, 1)).reduceByKey(
        add).sortBy(
        lambda s: int(s[1])).collect()

    print(
        "El tipo de severidad mas comun es: " + str(
            count[-1][0]) + " con " + str(
            count[-1][1]) + " ocurrencias.")
    end = timer()
    elapsed = end - start
    print("Tiempo total: " + str(elapsed) + " segundos")

    spark_session.stop()


def get_medium_distance():
    '''
    Prints to stdout the medium distance at which the car accidents
    from the car accidents dataset happens
    '''
    start = timer()
    spark_session = SparkSession \
        .builder \
        .appName("CarAccidents_Spark_2") \
        .getOrCreate()
    sc = spark_session._sc
    car_accidents_file = "/user/practica6/preprocessed_car_accidents.csv"
    car_accidents = sc.textFile(car_accidents_file)
    distances = car_accidents.map(lambda s: s.split(",")[1])
    count = distances.map(lambda distance: ("Media", distance))
    #rdd = sc.parallelize(count)
    #suma = rdd.values().sum()
    df = count.toDF()
    df.printSchema()
    #print("Tipo de suma:"+str(type(rdd)))

    #df_basket1.groupby('Item_group').agg({'Media': 'mean'}).show()
    '''
    list = car_accidents.map(lambda s: s.split(",")[1]).collect()
    media = sc.parallelize(list).mean.take(1)
    print("Tipo de distancia media" + str(type(media)))
    print("Suma: "+str(suma))
    numero_de_registros = car_accidents.count()
    print("Numero de registros:" + str(numero_de_registros))
    '''
    end = timer()
    elapsed = end - start
    print("Tiempo total: " + str(elapsed) + " segundos")
    # for result in max_severities:
    # print("Severidad: "+str(result.severity)+" Numero de 	ocurrencias: "+str(result.cuenta.value))
    spark_session.stop()


def get_most_common_side():
    '''
    Prints to stdout the most common side of the street at which the car accidents
    from the car accidents dataset happens
    '''
    spark_session = SparkSession \
        .builder \
        .appName("CarAccidents_Spark_3") \
        .getOrCreate()
    sc = spark_session._sc
    car_accidents_file = "/user/practica7/preprocessed_car_accidents.csv"
    car_accidents = sc.textFile(car_accidents_file)
    start = timer()
    side = car_accidents.map(lambda s: s.split(",")[2])
    count = side.map(lambda lado: (lado, 1)).reduceByKey(add).sortBy(
        lambda s: int(s[1])).collect()
    lado = ''
    if count[-1][0] == "R":
        lado = 'derecho'
    else:
        lado = 'izquierdo'
    print(
        "El lado en el que ocurren mas accidente es: " + lado + " con " + str(
            count[-1][1]) + " ocurrencias.")
    end = timer()
    elapsed = end - start
    print("Tiempo total: " + str(elapsed) + " segundos")

    spark_session.stop()


def get_most_common_weather_condition():
    '''
    Prints to stdout the most common type of weather condition from the car accidents dataset
    '''
    spark_session = SparkSession \
        .builder \
        .appName("CarAccidents_Spark_4") \
        .getOrCreate()
    sc = spark_session._sc
    car_accidents_file = "/user/practica7/preprocessed_car_accidents.csv"
    car_accidents = sc.textFile(car_accidents_file)
    start = timer()
    severity = car_accidents.map(lambda s: s.split(",")[3])
    count = severity.map(lambda w_condition: (w_condition, 1)).reduceByKey(
        add).sortBy(
        lambda s: int(s[1])).collect()

    print(
        "El tipo de condicion meteorologica mas comun es: " + str(
            count[-1][0]) + " con " + str(
            count[-1][1]) + " ocurrencias.")
    end = timer()
    elapsed = end - start
    print("Tiempo total: " + str(elapsed) + " segundos")

    spark_session.stop()


def get_visibility_occurrences_under_threshold(threshold):
    '''
    Prints to stdout the number of accidents that happens under a
    visibility threshold given to the program as an argument
    '''
    spark_session = SparkSession \
        .builder \
        .appName("CarAccidents_Spark_5") \
        .getOrCreate()
    sc = spark_session._sc
    car_accidents_file = "/user/practica7/preprocessed_car_accidents.csv"
    car_accidents = sc.textFile(car_accidents_file)
    start = timer()
    incidents_under_v = car_accidents.map(lambda s: s.split(",")[4]).filter(
        lambda s: float(s) <= float(threshold)).collect()
    print(
        "Numero de ocurrencias bajo el umbral: " + str(len(incidents_under_v)))
    end = timer()
    elapsed = end - start
    print("Tiempo total: " + str(elapsed) + " segundos")
    spark_session.stop()


if __name__ == "__main__":

    if sys.argv[1]:
        if sys.argv[1] == "1":
            get_most_common_severity()
        elif sys.argv[1] == "2":
            get_medium_distance()
        elif sys.argv[1] == "3":
            # DONE
            get_most_common_side()
        elif sys.argv[1] == "4":
            get_most_common_weather_condition()
        elif sys.argv[1] == "5":
            get_visibility_occurrences_under_threshold(sys.argv[2])
    else:
        print("Error no arguments provided")
