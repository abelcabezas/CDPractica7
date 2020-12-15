from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row, SparkSession

def get_most_common_severity():

    spark_session= SparkSession\
        .builder\
        .appName("CarAccidents_Spark_01")\
        .getOrCreate()
    sc= spark_session.__sc
    car_accidents_file="/user/practica7/preprocessed_car_accidents.csv"
    car_accidents = sc.textFile(car_accidents_file)
    count = car_accidents.map(lambda severidad: (severidad, 1)).reduceByKey(add)
    severity_columns = count.map(lambda p: Row(severity=p[0],cuenta=int(p[1])))
    sqlContext = SQLContext(sc)
    schemaSeverity =  sqlContext.createDataFrame(severity_columns)
    schemaSeverity.registerTempTable("ocurrencias")
    print("Los diferentes tipos de severidad son: ")
    sqlContext.sql("SELECT severity, cuenta FROM ocurrencias order by cuenta DESC").show()
    print("La severidad mas comun es: ")
    sqlContext.sql("SELECT severity, cuenta FROM ocurrencias order by cuenta DESC limit 1")





if __name__  == "__main__":
    if sys.argv[1]:
        if sys.argv[1] == "1":
            get_most_common_severity()
        elif sys.argv[1] == "2":
            pass
        elif sys.argv[1] == "3":
            pass
        elif sys.argv[1] == "4":
            pass
        elif sys.argv[1] == "5":
            pass
    else:
        print("Error no arguments provided")
