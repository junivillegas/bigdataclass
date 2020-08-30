from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (IntegerType, StructField,
                               StructType, StringType)

spark = SparkSession.builder.appName("Read HomeWork 1").getOrCreate()

## Leer csv estudiante.csv

csv_schema_estudiantes = StructType(
    [
        StructField('carnet', IntegerType()),
        StructField('nombre', StringType()),
        StructField('carrera', StringType()),
    ])

dataframe_curso = spark.read.csv("estudiante.csv",
                           schema=csv_schema_estudiantes,
                           header=False)

dataframe_curso.show()

## Leer csv curso.csv

csv_schema_curso = StructType(
    [
        StructField('codigo', IntegerType()),
        StructField('creditos', IntegerType()),
        StructField('nombre', StringType()),
    ])

dataframe_curso = spark.read.csv("curso.csv",
                           schema=csv_schema_curso,
                           header=False)

dataframe_curso.show()

## Leer csv nota.csv

csv_schema_nota = StructType(
    [
        StructField('estudiante_id', IntegerType()),
        StructField('curso_id', IntegerType()),
        StructField('nota', IntegerType()),
    ])

dataframe_nota = spark.read.csv("nota.csv",
                           schema=csv_schema_nota,
                           header=False)

dataframe_nota.show()