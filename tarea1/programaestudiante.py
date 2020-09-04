from pyspark.sql import SparkSession
from pyspark.sql.functions import col, asc
from pyspark.sql.types import (IntegerType, StructField,
                               StructType, StringType)

def load_CSV():
    spark = SparkSession.builder.appName("Read HomeWork 1").getOrCreate()

    ## Leer csv estudiante.csv
    csv_schema_estudiantes = StructType(
        [
            StructField('carnet', IntegerType()),
            StructField('nombre', StringType()),
            StructField('carrera', StringType()),
        ])

    dataframe_estudiantes = spark.read.csv("estudiante.csv",
                            schema=csv_schema_estudiantes,
                            header=False)

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

    ## Leer csv nota.csv
    csv_schema_nota = StructType(
        [
            StructField('estudiante_id', IntegerType()),
            StructField('curso_id', IntegerType()),
            StructField('nota', IntegerType()),
        ])

    dataframe_notas = spark.read.csv("nota.csv",
                            schema=csv_schema_nota,
                            header=False)

    return dataframe_estudiantes, dataframe_curso, dataframe_notas

def join_datasets(estudiante, curso, notas):
    # Left Join entre estudiantes y notas 
    # Falta probar caso en que no existan relaciones entre estudiantes y notas
    joint_df_estudiantes_notas = estudiante.join(notas, estudiante.carnet == notas.estudiante_id, "left")
    # Left Join entre estudiantes/notas y cursos 
    joint_df_estNot_curso = joint_df_estudiantes_notas.join(curso, joint_df_estudiantes_notas.curso_id == curso.codigo)
    return joint_df_estNot_curso

def aggregate_datasets(df):
    ## Crea nueva columna nota x creditos
    temp_df = df.withColumn("NotaCreditos", col("nota") * col("creditos"))
    ## Agruga por carnet y suma creditos y notas x creditos 
    temp_df2 = temp_df.groupBy("carnet").sum("creditos","NotaCreditos")
    ## Crea nueva columna con el ponderado
    temp_df3 = temp_df2.withColumn("ponderado", col("sum(NotaCreditos)") / col("sum(creditos)"))
    return temp_df3

def sort_by_notes(df, quantity):
    return df.orderBy(col('ponderado'), ascending=False).limit(quantity)

# Cargar CSV
dataframe_estudiantes, dataframe_curso, dataframe_notas = load_CSV()

# dataframe_estudiantes.show()
# dataframe_curso.show()
# dataframe_notas.show()

joined_df = join_datasets(dataframe_estudiantes,dataframe_curso,dataframe_notas)
# joined_df.show()

df_aggregate = aggregate_datasets(joined_df)
# df_aggregate.show()

top_10 = sort_by_notes(df_aggregate, 10)
top_10.show()

