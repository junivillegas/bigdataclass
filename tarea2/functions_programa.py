import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from pyspark.sql.types import (IntegerType, StructField, ArrayType,
                               StructType, StringType, LongType)
                               

spark = SparkSession.builder.appName("Read JSON").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

_schema = StructType([
    StructField('numero_caja', LongType(), True),
    StructField('compras', ArrayType(
        ArrayType(StructType([
            StructField('cantidad', IntegerType(), True),
            StructField('nombre', StringType(), True),
            StructField('precio', LongType(), True)
        ]))
    ), True)
])

def load_json_to_df():
    # Lee todos los archivos 
    json_files = sys.argv[1:]

    if json_files:
        return spark.read.option("multiLine", True).json(json_files, _schema)
    print("No hay archivos que leer")
    return None

def format_df(df):
    # explode descompone el objeto 
    if df:
        return (df
                .select("numero_caja", explode("compras").alias("compra"))
                .select("numero_caja", explode("compra").alias("compras"))
                .select("numero_caja", "compras.*")
                .withColumnRenamed("cantidad", "cantidad")
                .withColumnRenamed("nombre", "producto")
                .withColumnRenamed("precio", "precio"))
    print("No hay DF que leer")
    return None

def export_to_csv(df, name, header):
    df.coalesce(1) \
        .write.format("csv") \
        .option("header",header) \
        .option("delimiter",",") \
        .mode('overwrite') \
        .save("exports/" + name) 

# agrupa los productos y suma su total en todas las cajas 
def get_total_products(df):
    return df.groupBy("producto").sum("cantidad")

# Agrega una columna con cantidad * precio 
def get_total_products_sales(df):
    temp = df.withColumn("total_producto",col("cantidad")*col("precio"))
    return temp.groupBy("producto").sum("total_producto")

# sumas las ventas y se agrupan por caja
def get_total_sales(df):
    return df.groupBy("numero_caja").sum("precio")

# Obtiene los percentiles 
def calculare_percentile(df, value, percentile):
    # Formula del percentil, ordenar datos de mayor a menor
    # Sacar indice (percentil/100)*cantidad de registros, resultado redondear al mayor
    # df.orderBy(col('sum(precio)'), ascending=False)
    # math.ceil(0.25 * df.count())
    temp = df.approxQuantile(value, [percentile / 100], 0)
    if temp:
        return df.approxQuantile(value, [percentile / 100], 0)[0]
    else: 
        return None

# Obtiene la caja con mas o menos ventas 
def get_sale_producto_cash_register(df, descendingT):
    temp = df.orderBy(col('sum(precio)'), ascending=descendingT).limit(1).select('numero_caja')
    if temp.collect() and temp.collect()[0] and temp.collect()[0]['numero_caja']:
        return temp.collect()[0]['numero_caja']
    else:
        return None

# Obtiene el producto mas vendido en unidades
def get_best_selling_product_items(df):
    temp = get_total_products(df).orderBy(col('sum(cantidad)'), ascending=False).limit(1).select('producto')
    if temp.collect() and temp.collect()[0] and temp.collect()[0]['producto']:
        return temp.collect()[0]['producto']
    else:
        return None

# Obtiene el producto mas vendido dinero 
def get_best_selling_product_cash(df):
    temp = get_total_products_sales(df).orderBy(col('sum(total_producto)'), ascending=False).limit(1).select('producto')
    if temp.collect() and temp.collect()[0] and temp.collect()[0]['producto']:
        return temp.collect()[0]['producto']
    else: 
        return None

def create_metrics(df):

    schema = StructType([
        StructField('metrica', StringType(), False),
        StructField('valor', StringType(), False)
    ])

    total_sales = get_total_sales(df)

    more_sales = get_sale_producto_cash_register(total_sales, False)
    less_sales = get_sale_producto_cash_register(total_sales, True)
    per25 = calculare_percentile(total_sales, 'sum(precio)', 25)
    per50 = calculare_percentile(total_sales, 'sum(precio)', 50)
    per75 = calculare_percentile(total_sales, 'sum(precio)', 75)
    best_selling_items = get_best_selling_product_items(df)
    best_selling_cash = get_best_selling_product_cash(df)

    data = [('caja_con_mas_ventas', more_sales),
        ('caja_con_menos_ventas', less_sales),
        ('percentil_25_por_caja', per25),
        ('percentil_50_por_caja', per50),
        ('percentil_75_por_caja', per75),
        ('producto_mas_vendido_por_unidad', best_selling_items),
        ('producto_mas_vendido_por_ingreso', best_selling_cash)]

    return spark.createDataFrame(data,schema)