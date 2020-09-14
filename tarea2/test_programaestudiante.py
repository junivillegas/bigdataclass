import json

from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.types import (IntegerType, StructField, LongType,
                               StructType, StringType , ArrayType)

from .functions_programa import (format_df, get_total_products, get_total_sales, get_sale_producto_cash_register, 
                                calculare_percentile, get_best_selling_product_items, get_best_selling_product_cash)

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

_caja1 = '[{"numero_caja": 8, "compras":[[ \
                {"nombre": "Sandia", "cantidad": 1, "precio": 2500 } \
            ]]}]'

# Extra data  caja 1
_caja1_1 = '[{"numero_caja": 20, "compras":[[]]}]'


_caja1_2 = '[{"numero_caja": 21, "compras":[[ \
                {"nombre": null, "cantidad": 1, "precio": 2500 }, \
                {"nombre": null, "cantidad": 1, "precio": 2500 } \
            ]]}]'

_caja1_3 = '[{"numero_caja": 22, "compras":[[ \
                {"nombre": "Sandia", "cantidad": null, "precio": 2500 } \
            ]]}]'

_caja1_4 = '[{"numero_caja": 23, "compras":[[ \
                {"nombre": "Sandia", "cantidad": 1, "precio": null } \
            ]]}]'
# Fin extra data

_caja2 = '[{"numero_caja": 9, "compras": [[ \
                {"nombre": "Papaya","cantidad": 2,"precio": 1500} \
            ]]}]'

_caja3 = '[{"numero_caja": 10, "compras":[[ \
                {"nombre": "Sandia", "cantidad": 1, "precio": 2500 }, \
                {"nombre": "Papaya","cantidad": 1,"precio": 1500} \
            ]]}]'

# Data extra caja 3 
_caja3_1 = '[{"numero_caja": 24, "compras":[[ \
                {"nombre": "Sandia", "cantidad": 1, "precio": 2500 }, \
                {"nombre": "Sandia","cantidad": 1,"precio": 2500} \
            ]]}]'

_caja3_2 = '[{"numero_caja": null, "compras":[[ \
                {"nombre": "Sandia", "cantidad": 1, "precio": 2500 }, \
                {"nombre": "Sandia","cantidad": 1,"precio": 2500} \
            ]]}]'
# Fin data extra caja 3 

_caja4 = '[{"numero_caja": 11, "compras": [[ \
                {"nombre": "Papaya","cantidad": 2,"precio": 1500} \
            ]]}]'

_caja5 = '[{"numero_caja": 12, "compras": [[ \
                {"nombre": "Sandia", "cantidad": 1, "precio": 2500 } \
            ]]}]'

_caja6 = '[{"numero_caja": 13, "compras": [[ \
                {"nombre": "Sandia", "cantidad": 4, "precio": 2500 }, \
                {"nombre": "Papaya","cantidad": 1,"precio": 1500} \
            ]]}]'

_caja7 = '[{"numero_caja": 14, "compras": [[ \
                {"nombre": "Sandia", "cantidad": 10, "precio": 2500 } \
            ]]}]'

# Pruebas de carga de archivos
# Se asume que las cajas pueden ser repetidas dado que se pueden abrir y cerrar varias veces al dia
# Por lo que generan varios json con el mismo numero de caja 

def test_load_files(spark_session):

    file1 = json.loads(_caja1)
    file2 = json.loads(_caja2)

    file_df1 = spark_session.createDataFrame(file1, _schema)
    file_df2 = spark_session.createDataFrame(file2, _schema)

    df_temp = file_df1.union(file_df2)

    df = format_df(df_temp)

    expected_ds = spark_session.createDataFrame(
        [
            (8, 1, 'Sandia', 2500),
            (9, 2, 'Papaya', 1500),
        ],
        ['numero_caja', 'cantidad', 'producto', 'precio'])
        
    expected_ds.show()
    df.show()

    assert expected_ds.collect() == df.collect()

def test_load_file_1(spark_session):

    file1 = json.loads(_caja1)

    file_df1 = spark_session.createDataFrame(file1, _schema)

    df = format_df(file_df1)

    expected_ds = spark_session.createDataFrame(
        [
            (8, 1, 'Sandia', 2500)
        ],
        ['numero_caja', 'cantidad', 'producto', 'precio'])
        
    expected_ds.show()
    df.show()

    assert expected_ds.collect() == df.collect()

def test_load_file_out_sales(spark_session):

    file1 = json.loads(_caja1_1)

    file_df1 = spark_session.createDataFrame(file1, _schema)

    df = format_df(file_df1)    
    
    expected_ds = spark_session.sparkContext.parallelize([]).toDF(_schema)

    expected_ds.show()
    df.show()

    assert expected_ds.collect() == df.collect()

def test_load_files_1_sale_1_out_sales(spark_session):

    file1 = json.loads(_caja1_1)
    file2 = json.loads(_caja2)

    file_df1 = spark_session.createDataFrame(file1, _schema)
    file_df2 = spark_session.createDataFrame(file2, _schema)

    df_temp = file_df1.union(file_df2)

    df = format_df(df_temp)

    expected_ds = spark_session.createDataFrame(
        [
            (9, 2, 'Papaya', 1500),
        ],
        ['numero_caja', 'cantidad', 'producto', 'precio'])
        
    expected_ds.show()
    df.show()

    assert expected_ds.collect() == df.collect()

# Pruebas total de productos 

def test_total_products(spark_session):
    file1 = json.loads(_caja2)
    file2 = json.loads(_caja3)

    file_df1 = spark_session.createDataFrame(file1, _schema)
    file_df2 = spark_session.createDataFrame(file2, _schema)

    df_temp = file_df1.union(file_df2)

    df = get_total_products(format_df(df_temp))

    expected_ds = spark_session.createDataFrame(
        [
            ('Papaya', 3),
            ('Sandia', 1),
        ],
        ['producto', 'sum(cantidad)'])
        
    expected_ds.show()
    df.show()

    assert expected_ds.collect() == df.collect()

def test_total_products_same_cash_number(spark_session):
    file1 = json.loads(_caja2)
    file2 = json.loads(_caja2)

    file_df1 = spark_session.createDataFrame(file1, _schema)
    file_df2 = spark_session.createDataFrame(file2, _schema)

    df_temp = file_df1.union(file_df2)

    df = get_total_products(format_df(df_temp))

    expected_ds = spark_session.createDataFrame(
        [
            ('Papaya', 4),
        ],
        ['producto', 'sum(cantidad)'])
        
    expected_ds.show()
    df.show()

    assert expected_ds.collect() == df.collect()

def test_total_products_one_file(spark_session):

    file1 = json.loads(_caja3)

    file_df1 = spark_session.createDataFrame(file1, _schema)

    df = get_total_products(format_df(file_df1))

    expected_ds = spark_session.createDataFrame(
        [
            ('Papaya', 1),
            ('Sandia', 1),
        ],
        ['producto', 'sum(cantidad)'])
        
    expected_ds.show()
    df.show()

    assert expected_ds.collect() == df.collect()

def test_total_products_missing_sales(spark_session):
    file1 = json.loads(_caja1)
    file2 = json.loads(_caja1_1)

    file_df1 = spark_session.createDataFrame(file1, _schema)
    file_df2 = spark_session.createDataFrame(file2, _schema)

    df_temp = file_df1.union(file_df2)

    df = get_total_products(format_df(df_temp))

    expected_ds = spark_session.createDataFrame(
        [
            ('Sandia', 1)
        ],
        ['producto', 'sum(cantidad)'])
        
    expected_ds.show()
    df.show()

    assert expected_ds.collect() == df.collect()

def test_total_products_missing_values(spark_session):
    file1 = json.loads(_caja1)
    file2 = json.loads(_caja1_2) # Producto sin nombre 
    file3 = json.loads(_caja1_3) # Sandia null cantidad  - No suma cantidad
    file4 = json.loads(_caja1_4) # Sandia sin precio - suma cantidad 

    file_df1 = spark_session.createDataFrame(file1, _schema)
    file_df2 = spark_session.createDataFrame(file2, _schema)
    file_df3 = spark_session.createDataFrame(file3, _schema)
    file_df4 = spark_session.createDataFrame(file4, _schema)

    union_df = [file_df1, file_df2, file_df3, file_df4]
    
    df_temp = reduce(DataFrame.unionAll, union_df)
    
    df = get_total_products(format_df(df_temp))

    expected_ds = spark_session.createDataFrame(
        [
            (None, 2),
            ('Sandia', 2)
        ],
        ['producto', 'sum(cantidad)'])
        
    expected_ds.show()
    df.show()

    assert expected_ds.collect() == df.collect()

# Test para total de cajas
def test_total_sales(spark_session):
    file1 = json.loads(_caja1)
    file2 = json.loads(_caja2)
    file3 = json.loads(_caja3)

    file_df1 = spark_session.createDataFrame(file1, _schema)
    file_df2 = spark_session.createDataFrame(file2, _schema)
    file_df3 = spark_session.createDataFrame(file3, _schema)

    union_df = [file_df1, file_df2, file_df3]
    
    df_temp = reduce(DataFrame.unionAll, union_df)
    
    df = get_total_sales(format_df(df_temp))

    expected_ds = spark_session.createDataFrame(
        [
            (9, 1500),
            (10, 4000),
            (8, 2500)
        ],
        ['numero_caja', 'sum(precio)'])
        
    expected_ds.show()
    df.show()

    assert expected_ds.collect() == df.collect()

def test_total_sales_one_file(spark_session):
    file1 = json.loads(_caja3)

    file_df1 = spark_session.createDataFrame(file1, _schema)
        
    df = get_total_sales(format_df(file_df1))

    expected_ds = spark_session.createDataFrame(
        [
            (10, 4000)
        ],
        ['numero_caja', 'sum(precio)'])
        
    expected_ds.show()
    df.show()

    assert expected_ds.collect() == df.collect()

def test_total_sales_duplicate_product(spark_session):
    file1 = json.loads(_caja3_1)

    file_df1 = spark_session.createDataFrame(file1, _schema)
        
    df = get_total_sales(format_df(file_df1))

    expected_ds = spark_session.createDataFrame(
        [
            (24, 5000)
        ],
        ['numero_caja', 'sum(precio)'])
        
    expected_ds.show()
    df.show()

    assert expected_ds.collect() == df.collect()

def test_total_sales_missing_values(spark_session):
    file1 = json.loads(_caja1)
    file2 = json.loads(_caja1_2) # Producto sin nombre 
    file3 = json.loads(_caja1_3) # Sandia null cantidad  - No suma cantidad
    file4 = json.loads(_caja1_4) # Sandia sin precio - suma cantidad 

    file_df1 = spark_session.createDataFrame(file1, _schema)
    file_df2 = spark_session.createDataFrame(file2, _schema)
    file_df3 = spark_session.createDataFrame(file3, _schema)
    file_df4 = spark_session.createDataFrame(file4, _schema)

    union_df = [file_df1, file_df2, file_df3, file_df4]
    
    df_temp = reduce(DataFrame.unionAll, union_df)
    
    df = get_total_sales(format_df(df_temp))
        
    expected_ds = spark_session.createDataFrame(
        [
            (22, 2500),
            (8, 2500),
            (21, 5000),
            (23, None),
        ],
        ['numero_caja', 'sum(precio)'])
        
    expected_ds.show()
    df.show()

    assert expected_ds.collect() == df.collect()

def test_total_sales_missing_cash_number(spark_session):
    file1 = json.loads(_caja3_2) # Caja sin numero 
    file2 = json.loads(_caja1)

    file_df1 = spark_session.createDataFrame(file1, _schema)
    file_df2 = spark_session.createDataFrame(file2, _schema)
        
    df_temp = file_df1.union(file_df2)

    df = get_total_sales(format_df(df_temp))

    expected_ds = spark_session.createDataFrame(
        [
            (None, 5000),
            (8, 2500)

        ],
        ['numero_caja', 'sum(precio)'])

    expected_ds.show()
    df.show()

    assert expected_ds.collect() == df.collect()

def test_total_sales_missing_cash_sales(spark_session):
    file1 = json.loads(_caja1_1)

    file_df1 = spark_session.createDataFrame(file1, _schema)
        
    df = get_total_sales(format_df(file_df1))

    expected_ds = spark_session.sparkContext.parallelize([]).toDF(_schema)

    expected_ds.show()
    df.show()

    assert expected_ds.collect() == df.collect()

# Pruebas Métricas - Caja con mas ventas 
def test_cash_with_more_sales(spark_session):
    file1 = json.loads(_caja1)
    file2 = json.loads(_caja2)
    file3 = json.loads(_caja3)

    file_df1 = spark_session.createDataFrame(file1, _schema)
    file_df2 = spark_session.createDataFrame(file2, _schema)
    file_df3 = spark_session.createDataFrame(file3, _schema)

    union_df = [file_df1, file_df2, file_df3]
    
    df_temp = reduce(DataFrame.unionAll, union_df)
    
    df_total_sales = get_total_sales(format_df(df_temp))
    
    df_total_sales.show()
    
    more_sales = get_sale_producto_cash_register(df_total_sales, False)
    # less_sales = get_sale_producto_cash_register(total_sales, True)

    expected = 10

    assert expected == more_sales

def test_cash_with_more_sales_missing_values(spark_session):
    file1 = json.loads(_caja1)
    file2 = json.loads(_caja1_3) # Sandia null cantidad  - No suma cantidad
    file3 = json.loads(_caja1_4) # Sandia sin precio - suma cantidad 
    file4 = json.loads(_caja3_2) # Caja sin numero 

    file_df1 = spark_session.createDataFrame(file1, _schema)
    file_df2 = spark_session.createDataFrame(file2, _schema)
    file_df3 = spark_session.createDataFrame(file3, _schema)
    file_df4 = spark_session.createDataFrame(file4, _schema)

    union_df = [file_df1, file_df2, file_df3, file_df4]
    
    df_temp = reduce(DataFrame.unionAll, union_df)
    
    df_total_sales = get_total_sales(format_df(df_temp))
    
    df_total_sales.show()
    
    more_sales = get_sale_producto_cash_register(df_total_sales, False)
    # less_sales = get_sale_producto_cash_register(total_sales, True)

    expected = None

    assert expected == more_sales

def test_cash_with_more_sales_missing_sales(spark_session):
    file1 = json.loads(_caja1_1)
    file_df1 = spark_session.createDataFrame(file1, _schema)
    
    df_total_sales = get_total_sales(format_df(file_df1))
    df_total_sales.show()
    
    more_sales = get_sale_producto_cash_register(df_total_sales, False)

    expected = None
    
    assert expected == more_sales

# Pruebas Métricas - Caja con menos ventas 
def test_cash_with_less_sales(spark_session):
    file1 = json.loads(_caja1)
    file2 = json.loads(_caja2)
    file3 = json.loads(_caja3)

    file_df1 = spark_session.createDataFrame(file1, _schema)
    file_df2 = spark_session.createDataFrame(file2, _schema)
    file_df3 = spark_session.createDataFrame(file3, _schema)

    union_df = [file_df1, file_df2, file_df3]
    
    df_temp = reduce(DataFrame.unionAll, union_df)
    
    df_total_sales = get_total_sales(format_df(df_temp))
    
    df_total_sales.show()
    
    less_sales = get_sale_producto_cash_register(df_total_sales, True)

    expected = 9

    assert expected == less_sales

def test_cash_with_less_sales_missing_values(spark_session):
    file1 = json.loads(_caja1)
    file2 = json.loads(_caja1_3) # Sandia null cantidad  - No suma cantidad
    file3 = json.loads(_caja1_4) # Sandia sin precio - suma cantidad 
    file4 = json.loads(_caja3_2) # Caja sin numero 

    file_df1 = spark_session.createDataFrame(file1, _schema)
    file_df2 = spark_session.createDataFrame(file2, _schema)
    file_df3 = spark_session.createDataFrame(file3, _schema)
    file_df4 = spark_session.createDataFrame(file4, _schema)

    union_df = [file_df1, file_df2, file_df3, file_df4]
    
    df_temp = reduce(DataFrame.unionAll, union_df)
    
    df_total_sales = get_total_sales(format_df(df_temp))
    
    df_total_sales.show()
    
    less_sales = get_sale_producto_cash_register(df_total_sales, True)

    expected = 23

    assert expected == less_sales

def test_cash_with_less_sales_missing_sales(spark_session):
    file1 = json.loads(_caja1_1)
    file_df1 = spark_session.createDataFrame(file1, _schema)
    
    df_total_sales = get_total_sales(format_df(file_df1))
    df_total_sales.show()
    
    more_sales = get_sale_producto_cash_register(df_total_sales, True)

    expected = None
    
    assert expected == more_sales

# Pruebas Métricas - Percentil 25
def test_percentil_25(spark_session):
    file1 = json.loads(_caja1)
    file2 = json.loads(_caja2)
    file3 = json.loads(_caja3)
    file4 = json.loads(_caja4)
    file5 = json.loads(_caja5)

    file_df1 = spark_session.createDataFrame(file1, _schema)
    file_df2 = spark_session.createDataFrame(file2, _schema)
    file_df3 = spark_session.createDataFrame(file3, _schema)
    file_df4 = spark_session.createDataFrame(file4, _schema)
    file_df5 = spark_session.createDataFrame(file5, _schema)

    union_df = [file_df1, file_df2, file_df3, file_df4, file_df5]
    
    df_temp = reduce(DataFrame.unionAll, union_df)
    
    df_total_sales = get_total_sales(format_df(df_temp))
        
    less_sales = calculare_percentile(df_total_sales, 'sum(precio)', 25)

    df_total_sales.show()

    expected = 1500.0

    assert expected == less_sales

def test_percentil_25_with_empty_cash(spark_session):
    file1 = json.loads(_caja1)
    file2 = json.loads(_caja2)
    file3 = json.loads(_caja3)
    file4 = json.loads(_caja1_1)
    file5 = json.loads(_caja5)

    file_df1 = spark_session.createDataFrame(file1, _schema)
    file_df2 = spark_session.createDataFrame(file2, _schema)
    file_df3 = spark_session.createDataFrame(file3, _schema)
    file_df4 = spark_session.createDataFrame(file4, _schema)
    file_df5 = spark_session.createDataFrame(file5, _schema)

    union_df = [file_df1, file_df2, file_df3, file_df4, file_df5]
    
    df_temp = reduce(DataFrame.unionAll, union_df)
    
    df_total_sales = get_total_sales(format_df(df_temp))
        
    less_sales = calculare_percentile(df_total_sales, 'sum(precio)', 25)

    df_total_sales.show()

    expected = 1500.0

    assert expected == less_sales

# Pruebas Métricas - Percentil 50
def test_percentil_50(spark_session):
    file1 = json.loads(_caja1)
    file2 = json.loads(_caja2)
    file3 = json.loads(_caja3)
    file4 = json.loads(_caja4)
    file5 = json.loads(_caja5)

    file_df1 = spark_session.createDataFrame(file1, _schema)
    file_df2 = spark_session.createDataFrame(file2, _schema)
    file_df3 = spark_session.createDataFrame(file3, _schema)
    file_df4 = spark_session.createDataFrame(file4, _schema)
    file_df5 = spark_session.createDataFrame(file5, _schema)

    union_df = [file_df1, file_df2, file_df3, file_df4, file_df5]
    
    df_temp = reduce(DataFrame.unionAll, union_df)
    
    df_total_sales = get_total_sales(format_df(df_temp))
        
    less_sales = calculare_percentile(df_total_sales, 'sum(precio)', 50)

    df_total_sales.show()

    expected = 2500.0

    assert expected == less_sales

def test_percentil_50_with_empty_cash(spark_session):
    file1 = json.loads(_caja1)
    file2 = json.loads(_caja2)
    file3 = json.loads(_caja3)
    file4 = json.loads(_caja1_1)
    file5 = json.loads(_caja5)

    file_df1 = spark_session.createDataFrame(file1, _schema)
    file_df2 = spark_session.createDataFrame(file2, _schema)
    file_df3 = spark_session.createDataFrame(file3, _schema)
    file_df4 = spark_session.createDataFrame(file4, _schema)
    file_df5 = spark_session.createDataFrame(file5, _schema)

    union_df = [file_df1, file_df2, file_df3, file_df4, file_df5]
    
    df_temp = reduce(DataFrame.unionAll, union_df)
    
    df_total_sales = get_total_sales(format_df(df_temp))
        
    less_sales = calculare_percentile(df_total_sales, 'sum(precio)', 50)

    df_total_sales.show()

    expected = 2500.0

    assert expected == less_sales

# Pruebas Métricas - Percentil 75
def test_percentil_75(spark_session):
    file1 = json.loads(_caja1)
    file2 = json.loads(_caja2)
    file3 = json.loads(_caja3)
    file4 = json.loads(_caja4)
    file5 = json.loads(_caja6)

    file_df1 = spark_session.createDataFrame(file1, _schema)
    file_df2 = spark_session.createDataFrame(file2, _schema)
    file_df3 = spark_session.createDataFrame(file3, _schema)
    file_df4 = spark_session.createDataFrame(file4, _schema)
    file_df5 = spark_session.createDataFrame(file5, _schema)

    union_df = [file_df1, file_df2, file_df3, file_df4, file_df5]
    
    df_temp = reduce(DataFrame.unionAll, union_df)
    
    df_total_sales = get_total_sales(format_df(df_temp))
        
    less_sales = calculare_percentile(df_total_sales, 'sum(precio)', 75)

    df_total_sales.show()

    expected = 4000.0

    assert expected == less_sales

def test_percentil_75_with_empty_cash(spark_session):
    file1 = json.loads(_caja1)
    file2 = json.loads(_caja2)
    file3 = json.loads(_caja3)
    file4 = json.loads(_caja1_1)
    file5 = json.loads(_caja6)

    file_df1 = spark_session.createDataFrame(file1, _schema)
    file_df2 = spark_session.createDataFrame(file2, _schema)
    file_df3 = spark_session.createDataFrame(file3, _schema)
    file_df4 = spark_session.createDataFrame(file4, _schema)
    file_df5 = spark_session.createDataFrame(file5, _schema)

    union_df = [file_df1, file_df2, file_df3, file_df4, file_df5]
    
    df_temp = reduce(DataFrame.unionAll, union_df)
    
    df_total_sales = get_total_sales(format_df(df_temp))
        
    less_sales = calculare_percentile(df_total_sales, 'sum(precio)', 75)

    df_total_sales.show()

    expected = 4000.0

    assert expected == less_sales

# Pruebas Métricas - Producto mas vendido por unidad 
def test_more_selling_product_unit(spark_session):
    file1 = json.loads(_caja1)
    file2 = json.loads(_caja2)
    file3 = json.loads(_caja3)
    file4 = json.loads(_caja4)
    file5 = json.loads(_caja6)

    file_df1 = spark_session.createDataFrame(file1, _schema)
    file_df2 = spark_session.createDataFrame(file2, _schema)
    file_df3 = spark_session.createDataFrame(file3, _schema)
    file_df4 = spark_session.createDataFrame(file4, _schema)
    file_df5 = spark_session.createDataFrame(file5, _schema)

    union_df = [file_df1, file_df2, file_df3, file_df4, file_df5]
    
    df_temp = reduce(DataFrame.unionAll, union_df)
    
    product = get_best_selling_product_items(format_df(df_temp))

    expected = 'Sandia'

    assert expected == product

def test_more_selling_product_unit_missing_values(spark_session):
    file1 = json.loads(_caja1)
    file2 = json.loads(_caja2)
    file3 = json.loads(_caja1_3) # Producto con cantidad en nulo
    file4 = json.loads(_caja1_1) # Caja sin ventas
    file5 = json.loads(_caja6)

    file_df1 = spark_session.createDataFrame(file1, _schema)
    file_df2 = spark_session.createDataFrame(file2, _schema)
    file_df3 = spark_session.createDataFrame(file3, _schema)
    file_df4 = spark_session.createDataFrame(file4, _schema)
    file_df5 = spark_session.createDataFrame(file5, _schema)

    union_df = [file_df1, file_df2, file_df3, file_df4, file_df5]
    
    df_temp = reduce(DataFrame.unionAll, union_df)
    
    product = get_best_selling_product_items(format_df(df_temp))

    expected = 'Sandia'

    assert expected == product

# Pruebas Métricas - Producto mas vendido por monto
def test_more_selling_product_amount(spark_session):
    file1 = json.loads(_caja1)
    file2 = json.loads(_caja2)
    file3 = json.loads(_caja3)
    file4 = json.loads(_caja4)
    file5 = json.loads(_caja6)

    file_df1 = spark_session.createDataFrame(file1, _schema)
    file_df2 = spark_session.createDataFrame(file2, _schema)
    file_df3 = spark_session.createDataFrame(file3, _schema)
    file_df4 = spark_session.createDataFrame(file4, _schema)
    file_df5 = spark_session.createDataFrame(file5, _schema)

    union_df = [file_df1, file_df2, file_df3, file_df4, file_df5]
    
    df_temp = reduce(DataFrame.unionAll, union_df)
    
    product = get_best_selling_product_cash(format_df(df_temp))

    expected = 'Sandia'

    assert expected == product

def test_more_selling_product_amount_missing_values(spark_session):
    file1 = json.loads(_caja1)
    file2 = json.loads(_caja2)
    file3 = json.loads(_caja1_3) # Producto con cantidad en nulo
    file4 = json.loads(_caja1_1) # Caja sin ventas
    file5 = json.loads(_caja6)

    file_df1 = spark_session.createDataFrame(file1, _schema)
    file_df2 = spark_session.createDataFrame(file2, _schema)
    file_df3 = spark_session.createDataFrame(file3, _schema)
    file_df4 = spark_session.createDataFrame(file4, _schema)
    file_df5 = spark_session.createDataFrame(file5, _schema)

    union_df = [file_df1, file_df2, file_df3, file_df4, file_df5]
    
    df_temp = reduce(DataFrame.unionAll, union_df)
    
    product = get_best_selling_product_cash(format_df(df_temp))

    expected = 'Sandia'

    assert expected == product