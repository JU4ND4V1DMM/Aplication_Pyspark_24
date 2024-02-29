import pyspark
import os
from datetime import datetime
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.functions import col, concat, lit, upper, regexp_replace, trim, format_number, expr, when

spark = SparkSession.builder.appName("FindNewAccounts").getOrCreate()

sqlContext = SQLContext(spark)

directory_path = 'C:/Users/c.operativo/Downloads/PRUEBA/Código/Phyton/DF/Old/'

first_file_path = os.path.join(directory_path, os.listdir(directory_path)[0])
main_df = spark.read.csv(first_file_path, header=True, sep=";", inferSchema=True)

# Iterar sobre los archivos de la compartida
for file_name in os.listdir(directory_path):
    file_path = os.path.join(directory_path, file_name)
    if file_name.endswith(".csv") and file_path != first_file_path:
        new_df = spark.read.csv(file_path, header=True, inferSchema=True)

        main_df = main_df.union(new_df)
        
File_Name = "reporte_clientes2 (3)"
another_file_path = f'C:/Users/c.operativo/Downloads/PRUEBA/Código/Phyton/DF/New/{File_Name}.csv'
another_df = spark.read.csv(another_file_path, header=True, sep=";", inferSchema=True)

new_accounts_df = another_df.join(main_df.select("cuenta"), "cuenta", "left_anti")

new_accounts_df.show()

output_path = 'C:/Users/c.operativo/Downloads/PRUEBA/Código/Phyton/DF/New_Accounts.csv'
new_accounts_df.write.mode("overwrite").csv(output_path, header=True)
print(f"Cuentas nuevas guardadas en: {output_path}")