import pyspark
from datetime import datetime
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.functions import col, lit, regexp_replace, lower, upper, when, sum, format_number

spark = SparkSession \
    .builder.appName("Trial") \
    .getOrCreate()
spark.conf.set("mapreduce.fileoutputcomitter.marksuccessfuljobs","false")

sqlContext = SQLContext(spark)

### Proceso con todas las funciones desarrolladas
def Function_Complete(path, output_directory):

    Data_Frame = First_Changes_DataFrame(path)
    Data_Frame = Data_Frame.select("identificacion", "nombrecompleto", "email", "email1", "email2", "marca")
    Save_Data_Frame(Data_Frame, output_directory)

    #filtered_data = Data_Frame.filter(col("marca").isin("0", "30", "60", "prechurn", "potencial", "castigo"))
    #Total_count = filtered_data.count()
    #Total_count = format_number(Total_count, 0)

    total_length = 50


def First_Changes_DataFrame(Root_Path):
    
    Data_Root = spark.read.csv(path, header= True, sep=";")
    DF = Data_Root.select([col(c).cast(StringType()).alias(c) for c in Data_Root.columns])
    DF = change_name_column(DF, "cuenta")
    DF = change_email(DF)
    
    return DF

### Limpieza de columna con nombres
def change_name_column (Data_, Column):

    character_list = ["SR/SRA", "SR./SRA.", "SR.", "SRA.", "SR(A).","SR ", "SRA ", "SR(A)","  ",\
                    "\\.",'#', '$', '/','<', '>', "\\*"]

    Data_ = Data_.withColumn(Column, upper(col(Column)))

    for character in character_list:
        Data_ = Data_.withColumn(Column, regexp_replace(col(Column), character, ""))

    
    return Data_

### Limpieza de correos electrónicos
def change_email (Data_):
    
    columns_to_check = ["email", "email1", "email2"]
    list_email_replace  = ["notiene", "nousa", "nobrinda"]

    for column in columns_to_check:
        for pattern in list_email_replace :
            Data_ = Data_.withColumn(column, when(col(column).contains(pattern), lit(0)).otherwise(col(column)))

    character_list = ["1. ", "2. ", "3. ", "4. ", "5. ", "6. ", "7. ", "8. ", "9. " "  ",\
                    "#", '$', "&", "\"", ";", ",", '/','<', '>', "\\*"]
    
    for character in character_list:
        for column in columns_to_check:
            Data_ = Data_.withColumn(column, regexp_replace(col(column), character, ""))

    for column in columns_to_check:
        Data_ = Data_.withColumn(column, lower(col(column)))

    return Data_

### Proceso de guardado del RDD
def Save_Data_Frame (Data_Frame, Directory_to_Save):

    now = datetime.now()
    Time_File = now.strftime("%Y%m%d_%H%M")
    Type_File = f"Reporte_Claro_{Time_File}"

    output_path = f'{Directory_to_Save}{Type_File}'
    Data_Frame.repartition(1).write.mode("overwrite").option("header", "true").csv(output_path)
    print("\n")
    print(f"DataFrame guardado en: {output_path}\n")
    return Data_Frame

File_Name = "reporte_clientes2"
path = f'C:/Users/c.operativo/Downloads/PRUEBA/Código/Phyton/Data/{File_Name}.csv'
output_directory = 'C:/Users/c.operativo/Downloads/PRUEBA/Código/Phyton/Results/'

Function_Complete(path, output_directory)