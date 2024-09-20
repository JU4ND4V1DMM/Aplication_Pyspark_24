import os
from datetime import datetime
from PyQt6.QtCore import QDate
from PyQt6 import QtWidgets, uic
from pyspark.sql.window import Window
from PyQt6.QtWidgets import QMessageBox
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, DateType
from pyspark.sql.functions import col, lit, upper, regexp_replace, trim, format_number, expr, when, coalesce, current_date
from pyspark.sql.functions import expr, when, row_number, collect_list, to_date
from pyspark.sql.functions import col, concat, lit, upper, regexp_replace, expr, length, size, split, lower, when
from pyspark.sql.functions import regexp_replace, concat_ws, concat, datediff, max
    
def BD_Control_Next(Path, outpath, partitions):

    Path = f"{Path}Bases/"

    spark = SparkSession \
        .builder.appName("BD_MONTH") \
        .getOrCreate()
    spark.conf.set("mapreduce.fileoutputcomitter.marksuccessfuljobs","false")

    sqlContext = SQLContext(spark)

    list_origins = ["ASCARD", "RR", "BSCS", "SGA"]

    now = datetime.now()
    Time_File = now.strftime("%Y%m%d_%H%M")

    files = [os.path.join(Path, file) for file in os.listdir(Path) if file.endswith(".csv")]
    Data_Root = spark.read.option("header", "true").option("sep", ";").csv(files)
    
    Data_Root = Data_Root.select([col(c).cast(StringType()).alias(c) for c in Data_Root.columns])

    Data_Root = Data_Root.filter(col("3_").isin(list_origins))

    potencial = (col("5_") == "Y") & (col("3_") == "BSCS")
    churn = (col("5_") == "Y") & ((col("3_") == "RR") | (col("3_") == "SGA"))
    provision = (col("5_") == "Y") & (col("3_") == "ASCARD")
    prepotencial = (col("6_") == "Y") & (col("3_") == "BSCS")
    prechurn = (col("6_") == "Y") & ((col("3_") == "RR") | (col("3_") == "SGA"))
    preprovision = (col("6_") == "Y") & (col("3_") == "ASCARD")
    castigo = col("7_") == "Y"
    potencial_a_castigar = (col("5_") == "N") & (col("6_") == "N") & (col("7_") == "N") & (col("43_") == "Y")
    marcas = col("13_")

    Data_Root = Data_Root.withColumn("MARCA", when(potencial, "Potencial")\
                                        .when(churn, "Churn")\
                                        .when(provision, "Provision")\
                                        .when(prepotencial, "Prepotencial")\
                                        .when(prechurn, "Prechurn")\
                                        .when(preprovision, "Preprovision")\
                                        .when(castigo, "Castigo")\
                                        .when(potencial_a_castigar, "Potencial a Castigar")\
                                        .otherwise(marcas))
    
    moras_numericas = (col("MARCA") == "120") | (col("MARCA") == "150") | (col("MARCA") == "180")
    prepotencial_especial = (col("MARCA") == "Prepotencial") & (col("3_") == "BSCS") & ((col("12_") == "PrePotencial Convergente Masivo_2") | (col("12_") == "PrePotencial Convergente Pyme_2"))

    Data_Root = Data_Root.withColumn("MARCA", when(moras_numericas, "120 - 180")\
                                        .when(prepotencial_especial, "Prepotencial Especial")\
                                        .otherwise(col("MARCA")))

    Data_Root = Data_Root.withColumn("CUENTA_NEXT", regexp_replace(col("2_"), "[.-]", ""))
    Data_Root = Data_Root.withColumn("CUENTA_NEXT", concat(col("CUENTA_NEXT"), lit("-")))
    Data_Root = Data_Root.withColumn("CUENTA", concat(col("2_"), lit("-")))

    Data_Root = Data_Root.withColumn("VALOR DEUDA", col("9_").cast("double"))

    Data_Root = Data_Root.withColumn("VALOR DEUDA", regexp_replace("VALOR DEUDA", "\\.", ","))

    Data_Root = Data_Root.withColumn("RANGO DEUDA", \
        when((col("9_") <= 20000), lit("1 Menos a 20 mil")) \
            .when((col("9_") <= 50000), lit("2 Entre 20 a 50 mil")) \
            .when((col("9_") <= 100000), lit("3 Entre 50 a 100 mil")) \
            .when((col("9_") <= 150000), lit("4 Entre 100 a 150 mil")) \
            .when((col("9_") <= 200000), lit("5 Entre 150 mil a 200 mil")) \
            .when((col("9_") <= 300000), lit("6 Entre 200 mil a 300 mil")) \
            .when((col("9_") <= 500000), lit("7 Entre 300 mil a 500 mil")) \
            .when((col("9_") <= 1000000), lit("8 Entre 500 mil a 1 Millon")) \
            .when((col("9_") <= 2000000), lit("9 Entre 1 a 2 millones")) \
            .otherwise(lit("9.1 Mayor a 2 millones")))
    
    Segment = ((col("42_") == "81") | (col("42_") == "84") | (col("42_") == "87"))
    Data_Root = Data_Root.withColumn("SEGMENTO",
                        when(Segment, "Personas")
                        .otherwise("Negocios"))

    Data_Root = Data_Root.withColumn(
        "27_", 
        when((col("27_").isNull()) & (col("3_") == "RR"), col("3_"))
        .when(col("27_").isNull(), lit("0"))
        .otherwise(col("27_")))
    
    Data_Root = Data_Root.withColumnRenamed("1_", "DOCUMENTO")
    Data_Root = Data_Root.withColumnRenamed("3_", "CRM")
    Data_Root = Data_Root.withColumnRenamed("26_", "FECHA VENCIMIENTO")
    Data_Root = Data_Root.withColumn("REFERENCIA", col("27_"))
    
    Data_Root = Data_Dates(Data_Root)
    Data_Root, max_value = Data_Dates_Div(Data_Root)
    Data_Root = count_character(Data_Root, max_value)

    Data_Root = Data_Root.withColumn("REFERENCIA",  when(col("CRM") == "RR", col("2_")).otherwise(col("REFERENCIA")))           
    Data_Root = Data_Root.withColumn("FILTRO REFERENCIA",  when(length(col("REFERENCIA")) > 3, lit("Con referencia")).otherwise(lit("Sin Referencia")))

    columns_data = []
    for column in range(1, 32):
         columns_data.append(f"Dia_{column}")

    columns_to_list = ["DOCUMENTO", "CUENTA", "CUENTA_NEXT", "MARCA", "CRM", "RANGO DEUDA", "VALOR DEUDA", "FECHA INGRESO",\
                       "FECHA RETIRO", "FECHA VENCIMIENTO", "DIAS DE MORA", "DIAS ASIGNADA", "DIAS RETIRADA", "REFERENCIA", "FILTRO REFERENCIA",\
                       "Date_min"]
    
    columns_to_list = columns_to_list + columns_data

    Data_Root = Data_Root.select(columns_to_list)

    filter_bd = "Multimarca"
    ##############
    Data_Root_M = Data_Root.filter(col("MARCA") != "Castigo")      #Filter Brand Different CASTIGO
    ##############
    Save_File(Data_Root_M, outpath, partitions, filter_bd, Time_File)

    filter_bd = "Castigo"
    ##############
    #Data_Root_C = Data_Root.filter(col("7_") == "Y")      #Filter Brand same CASTIGO
    ##############
    #Save_File(Data_Root_C, outpath, partitions, filter_bd, Time_File)

    return Data_Root

### Dinamización de columnas de celulares
def Data_Dates(Data_):

    all_columns_to_stack = ["57_"]
    columns_to_drop_contact = all_columns_to_stack
    Stacked_Data_Frame = Data_.select("*", *all_columns_to_stack)
    
    Stacked_Data_Frame = Stacked_Data_Frame.select(
        "*", \
        expr(f"stack({len(all_columns_to_stack)}, {', '.join(all_columns_to_stack)}) as Dato_Fecha")
        )
    
    Data_ = Stacked_Data_Frame.drop(*columns_to_drop_contact)
    Stacked_Data_Frame = Data_.select("*")

    windowSpec = Window.partitionBy("CUENTA").orderBy("Dato_Fecha")
    Stacked_Data_Frame = Stacked_Data_Frame.withColumn("Filtro", row_number().over(windowSpec))

    return Stacked_Data_Frame

def Data_Dates_Div(Data_Frame):

    Data_Frame = Data_Frame.withColumn("Date_Split", split(col("Dato_Fecha"), "/"))
    Data_Frame = Data_Frame.withColumn("Date", (Data_Frame["Date_Split"][0]))
    Data_Frame = Data_Frame.withColumn("Date", col("Date").cast("integer"))
    max_value = Data_Frame.agg(max("Date")).collect()[0][0]

    Data_Frame = Data_Frame.withColumn("FECHA_GENERAL", concat((Data_Frame["Date_Split"][1]), lit("/"), (Data_Frame["Date_Split"][2])))

    for day in range(1, 32): 
        Data_Frame = Data_Frame.withColumn(
            f"fecha_{day}",
            when(col("Filtro") == day, col("Date"))
        )

    consolidated_data = Data_Frame.groupBy("CUENTA").agg(
        *[collect_list(f"fecha_{day}").alias(f"fecha_list_{day}") for day in range(1, 32)]
    )

    pivoted_data = consolidated_data.select(
        "CUENTA", *[concat_ws(",", col(f"fecha_list_{day}")).alias(f"fecha_{day}") for day in range(1, 32)]
    )

    Data_Frame = Data_Frame.select('DOCUMENTO', '2_', 'CRM', '4_', '5_', '6_', '7_', '8_', '9_', '10_', '11_',\
                                    '12_', '13_', '14_', '15_', '16_', '17_', '18_', '19_', '20_', '21_', '22_', '23_', \
                                    '24_', '25_', 'FECHA VENCIMIENTO', '27_', '28_', '29_', '30_', '31_', '32_', '33_', '34_',\
                                    '35_', '36_', '37_', '38_', '39_', '40_', '41_', '42_', '43_', '44_', '45_', '46_', '47_', '48_',\
                                    '49_', '50_', '51_', '52_', '53_', '54_', '55_', '56_', 'MARCA', 'CUENTA_NEXT', 'CUENTA', 'VALOR DEUDA',\
                                    'RANGO DEUDA', 'SEGMENTO', 'REFERENCIA', 'Dato_Fecha', 'Date', "Filtro", "FECHA_GENERAL")
   
    Data_Frame = Data_Frame.join(pivoted_data, "CUENTA", "left")
    Data_Frame = Data_Frame.filter(col("Filtro") == 1)

    for day in range(1, 32): 
        Data_Frame = Data_Frame.withColumn(
            f"{day}",
            when(col("Filtro") == day, col("Date"))
        )

    return Data_Frame, max_value

def count_character(Data_, max_value):
    
    for item in range (1,32):

        Date_Filter = (
            (col("fecha_1") == item) | (col("fecha_2") == item) |
            (col("fecha_3") == item) | (col("fecha_4") == item) |
            (col("fecha_5") == item) | (col("fecha_6") == item) |
            (col("fecha_7") == item) | (col("fecha_8") == item) |
            (col("fecha_9") == item) | (col("fecha_10") == item) |
            (col("fecha_11") == item) | (col("fecha_12") == item) |
            (col("fecha_13") == item) | (col("fecha_14") == item) |
            (col("fecha_15") == item) | (col("fecha_16") == item) |
            (col("fecha_17") == item) | (col("fecha_18") == item) |
            (col("fecha_19") == item) | (col("fecha_20") == item) |
            (col("fecha_21") == item) | (col("fecha_22") == item) |
            (col("fecha_23") == item) | (col("fecha_24") == item) |
            (col("fecha_25") == item) | (col("fecha_26") == item) |
            (col("fecha_27") == item) | (col("fecha_28") == item) |
            (col("fecha_29") == item) | (col("fecha_30") == item) |
            (col("fecha_31") == item)
        )
        
        Data_ = Data_.withColumn(f"Dia_{item}", when(Date_Filter, "Asignada").otherwise(""))
    
    Data_ = Data_.withColumn("DIAS ASIGNADA", sum(when(col(f"Dia_{Day}") == "Asignada", 1).otherwise(0) for Day in range (1,32)))
    Data_ = Data_.withColumn("FECHA INGRESO", lit(""))
                              
    for item in range (1,32):
        
        Data_ = Data_.withColumn("FECHA INGRESO", when((col(f"Dia_{item}") == "Asignada") & (col("FECHA INGRESO") == ""), 
                                                       concat(lit(f"{item}/"), col("FECHA_GENERAL"))).otherwise(col("FECHA INGRESO")))
    
    Data_ = Data_.withColumn("Date_min", split(col("FECHA INGRESO"), "/"))
    Data_ = Data_.withColumn("Date_min", (Data_["Date_min"][0]))
    Data_ = Data_.withColumn("Date_min", col("Date_min").cast("integer"))
    max_value = int(max_value)

    for item in range (max_value, 0, -1):

        Data_ = Data_.withColumn(f"Dia_{item}", when((col(f"Dia_{item}") == "Asignada"), col(f"Dia_{item}"))
                                 .when((col("Date_min") <= item), lit("Retirada"))
                                 .otherwise(lit("")))
    
    Data_ = Data_.withColumn("FECHA RETIRO", lit("-"))

    for item in range (max_value, 0, -1):
        
        Data_ = Data_.withColumn("FECHA RETIRO", when((col(f"Dia_{item}") == "Asignada") & (col("FECHA RETIRO") == "-"), lit(""))
                                    .when((col("FECHA RETIRO") == "-"), concat(lit(f"{item+1}/"), col("FECHA_GENERAL")))
                                    .otherwise(lit("")))

    Data_ = Data_.withColumn("ASIGNACION_FECHA_FINAL", lit("999"))
    for item in range (max_value, 0, -1):
        
        Data_ = Data_.withColumn("ASIGNACION_FECHA_FINAL", 
                                 when((col("ASIGNACION_FECHA_FINAL") == "999") & (item == max_value) & (col(f"Dia_{item}") == "Asignada"), lit("99999"))
                                .when((col(f"Dia_{item}") == "Asignada") & (col("ASIGNACION_FECHA_FINAL") == "999"), item)
                                .otherwise(col("ASIGNACION_FECHA_FINAL")))
        
    for item in range (max_value, 0, -1):

        Data_ = Data_.withColumn(f"Dia_{item}", when((col(f"Dia_{item}") == "Asignada"), col(f"Dia_{item}"))
                                 .when((col("ASIGNACION_FECHA_FINAL") <= item + 1) & (col(f"Dia_{item}") == "Retirada"), lit("Retirada"))
                                 .otherwise(lit("")))

    Data_ = Data_.withColumn("DIAS RETIRADA", sum(when(col(f"Dia_{Day}") == "Retirada", 1).otherwise(0) for Day in range (1,32)))
    Data_ = Data_.withColumn("now", current_date())
    Data_ = Data_.withColumn("DIAS DE MORA", datediff(col("now"), col("FECHA VENCIMIENTO")))

    return Data_

def Save_File(Data_Frame, Directory_to_Save, Partitions, Filter_DataBase, Time_File):

    
    if Filter_DataBase == "Castigo":
        Type_File = f"Resumen_Asignación/BD_Castigo_Resumen_{Time_File}"
        extension = "csv"
        Name_File = f"Castigo {Time_File}"

    else: 
        Type_File = f"Resumen_Asignación/BD_Multimarca_Resumen_{Time_File}"
        extension = "csv"
        Name_File = f"Multimarca {Time_File}"
    
    output_path = f'{Directory_to_Save}{Type_File}'
    Data_Frame.repartition(Partitions).write.mode("overwrite").option("header", "true").option("delimiter",";").csv(output_path)
    
    for root, dirs, files in os.walk(output_path):
        for file in files:
            if file.startswith("._") or file == "_SUCCESS" or file.endswith(".crc"):
                os.remove(os.path.join(root, file))
    
    for i, file in enumerate(os.listdir(output_path), start=1):
        if file.endswith(".csv"):
            old_file_path = os.path.join(output_path, file)
            new_file_path = os.path.join(output_path, f'{Name_File} Part - {i}.{extension}')
            os.rename(old_file_path, new_file_path)

input_folder = "D:/BASES CLARO/2024/09. Septiembre/"
output_folder = "C:/Users/c.operativo/Downloads/"
num_partitions = 1

BD_Control_Next(input_folder, output_folder, num_partitions)