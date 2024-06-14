import os
import pyspark
from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.functions import col, concat, lit, upper, regexp_replace, trim, format_number, expr, when, to_date

spark = SparkSession \
    .builder.appName("Trial") \
    .getOrCreate()
spark.conf.set("mapreduce.fileoutputcomitter.marksuccessfuljobs","false")

sqlContext = SQLContext(spark)


### Proceso con todas las funciones desarrolladas
def Function_Complete(path, output_directory, Partitions, Type_Clients, Benefits_Pash, Contact_Pash, List_Credit, value_min, value_max, widget_filter):

    Data_Frame = First_Changes_DataFrame(path)
    Data_Frame = Phone_Data(Data_Frame)
    Data_Frame = change_name_column(Data_Frame, "Nombre")
    Data_Frame = IVR_Process(Data_Frame, Type_Clients, Benefits_Pash, Contact_Pash,\
                     List_Credit, value_min, value_max, output_directory, Partitions)


### Cambios Generales
def First_Changes_DataFrame(Root_Path):
    
    Data_Root = spark.read.csv(Root_Path, header= True, sep=";")
    DF = Data_Root.select([col(c).cast(StringType()).alias(c) for c in Data_Root.columns])

    DF = first_filters(DF)
    DF = change_name_column(DF, "Nombre")

    return DF

def first_filters (Data_):

    Data_ = Data_.filter(col("PAGO_REAL") == "No ha pagado")
    Data_ = Data_.filter(col("ESTADO_INTERMEDIO_BOT") != "USUARIO NO ES TITULAR")

    list_exclusion = ["Dificultad de Pago", "No Asume Deuda", "Numero Errado", "Reclamacion"]
    
    for exc in  list_exclusion:
        Data_ = Data_.filter(col("ULTIMO_PERFIL_MES") != exc)

    return Data_

def change_name_column (Data_, Column):

    Data_ = Data_.withColumn(Column, upper(col(Column)))

    character_list_N = ["\\ÃƒÂ‘", "\\Ã‚Â¦", "\\Ã‘", "Ñ", "ÃƒÂ‘", "Ã‚Â¦", "Ã‘"]
    
    for character in character_list_N:
        Data_ = Data_.withColumn(Column, regexp_replace(col(Column), character, "NNNNN"))
    
    Data_ = Data_.withColumn(Column, regexp_replace(col(Column), "Ñ", "N"))
    Data_ = Data_.withColumn(Column, regexp_replace(col(Column), "NNNNN", "N"))
    Data_ = Data_.withColumn(Column, regexp_replace(col(Column), "Ã‡", "A"))
    Data_ = Data_.withColumn(Column, regexp_replace(col(Column), "ÃƒÂ", "I"))


    character_list = ["SR/SRA", "SR./SRA.", "SR/SRA.","SR.", "SRA.", "SR(A).","SR ", "SRA ", "SR(A)",\
                    "\\.",'#', '$', '/','<', '>', "\\*", "SEÑORES ","SEÑOR(A) ","SEÑOR ","SEÑORA ", "SENORES ",\
                    "SENOR(A) ","SENOR ","SENORA ", "¡", "!", "\\?" "¿", "_", "-", "}", "\\{", "\\+", "0 ", "1 ", "2 ", "3 ",\
                     "4 ", "5 ", "6 ", "7 ","8 ", "9 ", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "  "]

    for character in character_list:
        Data_ = Data_.withColumn(Column, regexp_replace(col(Column), character, ""))

    
    return Data_

### Renombramiento de columnas
def Renamed_Column(Data_Frame):

    Data_Frame = Data_Frame.withColumnRenamed("fechagestion_contactodirecto", "FECHA_CONTACTO")
    Data_Frame = Data_Frame.withColumnRenamed("nombrecompleto", "NOMBRE")
    Data_Frame = Data_Frame.withColumnRenamed("Credito", "FIRST NAME")
    Data_Frame = Data_Frame.withColumnRenamed("Franja", "LAST NAME")
    Data_Frame = Data_Frame.withColumnRenamed("Dato_Contacto", "PHONE NUMBER")
    Data_Frame = Data_Frame.withColumnRenamed("Identificacion", "VENDOR LEAD CODE")
    Data_Frame = Data_Frame.withColumnRenamed("Marca", "SOURCE ID")
    Data_Frame = Data_Frame.withColumnRenamed("HONORARIO_REAL", "Honorarios")

    Data_Frame = Data_Frame.select("VENDOR LEAD CODE", "SOURCE ID", "PHONE NUMBER", "TITLE", "FIRST NAME", "**", "LAST NAME", \
                         "NOMBRE", "Tipo de Cliente", "Dias_Mora", "Beneficio", "Honorarios")

    return Data_Frame

### Proceso de guardado del RDD
def Save_Data_Frame (Data_Frame, Directory_to_Save, Partitions, Type_Proccess):
    
    now = datetime.now()
    Time_File = now.strftime("%Y%m%d_%H%M")
    Type_File = f"BOT_{Type_Proccess}_"

    output_path = f'{Directory_to_Save}{Type_File}{Time_File}'
    Partitions = int(Partitions)
    Data_Frame.repartition(Partitions).write.mode("overwrite").option("header", "true").option("delimiter",";").csv(output_path)

    for root, dirs, files in os.walk(output_path):
        for file in files:
            if file.startswith("._") or file == "_SUCCESS" or file.endswith(".crc"):
                os.remove(os.path.join(root, file))
    
    for i, file in enumerate(os.listdir(output_path), start=1):
        if file.endswith(".csv"):
            old_file_path = os.path.join(output_path, file)
            new_file_path = os.path.join(output_path, f'IVR Part- {i}.csv')
            os.rename(old_file_path, new_file_path)
        
    return Data_Frame

### Dinamización de columnas de contacto
def Phone_Data(Data_):

    columns_to_stack = ["Telefono1", "Telefono2"]
    
    columns_to_drop = columns_to_stack
    Stacked_Data_Frame = Data_.select("*", *columns_to_stack)
    
    Stacked_Data_Frame = Stacked_Data_Frame.select(
        "*", \
        expr(f"stack({len(columns_to_stack)}, {', '.join(columns_to_stack)}) as Dato_Contacto")
        )
    
    Data_ = Stacked_Data_Frame.drop(*columns_to_drop)
    Stacked_Data_Frame = Data_.select("*")

    return Stacked_Data_Frame

### Proceso de filtrado de líneas
def IVR_Process (Data_, Type_Clients, Benefits_Pash, Contact_Pash, List_Credit, value_min, value_max, output_directory, Partitions):

    List_Credit_UPPER = [element.upper() for element in List_Credit]

    Data_ = Data_.filter(
        (F.col("Credito").isin(List_Credit_UPPER)) |
        (F.col("Marca").isin(List_Credit_UPPER)) |
        (F.col("Franja").isin(List_Credit))
    )

    Data_ = Data_.withColumnRenamed("Telefono1", "phone1")
    Data_ = Data_.withColumnRenamed("Telefono2", "phone2")
    Data_ = Data_.withColumn("phone3", lit(""))

    Data_ = Function_Filter(Data_, Type_Clients, Benefits_Pash, Contact_Pash, value_min, value_max)    
    
    Data_ = Data_.withColumn("Cruce_Cuentas", concat(col("Llave"), lit("-"), col("Dato_Contacto")))

    filter_cash = ["", "Pago Parcial", "Sin Pago"]
    Data_ = Data_.filter((col("tipo_pago").isin(filter_cash)) | (col("tipo_pago").isNull()) | (col("tipo_pago") == ""))

    Data_ = Data_.withColumn("**", lit(""))
    Data_ = Data_.withColumn("TITLE", lit(""))

    Data_ = Data_.select("Identificacion", "Marca", "Dato_Contacto", "TITLE", "Credito", "**", "Franja", \
                         "Nombre", "Tipo de Cliente", "Beneficio", "HONORARIO_REAL", "Dias_Mora")
    
    Data_ = Data_.dropDuplicates(["Dato_Contacto"])

    Data_ = Data_.orderBy(col("Dato_Contacto").desc())

    Data_ = Renamed_Column(Data_)

    Save_Data_Frame(Data_, output_directory, Partitions)
    
    return Data_

def Function_Filter(RDD, Type_Clients, Benefits, Contacts_Min, Value_Min, Value_Max):

    RDD = RDD.withColumn(
        "Tipo de Cliente", 
        when((col("Dias_Mora") >= 366), lit("Brigada"))
        .when((col("Dias_Mora") < 366) & (col("Dias_Mora") > 360), lit("Sin clasificar"))
        .otherwise(lit("Honorarios")))

    if Type_Clients == "Brigada":
        RDD = RDD.filter(col("Tipo de Cliente") == Type_Clients)

    elif Type_Clients == "Honorarios":
        RDD = RDD.filter(col("Tipo de Cliente") != "Brigada")

    else: 
        pass

    RDD = RDD.withColumn("Beneficio",  when(col("Tipo de Cliente") == "Brigada", lit("Con Descuento")).otherwise(lit("Sin Descuento")))           
    
    if Benefits == "Todo":
        pass
    
    else:
        RDD = RDD.filter(col("Beneficio") != Benefits)

    if Contacts_Min == "Celular":
        Data_C = RDD.filter(col("Dato_Contacto") >= 3000000009)
        Data_C = Data_C.filter(col("Dato_Contacto") <= 3599999999)
        RDD = Data_C

    elif Contacts_Min == "Fijo":
        Data_F = RDD.filter(col("Dato_Contacto") >= 6010000009)
        Data_F = Data_F.filter(col("Dato_Contacto") <= 6089999999)
        RDD = Data_F
    
    else:
        Data_C = RDD.filter(col("Dato_Contacto") >= 3000000009)
        Data_C = Data_C.filter(col("Dato_Contacto") <= 3599999999)
        Data_F = RDD.filter(col("Dato_Contacto") >= 6010000009)
        Data_F = Data_F.filter(col("Dato_Contacto") <= 6089999999)
        RDD = Data_C.union(Data_F)
    
    RDD = RDD.filter(col("HONORARIO_REAL") >= Value_Min)
    RDD = RDD.filter(col("HONORARIO_REAL") <= Value_Max)

    return RDD