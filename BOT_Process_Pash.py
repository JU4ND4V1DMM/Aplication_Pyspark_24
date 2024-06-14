from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from pyspark.sql.functions import col, concat, lit, upper, regexp_replace, concat_ws
from pyspark.sql.functions import expr, when, row_number, collect_list, length
import os

spark = SparkSession \
    .builder.appName("Trial") \
    .getOrCreate()
spark.conf.set("mapreduce.fileoutputcomitter.marksuccessfuljobs","false")

sqlContext = SQLContext(spark)

### Proceso con todas las funciones desarrolladas
def Function_Complete(path, output_directory, Partitions, Type_Clients, Benefits_Pash, Contact_Pash, List_Credit, value_min, value_max, widget_filter):

    Data_Frame = First_Changes_DataFrame(path)
    Data_Frame = Phone_Data(Data_Frame)

    BOT_list = ["ServiceBots", "WiseBot"] 

    for Type_Proccess in BOT_list:
        BOT_Process(Data_Frame, Type_Clients, Benefits_Pash, Contact_Pash,\
                     List_Credit, value_min, value_max, output_directory, Partitions, Type_Proccess)

### Cambios Generales
def First_Changes_DataFrame(Root_Path):
    
    Data_Root = spark.read.csv(Root_Path, header= True, sep=";")
    DF = Data_Root.select([col(c).cast(StringType()).alias(c) for c in Data_Root.columns])

    DF = first_filters(DF)
    DF = change_name_column(DF, "Nombre")

    return DF

### Limpieza de carácteres especiales en la columna de cuenta
def first_filters (Data_):

    Data_ = Data_.filter(col("PAGO_REAL") == "No ha pagado")
    Data_ = Data_.filter(col("ESTADO_INTERMEDIO_BOT") != "USUARIO NO ES TITULAR")

    list_exclusion = ["Dificultad de Pago", "No Asume Deuda", "Numero Errado", "Reclamacion"]
    
    for exc in  list_exclusion:
        Data_ = Data_.filter(col("ULTIMO_PERFIL_MES") != exc)

    return Data_

### Limpieza de nombres
def change_name_column (Data_, Column):

    character_list = ["SR/SRA", "SR./SRA.", "SR/SRA.","SR.", "SRA.", "SR(A).","SR ", "SRA ", "SR(A)",\
                    "\\.",'#', '$', '/','<', '>', "\\*", "SEÑORES ","SEÑOR(A) ","SEÑOR ","SEÑORA ", "  "]
    
    Data_ = Data_.withColumn(Column, upper(col(Column)))

    for character in character_list:
        Data_ = Data_.withColumn(Column, regexp_replace(col(Column), \
        character, ""))

    
    return Data_

### Renombramiento de columnas
def Renamed_Column(Data_Frame, Type_Proccess):

    Data_Frame = Data_Frame.withColumnRenamed("Pago_Minimo_a_la_fecha", "Pago Minimo")

    if Type_Proccess == "ServiceBots":

        Data_Frame = Data_Frame.withColumnRenamed("HONORARIO_REAL", "Honorarios")
        Data_Frame = Data_Frame.withColumnRenamed("Nombre", "Nombre")
        Data_Frame = Data_Frame.withColumnRenamed("Dias_Mora", "Dias de Mora")
        Data_Frame = Data_Frame.withColumnRenamed("phone1", "Telefono 1")
        Data_Frame = Data_Frame.withColumnRenamed("phone2", "Telefono 2")
        Data_Frame = Data_Frame.withColumnRenamed("phone3", "Telefono 3")

    else:

        Data_Frame = Data_Frame.withColumnRenamed("identificacion", "IDENTIFICACION")
        Data_Frame = Data_Frame.withColumnRenamed("nombrecompleto", "NOMBRE COMPLETO")
        Data_Frame = Data_Frame.withColumnRenamed("Dato_Contacto", "CELULAR")
        Data_Frame = Data_Frame.withColumnRenamed("Mod_init_cta", "MONTO")
        Data_Frame = Data_Frame.withColumnRenamed("fecha_asignacion", "Fecha_Asignacion_")
        Data_Frame = Data_Frame.withColumnRenamed("origen", "Producto_")
        Data_Frame = Data_Frame.withColumnRenamed("referencia", "Referencia_")
        Data_Frame = Data_Frame.withColumnRenamed("descuento", "DCTO_")
        Data_Frame = Data_Frame.withColumnRenamed("fecha_vencimiento", "FLP")
        
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
            new_file_path = os.path.join(output_path, f'BOT Part- {i}.csv')
            os.rename(old_file_path, new_file_path)
        
    return Data_Frame

### Dinamización de columnas de celulares
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

### Desdinamización de líneas
def Phone_Data_Div(Data_Frame):

    for i in range(1, 4):
        Data_Frame = Data_Frame.withColumn(f"phone{i}", when(col("Filtro") == i, col("Dato_Contacto")))

    consolidated_data = Data_Frame.groupBy("Llave").agg(*[collect_list(f"phone{i}").alias(f"phone_list{i}") for i in range(1, 4)])
    pivoted_data = consolidated_data.select("Llave", *[concat_ws(",", col(f"phone_list{i}")).alias(f"phone{i}") for i in range(1, 4)])
    
    Data_Frame = Data_Frame.select("Llave", "Identificacion", "Dias_Mora", "Marca", "Credito", "Franja", "Nombre", \
                                   "Tipo de Cliente", "Beneficio", "HONORARIO_REAL", "Pago_Minimo_a_la_fecha", "Cruce_Cuentas")

    Data_Frame = Data_Frame.join(pivoted_data, "Llave", "left")

    Data_Frame = Data_Frame.withColumn("Filtro", concat(col("Llave"), col("phone1"), col("phone2"), col("phone3")))

    Data_Frame = Data_Frame.dropDuplicates(["Filtro"])
    
    return Data_Frame

def ServiceBot(RDD, Type_Proccess):

    RDD = Phone_Data_Div(RDD)

    Price_Col = "Pago_Minimo_a_la_fecha"     

    RDD = RDD.withColumn(f"{Price_Col}", col(f"{Price_Col}").cast("double").cast("int"))
    
    for col_name, data_type in RDD.dtypes:
        if data_type == "double":
            RDD = RDD.withColumn(col_name, col(col_name).cast(StringType()))

    RDD = RDD.dropDuplicates(["Cruce_Cuentas"])

    ##Estructura para lectura de inteligencia

    RDD = RDD.select("Llave", "Identificacion", "phone1", "phone2", "phone3", "Nombre", \
                     f"{Price_Col}", "Dias_Mora", "Llave", "Marca", "Credito", "Franja", "Tipo de Cliente", "Beneficio", "HONORARIO_REAL")
    
    RDD = RDD.filter(length(col("phone1")) >5)

    RDD = RDD.sort(col("Identificacion"), col("Llave"))

    RDD = RDD.orderBy(col("phone1"))
    RDD = RDD.orderBy(col("phone2"))
    RDD = RDD.orderBy(col("phone3"))

    RDD = Renamed_Column(RDD, Type_Proccess)

    return RDD

def WiseBot(RDD, Type_Proccess):

    Price_Col = "Pago_Minimo_a_la_fecha"     

    RDD = RDD.withColumn(f"{Price_Col}", col(f"{Price_Col}").cast("double").cast("int"))
    
    for col_name, data_type in RDD.dtypes:
        if data_type == "double":
            RDD = RDD.withColumn(col_name, col(col_name).cast(StringType()))

    RDD = RDD.dropDuplicates(["Cruce_Cuentas"])

    RDD = RDD.withColumn("APELLIDO COMPLETO", lit(""))
    RDD = RDD.withColumn("VENCIMIENTO", lit(""))
    RDD = RDD.withColumn("PRODUCTO", lit(""))
    RDD = RDD.withColumn("FIJO", lit(""))
    RDD = RDD.withColumn("TELEFONO3", lit(""))
    RDD = RDD.withColumn("DEMOGRAFICOS1", lit(""))
    RDD = RDD.withColumn("DEMORAFICOS2", lit(""))
    RDD = RDD.withColumnRenamed("marca", "NOMBRE CAMPANA")
    RDD = RDD.withColumn("MARCA", lit(""))

    RDD = RDD.select("NOMBRE CAMPANA", "identificacion", "nombrecompleto", "APELLIDO COMPLETO", "Dato_Contacto", f"{Price_Col}","VENCIMIENTO", \
                     "PRODUCTO", "FIJO", "TELEFONO3", "DEMOGRAFICOS1", "DEMORAFICOS2", "MARCA", "fecha_vencimiento", "origen", "cuenta", "descuento", "cuenta2")
    
    RDD = RDD.sort(col("Identificacion"), col("Llave"))

    RDD = Renamed_Column(RDD, Type_Proccess)

    return RDD

### Proceso de mensajería
def BOT_Process (Data_, Type_Clients, Benefits_Pash, Contact_Pash, List_Credit, value_min, value_max, output_directory, Partitions, Type_Proccess):

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

    windowSpec = Window.partitionBy("Identificacion").orderBy("Llave","Dato_Contacto")
    Data_ = Data_.withColumn("Filtro", row_number().over(windowSpec))    

    if Type_Proccess == "ServiceBots":
        Data_ = ServiceBot(Data_, Type_Proccess)

    else:    
        #Data_ = WiseBot(Data_, Type_Proccess)
        pass

    Save_Data_Frame(Data_, output_directory, Partitions, Type_Proccess)
    
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