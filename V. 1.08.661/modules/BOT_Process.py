from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, concat, lit, upper, regexp_replace, concat_ws
from pyspark.sql.functions import expr, when, row_number, collect_list, length

spark = SparkSession \
    .builder.appName("Trial") \
    .getOrCreate()
spark.conf.set("mapreduce.fileoutputcomitter.marksuccessfuljobs","false")

sqlContext = SQLContext(spark)

Pictionary_Brands = {"0": "CERO", "30": "TREINTA", "60": "SESENTA", "90": "NOVENTA", \
                         "potencial": "POTENCIAL", "prechurn": "PRECHURN", "castigo": "CASTIGO"}

### Proceso con todas las funciones desarrolladas
def Function_Complete(path, output_directory, Partitions, Wallet_Brand, Origins_Filter, Dates, Benefits, Contacts_Min, Value_Min, Value_Max, widget_filter):

    Data_Frame = First_Changes_DataFrame(path)
    Data_Frame = Phone_Data(Data_Frame)

    BOT_list = ["ServiceBots", "WiseBot"] 

    for Type_Proccess in BOT_list:
        BOT_Process(Data_Frame, Wallet_Brand, Origins_Filter, output_directory, \
                    Partitions, Type_Proccess, Dates, Benefits, Contacts_Min, Value_Min, Value_Max, widget_filter)

### Cambios Generales
def First_Changes_DataFrame(Root_Path):
    
    Data_Root = spark.read.csv(Root_Path, header= True, sep=";")
    DF = Data_Root.select([col(c).cast(StringType()).alias(c) for c in Data_Root.columns])
    DF = change_character_account(DF, "cuenta")
    DF = change_character_account(DF, "cuenta2")
    DF = change_name_column(DF, "nombrecompleto")

    return DF

### Limpieza de carácteres especiales en la columna de cuenta
def change_character_account (Data_, Column):

    character_list = ["-"]

    for character in character_list:
        Data_ = Data_.withColumn(Column, regexp_replace(col(Column), \
        character, ""))

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

    if Type_Proccess == "ServiceBots":

        Data_Frame = Data_Frame.withColumn(
            "origen",
            when(col("origen") == "BSCS", "Linea Movil")
            .when(col("origen") == "ASCARD", "Equipo")
            .when(col("origen") == "RR", "Hogar")
            .when(col("origen") == "SGA", "Negocios")
            .otherwise(col("origen"))
        )

        Data_Frame = Data_Frame.withColumnRenamed("cuenta", "CUENTA")
        Data_Frame = Data_Frame.withColumnRenamed("fecha_asignacion", "Fecha_Asignacion")
        Data_Frame = Data_Frame.withColumnRenamed("marca", "Edad_Mora")
        Data_Frame = Data_Frame.withColumnRenamed("origen", "PRODUCTO")
        Data_Frame = Data_Frame.withColumnRenamed("Mod_init_cta", "saldo")
        Data_Frame = Data_Frame.withColumnRenamed("nombrecompleto", "full_name")
        Data_Frame = Data_Frame.withColumnRenamed("referencia", "Referencia")
        Data_Frame = Data_Frame.withColumnRenamed("descuento", "DCTO")
        Data_Frame = Data_Frame.withColumnRenamed("tipo_pago", "TIPO_PAGO")
        Data_Frame = Data_Frame.withColumnRenamed("fecha_vencimiento", "fecha_pago")
        Data_Frame = Data_Frame.withColumnRenamed("phone1", "phone_number")
        Data_Frame = Data_Frame.withColumnRenamed("phone2", "phone_number_2")
        Data_Frame = Data_Frame.withColumnRenamed("phone3", "phone_number_3")

    else:
        
        Data_Frame = Data_Frame.withColumn(
            "MARCA",
            when(col("origen") == "BSCS", "Linea Movil")
            .when(col("origen") == "ASCARD", "Equipo")
            .when(col("origen") == "RR", "Hogar")
            .when(col("origen") == "SGA", "Negocios")
            .otherwise(col("origen"))
        )

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
def Save_Data_Frame (Data_Frame, Directory_to_Save, Partitions, Wallet_Brand, Type_Proccess, widget_filter):
    
    if widget_filter != "Tables":
        now = datetime.now()
        Time_File = now.strftime("%Y%m%d_%H%M")
        Type_File = f"BOT_{Type_Proccess}_"

        output_path = f'{Directory_to_Save}{Type_File}{Time_File}'
        Partitions = int(Partitions)
        Data_Frame.repartition(Partitions).write.mode("overwrite").option("header", "true").csv(output_path)
    else:
        Data_Frame = Data_Frame
        
    return Data_Frame

### Dinamización de columnas de celulares
def Phone_Data(Data_):

    columns_to_stack_c = [f"celular{i}" for i in range(1, 11)]
    columns_to_stack_f = [f"fijo{i}" for i in range(1, 4)]
    columns_to_stack = columns_to_stack_f + columns_to_stack_c
    
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

    for i in range(1, 10):
        Data_Frame = Data_Frame.withColumn(f"phone{i}", when(col("Filtro") == i, col("Dato_Contacto")))

    consolidated_data = Data_Frame.groupBy("cuenta").agg(*[collect_list(f"phone{i}").alias(f"phone_list{i}") for i in range(1, 10)])
    pivoted_data = consolidated_data.select("cuenta", *[concat_ws(",", col(f"phone_list{i}")).alias(f"phone{i}") for i in range(1, 10)])
    
    Data_Frame = Data_Frame.select("identificacion","Cruce_Cuentas", "cuenta", "cuenta2", "marca", "origen", "Mod_init_cta", \
                                   "nombrecompleto", "referencia", "descuento", "Filtro", "fecha_vencimiento")

    Data_Frame = Data_Frame.join(pivoted_data, "cuenta", "left")
    Data_Frame = Data_Frame.filter(col("Filtro") == 1)
    
    return Data_Frame

def ServiceBot(RDD, Type_Proccess):

    RDD = Phone_Data_Div(RDD)

    RDD = RDD.withColumn(
        "Mod_init_cta", 
        when((col("descuento") == "0%") | (col("descuento").isNull()) | (col("descuento") == "N/A"), col("Mod_init_cta"))
        .otherwise(col("Mod_init_cta") * (1 - col("descuento") / 100)))
    
    Price_Col = "Mod_init_cta"     

    RDD = RDD.withColumn("cuenta", col("cuenta").cast("string"))
    RDD = RDD.withColumn(f"{Price_Col}", col(f"{Price_Col}").cast("double").cast("int"))
    
    for col_name, data_type in RDD.dtypes:
        if data_type == "double":
            RDD = RDD.withColumn(col_name, col(col_name).cast(StringType()))

    RDD = RDD.dropDuplicates(["Cruce_Cuentas"])

    RDD_1 = RDD.select("identificacion", "phone1", "phone2", "phone3", "nombrecompleto", \
                     f"{Price_Col}", "fecha_vencimiento", "cuenta", "origen", "marca", "descuento")
    
    RDD_2 = RDD.select("identificacion", "phone4", "phone5", "phone6", "nombrecompleto", \
                     f"{Price_Col}", "fecha_vencimiento", "cuenta", "origen", "marca", "descuento")

    for i in range(3):
        RDD_2 = RDD_2.withColumnRenamed(f"phone{i+3}", f"phone{i}")
    
    RDD_3 = RDD.select("identificacion", "phone7", "phone8", "phone9", "nombrecompleto", \
                     f"{Price_Col}", "fecha_vencimiento", "cuenta", "origen", "marca", "descuento")

    for i in range(3):
        RDD_3 = RDD_3.withColumnRenamed(f"phone{i+6}", f"phone{i}")

    RDD = RDD_1.union(RDD_2)
    RDD = RDD.union(RDD_3)
    RDD = RDD.filter(length(col("phone1")) >5)

    RDD = RDD.sort(col("identificacion"), col("cuenta"))

    RDD = RDD.orderBy(col("phone1"))
    RDD = RDD.orderBy(col("phone2"))
    RDD = RDD.orderBy(col("phone3"))

    RDD = Renamed_Column(RDD, Type_Proccess)

    return RDD

def WiseBot(RDD, Type_Proccess):

    RDD = RDD.withColumn(
        "Mod_init_cta", 
        when((col("descuento") == "0%") | (col("descuento").isNull()) | (col("descuento") == "N/A"), col("Mod_init_cta"))
        .otherwise(col("Mod_init_cta") * (1 - col("descuento") / 100)))
    
    Price_Col = "Mod_init_cta"     

    RDD = RDD.withColumn("cuenta", col("cuenta").cast("string"))
    
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
                     "PRODUCTO", "FIJO", "TELEFONO3", "DEMOGRAFICOS1", "DEMORAFICOS2", "MARCA", "fecha_vencimiento", "origen", "cuenta", "descuento")
    
    RDD = RDD.sort(col("identificacion"), col("cuenta"))

    RDD = Renamed_Column(RDD, Type_Proccess)

    return RDD

### Proceso de mensajería
def BOT_Process (Data_, Wallet_Brand, Origins_Filter, Directory_to_Save, Partitions, Type_Proccess, Dates, Benefits, Contacts_Min, Value_Min, Value_Max, widget_filter):
    
    filter_cash = ["", "Pago Parcial"]
    Data_ = Data_.filter((col("tipo_pago").isin(filter_cash)) | (col("tipo_pago").isNull()) | (col("tipo_pago") == ""))

    Data_ = Data_.filter(col("marca").isin(Wallet_Brand))
    Data_ = Data_.filter(col("origen").isin(Origins_Filter))

    Data_ = Function_Filter(Data_, Dates, Benefits, Contacts_Min, Value_Min, Value_Max)    
    
    Data_ = Data_.withColumn("Cruce_Cuentas", concat(col("cuenta"), lit("-"), col("Dato_Contacto")))

    windowSpec = Window.partitionBy("identificacion").orderBy("cuenta","Dato_Contacto")
    Data_ = Data_.withColumn("Filtro", row_number().over(windowSpec))    

    if Type_Proccess == "ServiceBots":
        Data_ = ServiceBot(Data_, Type_Proccess)
        
        Data_ = Data_.withColumn("order_filter", concat("identificacion", "phone_number", "phone_number_2", "phone_number_3"))
        Data_ = Data_.withColumn("order_filter", col("order_filter") * 1)
        Data_ = Data_.orderBy(col("order_filter").desc())

    else:    
        Data_ = WiseBot(Data_, Type_Proccess)

    Save_Data_Frame(Data_, Directory_to_Save, Partitions, Wallet_Brand, Type_Proccess, widget_filter)
    
    return Data_

def Function_Filter(RDD, Dates, Benefits, Contacts_Min, Value_Min, Value_Max):

    #RDD = RDD.withColumn("Filter_BSCS", when(((col("origen") == "BSCS") & (col("min") != col("Dato_Contacto"))), lit("BORRAR")) \
                         #.otherwise(lit("VALIDO")))
    
    #RDD = RDD.filter(col("Filter_BSCS") =="VALIDO")

    RDD = RDD.withColumn("Referencia",  when(col("origen") == "RR", col("cuenta")).otherwise(col("Referencia")))           
    RDD = RDD.filter(col("Referencia") != "")

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
    
    RDD = RDD.filter(col("Mod_init_cta") >= Value_Min)
    RDD = RDD.filter(col("Mod_init_cta") <= Value_Max)

    RDD.withColumn(
        "DTO_Filter", 
        when((col("descuento") == "0%") | (col("descuento") == "0") | (col("descuento").isNull()) | (col("descuento") == "N/A"), lit("Without_DTO"))
        .otherwise(lit("With_DTO")))
    
    if Benefits == "Con Descuento":
        RDD = RDD.filter(col("DTO_Filter") == "With_DTO")

    elif Benefits == "Sin Descuento":
        RDD = RDD.filter(col("DTO_Filter") == "Without_DTO")

    else:
        RDD = RDD

    return RDD