import pyspark
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
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
def Function_Complete(path, Wallet_Brand, output_directory, Partitions):

    Data_Frame = First_Changes_DataFrame(path)
    Data_Frame = Phone_Data(Data_Frame)
    Data_Frame = BOT_Process(Data_Frame, Wallet_Brand, output_directory, Partitions)

    Wallet_Brand = Pictionary_Brands.get(Wallet_Brand, Wallet_Brand)

    Brand = Wallet_Brand
    total_length = 50

    brand_message = f"MARCA DE REGISTROS:: {Brand}".ljust(total_length)
    Identation = " "*20

    print("\n")
    print(f"//////////////{Identation}{brand_message} ////////////////\n")
    print("\n")

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
                    "\\.",'#', '$', '/','<', '>', "\\*", "SEÑORES ","SEÑOR(A) ","SEÑOR ","  "]
    
    Data_ = Data_.withColumn(Column, upper(col(Column)))

    for character in character_list:
        Data_ = Data_.withColumn(Column, regexp_replace(col(Column), \
        character, ""))

    
    return Data_

### Renombramiento de columnas
def Renamed_Column(Data_Frame):

    Data_ = Data_Frame.withColumn(
        "origen",
        when(col("origen") == "BSCS", "Linea Postpago")
        .when(col("origen") == "ASCARD", "Equipo Financiado")
        .when(col("origen") == "RR", "Hogar")
        .when(col("origen") == "SGA", "Negocios")
        .otherwise(col("origen"))
    )

    Data_Frame = Data_Frame.withColumnRenamed("cuenta", "Cuenta_Next")
    Data_Frame = Data_Frame.withColumnRenamed("identificacion", "Identificacion")
    Data_Frame = Data_Frame.withColumnRenamed("cuenta2", "Cuenta")
    Data_Frame = Data_Frame.withColumnRenamed("fecha_asignacion", "Fecha_Asignacion")
    Data_Frame = Data_Frame.withColumnRenamed("marca", "Edad_Mora")
    Data_Frame = Data_Frame.withColumnRenamed("origen", "CRM")
    Data_Frame = Data_Frame.withColumnRenamed("Mod_init_cta", "Saldo_Asignado")
    Data_Frame = Data_Frame.withColumnRenamed("nombrecompleto", "Nombre_Completo")
    Data_Frame = Data_Frame.withColumnRenamed("referencia", "Referencia")
    Data_Frame = Data_Frame.withColumnRenamed("descuento", "DCTO")
    Data_Frame = Data_Frame.withColumnRenamed("tipo_pago", "TIPO_PAGO")

    return Data_Frame

### Proceso de guardado del RDD
def Save_Data_Frame (Data_Frame, Directory_to_Save, Partitions, Wallet_Brand):

    Brand_Selected = Pictionary_Brands.get(Wallet_Brand, Wallet_Brand)
    now = datetime.now()
    Time_File = now.strftime("%Y%m%d_%H%M")
    Type_File = f"BOT_Marca_{Brand_Selected}_"

    output_path = f'{Directory_to_Save}{Type_File}{Time_File}'
    Data_Frame.repartition(Partitions).write.mode("overwrite").option("header", "true").csv(output_path)
    print(f"DataFrame guardado en: {output_path}")

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

    for i in range(1, 7):
        Data_Frame = Data_Frame.withColumn(f"phone{i}", when(col("Filtro") == i, col("Dato_Contacto")))

    consolidated_data = Data_Frame.groupBy("cuenta").agg(*[collect_list(f"phone{i}").alias(f"phone_list{i}") for i in range(1, 7)])
    pivoted_data = consolidated_data.select("cuenta", *[concat_ws(",", col(f"phone_list{i}")).alias(f"phone{i}") for i in range(1, 7)])
    
    Data_Frame = Data_Frame.select("identificacion","Cruce_Cuentas", "cuenta", "cuenta2", "marca", "origen", "Mod_init_cta", \
                                   "nombrecompleto", "referencia", "descuento", "Filtro")

    Data_Frame = Data_Frame.join(pivoted_data, "cuenta", "left")
    Data_Frame = Data_Frame.filter(col("Filtro") == 1)
    
    return Data_Frame


### Proceso de mensajería
def BOT_Process (Data_, Wallet_Brand, Directory_to_Save, Partitions):

    Brand_Selected = Pictionary_Brands.get(Wallet_Brand, Wallet_Brand)
    
    filter_cash = ["", "Pago Parcial"]
    Data_ = Data_.filter((col("tipo_pago").isin(filter_cash)) | (col("tipo_pago").isNull()) | (col("tipo_pago") == ""))

    Data_ = Data_.filter(col("marca") == Wallet_Brand)

    Data_C = Data_.filter(col("Dato_Contacto") >= 3000000000)
    Data_C = Data_C.filter(col("Dato_Contacto") <= 3599999999)
    Data_F = Data_.filter(col("Dato_Contacto") >= 6010000000)
    Data_F = Data_F.filter(col("Dato_Contacto") <= 6089999999)
    
    Data_ = Data_C.union(Data_F)

    Data_ = Data_.filter(col("Referencia") != "")
    
    Data_ = Data_.withColumn("Cruce_Cuentas", concat(col("cuenta"), lit("-"), col("Dato_Contacto")))
    
    windowSpec = Window.partitionBy("identificacion").orderBy("cuenta","Dato_Contacto")
    Data_ = Data_.withColumn("Filtro", row_number().over(windowSpec))

    Data_ = Phone_Data_Div(Data_)

    Data_ = Data_.withColumn(
        "Mod_init_cta", 
        when((col("descuento") == "0%") | (col("descuento").isNull()) | (col("descuento") == "N/A"), col("Mod_init_cta"))
        .otherwise(col("Mod_init_cta") * (1 - col("descuento") / 100)))
    
    Price_Col = "Mod_init_cta"     

    Data_ = Data_.withColumn("cuenta", col("cuenta").cast("string"))
    Data_ = Data_.withColumn(f"{Price_Col}", col(f"{Price_Col}").cast("double").cast("int"))
    for col_name, data_type in Data_.dtypes:
        if data_type == "double":
            Data_ = Data_.withColumn(col_name, col(col_name).cast(StringType()))

    Data_ = Data_.dropDuplicates(["Cruce_Cuentas"])

    Data_ = Data_.select("identificacion", "cuenta", "cuenta2", "phone1", "phone2", "phone3", "phone4", "phone5", "phone6", \
                         "marca", "origen", f"{Price_Col}", "nombrecompleto", "referencia", "descuento")
    
    Data_ = Data_.orderBy(["identificacion", "cuenta", "phone1", "phone2", "phone3", "phone4", "phone5", "phone6"])
    Data_ = Renamed_Column(Data_)
    Save_Data_Frame(Data_, Directory_to_Save, Partitions, Wallet_Brand)
    
    return Data_