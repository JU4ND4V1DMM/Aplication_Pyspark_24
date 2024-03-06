import pyspark
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.functions import col, concat, lit, upper, regexp_replace, concat_ws
from pyspark.sql.functions import expr, when, row_number, collect_list, sum, length

spark = SparkSession \
    .builder.appName("Trial") \
    .getOrCreate()
spark.conf.set("mapreduce.fileoutputcomitter.marksuccessfuljobs","false")

sqlContext = SQLContext(spark)

### Proceso con todas las funciones desarrolladas
def Function_Complete(path, output_directory, Partitions):

    Data_Frame = First_Changes_DataFrame(path)
    Data_Frame = Phone_Data(Data_Frame)
    Data_Frame = BOT_Process(Data_Frame, output_directory, Partitions)


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
def Save_Data_Frame (Data_Frame, Directory_to_Save, Partitions):

    now = datetime.now()
    Time_File = now.strftime("%Y%m%d_%H%M")
    Type_File = f"REORDENACION_Numeros_Contacto_"

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

    for i in range(1, 14):
        Data_Frame = Data_Frame.withColumn(f"phone{i}", when(col("Filtro") == i, col("Dato_Contacto")))

    consolidated_data = Data_Frame.groupBy("cuenta").agg(*[collect_list(f"phone{i}").alias(f"phone_list{i}") for i in range(1, 14)])
    pivoted_data = consolidated_data.select("cuenta", *[concat_ws(",", col(f"phone_list{i}")).alias(f"phone{i}") for i in range(1, 14)])
    
    Data_Frame = Data_Frame.select("identificacion","Cruce_Cuentas", "cuenta", "cuenta2", "marca", "origen", "Mod_init_cta", \
                                   "nombrecompleto", "referencia", "descuento", "Filtro")

    Data_Frame = Data_Frame.join(pivoted_data, "cuenta", "left")
    Data_Frame = Data_Frame.filter(col("Filtro") == 1)
    
    return Data_Frame


### Proceso de mensajería
def BOT_Process (Data_, Directory_to_Save, Partitions):
    
    filter_cash = ["", "Pago Parcial"]
    Data_ = Data_.filter((col("tipo_pago").isin(filter_cash)) | (col("tipo_pago").isNull()) | (col("tipo_pago") == ""))

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
                         "phone7", "phone8", "phone9", "phone10", "phone11", "phone12", "phone13","marca", "origen", \
                         f"{Price_Col}", "nombrecompleto", "referencia", "descuento")
    
    Data_ = Data_.orderBy(["identificacion", "cuenta", "phone1", "phone2", "phone3", "phone4", "phone5", "phone6",\
                           "phone7", "phone8", "phone9", "phone10", "phone11", "phone12", "phone13"])
    
    Data_ = Renamed_Column(Data_)
    Save_Data_Frame(Data_, Directory_to_Save, Partitions)
    
    return Data_