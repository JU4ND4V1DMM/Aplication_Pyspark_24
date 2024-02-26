import pyspark
from datetime import datetime
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.functions import col, concat, lit, upper, regexp_replace, trim, format_number, expr, when, row_number, first, concat_ws

spark = SparkSession \
    .builder.appName("Trial") \
    .getOrCreate()
spark.conf.set("mapreduce.fileoutputcomitter.marksuccessfuljobs","false")

sqlContext = SQLContext(spark)

Pictionary_Brands = {"0": "CERO", "30": "TREINTA", "60": "SESENTA", "90": "NOVENTA", \
                         "potencial": "POTENCIAL", "prechurn": "PRECHURN", "castigo": "CASTIGO"}

### Proceso con todas las funciones desarrolladas
def Function_Complete(path, Wallet_Brand, output_directory, Type_Proccess):

    Data_Frame = First_Changes_DataFrame(path)
    Data_Frame = Phone_Data(Data_Frame)
    Data_Frame = SMS_Proccess(Data_Frame, Wallet_Brand, output_directory, Type_Proccess)

    Wallet_Brand = Pictionary_Brands.get(Wallet_Brand, Wallet_Brand)

### Cambios Generales
def First_Changes_DataFrame(Root_Path):
    
    Data_Root = spark.read.csv(Root_Path, header= True)
    DF = Data_Root.select([col(c).cast(StringType()).alias(c) for c in Data_Root.columns])
    DF = change_character_account(DF)
    
    return DF

### Limpieza de carácteres especiales en la columna de cuenta
def change_character_account (Data_):

    character_list = ["-", "\\."]

    for character in character_list:
        Data_ = Data_.withColumn("cuenta", regexp_replace(col("cuenta"), \
        character, ""))

    return Data_

### Renombramiento de columnas
def Renamed_Column(Data_Frame):

    Data_Frame = Data_Frame.withColumnRenamed("Dato_Contacto", "Dato_Contacto")
    Data_Frame = Data_Frame.withColumnRenamed("Dato_Contacto", "Dato_Contacto")
    Data_Frame = Data_Frame.withColumnRenamed("Dato_Contacto", "Dato_Contacto")
    Data_Frame = Data_Frame.withColumnRenamed("Dato_Contacto", "Dato_Contacto")
    Data_Frame = Data_Frame.withColumnRenamed("Dato_Contacto", "Dato_Contacto")
    Data_Frame = Data_Frame.withColumnRenamed("Dato_Contacto", "Dato_Contacto")

    return Data_Frame

### Proceso de guardado del RDD
def Save_Data_Frame (Data_Frame, Directory_to_Save, Wallet_Brand):

    Brand_Selected = Pictionary_Brands.get(Wallet_Brand, Wallet_Brand)
    now = datetime.now()
    Time_File = now.strftime("%Y%m%d_%H%M")
    Type_File = f"SMS_Marca_{Brand_Selected}_"

    output_path = f'{Directory_to_Save}{Type_File}{Time_File}'
    Data_Frame.repartition(1).write.mode("overwrite").option("header", "true").csv(output_path)
    print(f"DataFrame guardado en: {output_path}")

    return Data_Frame

### Dinamización de columnas de celulares
def Phone_Data(Data_):

    columns_to_stack = [f"celular{i}" for i in range(1, 11)]
    columns_to_drop = columns_to_stack
    Stacked_Data_Frame = Data_.select("*", *columns_to_stack)
    
    Stacked_Data_Frame = Stacked_Data_Frame.select(
        "*", \
        expr(f"stack({len(columns_to_stack)}, {', '.join(columns_to_stack)}) as Dato_Contacto")
        )
    
    Data_ = Stacked_Data_Frame.drop(*columns_to_drop)
    Stacked_Data_Frame = Data_.select("*")

    return Stacked_Data_Frame

def Lines(Data_):

    # Asignar un número de fila a cada combinación de cuenta y dato_contacto
    window_spec = Window.partitionBy("cuenta").orderBy("Dato_Contacto")
    Data_ = Data_.withColumn("row_num", row_number().over(window_spec))

    # Crear una columna de concatenación para manejar múltiples números por cuenta
    Data_ = Data_.withColumn("concatenated", concat(col("Dato_Contacto"), lit(" ")))

    # Pivotear los datos y seleccionar solo las primeras tres columnas
    Data_ = Data_.groupBy("cuenta", "nombre completo", "origen", "fecha_asignacion",
                          "referencia", "marca").pivot("row_num", [1, 2, 3]).agg(first("concatenated"))

    # Renombrar las columnas según el requerimiento
    Data_ = Data_.withColumnRenamed("1", "phone1").withColumnRenamed("2", "phone2").withColumnRenamed("3", "phone3")

    return Data_

### Proceso de mensajería
def SMS_Proccess (Data_, Wallet_Brand, Directory_to_Save, Type_Proccess):

    Brand_Selected = Pictionary_Brands.get(Wallet_Brand, Wallet_Brand)

    now = datetime.now()
    Time_File = now.strftime("%Y%m%d_%H%M")
    Type_File = f"SMS_Marca_{Brand_Selected}_"
    Data_ = Data_.filter(col("marca") == Wallet_Brand)

    #Data_ = Data_.filter(col("descuento") == "50")                                         # Filtro de descuento
    #Data_ = Data_.filter(col("origen") == "")                                              # Filtro de BSCS, ASCARD, RR, SGA
    #Date_Filter = datetime(2024, 1, 1)
    #Data_ = Data_.filter(col("fecha_asignacion") < Date_Filter)                            # Filtro de fecha máxima / mínima

    Data_C = Data_.filter(col("Dato_Contacto") >= 3000000000)
    Data_C = Data_C.filter(col("Dato_Contacto") <= 3599999999)
    Data_F = Data_.filter(col("Dato_Contacto") >= 6010000000)
    Data_F = Data_F.filter(col("Dato_Contacto") <= 6089999999)
    
    Data_ = Data_C.union(Data_F)


    Data_ = Data_.withColumn("Proceso", lit(f"SMS_{Type_Proccess}"))
    
    Price_Col = "Mod_init_cta"                                                              # Filtro de valor a pagar
    #Price_Col = "descuento"

    Data_ = Data_.withColumn("cuenta", col("cuenta").cast("string"))
    Data_ = Data_.withColumn(f"{Price_Col}", col(f"{Price_Col}").cast("double").cast("int"))
    for col_name, data_type in Data_.dtypes:
        if data_type == "double":
            Data_ = Data_.withColumn(col_name, col(col_name).cast(StringType()))

    Data_ = Data_.withColumn("Form_Moneda", concat(lit("$ "), format_number(col(Price_Col), 0)))
    Data_ = Data_.withColumn("cuenta_Unit", concat(col("cuenta"), lit("-"), col("Dato_Contacto")))

    
    Data_ = Data_.withColumn("Hora", lit(now.strftime("%H:%M")))
    Data_ = Data_.withColumn("Fecha_Hoy", lit(now.strftime("%d/%m/%Y")))

    Data_ = Data_.select("Identificaci�n", "origen","fecha_asignacion", \
                        "cuenta", "nombre completo", "referencia", f"{Price_Col}", \
                        "marca", "Form_Moneda", "Dato_Contacto", "Proceso", "Hora", "Fecha_Hoy", "cuenta_Unit")
    
    Data_ = Data_.dropDuplicates(["Dato_Contacto"])

    #Renamed_Column(Data_)
    Data_ = Lines(Data_)
    Save_Data_Frame(Data_, Directory_to_Save, Wallet_Brand)
    
    return True