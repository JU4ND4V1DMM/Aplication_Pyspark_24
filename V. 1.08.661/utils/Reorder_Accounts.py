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
    Data_Frame = ORDER_Process(Data_Frame, output_directory, Partitions)

### Cambios Generales
def First_Changes_DataFrame(Root_Path):
    
    Data_Root = spark.read.csv(Root_Path, header= True, sep=";")
    DF = Data_Root.select([col(c).cast(StringType()).alias(c) for c in Data_Root.columns])
    DF = change_character_account(DF, "cuenta")
    DF = change_character_account(DF, "cuenta2")

    return DF

### Limpieza de carácteres especiales en la columna de cuenta
def change_character_account (Data_, Column):

    character_list = ["-"]

    for character in character_list:
        Data_ = Data_.withColumn(Column, regexp_replace(col(Column), \
        character, ""))

    return Data_

### Renombramiento de columnas
def Renamed_Column(Data_Frame):

    Data_Frame = Data_Frame.withColumnRenamed("identificacion", "Identificacion")
    Data_Frame = Data_Frame.withColumnRenamed("cta_1", "Cuentas")

    return Data_Frame

### Proceso de guardado del RDD
def Save_Data_Frame (Data_Frame, Directory_to_Save, Partitions):

    now = datetime.now()
    Time_File = now.strftime("%Y%m%d_%H%M")
    Type_File = f"REORDENACION_Cuentas_"

    output_path = f'{Directory_to_Save}{Type_File}{Time_File}'
    Data_Frame.repartition(Partitions).write.mode("overwrite").option("header", "true").csv(output_path)
    print(f"DataFrame guardado en: {output_path}")

    return Data_Frame

### Desdinamización de cuentas
def CTA_Data_Div(Data_Frame):

    for i in range(1, 25):
        Data_Frame = Data_Frame.withColumn(f"cta_{i}_temp", when(col("Filtro") == i, col("cuenta")).otherwise(lit("")))

    Data_Frame = Data_Frame.groupBy("identificacion").agg(*[concat_ws(";", collect_list(col(f"cta_{i}_temp"))).alias(f"cta_{i}") for i in range(1, 25)])
    
    return Data_Frame


### Proceso de reordenación
def ORDER_Process(Data_, Directory_to_Save, Partitions):

    Wallet_Brand = ["30", "potencial", "prechurn", "castigo"]
    Data_ = Data_.filter(col("marca").isin(Wallet_Brand))

    Data_ = Data_.withColumn("Cruce_DOCS", concat(col("cuenta"), lit("-"), col("identificacion")))

    windowSpec = Window.partitionBy("Cruce_DOCS").orderBy("identificacion", "cuenta")
    Data_ = Data_.withColumn("Filtro", row_number().over(windowSpec))
    
    Data_ = Data_.withColumn("cuenta", col("cuenta").cast("string"))
    Data_ = Data_.withColumn("cuenta", concat(col("cuenta"), lit ("_")))

    Data_ = CTA_Data_Div(Data_)

    VALUE = "000000000"

    for col_name, data_type in Data_.dtypes:
        if data_type == "double":
            Data_ = Data_.withColumn(col_name, col(col_name).cast(StringType()))

    Data_ = Data_.select("identificacion", "cta_1")
    Data_ = Renamed_Column(Data_)
    Save_Data_Frame(Data_, Directory_to_Save, Partitions)
    
    return Data_