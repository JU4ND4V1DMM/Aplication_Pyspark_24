import pyspark
from datetime import datetime
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.functions import col, concat, lit, upper, regexp_replace, trim, format_number, expr, when

spark = SparkSession \
    .builder.appName("Trial") \
    .getOrCreate()
spark.conf.set("mapreduce.fileoutputcomitter.marksuccessfuljobs","false")

sqlContext = SQLContext(spark)


### Proceso con todas las funciones desarrolladas
def Function_Complete(path, output_directory):

    Data_Frame = First_Changes_DataFrame(path)
    Data_Frame = Phone_Data(Data_Frame)
    Data_Frame = IVR_Process(Data_Frame, output_directory)


### Cambios Generales
def First_Changes_DataFrame(Root_Path):
    
    Data_Root = spark.read.csv(path, header= True,sep=";")
    DF = Data_Root.select([col(c).cast(StringType()).alias(c) for c in Data_Root.columns])

    return DF

### Renombramiento de columnas
def Renamed_Column(Data_Frame):

    Data_Frame = Data_Frame.withColumnRenamed("fechagestion_contactodirecto", "FECHA_CONTACTO")
    Data_Frame = Data_Frame.withColumnRenamed("fecha_asignacion", "FECHA_ASIGNACION")
    Data_Frame = Data_Frame.withColumnRenamed("marca", "MARCA")
    Data_Frame = Data_Frame.withColumnRenamed("fecha_vencimiento", "FLP_VENCIMIENTO")
    Data_Frame = Data_Frame.withColumnRenamed("Mod_init_cta", "MONTO_INICIAL")
    Data_Frame = Data_Frame.withColumnRenamed("descuento", "DESCUENTO")
    Data_Frame = Data_Frame.withColumnRenamed("Dato_Contacto", "Telefono 1")
    Data_Frame = Data_Frame.withColumnRenamed("identificacion", "cc")
    Data_Frame = Data_Frame.withColumnRenamed("origen", "CRM Origen")
    Data_Frame = Data_Frame.withColumnRenamed("cuenta", "ccd")

    return Data_Frame

### Proceso de guardado del RDD
def Save_Data_Frame (Data_Frame, Directory_to_Save):

    now = datetime.now()
    Time_File = now.strftime("%Y%m%d_%H%M")
    Type_File = "IVR_"

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

### Proceso de filtrado de líneas
def IVR_Process (Data_, Directory_to_Save):

    Wallet_Brands = ["0", "30", "60", "potencial", "prechurn", "castigo"]
    #Wallet_Brands = ["0", "30"]

    Data_ = Data_.filter(col("marca").isin(Wallet_Brands))

    Data_C = Data_.filter(col("Dato_Contacto") >= 3000000000)
    Data_C = Data_C.filter(col("Dato_Contacto") <= 3599999999)
    Data_F = Data_.filter(col("Dato_Contacto") >= 6010000000)
    Data_F = Data_F.filter(col("Dato_Contacto") <= 6089999999)
    
    Data_ = Data_C.union(Data_F)

    Data_ = Data_.withColumn("Telefono 2", lit(""))
    Data_ = Data_.withColumn("Telefono 3", lit(""))
    Data_ = Data_.withColumn("**", lit(""))
    Data_ = Data_.withColumn("**2", lit(""))

    Data_ = Data_.withColumn("cuenta", col("cuenta").cast("string"))
    Data_ = Data_.withColumn("Mod_init_cta", col("Mod_init_cta").cast("double").cast("int"))
    for col_name, data_type in Data_.dtypes:
        if data_type == "double":
            Data_ = Data_.withColumn(col_name, col(col_name).cast(StringType()))

    Data_ = Data_.select("Dato_Contacto", "Telefono 2", "Telefono 3", "**", "identificacion", "origen", "**2", "cuenta", \
                         "fechagestion_contactodirecto", "fecha_asignacion", "fecha_vencimiento", "marca", "Mod_init_cta", \
                         "descuento")
    
    Data_ = Data_.dropDuplicates(["Dato_Contacto"])
    Data_ = Data_.orderBy(['Mod_init_cta','marca'])
    Data_ = Renamed_Column(Data_)
    Save_Data_Frame(Data_, Directory_to_Save)
    
    return Data_

File_Name = "reporte_clientes2_24_02_2024_0900"
path = f'C:/Users/c.operativo/Downloads/PRUEBA/Código/Phyton/Data/{File_Name}.csv'
output_directory = 'C:/Users/c.operativo/Downloads/PRUEBA/Código/Phyton/Results/'

Function_Complete(path, output_directory)