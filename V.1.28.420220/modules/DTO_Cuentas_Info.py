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
    Data_Frame = DTO_Process(Data_Frame, output_directory)


### Cambios Generales
def First_Changes_DataFrame(Root_Path):
    
    Data_Root = spark.read.csv(path, header= True,sep=" ")
    DF = Data_Root.select([col(c).cast(StringType()).alias(c) for c in Data_Root.columns])

    return DF

### Proceso de guardado del RDD
def Save_Data_Frame (Data_Frame, Directory_to_Save):

    now = datetime.now()
    Time_File = now.strftime("%Y%m%d_%H%M")
    Type_File = "DTO_Base_General"

    output_path = f'{Directory_to_Save}{Type_File}{Time_File}'
    Data_Frame.repartition(1).write.mode("overwrite").option("header", "true").csv(output_path)
    print(f"DataFrame guardado en: {output_path}")

    return Data_Frame

### Proceso de filtrado de líneas
def DTO_Process (Data_, Directory_to_Save):

    #Data_ = Data_.filter(col("descuento") == "30")                                         # Filtro de descuento
    #Data_ = Data_.filter(col("origen") == "")                                              # Filtro de BSCS, ASCARD, RR, SGA
    #Date_Filter = datetime(2024, 1, 1)
    #Data_ = Data_.filter(col("fecha_asignacion") < Date_Filter)                            # Filtro de fecha máxima / mínima
    #Price_Col = "descuento"

    Data_ = Data_.withColumn('CUENTA', col('CUENTA').cast("string"))
    for col_name, data_type in Data_.dtypes:
        if data_type == "double":
            Data_ = Data_.withColumn(col_name, col(col_name).cast(StringType()))

    Data_ = Data_.select('CUENTA', "CRM Origen", "Edad de Deuda", \
                         'POTENCIAL_MARK', 'PREPOTENCIAL_MARK', 'WRITE_MARK', 'TYPE_ID')
    
    #Data_ = Renamed_Column(Data_)
    #Data_ = Lines_Corp(Data_)
    Save_Data_Frame(Data_, Directory_to_Save)
    
    return Data_

File_Name = "CAM - UNIF"
path = f'C:/Users/c.operativo/Downloads/PRUEBA/Código/Phyton/Data/{File_Name}.csv'
output_directory = 'C:/Users/c.operativo/Downloads/PRUEBA/Código/Phyton/Results/'

Function_Complete(path, output_directory)