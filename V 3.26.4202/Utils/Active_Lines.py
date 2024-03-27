import pyspark
import string
from itertools import chain
from datetime import datetime
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.functions import col, concat, lit, upper, regexp_replace, trim, format_number, expr, when, to_date, split, udf, create_map, length

spark = SparkSession \
    .builder.appName("Trial") \
    .getOrCreate()
spark.conf.set("mapreduce.fileoutputcomitter.marksuccessfuljobs","false")

sqlContext = SQLContext(spark)

### Proceso con todas las funciones desarrolladas
def Function_Complete(path, output_directory, partitions, TypeProccess):

    Data_Frame = First_Changes_DataFrame(path)
    Data_Frame = Phone_Data(Data_Frame)
    Data_Frame = Demographic_Proccess(Data_Frame, output_directory, partitions, TypeProccess)

### Cambios Generales
def First_Changes_DataFrame(Root_Path):
    
    Data_Root = spark.read.csv(Root_Path, header= True,sep=";")
    DF = Data_Root.select([col(c).cast(StringType()).alias(c) for c in Data_Root.columns])

    return DF

### Renombramiento de columnas
def Renamed_Column(Data_Frame):

    Data_Frame = Data_Frame.withColumnRenamed("1_", "identificacion")
    Data_Frame = Data_Frame.withColumnRenamed("2_", "cuenta")

    return Data_Frame

### Proceso de guardado del RDD
def Save_Data_Frame (Data_Frame, Directory_to_Save, partitions, TypeProccess):

    now = datetime.now()
    Time_File = now.strftime("%Y%m%d_%H%M")
    Type_File = f"ACTIVE_LINES_{TypeProccess}_"
    
    output_path = f'{Directory_to_Save}{Type_File}{Time_File}'
    Data_Frame.repartition(partitions).write.mode("overwrite").option("header", "true").csv(output_path)
    print(f"DataFrame guardado en: {output_path}")

    return Data_Frame

### Dinamización de columnas de contacto
def Phone_Data(Data_):

    columns_to_stack_min = ["28_"] #MIN
    columns_to_stack_mobile = ["47_", "48_", "49_", "50_"] #Telefono X
    columns_to_stack_activelines = ["52_", "53_", "54_", "55_", "56_"] #ActiveLines

    all_columns_to_stack = columns_to_stack_mobile + columns_to_stack_activelines + columns_to_stack_min
    columns_to_drop_contact = all_columns_to_stack
    stacked_contact_data_frame = Data_.select("*", *all_columns_to_stack)

    stacked_contact_data_frame = stacked_contact_data_frame.select(
        "*",
        expr(f"stack({len(all_columns_to_stack)}, {', '.join(all_columns_to_stack)}) as dato")
    )

    Data_ = stacked_contact_data_frame.drop(*columns_to_drop_contact)

    return Data_

def Remove_Dots(dataframe, column):

    dataframe = dataframe.withColumn(column, regexp_replace(col(column), "[.-]", ""))
    
    return dataframe

### Proceso de filtrado de líneas
def Demographic_Proccess(Data_, Directory_to_Save, partitions, TypeProccess):

    Data_ = Data_.withColumn("ciudad", lit("BOGOTA"))
    Data_ = Data_.withColumn("depto", lit("BOGOTA"))
    Data_ = Data_.withColumn("tipodato", lit("telefono"))

    Data_ = Data_.select("1_", "2_", "ciudad", "depto", "dato", "tipodato", "22_")
    
    character_list = list(string.ascii_uppercase)
    Punctuation_List = ["\\*"]
    character_list = character_list + Punctuation_List
    
    Data_ = Data_.withColumn("1_", upper(col("1_")))

    for character in character_list:
        Data_ = Data_.withColumn("1_", regexp_replace(col("1_"), character, ""))
        Data_ = Data_.withColumn("2_", regexp_replace(col("2_"), character, ""))
        Data_ = Data_.withColumn("dato", regexp_replace(col("dato"), character, ""))
    
    Data_ = Function_Filter(Data_, TypeProccess)
    Data_ = Data_.withColumn("cruice", concat(col("2_"), col("dato")))
    Data_ = Data_.dropDuplicates(["cruice"])

    Data_ = Remove_Dots(Data_, "1_")
    Data_ = Remove_Dots(Data_, "2_")

    Data_ = Renamed_Column(Data_)
    Save_Data_Frame(Data_, Directory_to_Save, partitions, TypeProccess)
    
    return Data_

def Function_Filter(RDD, TypeProccess):

    if TypeProccess == "valido":

        RDD = RDD.select("1_", "2_", "ciudad", "depto", "dato", "tipodato")

        Data_C = RDD.filter(col("dato") >= 3000000000)
        Data_C = Data_C.filter(col("dato") <= 3599999999)
        Data_F = RDD.filter(col("dato") >= 6010000000)
        Data_F = Data_F.filter(col("dato") <= 6089999999)
    
        RDD = Data_C.union(Data_F)
    
    else:

        RDD = RDD.withColumnRenamed("22_", "LUGAR")

        Data_1 = RDD.filter(col("dato") <= 3000000000)
        Data_2 = RDD.filter(col("dato") >= 3599999999)
        Data_2 = Data_2.filter(col("dato") <= 6010000000)
        Data_3 = RDD.filter(col("dato") >= 6089999999)

        RDD = Data_1.union(Data_2)
        RDD = RDD.union(Data_3)

        RDD = RDD.withColumn("LUGAR", when((col("LUGAR") == "") | (col("LUGAR").isNull()), "BOGOTA")
                                      .otherwise(col("LUGAR")))

        RDD = RDD.withColumn("LUGAR", split(col("LUGAR"), "/").getItem(0))
        RDD = RDD.withColumn("LARGO", length(col("dato")))
        RDD = RDD.withColumn("TELEFONO", col("dato"))
        RDD = RDD.withColumn("dato", lit(""))

        # 601
        list1 = ["BOGOTA"]
        # 602
        list2 = ["CAUCA", "NARIÑO", "VALLE"]
        # 604
        list3 = ["ANTIOQUIA", "CORDOBA", "CHOCO", "MEDELLÍN", "MEDELLIN"]
        # 605
        list4 = ["ATLANTICO", "BOLIVAR", "CESAR", "LA GUAJIRA", "MAGDALENA", "SUCRE"]
        # 606
        list5 = ["CALDAS", "QUINDIO", "RISARALDA"]
        # 607
        list6 = ["ARAUCA", "NORTE DE SANTANDER", "SANTANDER"]
        # 608
        list7 = ["AMAZONAS", "BOYACA", "CASANARE", "CAQUETA", "GUAVIARE", "GUAINIA", "HUILA", "META", "TOLIMA", "PUTUMAYO", \
                 "SAN ANDRES", "VAUPES", "VICHADA"]

        RDD = RDD.withColumn("INDICATIVO",
            when(col("LUGAR").isin(list1), lit("1"))
            .when(col("LUGAR").isin(list2), lit("2"))
            .when(col("LUGAR").isin(list3), lit("4"))
            .when(col("LUGAR").isin(list4), lit("5"))
            .when(col("LUGAR").isin(list5), lit("6"))
            .when(col("LUGAR").isin(list6), lit("7"))
            .when(col("LUGAR").isin(list7), lit("8"))
            .otherwise("000"))

        RDD = RDD.filter(col("INDICATIVO") != "000")

        ### Arreglo de MINS
        RDD = RDD.withColumn("dato",
            when(col("LARGO") == 7, concat(lit("60"), col("INDICATIVO"), col("TELEFONO")))
            .when(col("LARGO") == 8, concat(lit("60"), col("TELEFONO")))
            .otherwise(""))
        
        RDD = RDD.filter(col("LARGO") >= 7)
        RDD = RDD.filter(col("LARGO") <= 8)
        
        RDD = RDD.select("1_", "2_", "ciudad", "depto", "dato", "tipodato", "LUGAR", "INDICATIVO", "TELEFONO")

    return RDD

File_Name = "Linea"
TypeProccess = "valido"
#TypeProccess = "NO_valido"
path = f'C:/Users/c.operativo/Downloads/Dev_/Código/Phyton/Data/{File_Name}.csv'
output_directory = 'C:/Users/c.operativo/Downloads/Dev_/Código/Phyton/Results/'
partitions = 1

Function_Complete(path, output_directory, partitions, TypeProccess)