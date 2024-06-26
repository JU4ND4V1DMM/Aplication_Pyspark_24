import pyspark
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
def Function_Complete(path, output_directory, partitions, filter_brands, filter_origins, Dates, Benefits, Contacts_Min, Value_Min, Value_Max, widget_filter):

    Data_Frame = First_Changes_DataFrame(path)
    Data_Frame = Phone_Data(Data_Frame)
    Data_Frame = IVR_Process(Data_Frame, output_directory, partitions, filter_brands, filter_origins, \
                             Dates, Benefits, Contacts_Min, Value_Min, Value_Max, widget_filter)


### Cambios Generales
def First_Changes_DataFrame(Root_Path):
    
    Data_Root = spark.read.csv(Root_Path, header= True,sep=";")
    DF = Data_Root.select([col(c).cast(StringType()).alias(c) for c in Data_Root.columns])

    return DF

### Renombramiento de columnas
def Renamed_Column(Data_Frame):

    Data_Frame = Data_Frame.withColumnRenamed("fechagestion_contactodirecto", "FECHA_CONTACTO")
    Data_Frame = Data_Frame.withColumnRenamed("fecha_asignacion", "FECHA_ASIGNACION")
    Data_Frame = Data_Frame.withColumnRenamed("marca", "FIRST NAME")
    Data_Frame = Data_Frame.withColumnRenamed("fecha_vencimiento", "LAST NAME")
    Data_Frame = Data_Frame.withColumnRenamed("Mod_init_cta", "MONTO_INICIAL")
    Data_Frame = Data_Frame.withColumnRenamed("descuento", "DESCUENTO")
    Data_Frame = Data_Frame.withColumnRenamed("Dato_Contacto", "PHONE NUMBER")
    Data_Frame = Data_Frame.withColumnRenamed("identificacion", "VENDOR LEAD CODE")
    Data_Frame = Data_Frame.withColumnRenamed("origen", "TITLE")
    Data_Frame = Data_Frame.withColumnRenamed("cuenta", "SOURCE ID")

    Data_Frame = Data_Frame.select("VENDOR LEAD CODE", "SOURCE ID", "PHONE NUMBER", "TITLE", "FIRST NAME", "**", "LAST NAME", \
                         "FECHA_ASIGNACION", "FECHA_CONTACTO", "MONTO_INICIAL", "DESCUENTO")

    return Data_Frame

### Proceso de guardado del RDD
def Save_Data_Frame (Data_Frame, Directory_to_Save, partitions, widget_filter):

    if widget_filter != "Tables":

        now = datetime.now()
        Time_File = now.strftime("%Y%m%d_%H%M")
        Type_File = "IVR_"
        
        output_path = f'{Directory_to_Save}{Type_File}{Time_File}'
        Data_Frame.repartition(partitions).write.mode("overwrite").option("header", "true").csv(output_path)
        print(f"DataFrame guardado en: {output_path}")

    else:
        Data_Frame = Data_Frame
        
    return Data_Frame

### Dinamización de columnas de contacto
def Phone_Data(Data_):

    columns_to_stack_min = ["min"]
    columns_to_stack_celular = [f"celular{i}" for i in range(1, 11)]
    columns_to_stack_fijo = [f"fijo{i}" for i in range(1, 4)]
    all_columns_to_stack = columns_to_stack_celular + columns_to_stack_fijo + columns_to_stack_min
    columns_to_drop_contact = all_columns_to_stack
    stacked_contact_data_frame = Data_.select("*", *all_columns_to_stack)

    stacked_contact_data_frame = stacked_contact_data_frame.select(
        "*",
        expr(f"stack({len(all_columns_to_stack)}, {', '.join(all_columns_to_stack)}) as Dato_Contacto")
    )
    Data_ = stacked_contact_data_frame.drop(*columns_to_drop_contact)

    return Data_

### Proceso de filtrado de líneas
def IVR_Process (Data_, Directory_to_Save, partitions, filter_brands, filter_origins, Dates, Benefits, Contacts_Min, Value_Min, Value_Max, widget_filter):

    filter_cash = ["", "Pago Parcial"]
    Data_ = Data_.filter((col("tipo_pago").isin(filter_cash)) | (col("tipo_pago").isNull()) | (col("tipo_pago") == ""))
    Data_ = Data_.filter(col("marca").isin(filter_brands))
    Data_ = Data_.filter(col("origen").isin(filter_origins))

    Data_ = Function_Filter(Data_, Dates, Benefits, Contacts_Min, Value_Min, Value_Max)
    
    Data_ = Data_.withColumn("Telefono 2", lit(""))
    Data_ = Data_.withColumn("Telefono 3", lit(""))
    Data_ = Data_.withColumn("**2", lit(""))
    Data_ = Data_.withColumn("**", lit(""))

    Data_ = Data_.withColumn("cuenta", col("cuenta").cast("string"))
    
    Data_ = Data_.withColumn(
        "Mod_init_cta", 
        when((col("descuento") == "0%") | (col("descuento").isNull()) | (col("descuento") == "N/A"), col("Mod_init_cta"))
        .otherwise(col("Mod_init_cta") * (1 - col("descuento") / 100)))
    
    Data_ = Data_.withColumn(
        "fechagestion_contactodirecto", 
        when((col("fechagestion_contactodirecto").isNull()) , lit("0"))
        .otherwise(col("fechagestion_contactodirecto")))

    Data_ = Data_.withColumn("Mod_init_cta", col("Mod_init_cta").cast("double").cast("int"))
    for col_name, data_type in Data_.dtypes:
        if data_type == "double":
            Data_ = Data_.withColumn(col_name, col(col_name).cast(StringType()))

    Data_ = Data_.select("Dato_Contacto", "Telefono 2", "Telefono 3", "**", "identificacion", "origen", "**2", "cuenta", \
                         "marca", "fecha_vencimiento", "fecha_asignacion", "fechagestion_contactodirecto", "Mod_init_cta", \
                         "descuento")
    
    Data_ = Data_.dropDuplicates(["Dato_Contacto"])
    Order_Columns = ["Mod_init_cta", "origen","Dato_Contacto",'marca', "fechagestion_contactodirecto"]

    for Column in Order_Columns:
        Data_ = Data_.orderBy(col(Column).desc())

    Data_ = Renamed_Column(Data_)
    Save_Data_Frame(Data_, Directory_to_Save, partitions, widget_filter)
    
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