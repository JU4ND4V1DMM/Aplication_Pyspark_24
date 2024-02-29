import pyspark
from datetime import datetime
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.functions import col, concat, lit, upper, regexp_replace, trim, format_number, expr, when, lower

spark = SparkSession \
    .builder.appName("Trial") \
    .getOrCreate()
spark.conf.set("mapreduce.fileoutputcomitter.marksuccessfuljobs","false")

sqlContext = SQLContext(spark)

Pictionary_Brands = {"0": "CERO", "30": "TREINTA", "60": "SESENTA", "90": "NOVENTA", \
                         "potencial": "POTENCIAL", "prechurn": "PRECHURN", "castigo": "CASTIGO"}

### Proceso con todas las funciones desarrolladas
def Function_Complete(path, Wallet_Brand, output_directory, partitions):

    Data_Frame = First_Changes_DataFrame(path)
    Data_Frame = Email_Data(Data_Frame)
    Data_Frame = EMAIL_Proccess(Data_Frame, Wallet_Brand, output_directory, partitions)

    Wallet_Brand = Pictionary_Brands.get(Wallet_Brand, Wallet_Brand)

    Brand = Wallet_Brand
    total_length = 50

    Data_Frame.printSchema()
    Data_Frame.show(5)
    brand_message = f"MARCA DE REGISTROS:: {Brand}".ljust(total_length)
    Identation = " "*20

    print("\n")
    #print(f"//////////////{Identation}{records_message} ////////////////")
    print(f"//////////////{Identation}{brand_message} ////////////////\n")
    print("\n")

### Cambios Generales
def First_Changes_DataFrame(Root_Path):
    
    Data_Root = spark.read.csv(Root_Path, header= True,sep=";")
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

### Limpieza de correos
def change_email (Data_):
    
    columns_to_check = ["Dato_Contacto"]
    list_email_replace  = ["notiene", "nousa", "nobrinda", "000@00.com.co", "nolorecuerda"]

    for column in columns_to_check:
        for pattern in list_email_replace:
            regex_pattern = f".*{pattern}.*"
            Data_ = Data_.withColumn(column, regexp_replace(col(column), regex_pattern, "0"))
            regex_pattern = f"^{pattern}.*"
            Data_ = Data_.withColumn(column, regexp_replace(col(column), regex_pattern, "0"))

    for column in columns_to_check:
        Data_ = Data_.withColumn(column, lower(col(column)))

    character_list = ["sin segmento"]

    for character in character_list:
        Data_ = Data_.withColumn("Dato_Contacto", regexp_replace(col("Dato_Contacto"), \
        character, "0"))

    return Data_

### Limpieza de nombres
def change_name_column (Data_, Column):

    character_list = ["SR/SRA", "SR./SRA.", "SR/SRA.","SR.", "SRA.", "SR(A).","SR ", "SRA ", "SR(A)","SEÑORES ", "  ",\
                    "\\.",'#', '$', '/','<', '>', "\\*", "  "]
    
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
    Data_Frame = Data_Frame.withColumnRenamed("min", "Min")
    Data_Frame = Data_Frame.withColumnRenamed("referencia", "Referencia")
    Data_Frame = Data_Frame.withColumnRenamed("Fecha_Hoy", "Fecha_Envio")
    Data_Frame = Data_Frame.withColumnRenamed("customer_type_id", "Segmento")

    return Data_Frame

### Proceso de guardado del RDD
def Save_Data_Frame (Data_Frame, Directory_to_Save, partitions, Wallet_Brand):

    Brand_Selected = Pictionary_Brands.get(Wallet_Brand, Wallet_Brand)
    now = datetime.now()
    Time_File = now.strftime("%Y%m%d_%H%M")
    Type_File = f"EMAIL_Marca_{Brand_Selected}_"

    output_path = f'{Directory_to_Save}{Type_File}{Time_File}'
    Data_Frame.repartition(1).write.mode("overwrite").option("header", "true").csv(output_path)
    print(f"DataFrame guardado en: {output_path}")

    return Data_Frame

### Dinamización de columnas de celulares
def Email_Data(Data_):

    columns_to_stack = [f"email{i}" for i in range(1, 2)]
    columns_to_drop = columns_to_stack
    Stacked_Data_Frame = Data_.select("*", *columns_to_stack)
    
    Stacked_Data_Frame = Stacked_Data_Frame.select(
        "*", \
        expr(f"stack({len(columns_to_stack)}, {', '.join(columns_to_stack)}) as Dato_Contacto")
        )
    
    Data_ = Stacked_Data_Frame.drop(*columns_to_drop)
    Stacked_Data_Frame = Data_.select("*")



    return Stacked_Data_Frame

### Proceso de mensajería
def EMAIL_Proccess (Data_, Wallet_Brand, Directory_to_Save, partitions):

    Brand_Selected = Pictionary_Brands.get(Wallet_Brand, Wallet_Brand)

    now = datetime.now()
    Time_File = now.strftime("%Y%m%d_%H%M")
    Type_File = f"EMAIL_Marca_{Brand_Selected}_"
    Data_ = Data_.filter(col("marca") == Wallet_Brand)
    
    filter_cash = ["", "Pago Parcial"]
    Data_ = Data_.filter((col("tipo_pago").isin(filter_cash)) | (col("tipo_pago").isNull()) | (col("tipo_pago") == ""))
    Data_ = Data_.filter(~col("Dato_Contacto").isNull() & (col("Dato_Contacto") != "0") & (col("Dato_Contacto") != ""))
    Data_ = Data_.withColumn("Cruce_Cuentas", concat(col("cuenta"), lit("-"), col("Dato_Contacto")))

    Data_ = Data_.withColumn("Canal", lit(f"EMAIL"))
    
    Price_Col = "Mod_init_cta"                                                              

    Data_ = Data_.withColumn("cuenta", col("cuenta").cast("string"))
    Data_ = Data_.withColumn(f"{Price_Col}", col(f"{Price_Col}").cast("double").cast("int"))
    for col_name, data_type in Data_.dtypes:
        if data_type == "double":
            Data_ = Data_.withColumn(col_name, col(col_name).cast(StringType()))

    Data_ = Data_.withColumn("Form_Moneda", concat(lit("$ "), format_number(col(Price_Col), 0)))
    
    #Modificar Mod_init_cta o descuento
    Data_ = Data_.withColumn("Rango", \
            when((Data_.Mod_init_cta <= 20000), lit("1 Menos a 20 mil")) \
                .when((Data_.Mod_init_cta <= 50000), lit("2 Entre 20 a 50 mil")) \
                .when((Data_.Mod_init_cta <= 100000), lit("3 Entre 50 a 100 mil")) \
                .when((Data_.Mod_init_cta <= 150000), lit("4 Entre 100 a 150 mil")) \
                .when((Data_.Mod_init_cta <= 200000), lit("5 Entre 150 mil a 200 mil")) \
                .when((Data_.Mod_init_cta <= 300000), lit("6 Entre 200 mil a 300 mil")) \
                .when((Data_.Mod_init_cta <= 500000), lit("7 Entre 300 mil a 500 mil")) \
                .when((Data_.Mod_init_cta <= 1000000), lit("8 Entre 500 mil a 1 Millón")) \
                .when((Data_.Mod_init_cta <= 2000000), lit("9 Entre 1 a 2 millones")) \
                .otherwise(lit("9.1 Mayor a 2 millones")))
    
    Data_ = Data_.withColumn("Hora_Envio", lit(now.strftime("%H")))
    Data_ = Data_.withColumn("Hora_Real", lit(now.strftime("%H:%M")))
    Data_ = Data_.withColumn("Fecha_Hoy", lit(now.strftime("%d/%m/%Y")))

    Data_ = Data_.dropDuplicates(["Cruce_Cuentas"])

    Data_ = Data_.select("identificacion", "cuenta", "cuenta2", "fecha_asignacion", "marca", \
                         "origen", f"{Price_Col}", "customer_type_id", "Form_Moneda", "nombrecompleto", \
                        "Rango", "referencia", "min", "Dato_Contacto", "Canal", "Hora_Envio", "Hora_Real", \
                        "Fecha_Hoy", "descuento")

    Data_ = change_email(Data_)
    Data_ = Renamed_Column(Data_)
    Save_Data_Frame(Data_, Directory_to_Save, partitions, Brand_Selected)
    
    return Data_