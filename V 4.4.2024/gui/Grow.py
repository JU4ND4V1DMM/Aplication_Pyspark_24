from datetime import datetime
from PyQt6.QtCore import QDate
from PyQt6 import QtWidgets, uic
from PyQt6.QtWidgets import QMessageBox
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.functions import col, concat, lit, upper, regexp_replace, trim, format_number, expr, when, coalesce

class Grow_DB(QtWidgets.QMainWindow):

    def __init__(self, row_count, file_path, folder_path, partitions):
        super().__init__()
        
        self.file_path = file_path
        self.folder_path = folder_path
        self.partitions = partitions
        self.process_data = uic.loadUi("C:/Users/juan_/Downloads/App_Recupera/gui/Grow.ui")
        self.process_data.label_Total_Registers_2.setText(row_count)
        self.process_data.show()
        self.exec_process()

    def exec_process(self):
        self.data_to_process = []
        self.process_data.pushButton_BASE_Brands.clicked.connect(self.generate_DB)
        self.process_data.pushButton_Graphic.clicked.connect(self.data_tables)

    def generate_DB(self):

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("Procesando")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa el archivo.")
        Mbox_In_Process.exec()

        self.DB_Create()

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Proceso de creación ejecutado exitosamente.")
        Mbox_In_Process.exec()

    def DB_Create(self):
        
        list_data = [self.file_path, self.folder_path, self.partitions]

        file = list_data[0]
        root = list_data[1]
        partitions = int(list_data[2])

        list_brands = ["castigo", "different"]

        for brand in list_brands:
            RDD_Data = self.Function_Complete(file, brand)
            self.Save_File(RDD_Data, root, partitions, brand)


    def Function_Complete(path, brand_filter):

        spark = SparkSession \
            .builder.appName("Trial") \
            .getOrCreate()
        spark.conf.set("mapreduce.fileoutputcomitter.marksuccessfuljobs","false")

        sqlContext = SQLContext(spark)

        Data_Root = spark.read.csv(path, header= True, sep=";")
        Data_Root = Data_Root.select([col(c).cast(StringType()).alias(c) for c in Data_Root.columns])

        #ActiveLines
        Data_Root = Data_Root.withColumn("52_", concat(col("53_"), lit(","), col("54_"), col("55_"), lit(","), col("56_")))

        columns_to_list = [f"{i}_" for i in range(1, 53)]
        Data_Root = Data_Root.select(columns_to_list)
        
        potencial = (col("5_") == "Y") & (col("3_") == "BSCS")
        churn = (col("5_") == "Y") & (col("3_") == "RR")
        provision = (col("5_") == "Y") & (col("3_") == "ASCARD")
        prepotencial = (col("6_") == "Y") & (col("3_") == "BSCS")
        prechurn = (col("6_") == "Y") & (col("3_") == "RR")
        preprovision = (col("6_") == "Y") & (col("3_") == "ASCARD")
        castigo = col("7_") == "Y"
        potencial_a_castigar = (col("5_") == "N") & (col("6_") == "N") & (col("7_") == "N") & (col("43_") == "Y")
        marcas = col("13_")

        Data_Root = Data_Root.withColumn("53_", when(potencial, "Potencial")\
                                         .when(churn, "Churn")\
                                         .when(provision, "Provision")\
                                         .when(prepotencial, "Prepotencial")\
                                         .when(prechurn, "Prechurn")\
                                         .when(preprovision, "Preprovision")\
                                         .when(castigo, "Castigo")\
                                         .when(potencial_a_castigar, "Potencial a Castigar")\
                                         .otherwise(marcas))

        if brand_filter == "castigo":
            Data_Root = Data_Root.filter(col("53_") == brand_filter)
        
        else: 
            Data_Root = Data_Root.filter(col("53_") != "castigo")

        Data_Root = Data_Root.withColumn("54_", regexp_replace(col("2_"), "[.-]", ""))

        Data_Root = Data_Root.withColumn("55_", col("8_").cast("double").cast("int"))
        Data_Root = Data_Root.withColumn("55_", format_number(col("55_"), 0))
        
        Segment = (col("42_") % 2 == 0)
        Data_Root = Data_Root.withColumn("56_",
                          when(Segment, "Personas")
                          .otherwise("Negocios"))

        Data_Root = Data_Root.withColumn("57_", \
            when((Data_Root.Mod_init_cta <= 20000), lit("1 Menos a 20 mil")) \
                .when((Data_Root.Mod_init_cta <= 50000), lit("2 Entre 20 a 50 mil")) \
                .when((Data_Root.Mod_init_cta <= 100000), lit("3 Entre 50 a 100 mil")) \
                .when((Data_Root.Mod_init_cta <= 150000), lit("4 Entre 100 a 150 mil")) \
                .when((Data_Root.Mod_init_cta <= 200000), lit("5 Entre 150 mil a 200 mil")) \
                .when((Data_Root.Mod_init_cta <= 300000), lit("6 Entre 200 mil a 300 mil")) \
                .when((Data_Root.Mod_init_cta <= 500000), lit("7 Entre 300 mil a 500 mil")) \
                .when((Data_Root.Mod_init_cta <= 1000000), lit("8 Entre 500 mil a 1 Millón")) \
                .when((Data_Root.Mod_init_cta <= 2000000), lit("9 Entre 1 a 2 millones")) \
                .otherwise(lit("9.1 Mayor a 2 millones")))
        
        return Data_Root

    def Save_File(Data_Frame, Directory_to_Save, Partitions, Brand_Filter):

        now = datetime.now()
        Time_File = now.strftime("%Y%m%d_%H%M")
        
        if Brand_Filter == "castigo":
            Type_File = f"BASE_Planilla_{Brand_Filter}"
        
        else: 
            Type_File = f"BASE_Planilla_MULTIMARCA"
        
        output_path = f'{Directory_to_Save}{Type_File}{Time_File}'
        Data_Frame.repartition(Partitions).write.mode("overwrite").option("header", "true").csv(output_path)
        print(f"DataFrame guardado en: {output_path}")


