import shutil
import os
import utils.Active_Lines
from datetime import datetime
from PyQt6.QtCore import QDate
from PyQt6 import QtWidgets, uic
from PyQt6.QtWidgets import QMessageBox
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.functions import col, concat, lit, upper, regexp_replace, trim, format_number, expr, when, coalesce

class Charge_DB(QtWidgets.QMainWindow):

    def __init__(self, row_count, file_path, folder_path, partitions):
        super().__init__()
        
        self.file_path = file_path
        self.folder_path = folder_path
        self.partitions = partitions
        self.process_data = uic.loadUi("C:/winutils/cpd/gui/Base.ui")
        self.process_data.label_Total_Registers_2.setText(row_count)
        self.process_data.show()
        self.exec_process()

    def exec_process(self):
        self.data_to_process = []
        self.process_data.pushButton_CAM.clicked.connect(self.generate_DB)
        self.process_data.pushButton_Partitions_BD.clicked.connect(self.Partitions_Data_Base)
        self.process_data.pushButton_MINS.clicked.connect(self.mins_from_bd)
        self.process_data.pushButton_Coding.clicked.connect(self.copy_code)

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

    def Partitions_Data_Base(self):

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("Procesando")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa el archivo.")
        Mbox_In_Process.exec()

        self.partition_DATA()

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Proceso de partición ejecutado exitosamente.")
        Mbox_In_Process.exec()

    def mins_from_bd(self):

        path =  self.file_path
        output_directory = self.folder_path
        partitions = self.partitions

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("Procesando")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa el archivo.")
        Mbox_In_Process.exec()

        utils.Active_Lines.Function_Complete(path, output_directory, partitions)

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Proceso de valdiación de líneas ejecutado exitosamente.")
        Mbox_In_Process.exec()
    
    def copy_code(self):

        output_directory = self.folder_path

        self.function_coding(output_directory)

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Codigos exportados en el directorio de Descargas.")
        Mbox_In_Process.exec()

    def function_coding(self, output_directory):

        code1 = "C:/winutils/cpd/vba/Macro - Cargue de BD.txt"
        code2 = "C:/winutils/cpd/vba/Macro - Filtro Lineas - Filtro Marcas.txt"
        code3 = "C:/winutils/cpd/vba/PowerShell - Union Archivos.txt"

        shutil.copy(code1, output_directory)
        shutil.copy(code2, output_directory)
        shutil.copy(code3, output_directory)

    def DB_Create(self):
        
        list_data = [self.file_path, self.folder_path, self.partitions]

        file = list_data[0]
        root = list_data[1]
        partitions = int(list_data[2])

        list_brands = ["Castigo_ASCARD", "Castigo_multiorigen", "different"]

        for brand in list_brands:
            RDD_Data = self.Function_Complete(file, brand)
            self.Save_File(RDD_Data, root, partitions, brand)


    def Function_Complete(self, path, brand_filter):

        spark = SparkSession \
            .builder.appName("Trial") \
            .getOrCreate()
        spark.conf.set("mapreduce.fileoutputcomitter.marksuccessfuljobs","false")

        sqlContext = SQLContext(spark)

        Data_Root = spark.read.csv(path, header= True, sep=";")
        Data_Root = Data_Root.select([col(c).cast(StringType()).alias(c) for c in Data_Root.columns])

        #ActiveLines
        Data_Root = Data_Root.withColumn("52_", concat(col("53_"), lit(","), col("54_"), lit(", "), col("55_"), lit(","), col("56_")))

        columns_to_list = [f"{i}_" for i in range(1, 53)]
        Data_Root = Data_Root.select(columns_to_list)
        
        potencial = (col("5_") == "Y") & (col("3_") == "BSCS")
        churn = (col("5_") == "Y") & ((col("3_") == "RR") | (col("3_") == "SGA"))
        provision = (col("5_") == "Y") & (col("3_") == "ASCARD")
        prepotencial = (col("6_") == "Y") & (col("3_") == "BSCS")
        prechurn = (col("6_") == "Y") & ((col("3_") == "RR") | (col("3_") == "SGA"))
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

        if brand_filter == "Castigo_ASCARD":
            Data_Root = Data_Root.filter(col("53_") == "Castigo")
            Data_Root = Data_Root.filter(col("3_") == "ASCARD")
        
        elif brand_filter == "Castigo_multiorigen":
            Data_Root = Data_Root.filter(col("53_") == "Castigo")
            Data_Root = Data_Root.filter(col("3_") != "ASCARD")
        
        else: 
            Data_Root = Data_Root.filter(col("53_") != "Castigo")

        Data_Root = Data_Root.withColumn("54_", regexp_replace(col("2_"), "[.-]", ""))

        Data_Root = Data_Root.withColumn("55_", col("9_").cast("double").cast("int"))
        Data_Root = Data_Root.withColumn("55_", format_number(col("55_"), 0))
        
        Segment = (col("42_") % 2 == 0)
        Data_Root = Data_Root.withColumn("56_",
                            when(Segment, "Personas")
                            .otherwise("Negocios"))

        Data_Root = Data_Root.withColumn("57_", \
            when((col("9_") <= 20000), lit("1 Menos a 20 mil")) \
                .when((col("9_") <= 50000), lit("2 Entre 20 a 50 mil")) \
                .when((col("9_") <= 100000), lit("3 Entre 50 a 100 mil")) \
                .when((col("9_") <= 150000), lit("4 Entre 100 a 150 mil")) \
                .when((col("9_") <= 200000), lit("5 Entre 150 mil a 200 mil")) \
                .when((col("9_") <= 300000), lit("6 Entre 200 mil a 300 mil")) \
                .when((col("9_") <= 500000), lit("7 Entre 300 mil a 500 mil")) \
                .when((col("9_") <= 1000000), lit("8 Entre 500 mil a 1 Millón")) \
                .when((col("9_") <= 2000000), lit("9 Entre 1 a 2 millones")) \
                .otherwise(lit("9.1 Mayor a 2 millones")))
        
        return Data_Root

    def Save_File(self, Data_Frame, Directory_to_Save, Partitions, Brand_Filter):

        now = datetime.now()
        Time_File = now.strftime("%Y%m%d_%H%M")
        
        if Brand_Filter == "Castigo_multiorigen":
            Type_File = f"BASE_Planilla_Castigo_multiorigen_"

        elif Brand_Filter == "Castigo_ASCARD":
            Type_File = f"BASE_Planilla_Castigo_ASCARD_"
        
        else: 
            Type_File = f"BASE_Planilla_MULTIMARCA_"
        
        output_path = f'{Directory_to_Save}{Type_File}{Time_File}'
        Data_Frame.repartition(Partitions).write.mode("overwrite").option("header", "true").option("delimiter",";").csv(output_path)
        print(f"DataFrame guardado en: {output_path}")

    def partition_DATA(self):

        list_data = [self.file_path, self.folder_path, self.partitions]

        file = list_data[0]
        root = list_data[1]
        partitions = int(list_data[2])

        try:
            with open(file, 'r', encoding='utf-8') as origin_file:
                rows = origin_file.readlines()

                rows_por_particion = len(rows) // partitions

                for i in range(partitions):
                    begining = i * rows_por_particion
                    end = (i + 1) * rows_por_particion if i < partitions - 1 else len(rows)

                    nombre_particion = os.path.join(root, f"part{i+1}.csv")

                    with open(nombre_particion, 'w', encoding='utf-8') as file_output:
                        file_output.writelines(rows[begining:end])

                if end < len(rows):
                    nombre_particion = os.path.join(root, f"File_Part_{partitions+1}.csv")
                    with open(nombre_particion, 'w', encoding='utf-8') as file_output:
                        file_output.writelines(rows[end:])

        except:
            pass

    
    