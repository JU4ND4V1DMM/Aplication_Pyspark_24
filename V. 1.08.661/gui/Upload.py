import shutil
import os
import utils.Phone_Process_Order
import utils.Reorder_Accounts
from datetime import datetime
from PyQt6.QtCore import QDate
from PyQt6 import QtWidgets, uic
from PyQt6.QtWidgets import QMessageBox
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.functions import col, concat, lit, upper, regexp_replace, trim, format_number, expr, when, coalesce

class Process_Uploaded(QtWidgets.QMainWindow):

    def __init__(self, row_count, file_path, folder_path, partitions):
        super().__init__()
        
        self.file_path = file_path
        self.folder_path = folder_path
        self.partitions = int(partitions)
        self.process_data = uic.loadUi("C:/winutils/cpd/gui/Upload.ui")
        self.process_data.label_Total_Registers_2.setText(row_count)
        self.process_data.show()
        self.exec_process()

    def exec_process(self):
        self.data_to_process = []
        self.process_data.pushButton_Reorder_MINS.clicked.connect(self.Reorder_MINS)
        self.process_data.pushButton_Reorder_Accounts.clicked.connect(self.Reorder_Accounts)
        self.process_data.pushButton_Files_Guide.clicked.connect(self.copy_file_guide)
    
    def copy_file_guide(self):

        output_directory = self.folder_path

        self.function_file_guide(output_directory)

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Archivos exportados en el directorio de Descargas.")
        Mbox_In_Process.exec()
    
    def function_file_guide(self, output_directory):

        file1 = "C:/winutils/cpd/files/dsh_bd/Reporte BOTS.xlsm"
        file2 = "C:/winutils/cpd/files/dsh_bd/Reporte IVR.xlsm"
        file3 = "C:/winutils/cpd/files/dsh_bd/Reporte RECURSOS.xlsm"

        shutil.copy(file1, output_directory)
        shutil.copy(file2, output_directory)
        shutil.copy(file3, output_directory)
    
    def Reorder_MINS(self):

        path =  self.file_path
        output_directory = self.folder_path
        partitions = self.partitions

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("Procesando")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa el archivo.")
        Mbox_In_Process.exec()

        utils.Phone_Process_Order.Function_Complete(path, output_directory, partitions)

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Proceso de creación ejecutado exitosamente.")
        Mbox_In_Process.exec()

    def Reorder_Accounts(self):

        path =  self.file_path
        output_directory = self.folder_path
        partitions = self.partitions

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("Procesando")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa el archivo.")
        Mbox_In_Process.exec()

        utils.Reorder_Accounts.Function_Complete(path, output_directory, partitions)

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Proceso de creación ejecutado exitosamente para especiales, castigo y 30.")
        Mbox_In_Process.exec()
    
        
