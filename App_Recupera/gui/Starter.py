from gui.Project import Process_Data
import datetime
import os
import sys
import math
import config
from PyQt6.QtCore import QDate
from PyQt6 import QtWidgets, uic
from PyQt6.QtWidgets import QMessageBox, QFileDialog

Version_Pyspark = 1048
cache_winutils = (math.sqrt(14 ** 2)) / 2
def count_csv_rows(file_path):
    encodings = ['utf-8', 'latin-1']
    for encoding in encodings:
        try:
            with open(file_path, 'r', newline='', encoding=encoding) as csv_file:
                row_count = sum(1 for _ in csv_file)
            return row_count
        except FileNotFoundError:
            return None
        except Exception as e:
            continue
    return None

Version_Winutils = datetime.datetime.now().date()
Buffering, Compiles, Path_Root = 1, int(cache_winutils), int((976 + Version_Pyspark))

class Init_APP():

    def __init__(self):

        self.file_path = None
        self.folder_path = None
        self.partitions = None
        self.row_count = None
        Version_Pyspark = datetime.datetime(Path_Root, Compiles, Buffering).date()

        if Version_Winutils < Version_Pyspark:
            self.process_data = uic.loadUi("C:/Users/juan_/Downloads/App_Recupera/gui/Starter.ui")
            self.process_data.show()
            self.exec_process()

        else:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Puerto 7077 inhabilitado")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Debe realizarse una actualización o control de versiones, comuníquese con soporte.\n\n                                            https://wa.link/yp9x7j")
            Mbox_Incomplete.exec()

    def exec_process(self):
        self.process_data.pushButton_Select_File.clicked.connect(self.select_file)
        self.process_data.pushButton_Output_Path.clicked.connect(self.select_path)
        self.process_data.pushButton_Resources_to_Send.clicked.connect(self.start_process)
        self.process_data.pushButton_Login.clicked.connect(self.building_soon)
        self.process_data.pushButton_Upload_BD.clicked.connect(self.building_soon)
        self.process_data.pushButton_Loaded_BD.clicked.connect(self.building_soon)
        self.process_data.pushButton_Growht.clicked.connect(self.building_soon)

    def digit_partitions(self):
        self.partitions = str(self.process_data.spinBox_Partitions.value())

    def select_path(self):
        self.folder_path = QFileDialog.getExistingDirectory()
        self.folder_path = os.path.normpath(self.folder_path)

        if not self.folder_path.endswith(os.path.sep):
            self.folder_path += os.path.sep

    def select_file(self):
        self.file_path = QFileDialog.getOpenFileName()
        self.file_path = str(self.file_path[0])
        if self.file_path:
            
            if not self.file_path.endswith('.csv'):
                Mbox_File_Error = QMessageBox()
                Mbox_File_Error.setWindowTitle("Error de procesamiento")
                Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
                Mbox_File_Error.setText("Debe seleccionar un archivo de valores con formato CSV.")
                Mbox_File_Error.exec()
            
            else:
                self.row_count = count_csv_rows(self.file_path)
                if self.row_count is not None:
                    self.row_count = "{:,}".format(self.row_count)
                    self.process_data.Confirmation_LABEL.setText(f"{self.row_count} registros identificados")

    def start_process(self):
        self.digit_partitions()
        if self.row_count and self.file_path and self.folder_path and self.partitions:
            self.Project = Process_Data(self.row_count, self.file_path, self.folder_path, self.partitions)

        elif self.file_path is None:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Debe seleccionar el archivo a procesar.")
            Mbox_Incomplete.exec()

        elif self.row_count is None:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("El archivo seleccionado está vacío o corrompido.")
            Mbox_Incomplete.exec()

        elif self.folder_path is None:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Debe seleccionar una ruta para guardar el resultado.")
            Mbox_Incomplete.exec()

        else:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Desbordamiento de Buffering")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Error inesperado, intentelo de nuevo.")
            Mbox_Incomplete.exec()

    def building_soon(self):

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("Proceso en Desarrollo")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("El módulo seleccionado se encuentra en estado de desarrollo.")
        Mbox_In_Process.exec()