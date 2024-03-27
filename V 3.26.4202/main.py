import os
import sys
import math
import datetime
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QLabel, QPushButton, QLineEdit, QFileDialog, 
    QComboBox, QCheckBox, QSpinBox, QMessageBox, QFrame, QVBoxLayout, QHBoxLayout, QVBoxLayout
)
from PyQt5.QtGui import QPixmap
import config

Version_Pyspark = 48
cache_winutils = (math.sqrt(8 ** 2)) / 2
Buffering, Compiles, File_Loaded = 1, int(cache_winutils), int((1976 + Version_Pyspark))

class InfoWidget(QWidget):
    def __init__(self):
        super().__init__()

        info_layout = QVBoxLayout(self)

        folder_label = QLabel("Ruta de destino del resultado:")
        self.folder_entry = QLineEdit()
        self.folder_entry.setObjectName("folder_entry")
        browse_folder_button = QPushButton("Seleccionar")
        browse_folder_button.clicked.connect(self.browse_folder)

        file_label = QLabel("Archivo a procesar:")
        self.file_entry = QLineEdit()
        self.file_entry.setObjectName("file_entry") 
        browse_file_button = QPushButton("Seleccionar")
        browse_file_button.clicked.connect(self.browse_file)

        process_label = QLabel("Proceso:")
        self.process_combobox = QComboBox()
        self.process_combobox.setObjectName("process_combobox") 
        self.process_combobox.addItems(["SMS", "EMAIL", "IVR", "BOT"])
        self.process_combobox.setCurrentIndex(-1)

        partitions_label = QLabel("Particiones:")
        self.partitions_spinbox = QSpinBox()
        self.partitions_spinbox.setObjectName("partitions_spinbox")
        self.partitions_spinbox.setRange(1, 40)

        info_layout.addWidget(folder_label)
        info_layout.addWidget(self.folder_entry)
        info_layout.addWidget(browse_folder_button)
        info_layout.addWidget(file_label)
        info_layout.addWidget(self.file_entry)
        info_layout.addWidget(browse_file_button)
        info_layout.addWidget(process_label)
        info_layout.addWidget(self.process_combobox)
        info_layout.addWidget(partitions_label)
        info_layout.addWidget(self.partitions_spinbox)

    def browse_folder(self):
        folder_path = QFileDialog.getExistingDirectory(self, "Seleccionar carpeta")
        self.folder_entry.setText(folder_path)

    def browse_file(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Seleccionar archivo")
        self.file_entry.setText(file_path)

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("DESARROLLO RECUPERA - V.3.26.4202")

        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        layout = QVBoxLayout(central_widget)

        image_options_widget = QWidget()

        image_options_layout = QHBoxLayout(image_options_widget)

        image_label = QLabel()
        pixmap = QPixmap("C:/Users/juan_/Downloads/Py/obb/logo.jpeg")
        pixmap = pixmap.scaledToHeight(300) 
        image_label.setPixmap(pixmap)
        image_options_layout.addWidget(image_label)

        info_widget = InfoWidget()
        image_options_layout.addWidget(info_widget)

        layout.addWidget(image_options_widget)

        execute_button = QPushButton("Ejecutar")
        execute_button.clicked.connect(self.enter_data)
        layout.addWidget(execute_button)

    def enter_data(self):
        folder_entry = self.findChild(QLineEdit, "folder_entry")
        file_entry = self.findChild(QLineEdit, "file_entry")
        process_combobox = self.findChild(QComboBox, "process_combobox")
        partitions_spinbox = self.findChild(QSpinBox, "partitions_spinbox")

        if not folder_entry or not file_entry or not process_combobox or not partitions_spinbox:
            QMessageBox.warning(self, "Error", "Algunos campos no se encontraron. Por favor, verifique las opciones.")
            return

        folder_path = folder_entry.text()
        file_path = file_entry.text()
        if not folder_path or not file_path:
            QMessageBox.warning(self, "Error", "Por favor, complete los campos de ruta y archivo antes de proceder.")
            return

        process = process_combobox.currentText()
        if not process:
            QMessageBox.warning(self, "Error", "Por favor, seleccione un proceso antes de proceder.")
            return

        partitions = partitions_spinbox.value()

        if process == "SMS" or process == "IVR" or process == "EMAIL" or process == "BOT":
            
            folder_path = os.path.join(folder_path, "")
            config.option_process(folder_path, file_path, partitions, process)
            
        else:
            print("En proceso de desarrollo")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())
