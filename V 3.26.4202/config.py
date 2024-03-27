import sys
from PyQt5.QtWidgets import QApplication, QMainWindow, QWidget, QLabel, QVBoxLayout, QLineEdit, QGroupBox, QPushButton, QListWidget, QMessageBox
from PyQt5.QtGui import QPixmap, QRegExpValidator
from PyQt5.QtCore import QRegExp
from Modules import SMS_Process, IVR_Process, EMAIL_Process, BOT_Process

class OptionsWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("Aplicación de Filtros para Gestión")

        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        layout = QVBoxLayout(central_widget)

        brand_groupbox = QGroupBox("Marcas")
        brand_layout = QVBoxLayout(brand_groupbox)
        self.brand_listwidget = QListWidget()
        self.brand_listwidget.setSelectionMode(QListWidget.ExtendedSelection)
        self.brand_listwidget.addItems(["0", "30", "60", "90", "120", "150", "180", "210", "potencial", "prechurn", "castigo"])
        brand_layout.addWidget(self.brand_listwidget)
        layout.addWidget(brand_groupbox)

        origin_groupbox = QGroupBox("Orígenes")
        origin_layout = QVBoxLayout(origin_groupbox)
        self.origin_listwidget = QListWidget()
        self.origin_listwidget.setMaximumHeight(100)
        self.origin_listwidget.setSelectionMode(QListWidget.ExtendedSelection)
        self.origin_listwidget.addItems(["BSCS", "ASCARD", "RR", "SGA"])
        origin_layout.addWidget(self.origin_listwidget)
        layout.addWidget(origin_groupbox)

        min_max_groupbox = QGroupBox("Rango de Deuda Inicial")
        min_max_layout = QVBoxLayout(min_max_groupbox)

        min_value_label = QLabel("Valor Mínimo:")
        self.min_value_entry = QLineEdit()
        min_max_layout.addWidget(min_value_label)
        min_max_layout.addWidget(self.min_value_entry)

        integer_validator = QRegExpValidator(QRegExp("[0-9]+"), self.min_value_entry)
        self.min_value_entry.setValidator(integer_validator)

        max_value_label = QLabel("Valor Máximo:")
        self.max_value_entry = QLineEdit()
        min_max_layout.addWidget(max_value_label)
        min_max_layout.addWidget(self.max_value_entry)

        integer_validator = QRegExpValidator(QRegExp("[0-9]+"), self.max_value_entry)
        self.max_value_entry.setValidator(integer_validator)

        layout.addWidget(min_max_groupbox)

        execute_button = QPushButton("Ejecutar")
        execute_button.clicked.connect(self.execute_process)
        layout.addWidget(execute_button)

        # Atributos para almacenar los valores seleccionados
        self.selected_brands = []
        self.selected_origins = []
        self.min_value = ""
        self.max_value = ""

    def execute_process(self):
        self.selected_brands = [item.text() for item in self.brand_listwidget.selectedItems()]
        self.selected_origins = [item.text() for item in self.origin_listwidget.selectedItems()]
        self.min_value = self.min_value_entry.text()
        self.max_value = self.max_value_entry.text()
        # Cerrar la ventana después de obtener los valores
        self.close()

def option_process(folder_path, file_path, partitions, process):
    app = QApplication.instance()
    if app is None:
        app = QApplication([])

    window = OptionsWindow()
    window.show()
    app.exec_()

    selected_brands = window.selected_brands
    selected_origins = window.selected_origins
    min_value = window.min_value
    max_value = window.max_value

    if process == "SMS":
        SMS_Process.Function_Complete(file_path, selected_brands, selected_origins, min_value, max_value, folder_path, partitions)
    else:
        print("En proceso de desarrollo")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = OptionsWindow()
    window.show()
    sys.exit(app.exec_())