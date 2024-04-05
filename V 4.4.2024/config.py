from PyQt6.QtWidgets import QApplication
from gui.Starter import Init_APP

class main_window():

    def __init__(self):
        
        self.app_process = QApplication([])
        self.Project = Init_APP()
        self.app_process.exec()