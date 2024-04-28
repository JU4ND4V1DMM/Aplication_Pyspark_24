import pandas as pd
from PyQt6.QtWidgets import QMessageBox

def clean_file_login(path, outpath_file):

    try:
        df = pd.read_csv(path)

        df['usuhorasalida'] = df['usuhorasalida'].apply(lambda x: str(x).split(',')[-1])
        df['fecha'] = df['fecha'].apply(lambda x: str(x).split(' ')[0])
        df['usuhoraentrada'] = df['usuhoraentrada'].apply(lambda x: str(x).split(',')[0])

        columns_delete = ['path', 'outpath_file']
        exist_columns = df.columns.tolist()
        columns_delete = [col for col in columns_delete if col in exist_columns]

        if columns_delete:
            df.drop(columns=columns_delete, inplace=True)

        file_name = 'archivo_logueo.xlsx'
        root_file_path = outpath_file + '\\' + file_name
        df.to_excel(root_file_path, index=False)

        Mbox_Incomplete = QMessageBox()
        Mbox_Incomplete.setWindowTitle("Proceso Completado")
        Mbox_Incomplete.setIcon(QMessageBox.Icon.Information)
        Mbox_Incomplete.setText("Se ha limpiado y guardado el archivo.")
        Mbox_Incomplete.exec()

    except FileNotFoundError:

        Mbox_Incomplete = QMessageBox()
        Mbox_Incomplete.setWindowTitle("Error de Procesamiento")
        Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
        Mbox_Incomplete.setText("El archivo especificado no fue encontrado.")
        Mbox_Incomplete.exec()

    except Exception as e:

        Mbox_Incomplete = QMessageBox()
        Mbox_Incomplete.setWindowTitle("Error de Procesamiento")
        Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
        Mbox_Incomplete.setText(f"Ocurrió un error al procesar el archivo: {str(e)}")
        Mbox_Incomplete.exec()
