import os
import glob
import shutil
from pathlib import Path
from cloud.conversion_csv_to_parquet import convert_csv_to_parquet
import modules.telematic.claro.report_exclusions
from gui.dynamic_thread import DynamicThread
import utils.active_lines
from datetime import datetime
from PyQt6.QtCore import QDate
from PyQt6 import QtWidgets, uic
from PyQt6.QtWidgets import QMessageBox
from web.save_files import save_to_0csv, save_to_csv
from datetime import date
import io
import polars as pl
from polars import col, lit
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from polars import DataFrame
     
class Charge_DB(QtWidgets.QMainWindow):

    def __init__(self, row_count, file_path, folder_path, process_data, thread_class=DynamicThread):
        
        super().__init__()
        
        self.spinBox_Partitions = None
        self.partitions = None

        self.file_path = file_path
        self.folder_path = folder_path
        self.process_data = process_data
        self.digit_partitions()
        self.exec_process()

    def digit_partitions(self):

        partitions_CAM = self.process_data.spinBox_Partitions.value()
        print(partitions_CAM)
        self.partitions = partitions_CAM

    def exec_process(self):
        
        
        self.digit_partitions()
        self.data_to_process = []
        self.process_data.commandLinkButton_9.clicked.connect(self.upload_DB)
        self.process_data.commandLinkButton_11.clicked.connect(self.generate_DB)
        self.process_data.commandLinkButton_7.clicked.connect(self.Partitions_Data_Base)
        self.process_data.commandLinkButton_10.clicked.connect(self.mins_from_bd)
        self.process_data.commandLinkButton_12.clicked.connect(self.file_exclusions)

    def file_exclusions(self):

        list_data = [self.file_path, self.folder_path, self.partitions]
        lenght_list = len(list_data)

        file = list_data[0]
        root = list_data[1]
        partitions = int(list_data[2])

        if lenght_list >= 3:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmacion, mientras se procesa el archivo.")
            Mbox_In_Process.exec()

            modules.telematic.claro.report_exclusions.Function_Exclusions(file, root, partitions)

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Proceso de filtro de Reclamaciones ejecutado exitosamente.")
            Mbox_In_Process.exec()
        else:
            pass
        
    def upload_DB(self):

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("Procesando")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa el archivo.")
        Mbox_In_Process.exec()

        try:
            self.BD_Control_Next()
            self.convert_csv_to_parquet_bd()
            
            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Proceso de creación ejecutado exitosamente.")
            Mbox_In_Process.exec()
            
        except Exception as e:
            
            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Error")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Critical)
            Mbox_In_Process.setText(f"Se ha presentado un error durante el proceso: {e}")
            Mbox_In_Process.exec()
        
    def generate_DB(self):

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("Procesando")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa el archivo.")
        Mbox_In_Process.exec()

        try:
            self.DB_Create()
            self.convert_csv_to_parquet_bd()
            
            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Proceso de creación ejecutado exitosamente.")
            Mbox_In_Process.exec()
        
        except Exception as e:
            
            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Error")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Critical)
            Mbox_In_Process.setText(f"Se ha presentado un error durante el proceso: {e}")
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

        self.digit_partitions()
        path =  self.file_path
        output_directory = self.folder_path
        partitions = self.partitions

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("Procesando")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa el archivo.")
        Mbox_In_Process.exec()
        
        try:
            
            utils.active_lines.Function_Complete(path, output_directory, partitions)
            self.convert_csv_to_parquet_bd()
            
            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Proceso de valdiación de líneas ejecutado exitosamente.")
            Mbox_In_Process.exec()
        except Exception as e:
            
            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Error")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Critical)
            Mbox_In_Process.setText(f"Se ha presentado un error durante el proceso: {e}")
            Mbox_In_Process.exec()
    
    def read_file(self, file_path):
        # Map of columns that MUST be strings (Utf8)
        schema_override_map = {
            '4_': pl.Utf8, '20_': pl.Utf8, '27_': pl.Utf8,
            '36_': pl.Utf8, '38_': pl.Utf8, '39_': pl.Utf8,
            '51_': pl.Utf8, '52_': pl.Utf8, '57_': pl.Utf8,
        }
        
        if file_path.endswith('.parquet'):
            # 1. Read Parquet (Type inference is automatic)
            Data_Root = pl.read_parquet(file_path)
            
            # 2. FORCE CAST: Parquet often reads IDs/Codes as numbers (f64).
            # We must convert them to String to match your business logic.
            cols_to_cast = [col for col in schema_override_map.keys() if col in Data_Root.columns]
            if cols_to_cast:
                Data_Root = Data_Root.with_columns([
                    pl.col(c).cast(pl.Utf8) for c in cols_to_cast
                ])
        else:
            # 3. Read CSV (Uses your original configuration)
            Data_Root = pl.read_csv(
                file_path, 
                has_header=True, 
                separator=";", 
                infer_schema_length=100000, 
                encoding='latin1', 
                schema_overrides=schema_override_map
            )
        
        # Values for filtering
        values = ["RR", "ASCARD", "BSCS", "SGA"]
        
        # 4. Filter safely: Cast column '3_' to Utf8 just in case it's numeric in Parquet
        Data_Root = Data_Root.filter(
            pl.col('3_').cast(pl.Utf8).is_not_null() & 
            pl.col('3_').cast(pl.Utf8).is_in(values)
        )
                
        return Data_Root
    
    def Update_BD_ControlNext(self, Data_Root: pl.DataFrame) -> pl.DataFrame:
    
        # --- [AccountAccountCode?] updates ---
        Data_Root = Data_Root.with_columns(
            col("[AccountAccountCode?]")
            .str.replace_all("-", "") 
            .str.replace_all(r"\.", "")  # literal=False for regex
            .alias("[AccountAccountCode?]")
        ).with_columns(
            col("[AccountAccountCode?]").alias("[AccountAccountCode2?]")
        )
        
        # --- "Numero de Cliente" updates ---
        Data_Root = Data_Root.with_columns(
            # 1. Remove non-numeric characters (equivalent to regexp_replace("Numero de Cliente", "[^0-9]", ""))
            col("Numero de Cliente")
            .str.replace_all(r"[^0-9]", "", literal=False) # literal=False for regex
            .alias("Numero de Cliente")
        ).with_columns(
            # 2. If null, set to "0" (equivalent to when(col.isNull(), lit("0")).otherwise(col))
            col("Numero de Cliente")
            .fill_null(lit("0"))
            .alias("Numero de Cliente")
        ).with_columns(
            # 3. Cast to integer (CRITICAL FIX: Changed pl.Int32 to pl.Int64 to handle large IDs)
            col("Numero de Cliente").cast(pl.Int64).alias("Numero de Cliente")
        ).with_columns(
            # 4. Conditional update (when length < 2, use [AccountAccountCode?] instead)
            # Note: Polars checks the length of the string representation of the integer.
            pl.when(col("Numero de Cliente").cast(pl.Utf8).str.len_chars() < 2) 
            .then(col("[AccountAccountCode?]"))
            .otherwise(col("Numero de Cliente").cast(pl.Utf8)) # Ensure output type consistency
            .alias("Numero de Cliente")
        )
        
        # --- [Documento?] update ---
        Data_Root = Data_Root.with_columns(
            # Copy "Numero de Cliente"
            col("Numero de Cliente").alias("[Documento?]")
        )
        
        # --- Precio Subscripcion update ---
        Data_Root = Data_Root.with_columns(
            # Set to an empty string (equivalent to lit(""))
            lit("").alias("Precio Subscripcion")
        )
        
        # --- Date columns updates (d/MM/yyyy to yyyy-MM-dd) ---
        Data_Root = Data_Root.with_columns(
            # to_date(..., "d/MM/yyyy") -> date_format(..., "yyyy-MM-dd")
            col("Fecha de Aceleracion").str.strptime(pl.Date, "%d/%m/%Y", strict=False).dt.strftime("%Y-%m-%d").alias("Fecha de Aceleracion"),
            col("Fecha de Vencimiento").str.strptime(pl.Date, "%d/%m/%Y", strict=False).dt.strftime("%Y-%m-%d").alias("Fecha de Vencimiento")
        )

        # --- Date columns updates (d/M/yyyy after split to yyyy-MM-dd) ---
        Data_Root = Data_Root.with_columns(
            # split(col, " ")[0] -> to_date(..., "d/M/yyyy") -> date_format(..., "yyyy-MM-dd")
            col("Fecha Final ")
                .str.split(" ")
                .list.get(0)
                .str.strptime(pl.Date, "%d/%m/%Y", strict=False) # %d/%m/%Y handles d/M/yyyy in Polars
                .dt.strftime("%Y-%m-%d")
                .alias("Fecha Final "),
            col("Fecha de Asignacion")
                .str.split(" ")
                .list.get(0)
                .str.strptime(pl.Date, "%d/%m/%Y", strict=False)
                .dt.strftime("%Y-%m-%d")
                .alias("Fecha de Asignacion")
        )
        
        # --- Single Date column update (d/M/yyyy after split to yyyy-MM-dd) ---
        Data_Root = Data_Root.with_columns(
            # split(col, " ")[0] -> to_date(..., "d/M/yyyy") -> date_format(..., "yyyy-MM-dd")
            col("Fecha Digitacion y Activacion")
                .str.split(" ")
                .list.get(0)
                .str.strptime(pl.Date, "%d/%m/%Y", strict=False)
                .dt.strftime("%Y-%m-%d")
                .alias("Fecha Digitacion y Activacion")
        )

        Data_Root = Data_Root.with_columns(
            col("Direccion Completa")
            .str.replace_all(r"[^a-zA-Z0-9#\-_ ]", "", literal=False)
            .alias("Direccion Completa")
        )
        
        # --- Remove "|" from all columns ---
        # FIX: Replaced pl.all().map(...) with the efficient Polars selector pl.col(pl.Utf8)
        Data_Root = Data_Root.with_columns(
            # Selects all Utf8 columns and applies the string replacement
            pl.col(pl.Utf8).str.replace_all(r"\|", "", literal=True)
        )

        return Data_Root
    
    def change_name_column(self, Data_: "DataFrame", Column: str) -> "DataFrame":

        # 1. Convert the column to uppercase (equivalent to upper(col(Column)))
        Data_ = Data_.with_columns(
            col(Column).str.to_uppercase().alias(Column)
        )
        
        # 2. Handle 'Ñ' and related characters (first batch of replacements)
        # The strategy is: replace all variations of Ñ with a unique temporary string ("NNNNN"), 
        # then replace that temporary string with a single "N".
        character_list_N = ["\\ÃƒÂ‘", "\\Ã‚Â¦", "\\Ã‘", "Ñ", "ÃƒÂ‘", "Ã‚Â¦", "Ã‘"]
        
        # Replace all 'Ñ' variations with "NNNNN" in a loop (using fold/reduce for efficiency in Polars)
        ñ_replacements = col(Column)
        for character in character_list_N:
            # Use literal=True for characters that might be interpreted as regex (like \)
            # The list contains some escaped sequences that need literal=True
            ñ_replacements = ñ_replacements.str.replace_all(character, "NNNNN", literal=True) 
        
        # Apply the intermediate replacements and then the final replacements for 'N' and other special characters
        Data_ = Data_.with_columns(
            ñ_replacements
                .str.replace_all("NNNNN", "N", literal=True) # Replace the temporary string with "N"
                .str.replace_all("Ã‡", "A", literal=True)    # Replace 'Ã‡' with 'A'
                .str.replace_all("ÃƒÂ", "I", literal=True)    # Replace 'ÃƒÂ' with 'I'
                .alias(Column)
        )

        # 3. Remove titles, special characters, numbers, and excess spaces (second batch of replacements)
        character_list = [
            "SR/SRA", "SR./SRA.", "SR/SRA.","SR.", "SRA.", "SR(A).","SR ", "SRA ", "SR(A)",
            "\\.",'#', '$', '/','<', '>', "\\*", "SEÑORES ","SEÑOR(A) ","SEÑOR ","SEÑORA ", "SENORES ",
            "SENOR(A) ","SENOR ","SENORA ", "¡", "!", "\\?" "¿", "_", "-", "}", "\\{", "\\+", 
            "0 ", "1 ", "2 ", "3 ", "4 ", "5 ", "6 ", "7 ","8 ", "9 ", "0", "1", "2", "3", 
            "4", "5", "6", "7", "8", "9", "  "
        ]

        # Replace all characters in character_list with "" in a loop
        second_replacements = col(Column)
        for character in character_list:
            # Use literal=True for characters that might be interpreted as regex
            second_replacements = second_replacements.str.replace_all(character, "", literal=True) 
        
        # 4. Filter characters: keep only A-Z, & and space (equivalent to regexp_replace(Column, "[^A-Z& ]", ""))
        Data_ = Data_.with_columns(
            second_replacements
                .str.replace_all(r"[^A-Z& ]", "", literal=False) # literal=False for regex
                .alias(Column)
        )

        # 5. Remove remaining title variations (third batch of replacements)
        character_list_final = ["SEORES ","SEORA ","SEOR ","SEORA "]

        # Replace remaining title variations with ""
        final_replacements = col(Column)
        for character in character_list_final:
            final_replacements = final_replacements.str.replace_all(character, "", literal=True) 
        
        # 6. Remove leading "A " or spaces (equivalent to regexp_replace(col(Column), r'^(A\s+| )+', ''))
        Data_ = Data_.with_columns(
            final_replacements
                .str.replace_all(r'^(A\s+| )+', '', literal=False) # literal=False for regex
                .alias(Column)
        )
                
        return Data_

    def BD_Control_Next(self) -> "DataFrame":
    
        # --- Calling external methods (kept as in original code) ---
        self.digit_partitions()
        
        # --- Data parameters ---
        # Assuming self.file_path, self.folder_path, self.partitions exist
        list_data = [self.file_path, self.folder_path, self.partitions]
        file = list_data[0]
        root = list_data[1]
        partitions = int(list_data[2])

        list_origins = ["ASCARD", "RR", "BSCS", "SGA"]

        now = datetime.now()
        Time_File = now.strftime("%Y%m%d_%H%M")

        # 1. Schema Override Configuration for INGESTION
        # Columns that contain '.' as thousand separators must be read as strings (Utf8)
        # to prevent parsing errors (ComputeError) during the initial file load.
        
        Data_Root = self.read_file(file)

        # 2. Cleanup Thousands Separators and Cast to Numeric Types
        # We use Polars expressions to clean the string data and convert it.
        expressions = [
            # Column 36_: Intended to be a decimal number (Float64)
            pl.col('36_')
              .str.replace_all(r'\.', '')  # Remove all periods (thousand separators)
              .cast(pl.Float64)             # Cast to the correct decimal type
              .alias('36_'),
            
            # Column 39_: Intended to be a large integer (Int64)
            pl.col('39_')
              .str.replace_all(r'\.', '')  # Remove all periods (thousand separators)
              .cast(pl.Int64)              # Cast to the correct integer type
              .alias('39_'),
            
            # Add other conversions here if needed
        ]

        # Apply the transformations to the DataFrame
        Data_Root = Data_Root.with_columns(expressions)
        
        
        Data_Root = Data_Root.with_columns(pl.all().cast(pl.Utf8))
        
        # Initial selection of columns 1_ to 61_ (Equivalent to Data_Root.select(columns_to_list))
        columns_to_select = [f"{i}_" for i in range(1, 62)]
        Data_Root = Data_Root.select(columns_to_select)
        
        
        # Filter by origins (Equivalent to Data_Root.filter(col("3_").isin(list_origins)))
        Data_Root = Data_Root.filter(col("3_").is_in(list_origins))
        
        # Cruice with data TIPOBASE
        source_path = r"\\172.128.10.200\4. Gestion de Operaciones\2. Claro\Data compartida\NO BORRAR CONEXIÓN API\Nombre Campana"
        Data_Root = self.process_cta_cruice(Data_Root)
        Data_Root = self.handle_files_and_join(Data_Root, source_path)

        # --- Feature Engineering and Conditional Logic ---
        Data_Root = Data_Root.with_columns(
            # Equivalent to Data_Root.withColumn("Telefono X", lit(""))
            lit("").alias("Telefono 1"),
            lit("").alias("Telefono 2"),
            lit("").alias("Telefono 3"),
            lit("").alias("Telefono 4"),
            
            # Equivalent to Data_Root.withColumn("Valor Scoring", col("57_"))
            col("57_").alias("Valor Scoring"),
            
            # Equivalent to Data_Root.withColumn("[AccountAccountCode2?]", col("2_"))
            col("2_").alias("[AccountAccountCode2?]"),
            
            # Equivalent to Data_Root.withColumn("43_", lit(""))
            lit("").alias("43_")
        )

        correction_nnny = (col("5_") == lit("Y")) | (col("6_") == lit("Y")) | (col("7_") == lit("Y"))
        correction_nnny = correction_nnny & (col("42_") == lit("Y"))
        
        Data_Root = Data_Root.with_columns(
            pl.when(correction_nnny)
            .then(lit("")) # Equivalent to lit("")
            .otherwise(col("42_"))
            .alias("42_")
        )

        Data_Root = Data_Root.with_columns([
            pl.when(col("5_").is_null() | (~col("5_").is_in(["Y", "N"]))).then(lit("N")).otherwise(col("5_")).alias("5_"),
            pl.when(col("6_").is_null() | (~col("6_").is_in(["Y", "N"]))).then(lit("N")).otherwise(col("6_")).alias("6_"),
            pl.when(col("7_").is_null() | (~col("7_").is_in(["Y", "N"]))).then(lit("N")).otherwise(col("7_")).alias("7_"),
        ])

        # --- Final Column Selection and Ordering ---
        columns_to_list = ["1_", "2_", "3_", "4_", "5_", "6_", "7_", "8_", "9_", "10_", "11_", "12_", 
                        "13_", "14_", "15_", "16_", "17_", "18_", "50_", "Telefono 1", "Telefono 2", "Telefono 3", 
                        "Telefono 4", "Valor Scoring", "19_", "20_", "21_", "22_", "23_", "24_", "25_", 
                        "26_", "27_", "28_", "29_", "30_", "31_", "32_", "33_", "34_", "35_", "36_", "37_", 
                        "38_", "39_", "40_", "41_", "42_", "43_", "[AccountAccountCode2?]", "56_", "58_", "59_", "60_", "61_"]

        print("Columnas de Data_Root:", Data_Root.columns)

        Data_Root = Data_Root.select(columns_to_list)
                                            
        # --- Deduplication and Sorting ---
        # Equivalent to Data_Root.dropDuplicates(["2_"])
        Data_Root = Data_Root.unique(subset=["2_"])
        
        # Equivalent to Data_Root.orderBy(col("3_"))
        Data_Root = Data_Root.sort(by="3_")

        # --- Name Cleaning Logic ---
        Data_Root = Data_Root.with_columns(
            col("24_").alias("24_2")
        )
        
        # Calling external method (kept as in original code)
        Data_Root = self.change_name_column(Data_Root, "24_2")
        
        # Conditional update after cleaning (Equivalent to when(length(col("24_2")) < 7, col("24_")).otherwise(col("24_2")))
        Data_Root = Data_Root.with_columns(
            pl.when(pl.col("24_2").str.len_chars() < 7)
            .then(pl.col("24_"))
            .otherwise(pl.col("24_2"))
            .alias("24_")
        )
        
        # Final selection of required columns (excluding the temporary "24_2")
        Data_Root = Data_Root.select(columns_to_list)
        
        # --- Document Type Logic (Tipo_Documento) ---
        Data_Root = Data_Root.with_columns(
            col("1_").str.replace_all(r'[^a-zA-Z]', '', literal=False).alias("Tipo_Documento")
        ).with_columns(
            pl.when(col("Tipo_Documento") == lit("CC")).then(lit("Cedula de Ciudadania"))
            .when(col("Tipo_Documento") == lit("PS")).then(lit("Pasaporte"))
            .when(col("Tipo_Documento") == lit("PP")).then(lit("Pasaporte"))
            .when(col("Tipo_Documento") == lit("PTP")).then(lit("Permiso Temporal"))
            .when(col("Tipo_Documento") == lit("PPT")).then(lit("Permiso Temporal"))
            .when(col("Tipo_Documento") == lit("XPP")).then(lit("Permiso de Permanencia"))
            .when(col("Tipo_Documento") == lit("NT")).then(lit("Nit"))
            .when(col("Tipo_Documento") == lit("CD")).then(lit("Carnet Diplomatico"))
            .when(col("Tipo_Documento") == lit("CE")).then(lit("Cedula de Extranjeria"))
            .when(col("Tipo_Documento").is_null() | (col("Tipo_Documento") == lit(""))).then(lit("Sin tipologia"))
            .otherwise(lit("Errado"))
            .alias("Tipo_Documento")
        )
        
        # --- Add "Departamento" column ---
        Data_Root = Data_Root.with_columns(
            lit("Prueba").alias("Departamento")
        )
        
        # --- Rename Columns (ColumnRenamed) ---
        Data_Root = Data_Root.rename({
            "1_": "Numero de Cliente",
            "2_": "[AccountAccountCode?]",
            "3_": "CRM Origen",
            "4_": "Edad de Deuda",
            "5_": "[PotencialMark?]",
            "6_": "[PrePotencialMark?]",
            "7_": "[WriteOffMark?]",
            "8_": "Monto inicial",
            "9_": "[ModInitCta?]",
            "10_": "[DeudaRealCuenta?]",
            "11_": "[BillCycleName?]",
            "12_": "Nombre Campana",
            "13_": "[DebtAgeInicial?]",
            "14_": "Nombre Casa de Cobro",
            "15_": "Fecha de Asignacion",
            "16_": "Deuda Gestionable",
            "17_": "Direccion Completa",
            "18_": "Fecha Final ",
            "50_": "Email",
            "19_": "Segmento",
            "20_": "[Documento?]",
            "21_": "[AccStsName?]",
            "22_": "Ciudad",
            "23_": "[InboxName?]",
            "24_": "Nombre del Cliente",
            "25_": "Id_de_Ejecucion",
            "26_": "Fecha de Vencimiento",
            "27_": "Numero Referencia de Pago",
            "28_": "MIN",
            "29_": "Plan",
            "30_": "Cuotas Aceleradas",
            "31_": "Fecha de Aceleracion",
            "32_": "Valor Acelerado",
            "33_": "Intereses Contingentes",
            "34_": "Intereses Corrientes Facturados",
            "35_": "Intereses por mora facturados",
            "36_": "Iva Intereses Contigentes Facturado",
            "37_": "Iva Intereses Corrientes Facturados",
            "38_": "Iva Intereses por Mora Facturado",
            "39_": "Precio Subscripcion",
            "40_": "Codigo de proceso",
            "41_": "[CustomerTypeId?]",
            "42_": "[RefinanciedMark?]",
            "43_": "[Discount?]",
            "58_": "Cuotas Pactadas", 
            "59_": "Cuotas Facturadas", 
            "60_": "Cuotas Pendientes",
            "61_": "Fecha Digitacion y Activacion"
        })

        # Conditional rename (Equivalent to if "56_" in Data_Root.columns:...)
        if "56_" in Data_Root.columns:
            Data_Root = Data_Root.rename({"56_": "Monitor"})

        # --- Logging and Error Handling ---Id de Ejecucion
        Data_Error = Data_Root.clone() # Use clone() to create a separate copy of the DataFrame

        # --- Filtering and Saving Data_Root (Cargue) ---
        # Equivalent to Data_Root.filter(col("[CustomerTypeId?]") >= 80)
        # The original code implicitly casts to a numeric type for comparison. We must explicitly cast to Int32.
        customer_type = col("[CustomerTypeId?]")
        id_execute = col("Id_de_Ejecucion")
        field_activate = col("[AccStsName?]")
        
        Data_Root = Data_Root.filter(
            customer_type.cast(pl.Int32, strict=False).is_between(80, 89) &
            id_execute.is_not_null() &
            field_activate.is_not_null()
        )
        
        Data_Root = Data_Root.with_columns(
            # 1. Remove non-numeric characters (equivalent to regexp_replace("Valor Scoring", "[^0-9]", ""))
            col("Valor Scoring")
            .str.replace_all(r"[^0-9]", "", literal=False) # literal=False for regex
            .alias("Valor Scoring")
        )
        
        name = "Cargue" 
        origin = "Multiorigen"
        self.Save_File(Data_Root, root, partitions, name, origin, Time_File)

        # --- Filtering and Saving Data_Brands (Multimarca_Cargue) ---
        Data_Brands = Data_Root.filter(col("[WriteOffMark?]") != lit("Y"))
        name = "Multimarca_Cargue"
        origin = "Multiorigen"
        self.Save_File(Data_Brands, root, partitions, name, origin, Time_File)
        
        # --- Update and Save Data_Brands_Update (Multimarca_Cargue_Actualizacion) ---
        # Calling external method (kept as in original code)
        Data_Brands_Update = self.Update_BD_ControlNext(Data_Brands)
        name = "Multimarca_Cargue_Actualizacion"
        origin = "Multiorigen"
        self.Save_File(Data_Brands_Update, root, partitions, name, origin, Time_File)

        # --- Filtering and Saving Data_Error (Errores) ---
        # Equivalent to Data_Error.filter(...)
        # The logic checks for null, non-numeric, or outside the [80, 89] range.
        
        Data_Error = Data_Error.filter(
            customer_type.is_null() |
            id_execute.is_null() |
            field_activate.is_null() |
            customer_type.cast(pl.Float64, strict=False).is_null() |
            (~customer_type.cast(pl.Int32, strict=False).is_between(80, 89))
        )
        
        name = "Errores"
        origin = "Multiorigen"
        self.Save_File(Data_Error, root, partitions, name, origin, Time_File)
        
        return Data_Root
    
    def DB_Create(self):
    
        # --- Data parameters ---
        list_data = [self.file_path, self.folder_path, self.partitions]

        file = list_data[0]
        root = list_data[1]
        partitions = int(list_data[2])

        list_origins = ["ASCARD", "RR", "BSCS", "SGA"]

        now = datetime.now()
        Time_File = now.strftime("%Y%m%d_%H%M")

        origin_list = list_origins
        
        # --- Initial Data Load and Transformation (Assuming external functions return/accept pl.DataFrame) ---
        # Equivalent to RDD_Data = self.Function_Complete(file)
        RDD_Data: pl.DataFrame = self.Function_Complete(file)
        
        # Equivalent to RDD_Data = self.Renamed_column(RDD_Data)
        RDD_Data = self.Renamed_column(RDD_Data)

        # --- 1. CORPORATIVOS (CORP) ---
        origin = "Multimarca"
        brand = "Corporativos"
        
        RDD_Data_CORP = RDD_Data.filter(col("CRM_Origen").is_in(list_origins))
        RDD_Data_CORP = RDD_Data_CORP.filter(col("Nombre Campana") == "Clientes Corporativos")
        self.Save_File(RDD_Data_CORP, root, partitions, brand, origin, Time_File)
        
        # --- 2. MULTIMARCA (MULTIBRAND) ---
        RDD_Data_MULTIBRAND = RDD_Data.filter(col("CRM_Origen").is_in(list_origins))
        origin = "Multiorigen"
        brand = "Base_Completa"

        self.Save_File(RDD_Data_MULTIBRAND, root, partitions, brand, origin, Time_File)

        origin = "Multiorigen"
        brand = "Multimarca"
        
        # RDD_Data_Corp (Local temporary variable, filtered from RDD_Data_MULTIBRAND)
        RDD_Data_Corp = RDD_Data_MULTIBRAND.filter(col("Nombre Campana") == "Clientes Corporativos")
        # Filter out "Castigo" records
        RDD_Data_MULTIBRAND = RDD_Data_MULTIBRAND.filter(col("Marca_Asignada") != "Castigo")
        
        RDD_Data_MULTIBRAND = pl.concat([RDD_Data_MULTIBRAND, RDD_Data_Corp])
        RDD_Data_MULTIBRAND = RDD_Data_MULTIBRAND.unique(subset=["Cuenta"])

        self.Save_File(RDD_Data_MULTIBRAND, root, partitions, brand, origin, Time_File)

        # --- 3. CASTIGO (CAST) ---
        origin = "Multiorigen"
        brand = "castigo"
        
        RDD_Data_CAST = RDD_Data.filter(col("CRM_Origen").is_in(list_origins))
        
        RDD_Data_CAST = RDD_Data_CAST.filter(col("Marca_Asignada") == "Castigo")
        RDD_Data_CAST = RDD_Data_CAST.filter(col("Nombre Campana") != "Clientes Corporativos")

        self.Save_File(RDD_Data_CAST, root, partitions, brand, origin, Time_File)

        # --- 4. CASTIGO (ASCARD - RR - SGA) ---
        origin = "ASCARD - RR - SGA"
        brand = "castigo"
        list_origins = ["ASCARD", "RR", "SGA"]
        
        RDD_Data_CAST_AR = RDD_Data_CAST.filter(col("CRM_Origen").is_in(list_origins))
        self.Save_File(RDD_Data_CAST_AR, root, partitions, brand, origin, Time_File)

        # --- 5. CASTIGO (BSCS) ---
        origin = "BSCS"
        brand = "castigo"
        list_origins = ["BSCS"]
        
        RDD_Data_CAST_SB = RDD_Data_CAST.filter(col("CRM_Origen").is_in(list_origins))

        self.Save_File(RDD_Data_CAST_SB, root, partitions, brand, origin, Time_File)

    def Function_Complete(self, path) -> "DataFrame":

        """
        Ingests data from a regional CSV file, handles complex data type 
        parsing (like thousand separators), and cleans the data types.

        Args:
            path: The file path to the CSV data.

        Returns:
            The processed Polars DataFrame.
        """
        
        # 1. Schema Override Configuration for INGESTION
        # Columns that contain '.' as thousand separators must be read as strings (Utf8)
        # to prevent parsing errors (ComputeError) during the initial file load.
        Data_Root = self.read_file(path)

        # 2. Cleanup Thousands Separators and Cast to Numeric Types
        # We use Polars expressions to clean the string data and convert it.
        expressions = [
            # Column 36_: Intended to be a decimal number (Float64)
            pl.col('36_')
              .str.replace_all(r'\.', '')  # Remove all periods (thousand separators)
              .cast(pl.Float64)             # Cast to the correct decimal type
              .alias('36_'),
            
            # Column 39_: Intended to be a large integer (Int64)
            pl.col('39_')
              .str.replace_all(r'\.', '')  # Remove all periods (thousand separators)
              .cast(pl.Int64)              # Cast to the correct integer type
              .alias('39_'),
            
            # Add other conversions here if needed
        ]

        # Apply the transformations to the DataFrame
        Data_Root = Data_Root.with_columns(expressions)
        
        # Equivalent to Data_Root.select([col(c).cast(StringType()).alias(c) for c in Data_Root.columns])
        # Ensure all columns are Utf8 (string) for initial consistency with PySpark's StringType() cast
        Data_Root = Data_Root.with_columns(pl.all().cast(pl.Utf8))

        # --- ActiveLines (Concatenation and cleaning) ---
        # Equivalent to concat(coalesce(col("X"), lit("")), lit(","), ...)
        Data_Root = Data_Root.with_columns(
            (pl.coalesce(col("51_").fill_null(lit("")), lit("")) + lit(",") +
            pl.coalesce(col("52_").fill_null(lit("")), lit("")) + lit(",") +
            pl.coalesce(col("53_").fill_null(lit("")), lit("")) + lit(",") +
            pl.coalesce(col("54_").fill_null(lit("")), lit("")) + lit(",") +
            pl.coalesce(col("55_").fill_null(lit("")), lit("")))
            .alias("51_")
        ).with_columns(
            # Equivalent to Data_Root.withColumn("51_", regexp_replace(col("51_"), ",,", ",")) (3 times)
            col("51_")
            .str.replace_all(",,", ",", literal=True)
            .str.replace_all(",,", ",", literal=True)
            .str.replace_all(",,", ",", literal=True)
            .alias("51_")
        )
        
        # --- Final Column Selection ---
        columns_to_list = [f"{i}_" for i in range(1, 63)]
        Data_Root = Data_Root.select(columns_to_list)
        
        # --- Conditional Expressions (Polars) ---
        potencial = (col("5_") == lit("Y")) & (col("3_") == lit("BSCS"))
        churn = (col("5_") == lit("Y")) & ((col("3_") == lit("RR")) | (col("3_") == lit("SGA")))
        provision = (col("5_") == lit("Y")) & (col("3_") == lit("ASCARD"))
        prepotencial = (col("6_") == lit("Y")) & (col("3_") == lit("BSCS"))
        prechurn = (col("6_") == lit("Y")) & ((col("3_") == lit("RR")) | (col("3_") == lit("SGA")))
        preprovision = (col("6_") == lit("Y")) & (col("3_") == lit("ASCARD"))
        castigo = col("7_") == lit("Y")
        potencial_a_castigar = (col("5_") == lit("N")) & (col("6_") == lit("N")) & (col("7_") == lit("N")) & (col("42_") == lit("Y"))
        marcas = col("13_")

        # --- Deduplication ---
        # Equivalent to Data_Root.dropDuplicates(["2_"])
        Data_Root = Data_Root.unique(subset=["2_"])

        # --- Conditional Column "53_" (Marca_Asignada) ---
        Data_Root = Data_Root.with_columns(
            pl.when(potencial).then(lit("Potencial"))
            .when(churn).then(lit("Churn"))
            .when(provision).then(lit("Provision"))
            .when(prepotencial).then(lit("Prepotencial"))
            .when(prechurn).then(lit("Prechurn"))
            .when(preprovision).then(lit("Preprovision"))
            .when(castigo).then(lit("Castigo"))
            .when(potencial_a_castigar).then(lit("Potencial a Castigar"))
            .otherwise(marcas)
            .alias("53_")
        )
        
        # --- Further Conditional Logic on "53_" ---
        moras_numericas = (col("53_") == lit("120")) | (col("53_") == lit("150")) | (col("53_") == lit("180"))
        prepotencial_especial = (col("53_") == lit("Prepotencial")) & (col("3_") == lit("BSCS")) & ((col("12_") == lit("PrePotencial Convergente Masivo_2")) | (col("12_") == lit("PrePotencial Convergente Pyme_2")))

        Data_Root = Data_Root.with_columns(
            pl.when(moras_numericas).then(lit("120 - 180"))
            .when(prepotencial_especial).then(lit("Prepotencial Especial"))
            .otherwise(col("53_"))
            .alias("53_")
        )

        Data_Root = Data_Root.with_columns([
            pl.when(col("5_").is_null() | (~col("5_").is_in(["Y", "N"]))).then(lit("N")).otherwise(col("5_")).alias("5_"),
            pl.when(col("6_").is_null() | (~col("6_").is_in(["Y", "N"]))).then(lit("N")).otherwise(col("6_")).alias("6_"),
            pl.when(col("7_").is_null() | (~col("7_").is_in(["Y", "N"]))).then(lit("N")).otherwise(col("7_")).alias("7_"),
            pl.when(col("42_").is_null() | (~col("42_").is_in(["Y", "N"]))).then(lit("N")).otherwise(col("42_")).alias("42_")
        ])

        # --- Column "54_" (Cleaned '2_') ---
        # Equivalent to regexp_replace(col("2_"), "[.-]", "")
        Data_Root = Data_Root.with_columns(
            col("2_").str.replace_all(r"[.-]", "", literal=False).alias("54_")
        )

        # --- Column "55_" (Cast and Replace) ---
        Data_Root = Data_Root.with_columns(
            # Cast to double/Float64 (Equivalent to col("9_").cast("double"))
            col("9_").cast(pl.Float64, strict=False).alias("55_")
        ).with_columns(
            # Replace decimal point with comma (Equivalent to regexp_replace("55_", "\\.", ","))
            col("55_").cast(pl.Utf8).str.replace_all(r"\.", ",", literal=False).alias("55_")
        )
        
        # --- Column Renaming (56_ to 61_ to 63_ to 68_) ---
        Data_Root = Data_Root.rename({
            "56_": "63_", #Monitor
            "57_": "64_", #Scoring
            "58_": "65_", #Cuotas Pactadas
            "59_": "66_", #Cuotas Facturadas
            "60_": "67_", #Cuotas Pendientes
            "61_": "68_"  #Fecha Digitación/Activación
        })

        # --- Column "56_" (Segment) ---
        Segment = ((col("41_") == lit("81")) | (col("41_") == lit("84")) | (col("41_") == lit("87")))
        Segment2 = ((col("41_") == lit("82")) | (col("41_") == lit("85")) | (col("41_") == lit("88")))
        Segment3 = ((col("41_") == lit("83")) | (col("41_") == lit("86")) | (col("41_") == lit("89")))

        Data_Root = Data_Root.with_columns(
            pl.when(Segment).then(pl.lit("Personas"))
            .when(Segment2).then(pl.lit("Negocios"))
            .when(Segment3).then(pl.lit("Empresas"))
            .otherwise(pl.lit("Errado"))
            .alias("56_"))

        # --- Column "57_" (Debt Banding) ---
        # The original logic compares a string column ('9_') which contains numeric values (likely debts)
        # with numeric literals. In Polars, we must explicitly cast '9_' to numeric (Float64).
        debt_col = col("9_").cast(pl.Float64, strict=False)
        Data_Root = Data_Root.with_columns(
            pl.when(debt_col.is_null()).then(lit("9.1 Mayor a 2 millones")) # Handle non-numeric/null values
            .when(debt_col <= 20000).then(lit("1 Menos a 20 mil"))
            .when(debt_col <= 50000).then(lit("2 Entre 20 a 50 mil"))
            .when(debt_col <= 100000).then(lit("3 Entre 50 a 100 mil"))
            .when(debt_col <= 150000).then(lit("4 Entre 100 a 150 mil"))
            .when(debt_col <= 200000).then(lit("5 Entre 150 mil a 200 mil"))
            .when(debt_col <= 300000).then(lit("6 Entre 200 mil a 300 mil"))
            .when(debt_col <= 500000).then(lit("7 Entre 300 mil a 500 mil"))
            .when(debt_col <= 1000000).then(lit("8 Entre 500 mil a 1 Millon"))
            .when(debt_col <= 2000000).then(lit("9 Entre 1 a 2 millones"))
            .otherwise(lit("9.1 Mayor a 2 millones"))
            .alias("57_")
        )
        
        # --- Column "Multiproducto" and "58_" (Client Type) ---
        flp_filter_databse = ((col("12_") == lit("FLP 01")) | (col("12_") == lit("FLP 02")) | (col("12_") == lit("FLP 03")))
        
        Data_Root = Data_Root.with_columns(
            lit("").alias("Multiproducto"), # New column set to empty string
            
            # Equivalent to Data_Root.withColumn("58_", when(flp_filter_databse, concat(lit("CLIENTES "), col("12_")))...
            pl.when(flp_filter_databse).then(lit("CLIENTES ") + col("12_")) # Use '+' for string concat in Polars
            .when(col("12_") == lit("Clientes Corporativos")).then(lit("CLIENTES CORPORATIVOS"))
            .otherwise(lit("CLIENTES INVENTARIO"))
            .alias("58_")
        )
        
        # --- Tipo_Documento Logic ---
        Data_Root = Data_Root.with_columns(
            # 1. Clean '1_' (Equivalent to regexp_replace("1_", r'[^a-zA-Z]', ''))
            col("1_").str.replace_all(r'[^a-zA-Z]', '', literal=False).alias("Tipo_Documento")
        ).with_columns(
            # 2. Conditional mapping (using chained when/then/otherwise)
            pl.when(col("Tipo_Documento") == lit("CC")).then(lit("Cedula de Ciudadania"))
            .when(col("Tipo_Documento") == lit("PS")).then(lit("Pasaporte"))
            .when(col("Tipo_Documento") == lit("PP")).then(lit("Pasaporte"))
            .when(col("Tipo_Documento") == lit("PP")).then(lit("Permiso Temporal")) # Duplicated original logic is preserved
            .when(col("Tipo_Documento") == lit("XPP")).then(lit("Permiso de Permanencia"))
            .when(col("Tipo_Documento") == lit("NT")).then(lit("Nit"))
            .when(col("Tipo_Documento") == lit("CD")).then(lit("Carnet Diplomatico"))
            .when(col("Tipo_Documento") == lit("CE")).then(lit("Cedula de Extranjeria"))
            .when(col("Tipo_Documento").is_null() | (col("Tipo_Documento") == lit(""))).then(lit("Sin tipologia"))
            .otherwise(lit("Errado"))
            .alias("Tipo_Documento")
        )
        
        # --- Sort and Log ---
        # Equivalent to Data_Root.orderBy(col("3_"))
        Data_Root = Data_Root.sort(by="3_")
        
        return Data_Root
        
    def Save_File(self, Data_Frame: "DataFrame", Directory_to_Save: str, Partitions: int, Brand_Filter: str, Origin_Filter: str, Time_File: str):

        # Initialize variables
        Type_File = ""
        extension = ""
        Name_File = ""

        if Brand_Filter == "castigo":
            Type_File = f"---- Bases para CRUCE ----"
            extension = "0csv"
            Name_File = f"Cruce Castigo {Origin_Filter}"
        
        elif Brand_Filter == "Base_Completa":
            Type_File = f"---- Bases para CRUCE ----"
            extension = "csv"
            Name_File = f"Cruce Base Completa {Origin_Filter}"
        
        elif Brand_Filter == "Corporativos":
            Type_File = f"---- Bases para CRUCE ----"
            extension = "0csv"
            Name_File = f"Cruce Corporativos {Origin_Filter}"
        
        elif Brand_Filter in ["Cargue", "Errores", "Multimarca_Cargue", "Multimarca_Cargue_Actualizacion"]:
            Type_File = f"---- Bases para CARGUE ----"
            extension = "csv"

            if Brand_Filter == "Errores":
                Type_File = f"---- Bases para CARGUE ----"
                Name_File = "de Errores (NO RELACIONADA EN CARGUE)"
                extension = "0csv"

            elif Brand_Filter == "Multimarca_Cargue":
                Type_File = f"---- Bases para CARGUE ----"
                Name_File = "Cargue UNIF sin Castigo"
            
            elif Brand_Filter == "Multimarca_Cargue_Actualizacion":
                Type_File = f"---- Bases para CARGUE ----"
                Name_File = "Cargue UNIF Actualizacion sin Castigo"

            else: # Brand_Filter == "Cargue"
                Name_File = "Cargue UNIF"

        else: 
            Type_File = f"---- Bases para CRUCE ----"
            extension = "0csv"
            Name_File = "Cruce Multimarca"
            
        delimiter = ";"
        output_path = f'{Directory_to_Save}{Type_File}'
        Name_File = f'BD {Name_File}'
        
        if extension == "csv":
            save_to_csv(Data_Frame, output_path, Name_File, Partitions, delimiter)
            convert_csv_to_parquet(output_path, Directory_to_Save)
        else:
            save_to_0csv(Data_Frame, output_path, Name_File, Partitions, delimiter)

    def Renamed_column(self, Data_Root: "DataFrame") -> "DataFrame":
    
        # Create a dictionary for bulk column renaming
        rename_mapping = {
            "1_": "Documento",
            "2_": "Cuenta",
            "3_": "CRM_Origen",
            "4_": "Edad de Deuda",
            "5_": "Potencial_Mark",
            "6_": "PrePotencial_Mark",
            "7_": "Write_Off_Mark",
            "8_": "Monto inicial",
            "9_": "Mod_Init_Cta",
            "10_": "Deuda_Real_Cuenta",
            "11_": "Bill_CycleName",
            "12_": "Nombre Campana",
            "13_": "Debt_Age_Inicial",
            "14_": "Nombre_Casa_de_Cobro",
            "15_": "Fecha_de_Asignacion",
            "16_": "Deuda_Gestionable",
            "17_": "Direccion_Completa",
            "18_": "Fecha_Final",
            "19_": "Segmento",
            "20_": "Documento_Limpio",
            "21_": "[AccStsName?]",
            "22_": "Ciudad",
            "23_": "Inbox_Name",
            "24_": "Nombre_del_Cliente",
            "25_": "Id_de_Ejecucion",
            "26_": "Fecha_de_Vencimiento",
            "27_": "Numero_Referencia_de_Pago",
            "28_": "MIN",
            "29_": "Plan",
            "30_": "Cuotas_Aceleradas",
            "31_": "Fecha_de_Aceleracion",
            "32_": "Valor_Acelerado",
            "33_": "Intereses_Contingentes",
            "34_": "Intereses_Corrientes_Facturados",
            "35_": "Intereses_por_mora_facturados",
            "36_": "Iva_Intereses_Contigentes_Facturado",
            "37_": "Iva Intereses Corrientes_Facturados",
            "38_": "Iva_Intereses_por_Mora_Facturado",
            "39_": "Precio_Subscripcion",
            "40_": "Codigo_de_proceso",
            "41_": "Customer_Type_Id",
            "42_": "Refinancied_Mark",
            "43_": "Discount",
            "44_": "Permanencia",
            "45_": "Deuda_sin_Permanencia",
            "46_": "Telefono_1",
            "47_": "Telefono_2",
            "48_": "Telefono_3",
            "49_": "Telefono_4",
            "50_": "Email",
            "51_": "Active_Lines",
            "53_": "Marca_Asignada",
            "54_": "Cuenta_Next",
            "55_": "Valor_Deuda",
            "56_": "Segmento_CamUnif",
            "57_": "Rango_Deuda",
            "58_": "Tipo_Base",
            "63_": "Monitor",
            "64_": "Valor Scoring",
            "65_": "Cuotas Pactadas",
            "66_": "Cuotas_Facturadas",
            "67_": "Cuotas Pendientes",
            "68_": "Fecha Digitacion/Activacion",
            "Multiproducto": "Multiproducto",
            
            # New columns added in Function_Complete that need to be renamed/retained
            "Tipo_Documento": "Tipo_Documento"
        }

        # 1. Rename columns using the dictionary
        Data_Root = Data_Root.rename(rename_mapping)

        # 2. Add new columns with constant values (Equivalent to withColumn/lit/date_format)
        Data_Root = Data_Root.with_columns([
            pl.col("62_").alias("Fecha_Ingreso"),
            
            # Add columns with empty string literal
            lit("").alias("Fecha_Salida"),
            lit("").alias("Valor_Pago"),
            lit("").alias("Valor_Pago_Real"),
            lit("").alias("Fecha_Ult_Pago"),
            lit("").alias("Tipo_Pago"),
            lit("").alias("Descuento"),
            lit("").alias("Excl_Descuento"),
            lit("SI").alias("Liquidacion"),
        ])

        # 3. Final Column Selection (Order and select only necessary columns)
        columns_to_list = [
            "Documento", "Cuenta", "CRM_Origen", "Edad de Deuda", "Potencial_Mark", "PrePotencial_Mark",
            "Write_Off_Mark", "Monto inicial", "Mod_Init_Cta", "Deuda_Real_Cuenta", "Bill_CycleName",
            "Nombre Campana", "Debt_Age_Inicial", "Nombre_Casa_de_Cobro", "Fecha_de_Asignacion",
            "Deuda_Gestionable", "Direccion_Completa", "Fecha_Final", "Segmento", "Documento_Limpio",
            "[AccStsName?]", "Ciudad", "Inbox_Name", "Nombre_del_Cliente", "Id_de_Ejecucion",
            "Fecha_de_Vencimiento", "Numero_Referencia_de_Pago", "MIN", "Plan", "Cuotas_Aceleradas",
            "Fecha_de_Aceleracion", "Valor_Acelerado", "Intereses_Contingentes", "Intereses_Corrientes_Facturados",
            "Intereses_por_mora_facturados", "Iva_Intereses_Contigentes_Facturado",
            "Iva Intereses Corrientes_Facturados", "Iva_Intereses_por_Mora_Facturado", "Precio_Subscripcion",
            "Codigo_de_proceso", "Customer_Type_Id", "Refinancied_Mark", "Discount", "Permanencia",
            "Deuda_sin_Permanencia", "Telefono_1", "Telefono_2", "Telefono_3", "Telefono_4", "Email",
            "Active_Lines", "Monitor", "Valor Scoring", "Cuotas Pactadas", "Cuotas_Facturadas", "Cuotas Pendientes", "Fecha Digitacion/Activacion",
            "Marca_Asignada", "Cuenta_Next", "Valor_Deuda", "Segmento_CamUnif", "Rango_Deuda", "Multiproducto", "Tipo_Base", 
            "Tipo_Documento", "Fecha_Ingreso", "Fecha_Salida", "Valor_Pago", "Valor_Pago_Real", "Fecha_Ult_Pago", "Tipo_Pago", "Descuento", 
            "Excl_Descuento", "Liquidacion"
        ]
        
        Data_Root = Data_Root.select(columns_to_list)
        
        return Data_Root
    
    def partition_DATA(self):
        """
        Manually reads a CSV file, detects its delimiter and encoding, and
        splits its contents into a specified number of partition files.
        """
        self.digit_partitions()
        
        # Get necessary data from class attributes
        file = self.file_path
        root = self.folder_path
        try:
            partitions = int(self.partitions)
            if partitions <= 0:
                partitions = 1
        except ValueError:
            print("WARNING: Invalid partition number detected, defaulting to 1.")
            partitions = 1


        # Create the folder for partitions
        partition_folder = os.path.join(root, "--- PARTITIONS ----")
        os.makedirs(partition_folder, exist_ok=True)

        delimiter = None
        encoding_detected = None

        try:
            # 1. Detect delimiter and encoding
            # Try reading the file with common encodings
            for encoding in ['utf-8', 'latin-1', 'ISO-8859-1']:
                try:
                    # Use io.open for explicit encoding handling
                    with io.open(file, 'r', encoding=encoding) as f:
                        first_line = f.readline()
                        
                        # Delimiter detection logic
                        if ';' in first_line:
                            delimiter = ';'
                        elif ',' in first_line:
                            delimiter = ','
                        elif '\t' in first_line:
                            delimiter = '\t'
                        else:
                            # If no common delimiter is found, continue to the next encoding check
                            continue 

                        encoding_detected = encoding
                        break # Stop on successful detection
                except UnicodeDecodeError:
                    continue # Try next encoding

            if not encoding_detected or not delimiter:
                raise ValueError("Could not determine the file encoding or delimiter.")

            print(f"Detected delimiter: {delimiter}")
            print(f"Detected encoding: {encoding_detected}")

            # 2. Read all data
            with io.open(file, 'r', encoding=encoding_detected) as origin_file:
                rows = origin_file.readlines()

                # Separate the header from the rest of the rows
                header = rows[0]
                data_rows = rows[1:]

                # Calculate rows per partition
                num_data_rows = len(data_rows)
                rows_per_partition = num_data_rows // partitions
                end = 0 # Initialize end index

                # 3. Write partitions
                for i in range(partitions):
                    start = i * rows_per_partition
                    # Ensure the last partition captures all remaining rows calculated by the integer division
                    end = (i + 1) * rows_per_partition if i < partitions - 1 else num_data_rows

                    # Partition file name (with leading zero for i+1)
                    partition_name = os.path.join(partition_folder, f"Particion_{i+1:02}.csv")

                    # Write header and data rows to the partition file
                    with io.open(partition_name, 'w', encoding='utf-8') as file_output:
                        file_output.write(header)
                        file_output.writelines(data_rows[start:end])

                # 4. Handle remainder (if any, although covered by the last loop iteration)
                # This check ensures robustness if the division leaves unassigned rows
                if end < num_data_rows:
                    partition_name = os.path.join(partition_folder, f"Particion_{partitions+1:02}.csv")
                    print(f"WARNING: Creating extra partition for {num_data_rows - end} remaining rows.")
                    with io.open(partition_name, 'w', encoding='utf-8') as file_output:
                        file_output.write(header)
                        file_output.writelines(data_rows[end:])

            print(f"Successfully created partitions in: {partition_folder}")
            return partition_folder

        except Exception as e:
            print(f"Error during partitioning: {e}")
            return None

    def convert_csv_to_parquet_bd(self):
        """
        Minimalist CSV to Parquet converter with encoding detection.
        Skips processing if the input file is already a Parquet file.
        """
        file = self.file_path
        root = self.folder_path
        separator = ";"
        
        # --- Check if file is already Parquet ---
        if str(file).lower().endswith('.parquet'):
            print(f"⏩ Skipping: {file} is already a Parquet file.")
            pass 
            return # Exit the function

        # --- Continue with CSV conversion if it's not a Parquet file ---
        if root is None:
            root = Path(file).parent

        root = Path(root)

        # Generate output filename with date
        output_filename = f"Conversion_{datetime.now().strftime('%Y%m%d')}.parquet"
        output_file = root / output_filename
        
        if output_file.exists():
            print(f"❌ Output file already exists: {output_file}")
            return

        print(f"🔄 Converting: {file} -> {output_file}")
        
        try:
            # Try different encodings for CSV
            encodings = ['utf-8', 'latin1', 'iso-8859-1', 'windows-1252']
            df = None
            
            for encoding in encodings:
                try:
                    # We only need read_csv here because parquet was already skipped
                    df = pl.read_csv(
                        file, 
                        separator=separator,
                        truncate_ragged_lines=True,
                        ignore_errors=True,
                        infer_schema_length=100000,
                        encoding=encoding
                    )
                    print(f"✅ Successfully read with encoding: {encoding}")
                    break
                except Exception as e:
                    print(f"❌ Failed with encoding {encoding}: {e}")
                    continue
            
            if df is None:
                print(f"❌ Could not read {file} with any encoding")
                return
                
            # Write parquet file
            df.write_parquet(output_file)
            print(f"✅ Conversion completed: {len(df):,} rows, {len(df.columns)} cols")
            print(f"📁 Output: {output_file}")
            
        except Exception as e:
            print(f"❌ Error processing {file}: {e}")
            
    def process_cta_cruice(self, df):
        """
        Cleans column '2_' by removing dots and creating 'CTA_CRUCE'
        """
        return df.with_columns(
            pl.col("2_")
            .cast(pl.String) # Asegurar que sea texto
            .str.replace_all(r"\.", "")
            .alias("CTA_CRUCE")
        )
        
    def handle_files_and_join(self, Data_Root, src_folder):
        # 1. Setup paths
        script_dir = os.path.dirname(os.path.abspath(__file__))
        temp_folder = os.path.join(script_dir, "temp_spark_30042000")
        
        if not os.path.exists(temp_folder):
            os.makedirs(temp_folder, exist_ok=True)
            print(f"📁 Temporary folder created: {temp_folder}")

        try:
            # 2. Copy files
            copied_count = 0
            for file in os.listdir(src_folder):
                if file.lower().endswith('.csv'):
                    shutil.copy2(os.path.join(src_folder, file), os.path.join(temp_folder, file))
                    copied_count += 1
            
            print(f"✅ Successfully copied {copied_count} files to temporary directory")

            # 3. Read and Union CSV files
            csv_files = glob.glob(os.path.join(temp_folder, "*.csv"))
            
            if not csv_files:
                print("⚠️ Warning: No CSV files found for joining.")
                return Data_Root

            df_list = []
            for f in csv_files:
                # We force 'CUENTA' to be String during the read to avoid type mismatch
                temp_df = pl.read_csv(
                    f, 
                    separator=";", 
                    infer_schema_length=10000,
                    schema_overrides={"CUENTA": pl.String} # Force String type here
                )
                df_list.append(temp_df)
            
            df_cruce = pl.concat(df_list).unique(subset=["CUENTA"])
            print(f"📖 CSV data loaded. Total unique rows for join: {df_cruce.height}")

            # 4. Perform Left Join
            # Both keys (CTA_CRUCE and CUENTA) are now String
            result = Data_Root.join(
                df_cruce.select(["CUENTA", "TIPO_BASE"]),
                left_on="CTA_CRUCE",
                right_on="CUENTA",
                how="left"
            )

            # 5. Update column '12_' and cleanup
            result = result.with_columns(
                pl.coalesce(pl.col("TIPO_BASE"), pl.col("12_")).alias("12_")
            ).drop(["CTA_CRUCE", "TIPO_BASE"])

            print("🔗 Join completed and column '12_' updated.")
            return result

        except Exception as e:
            print(f"❌ Error during processing: {e}")
            return Data_Root

        finally:
            # 6. Cleanup
            if os.path.exists(temp_folder):
                shutil.rmtree(temp_folder)
                print("🗑️ Temporary folder deleted successfully")
        