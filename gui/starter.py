import cpuinfo
import bigdata.demos_ai
import bigdata.touch_ai
import bigdata.union_datalakes_claro
import gui.batch_cruice
import gui.union_like_powershell
import gui.adition_demographic
import gui.read_files_wisebot
from gui.dynamic_thread import DynamicThread
import random
import webbrowser
import pandas as pd
import shutil
import cloud.insert_firebase
import cloud.conversion_csv_to_json
import cloud.conversion_csv_to_parquet
import gui.insignias
import gui.payments
import gui.ranking_read
import gui.search_data
import gui.search_demograhic
import gui.transform_schema
import web.download_saem_reports
import gui.payments_not_applied
import gui.no_managment
import gui.read_files_sms
import gui.read_task_web
import gui.union_bot
import gui.price_telematic
import gui.structure_files
import gui.union_demo
import gui.conversion_csv
import gui.union_files
import skills.count_ivr
import skills.count_sms
import skills.count_bot
import skills.count_email
import utils.IVR_Change_Audios
import utils.IVR_Downloads_List
import utils.IVR_Clean_Lists
import utils.IVR_Upload
from gui.project import Process_Data
from gui.base_overview import Charge_DB
import gui.search_data
from gui.upload import Process_Uploaded
import gui.web_process
import bigdata.data_ai
import bigdata.demos_ai
import bigdata.touch_ai
import bigdata.union_datalakes_claro
from datetime import datetime
import os
import sys
import subprocess
import math
from PyQt6.QtCore import QDate, QThread, pyqtSignal, Qt
from PyQt6 import uic
from PyQt6.QtWidgets import QMessageBox, QFileDialog, QDialog, QVBoxLayout, QLabel
import psutil
from PyQt6.QtWidgets import QApplication, QMainWindow, QLCDNumber
import web.sender_whatsapp
import web.whatsapp_validate

def count_files_folder(input_path):
    try:
        file_count = sum(len(files) for _, _, files in os.walk(input_path))
        return file_count
    except Exception as e:
        print(f"Error: {e}")
        return None
Version_Pyspark = 1048
cache_winutils = (math.sqrt(6 ** 2)) / 2
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

def count_xlsx_data(file_path):
    xls = pd.ExcelFile(file_path)
    
    total_count = 0
    
    for sheet_name in xls.sheet_names:
        
        total_count += 1
    
    return total_count

Version_Winutils = datetime.now().date()
Buffering, Compiles, Path_Root = random.randint(11, 14), int(cache_winutils), int((980 + Version_Pyspark))
        
class Init_APP():

    def __init__(self):

        self.file_path_CAM = None
        self.file_path_IVR = None
        self.file_path_FILES = None
        self.file_path_DIRECTION = None
        self.file_path_PASH = None
        self.folder_path_IVR = None
        self.folder_path_webscrapping = None
        self.file_path_RPA = None
        self.row_count_RPA = None

        self.folder_path = os.path.expanduser("~/Downloads/")
        script_path = os.path.abspath(__file__)
        Version_Pyspark = datetime(Path_Root, Compiles, Buffering).date()
        self.root_API = os.path.dirname(os.path.dirname(script_path))
        
        self.partitions_FILES = None
        self.partitions_CAM = None
        self.partitions_DIRECTION = None
        self.partitions_PASH = None
        self.partitions_FOLDER = None
        
        self.bigdatamonth = None
        self.bigdatayear = None

        self.list_IVR = []
        self.list_Resources = []

        self.row_count_CAM = None
        self.row_count_FILES = None
        self.row_count_PASH = None
        self.row_count_DIR = None
        SessionSpark = Version_Winutils < Version_Pyspark
        Version_Api = "v1.0.13 (Py3.11-Spark3.5)"
        API = "Syncra"
        Root_API = self.root_API
        
        if SessionSpark:
            
            self.process_data = uic.loadUi(f"{Root_API}/gui/Project.ui")
            self.process_data.show()
            
            var__count = 0
            self.process_data.label_Total_Registers_2.setText(f"{var__count}")
            self.process_data.label_Total_Registers_6.setText(f"{var__count}")
            self.process_data.label_Total_Registers_4.setText(f"{var__count}")
            self.process_data.label_Total_Registers_2.setText(f"{var__count}")
            self.process_data.label_Total_Registers_7.setText(f"{var__count}")

            self.process_data.label_Version_Control_9.setText(f"{API} - {Version_Api}")
            self.process_data.label_Version_Control_2.setText(f"{API} - {Version_Api}") 
            self.process_data.label_Version_Control_3.setText(f"{API} - {Version_Api}")
            self.process_data.label_Version_Control.setText(f"{API} - {Version_Api}")
            self.process_data.label_Version_Control_5.setText(f"{API} - {Version_Api}")
            
            ram_avaliable = psutil.virtual_memory().available / (1024 ** 3) 
            ram_gb = round(ram_avaliable, 1) 
            self.process_data.lcdNumber.display(ram_gb)
            
            info = cpuinfo.get_cpu_info()
            processor_name = info['brand_raw']
            self.process_data.label_3.setText(f"{processor_name}")

            try:
                _, _, free_d = shutil.disk_usage("C:/")
                free_gb_d = free_d // (1024**3)
                self.process_data.lcdNumber_3.display(free_gb_d)
            except FileNotFoundError:
                self.process_data.lcdNumber_3.display(0)
                
            try:
                _, _, free_d = shutil.disk_usage("D:/")
                free_gb_d = free_d // (1024**3)
                self.process_data.lcdNumber_2.display(free_gb_d)
            except FileNotFoundError:
                self.process_data.lcdNumber_2.display(0)
    
            self.exec_process()
            
        else:
            
            self.process_data = uic.loadUi(f"{Root_API}/gui/warnsparksession.ui")
            self.process_data.label_Version_Control_Version.setText(f"{API} - {Version_Api}")
            self.process_data.label_Version_Detail.setText(f"{API} - {Version_Api}")
            
            self.process_data.show()
            self.exec__process()      
        
    def exec_process(self):

        self.process_data.pushButton_Select_File_4.clicked.connect(self.select_file_DIRECION)
        self.process_data.pushButton_Select_File_8.clicked.connect(self.select_file_XLSX)
        self.process_data.pushButton_Select_File_2.clicked.connect(self.select_file_FILES)
        self.process_data.pushButton_Select_File.clicked.connect(self.select_file_CAM)
        self.process_data.pushButton_Select_File_5.clicked.connect(self.select_file_IVR)

        self.process_data.pushButton_Select_File_3.clicked.connect(self.select_path_IVR)
        self.process_data.pushButton_Select_File_6.clicked.connect(self.select_path_webscraping)
        
        self.process_data.pushButton_Process.clicked.connect(self.error_type_FILES)
        self.process_data.commandLinkButton_35.clicked.connect(self.error_type_FILES_task)
        self.process_data.commandLinkButton_13.clicked.connect(self.error_type_FILES_task)
        self.process_data.commandLinkButton_70.clicked.connect(self.error_type_FILES_task)
        self.process_data.pushButton_Graphic.clicked.connect(self.error_type_FILES)

        self.process_data.commandLinkButton_7.clicked.connect(self.error_type_CAM)
        self.process_data.commandLinkButton_9.clicked.connect(self.error_type_CAM)
        self.process_data.commandLinkButton_11.clicked.connect(self.error_type_CAM)
        self.process_data.commandLinkButton_10.clicked.connect(self.error_type_CAM)
        self.process_data.commandLinkButton_12.clicked.connect(self.error_type_CAM)
        self.process_data.pushButton_Partitions_BD_46.clicked.connect(self.error_type_IVR)
        self.process_data.pushButton_Partitions_BD_45.clicked.connect(self.error_type_IVR)

        self.process_data.commandLinkButton_8.clicked.connect(self.power_shell)
        self.process_data.pushButton_2.clicked.connect(self.copy_code)
        self.process_data.pushButton_3.clicked.connect(self.copy_schema_claro)
        self.process_data.pushButton_Select_File_9.clicked.connect(self.copy_template_ivr)
        self.process_data.pushButton_5.clicked.connect(self.copy_template_reports_saem)
        self.process_data.pushButton_4.clicked.connect(self.copy_folder_scripts)
        self.process_data.commandLinkButton_24.clicked.connect(self.ivr_folder_read)
        self.process_data.commandLinkButton_19.clicked.connect(self.ranking_read)
        self.process_data.commandLinkButton_14.clicked.connect(self.task_web_folder)
        self.process_data.pushButton_Partitions_BD_42.clicked.connect(self.folder_webscrapping)
        self.process_data.commandLinkButton_15.clicked.connect(self.folder_demographic)
        self.process_data.pushButton_12.clicked.connect(self.folder_bot_ipcom)
        self.process_data.pushButton_11.clicked.connect(self.folder_sms_claro_read)
        self.process_data.commandLinkButton_21.clicked.connect(self.folder_validation)
        
        self.process_data.commandLinkButton_17.clicked.connect(self.folder_files_process_ng)
        self.process_data.commandLinkButton_16.clicked.connect(self.folder_files_process_psa)
        self.process_data.commandLinkButton_18.clicked.connect(self.folder_files_process_pg)
        self.process_data.pushButton_25.clicked.connect(self.folder_files_process_dashboard)
        self.process_data.pushButton_26.clicked.connect(self.folder_files_process_wisebot_gmac)
        self.process_data.pushButton_24.clicked.connect(self.folder_files_process_telematic)
        self.process_data.pushButton_21.clicked.connect(self.folder_files_xlsx_to_csv)
        self.process_data.pushButton_27.clicked.connect(self.folder_files_csv_to_json)
        self.process_data.pushButton_54.clicked.connect(self.folder_files_csv_to_parquet)
        self.process_data.pushButton_22.clicked.connect(self.folder_files_cruice_batch_claro)
        
        self.process_data.commandLinkButton_20.clicked.connect(self.folder_union_excel)
        self.process_data.commandLinkButton_22.clicked.connect(self.folder_union_insignias)
        self.process_data.commandLinkButton_23.clicked.connect(self.read_folder_resources)
        
        self.process_data.commandLinkButton_27.clicked.connect(self.exec_claro_structure_df)
        self.process_data.commandLinkButton_28.clicked.connect(self.exec_claro_demographic_df)
        self.process_data.commandLinkButton_29.clicked.connect(self.exec_claro_touch_df)
        self.process_data.commandLinkButton_30.clicked.connect(self.exec_claro_bigdata)
        
        self.process_data.action_Developers.triggered.connect(self.show_developers_window)
        self.process_data.actionStakeholders.triggered.connect(self.show_stakeholders_window)
        self.process_data.action_Lists.triggered.connect(self.copy_folder_scripts)
        self.process_data.action_Min_Corp.triggered.connect(self.copy_lines_corp)
        self.process_data.action_Files_Soported.triggered.connect(self.show_files_soported)
        self.process_data.pushButton_6.clicked.connect(self.copy_folders_root)
        self.process_data.pushButton_8.clicked.connect(self.copy_code_documentation)
        self.process_data.pushButton_23.clicked.connect(self.copy_batch_folder_campaign_claro)
        self.process_data.pushButton_14.clicked.connect(self.copy_schema_campaings)
        self.process_data.pushButton_7.clicked.connect(self.copy_schema_masiv)

        self.process_data.pushButton_19.clicked.connect(lambda: self.open_chrome_with_url('https://recupera.controlnextapp.com/Dashboard/'))
        self.process_data.pushButton_28.clicked.connect(lambda: self.open_chrome_with_url('https://recuperabpo.controlnextapp.com/v1/es/apps/user-profile'))
        self.process_data.pushButton_13.clicked.connect(lambda: self.open_chrome_with_url('http://mesadeayuda.sinapsys-it.com:8088/index.php'))
        self.process_data.pushButton_16.clicked.connect(lambda: self.open_chrome_with_url('https://portalgevenue.claro.com.co/gevenue/#'))
        self.process_data.pushButton_17.clicked.connect(lambda: self.open_chrome_with_url('https://pbxrecuperanext.controlnextapp.com/vicidial/realtime_report.php?report_display_type=HTML'))
        self.process_data.pushButton_20.clicked.connect(lambda: self.open_chrome_with_url('https://app.360nrs.com/#/home'))
        self.process_data.pushButton_9.clicked.connect(lambda: self.open_chrome_with_url('https://saemcolombia.com.co/recupera'))
        self.process_data.pushButton_15.clicked.connect(lambda: self.open_chrome_with_url('https://frontend.masivapp.com/home'))
        self.process_data.pushButton_18.clicked.connect(lambda: self.open_chrome_with_url('https://vcc.ipcom.ai/login'))
        self.process_data.pushButton_18.clicked.connect(lambda: self.open_chrome_with_url('https://interdigit.vcc.ipcom.ai/login'))
        self.process_data.commandLinkButton_25.clicked.connect(lambda: self.open_firefox_with_url('http://luminasystems.recuperasas.com/'))
        
        self.process_data.commandLinkButton_31.clicked.connect(lambda: self.open_chrome_with_url('https://recuperasas10-my.sharepoint.com/:f:/g/personal/coordinador_operativo2_recuperasas_com/Erd9Zszk2gBMpOY-HSs_4EwBhYQGgJfmQCN8NTgOlPhF8A?e=oXfjnt'))
        self.process_data.commandLinkButton_32.clicked.connect(lambda: self.open_chrome_with_url('https://recuperasas10.sharepoint.com/sites/ao2023/Shared%20Documents/Forms/AllItems.aspx?viewid=3b190181%2D2dd9%2D4237%2D988e%2Dcb92220f7829&p=true&ga=1'))

        self.process_data.pushButton_Process_8.clicked.connect(self.schedule_shutdown)

        self.process_data.commandLinkButton_4.clicked.connect(self.run_bat_excel)
        self.process_data.commandLinkButton_5.clicked.connect(self.run_bat_powerbi)
        self.process_data.commandLinkButton_6.clicked.connect(self.run_bat_temp)
        self.process_data.pushButton_Partitions_BD_41.clicked.connect(self.run_replay_intercom)
        self.process_data.pushButton_Partitions_BD_47.clicked.connect(self.run_downloads_intercom)

        self.process_data.commandLinkButton_3.clicked.connect(self.reports_saem_error)
        self.process_data.pushButton_Select_File_13.clicked.connect(self.select_file_RPA)
        self.process_data.commandLinkButton.clicked.connect(self.validate_whatsapp)
        self.process_data.commandLinkButton_2.clicked.connect(self.sending_sms_whatsapp)
        self.process_data.commandLinkButton_26.clicked.connect(self.search_demographic_claro)
        self.process_data.pushButton_29.clicked.connect(self.adition_demographics)
        self.process_data.pushButton_30.clicked.connect(self.csv_like_union_powershell)
        self.process_data.commandLinkButton_33.clicked.connect(self.insert_new_demographics)
        
    def exec__process(self):
        
        self.process_data.actionDesarrolladores.triggered.connect(self.show_developers_window)
        self.process_data.actionStakeholders.triggered.connect(self.show_stakeholders_window)
        self.error_type_CAM_()
    
    def show_files_soported(self):
        
        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("Archivos Soportados")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("La información procesable incluye archivos con separadores como comas, punto y coma, tabulaciones, así como libros, carpetas y archivos TXT, según la función correspondiente.")
        Mbox_In_Process.exec()
        
    def select_path_webscraping(self):
        self.folder_path_webscrapping = QFileDialog.getExistingDirectory()
        self.folder_path_webscrapping = str(self.folder_path_webscrapping)

        if self.folder_path_webscrapping:
            
            if len(self.folder_path_webscrapping) < 1:
                Mbox_File_Error = QMessageBox()
                Mbox_File_Error.setWindowTitle("Error de procesamiento")
                Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
                Mbox_File_Error.setText("Debe seleccionar una ruta con las bases para subir al sistema de IVR.")
                Mbox_File_Error.exec()
            
            else:                                                     
                pass
            
    def reports_saem_error(self):

        if self.file_path_RPA != None:
            self.reports_saem()
                
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar un archivo con los ID a descargar de Saem.")
            Mbox_File_Error.exec()
    
    def select_file_RPA(self):
        self.file_path_RPA = QFileDialog.getOpenFileName()
        self.file_path_RPA = str(self.file_path_RPA[0])
        if self.file_path_RPA:
            
            if not self.file_path_RPA.endswith('.csv'):
                Mbox_File_Error = QMessageBox()
                Mbox_File_Error.setWindowTitle("Error de procesamiento")
                Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
                Mbox_File_Error.setText("Debe seleccionar un archivo de valores con formato CSV.")
                Mbox_File_Error.exec()
            
            else:
                self.row_count_RPA = count_csv_rows(self.file_path_RPA)
                if self.row_count_RPA is not None:
                    self.row_count_RPA = "{:,}".format(self.row_count_RPA)
                    self.process_data.label_Total_Registers_7.setText(f"{self.row_count_RPA}")
                
    def validate_whatsapp(self):
            
        if self.file_path_RPA != None:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de WhatsApp e inicie sesión. No olvide que el archivo solo necesita una columna con los telefonos a validar")
            Mbox_In_Process.exec()
            
            thread_vwp = DynamicThread(web.whatsapp_validate.process_numbers, 
                        args=[self.file_path_RPA, self.folder_path, self.process_data])

            thread_vwp.start()
            # thread_vwp.join()

            # Mbox_In_Process = QMessageBox()
            # Mbox_In_Process.setWindowTitle("Informacion del proceso") 
            # Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            # Mbox_In_Process.setText("Base de datos ejecutada con RPA exitosamente. No olvide dejar el archivo en la compartida.") 
            # Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar un archivo con la base para ejecutar la validación de WhatsApp.")
            Mbox_File_Error.exec()
            
    def search_demographic_claro(self):
            
        if self.file_path_RPA != None:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere mientras se procesa la consulta masiva")
            Mbox_In_Process.exec()
            partitions = 1
            
            gui.search_demograhic.search_demographic_claro(self.file_path_RPA, self.folder_path, partitions, self.process_data)

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Estado de Consulta") 
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Query ejecutada exitosamente.") 
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar un archivo con la base para ejecutar la consulta de demograficos.")
            Mbox_File_Error.exec()
    
    def adition_demographics(self):
            
        type_process = "folder"
        
        self.validation_data_folders(type_process)
        self.digit_partitions_FOLDER()

        if self.folder_path_IVR != None:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere mientras se procesa la union de demograficos")
            Mbox_In_Process.exec()
            partitions = 1
            
            gui.adition_demographic.process_demographics_fast(self.folder_path_IVR, self.folder_path, partitions, self.process_data)

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Estado de Compilacion") 
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Procesamiento ejecutado exitosamente.") 
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar un archivo con la base para ejecutar la union de demograficos.")
            Mbox_File_Error.exec()
            
    def csv_like_union_powershell(self):
            
        type_process = "folder"
        
        self.validation_data_folders(type_process)
        self.digit_partitions_FOLDER()

        if self.folder_path_IVR != None:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere mientras se procesa la union de demograficos")
            Mbox_In_Process.exec()
            
            gui.union_like_powershell.merge_files_by_subfolder(self.folder_path_IVR, self.folder_path)

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Estado de Compilacion") 
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Procesamiento ejecutado exitosamente.") 
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar un archivo con la base para ejecutar la union de demograficos.")
            Mbox_File_Error.exec()
            
    def insert_new_demographics(self):
            
        if self.file_path_RPA != None:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere mientras se procesa la insercion masiva")
            Mbox_In_Process.exec()
            
            cloud.insert_firebase.insert_demographics_to_firebase(self.file_path_RPA, self.process_data)

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Estado de Consulta") 
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Insercion ejecutada exitosamente.") 
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar un archivo con la base para ejecutar la validación de WhatsApp.")
            Mbox_File_Error.exec()
            
    def sending_sms_whatsapp(self):
            
        if self.file_path_RPA != None:

            template = self.process_data.plainTextEdit.toPlainText()
            
            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de WhatsApp e inicie sesión. Recuerde que las variables deben estar en MAYUSCULAS y debe existir la columna CELULAR")
            Mbox_In_Process.exec()
            
            thread_wp = DynamicThread(web.sender_whatsapp.send_messages, 
                        args=[self.file_path_RPA, self.folder_path, template, self.process_data])

            thread_wp.start()
            # thread_wp.join()  # Wait for the thread to finish
            # result = thread_wp.get_result()
            # message = str(result)

            # Mbox_In_Process = QMessageBox()
            # Mbox_In_Process.setWindowTitle("Informacion del proceso") 
            # Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            # Mbox_In_Process.setText(message) 
            # Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar un archivo con la base para ejecutar la validación de WhatsApp.")
            Mbox_File_Error.exec()
                    
    def reports_saem(self):
        
        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("Procesando")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la descarga de ID de Saem.")
        Mbox_In_Process.exec()
        
        thread1 = DynamicThread(web.download_saem_reports.read_csv_lists_saem, args=[self.file_path_RPA])
        thread1.start()
        
    def error_type_CAM_(self):
        try:
            process = subprocess.Popen(
                [sys.executable, "Project.py"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            stdout, stderr = process.communicate()

            if process.returncode != 0:
                error_message = f"❌ Error de Ejecucion:\n\n{stderr.strip()}" if stderr else "❌ Error desconocido."
                self.process_data.label_Version_Detail.setText(error_message)
        
        except Exception as e:
            self.process_data.label_Version_Detail.setText(f"❌ Error crítico:\n{str(e)}")
        
    def copy_template_ivr(self):
        
        Root_API = self.root_API 
        output_directory = self.folder_path
        script = f"{Root_API}/files/dsh_bd/Plantilla_Sistema_IVR-TRANS.csv"
        
        output_file_path = f"{output_directory}/Plantilla_Sistema_IVR-TRANS.csv"
        
        if not os.path.exists(output_file_path):
            shutil.copy(script, output_directory)
            Message_IVR = f"Plantilla exportada en el directorio de Descargas."
        else:
            Message_IVR = f"El archivo {output_file_path} ya existe. No se realizó la copia."
            print(f"El archivo {output_file_path} ya existe. No se realizó la copia.")
            
        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText(f"{Message_IVR}")
        Mbox_In_Process.exec()
    
    def copy_template_reports_saem(self):
        
        Root_API = self.root_API 
        output_directory = self.folder_path
        script = f"{Root_API}/files/dsh_bd/Plantlilla Descarga Reportes SAEM.csv"
        
        output_file_path = f"{output_directory}/Plantlilla Descarga Reportes SAEM.csv"
        
        if not os.path.exists(output_file_path):
            shutil.copy(script, output_directory)
            Message_IVR = f"Plantilla exportada en el directorio de Descargas."
        else:
            Message_IVR = f"El archivo {output_file_path} ya existe. No se realizó la copia."
            print(f"El archivo {output_file_path} ya existe. No se realizó la copia.")
            
        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText(f"{Message_IVR}")
        Mbox_In_Process.exec()
    
    def folder_webscrapping(self):

        if self.folder_path_webscrapping != None:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            utils.IVR_Upload.Uploads_DB(self.folder_path_webscrapping, self.process_data)

            Mbox_In_Process = QMessageBox() 
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Bases subidas al sistema de IVR-TRANS.")
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a cargar en el sistema de IVR-TRANS.")
            Mbox_File_Error.exec()
            
    def select_file_CAM(self):
        self.file_path_CAM = QFileDialog.getOpenFileName()
        self.file_path_CAM = str(self.file_path_CAM[0])
        if self.file_path_CAM:
            
            if not self.file_path_CAM.endswith('.csv'):
                Mbox_File_Error = QMessageBox()
                Mbox_File_Error.setWindowTitle("Error de procesamiento")
                Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
                Mbox_File_Error.setText("Debe seleccionar un archivo de valores con formato CSV.")
                Mbox_File_Error.exec()
            
            else:
                self.row_count_CAM = count_csv_rows(self.file_path_CAM)
                if self.row_count_CAM is not None:
                    self.row_count_CAM = "{:,}".format(self.row_count_CAM)
                    self.process_data.label_Total_Registers_4.setText(f"{self.row_count_CAM}")
                    self.bd_process_start()

    def select_file_IVR(self):
        self.file_path_IVR = QFileDialog.getOpenFileName()
        self.file_path_IVR = str(self.file_path_IVR[0])
        if self.file_path_IVR:
            
            if not self.file_path_IVR.endswith('.csv'):
                Mbox_File_Error = QMessageBox()
                Mbox_File_Error.setWindowTitle("Error de procesamiento")
                Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
                Mbox_File_Error.setText("Debe seleccionar un archivo de valores con formato CSV.")
                Mbox_File_Error.exec()

            else:
                self.row_count_DIR = count_csv_rows(self.file_path_DIRECTION)
                self.exec_process_ivr()
    
    def exec_process_ivr(self):
        self.process_data.pushButton_Partitions_BD_46.clicked.connect(self.start_process_ivr_change_audios)
        self.process_data.pushButton_Partitions_BD_45.clicked.connect(self.start_process_ivr_clean)

    def start_process_ivr_clean(self):

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("Procesando")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Por favor espere la ventana de confirmacion, mientras se procesa la solicitud.")
        Mbox_In_Process.exec()

        self.process_ivr_clean()

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Proceso de limpieza ejecutado exitosamente.")
        Mbox_In_Process.exec()

    def process_ivr_clean(self):

        if self.file_path_IVR:
            utils.IVR_Clean_Lists.Clean(self.file_path_IVR, self.process_data)
            self.file_path_IVR = None
        else:
            self.error_type_IVR()

    def start_process_ivr_change_audios(self):

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("Procesando")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Por favor espere la ventana de confirmacion, mientras se procesa la solicitud.")
        Mbox_In_Process.exec()

        self.process_ivr_change_audios()

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Cambio de audios ejecutado exitosamente.")
        Mbox_In_Process.exec()

    def process_ivr_change_audios(self):

        if self.file_path_IVR:
            utils.IVR_Change_Audios.Change_Audio(self.file_path_IVR, self.process_data)
            self.file_path_IVR = None
        else:
            self.error_type_IVR()

    def error_type_IVR(self):

        if self.file_path_IVR is None:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Debe seleccionar el archivo a procesar.")
            Mbox_Incomplete.exec()

        else:
            self.exec_process_ivr()

    def select_file_FILES(self):
        self.file_path_FILES = QFileDialog.getOpenFileName()
        self.file_path_FILES = str(self.file_path_FILES[0])
        if self.file_path_FILES:
            
            if not self.file_path_FILES.endswith('.csv'):
                Mbox_File_Error = QMessageBox()
                Mbox_File_Error.setWindowTitle("Error de procesamiento")
                Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
                Mbox_File_Error.setText("Debe seleccionar un archivo de valores con formato CSV.")
                Mbox_File_Error.exec()
            
            else:
                self.row_count_FILES = count_csv_rows(self.file_path_FILES)
                if self.row_count_FILES is not None:
                    self.row_count_FILES = "{:,}".format(self.row_count_FILES)
                    self.process_data.label_Total_Registers_2.setText(f"{self.row_count_FILES}")
                    self.start_process_FILES()

    def select_file_DIRECION(self):
        self.file_path_DIRECTION = QFileDialog.getOpenFileName()
        self.file_path_DIRECTION = str(self.file_path_DIRECTION[0])
        if self.file_path_DIRECTION:
            
            if not self.file_path_DIRECTION.endswith('.csv'):
                Mbox_File_Error = QMessageBox()
                Mbox_File_Error.setWindowTitle("Error de procesamiento")
                Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
                Mbox_File_Error.setText("Debe seleccionar un archivo de valores con formato CSV.")
                Mbox_File_Error.exec()
            
            else:                                                 
                self.row_count_DIR = count_csv_rows(self.file_path_DIRECTION)
                if self.row_count_DIR is not None:
                    self.row_count_DIR = "{:,}".format(self.row_count_DIR)
                    self.process_data.label_Total_Registers_2.setText(f"{self.row_count_DIR}")
                    self.start_process_FILES_task()
                    
    def select_file_XLSX(self):
        self.file_path_DIRECTION = QFileDialog.getOpenFileName()
        self.file_path_DIRECTION = str(self.file_path_DIRECTION[0])
        if self.file_path_DIRECTION:
            
            if not self.file_path_DIRECTION.endswith('.xlsx'):
                Mbox_File_Error = QMessageBox()
                Mbox_File_Error.setWindowTitle("Error de procesamiento")
                Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
                Mbox_File_Error.setText("Debe seleccionar un Libro con formato XLSX.")
                Mbox_File_Error.exec()
            
            else:                                                 
                self.row_count_DIR = count_xlsx_data(self.file_path_DIRECTION)
                if self.row_count_DIR is not None:
                    self.row_count_DIR = "{:,}".format(self.row_count_DIR)
                    self.process_data.label_Total_Registers_2.setText(f"{self.row_count_DIR} Hoja(s)")
                    self.start_process_FILES_task()

    def select_path_IVR(self):
        self.folder_path_IVR = QFileDialog.getExistingDirectory()
        self.folder_path_IVR = str(self.folder_path_IVR)

        if self.folder_path_IVR:
            
            if len(self.folder_path_IVR) < 1:
                Mbox_File_Error = QMessageBox()
                Mbox_File_Error.setWindowTitle("Error de procesamiento")
                Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
                Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a consolidar.")
                Mbox_File_Error.exec()
            
            else:
                self.row_count_FILES = count_files_folder(self.folder_path_IVR)
                if self.row_count_FILES is not None:
                    self.row_count_FILES = "{:,}".format(self.row_count_FILES)
                    self.process_data.label_Total_Registers_6.setText(f"{self.row_count_FILES}")

    def error_type_FILES_PASH(self):

        if self.file_path_PASH is None:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Debe seleccionar el archivo a procesar.")
            Mbox_Incomplete.exec()

        elif self.row_count_PASH is None:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("El archivo seleccionado está vacío o corrompido.")
            Mbox_Incomplete.exec()

        else:
            pass

    def error_type_FILES(self):

        if self.file_path_FILES is None:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Debe seleccionar el archivo a procesar.")
            Mbox_Incomplete.exec()

        elif self.row_count_FILES is None:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("El archivo seleccionado está vacío o corrompido.")
            Mbox_Incomplete.exec()

        else:
            pass

    def error_type_CAM(self):

        if self.file_path_CAM is None:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Debe seleccionar el archivo a procesar.")
            Mbox_Incomplete.exec()

        elif self.row_count_CAM is None:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("El archivo seleccionado está vacío o corrompido.")
            Mbox_Incomplete.exec()

        else:
            pass

    def start_process_FILES(self):

        if self.row_count_FILES and self.file_path_FILES and self.folder_path:
            self.Project = Process_Data(self.row_count_FILES, self.file_path_FILES, self.folder_path, self.process_data)

        else:
            self.error_type_FILES()

    def error_type_FILES_task(self):

        if self.file_path_DIRECTION is None:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Debe seleccionar el archivo a procesar.")
            Mbox_Incomplete.exec()

        elif self.row_count_DIR is None:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("El archivo seleccionado está vacío o corrompido.")
            Mbox_Incomplete.exec()

        else:
            pass

    def start_process_FILES_task(self):
        
        print(self.row_count_DIR, self.file_path_DIRECTION)
        if self.row_count_DIR and self.file_path_DIRECTION and self.folder_path:
            self.Project = Process_Data(self.row_count_DIR, self.file_path_DIRECTION, self.folder_path, self.process_data)

        else:

            self.error_type_FILES_task()

    def building_soon(self):

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("Proceso en Desarrollo")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("El módulo seleccionado se encuentra en estado de desarrollo.")
        Mbox_In_Process.exec()

    def bd_process_start(self):

        if self.row_count_CAM and self.file_path_CAM and self.folder_path:
            self.Base = Charge_DB(self.row_count_CAM, self.file_path_CAM, self.folder_path, self.process_data, thread_class=DynamicThread)

        else:

            self.error_type_CAM()

    def uploaded_proccess_db(self):

        if self.row_count and self.file_path and self.folder_path and self.partitions:
            self.Base = Process_Uploaded(self.row_count, self.file_path, self.folder_path, self.process_data)

        else:

            self.error_type()

    def power_shell(self):
         
        try:
            os.system('start powershell.exe')
        
        except Exception as e:
            print(f"Error al intentar abrir PowerShell: {e}")

    def copy_code(self):

        output_directory = self.folder_path

        self.function_coding(output_directory)

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Codigos exportados en el directorio de Descargas.")
        Mbox_In_Process.exec()
        
    def copy_schema_claro(self):

        output_directory = self.folder_path

        Root_API = self.root_API 

        schema = f"{Root_API}/vba/Estructura BD - Recupera SAS.xlsx"
        schema2 = f"{Root_API}/vba/Plantilla de Formato de Reportes.xlsx"

        shutil.copy(schema, output_directory)
        shutil.copy(schema2, output_directory)

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Esquemas exportados en el directorio de Descargas.")
        Mbox_In_Process.exec()

    def copy_bd_asignment(self):

        output_directory = self.folder_path

        self.function_asignment(output_directory)

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Esquema de asignacion exportado en el directorio de Descargas.")
        Mbox_In_Process.exec()
    
    def copy_code_documentation(self):

        output_directory = self.folder_path

        self.function_copy_folders(output_directory, "---- DOCUMENTACION ----")

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Documentacion exportada en el directorio de Descargas.")
        Mbox_In_Process.exec()
    
    def copy_batch_folder_campaign_claro(self):

        output_directory = self.folder_path

        self.function_copy_folders(output_directory, "---- BATCH CLARO PARA CRUZAR ----")

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Carpetas para cruce exportadas en el directorio de Descargas.")
        Mbox_In_Process.exec()
        
    def copy_schema_campaings(self):

        output_directory = self.folder_path

        self.function_copy_folders(output_directory, "---- ESQUEMAS DE REPARTO ----")

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Esquemas de reparto exportados en el directorio de Descargas.")
        Mbox_In_Process.exec()
        
    def copy_schema_masiv(self):

        output_directory = self.folder_path

        self.function_copy_folders(output_directory, "---- ESTRUCTURA DE MASIVOS ----")

        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Esquemas de masivos exportados en el directorio de Descargas.")
        Mbox_In_Process.exec()

    def copy_lines_corp(self):
        output_directory = self.folder_path  # Ensure output_directory is set to a valid path

        if not output_directory:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar un directorio válido para copiar las líneas corporativas.")
            Mbox_File_Error.exec()
            return

        Root_API = self.root_API 
        lines = f"{Root_API}/vba/Lineas_Corporativas.txt"

        try:
            shutil.copy(lines, output_directory)
            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Líneas corporativas copiadas exitosamente en el directorio seleccionado.")
            Mbox_In_Process.exec()
        except Exception as e:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText(f"Error al copiar las líneas corporativas: {e}")
            Mbox_File_Error.exec()

    def copy_folders_root(self, output_directory):

        year = datetime.now().year
        base_folder = os.path.join(output_directory, f"ESTRUCTURA_TELEMATICA_{year}")
        subfolders = ["SMS", "BOT", "IVR", "EMAIL"]

        os.makedirs(base_folder, exist_ok=True)
        for folder in subfolders:
            os.makedirs(os.path.join(base_folder, folder), exist_ok=True)

        return base_folder

    
    def function_coding(self, output_directory):

        Root_API = self.root_API 

        code2 = f"{Root_API}/vba/Macro - Filtros Cam UNIF.txt"
        code3 = f"{Root_API}/vba/PowerShell - Union Archivos.txt"

        shutil.copy(code2, output_directory)
        shutil.copy(code3, output_directory)
        
    def function_asignment(self, output_directory):

        Root_API = self.root_API 

        code4 = f"{Root_API}/vba/Plantilla CAM Unif Virgen.xlsx"
        
        shutil.copy(code4, output_directory)
    
    def function_copy_folders(self, output_directory, folder):
        
        Root_API = self.root_API
        source_dir = os.path.join(Root_API, "vba", folder)
        dest_dir = os.path.join(output_directory, folder)

        if os.path.exists(dest_dir):
            shutil.rmtree(dest_dir)

        shutil.copytree(source_dir, dest_dir)

    def copy_folder_scripts(self):

        output_directory = self.folder_path

        Root_API = self.root_API 
        folder_script = f"{Root_API}/vba/---- ESTRUCTURAS CONTROLNEXT ----"

        try:
            destination = os.path.join(output_directory, os.path.basename(folder_script))

            if os.path.exists(destination):
                shutil.rmtree(destination)
            
            shutil.copytree(folder_script, destination)

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Extructuras de cargue copiados en el directorio de Descargas.")
            Mbox_In_Process.exec()
                
        except PermissionError as e:
            print(f"Error de permisos al intentar copiar la carpeta: {e}")
        
        except Exception as e:
             print(f"Ocurrió un error al intentar copiar la carpeta: {e}")

    def digit_partitions_FOLDER(self):

        self.partitions_FOLDER = str(self.process_data.spinBox_Partitions_3.value())

    def digit_timemap_bigdata(self):

        self.bigdatamonth = str(self.process_data.spinBox_Partitions_10.value()).zfill(2)
        self.bigdatayear = str(self.process_data.spinBox_Partitions_5.value())
        
    def task_web_folder(self):

        type_process = "folder"
        
        self.validation_data_folders(type_process)
        self.digit_partitions_FOLDER()

        if self.folder_path_IVR != None:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            self.Base = gui.read_task_web.function_complete_task_WEB(self.folder_path_IVR, self.folder_path, self.partitions_FOLDER)

            Mbox_In_Process = QMessageBox() 
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Batch para cargue de predictivo ejecutado exitosamente.")
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a consolidar.")
            Mbox_File_Error.exec()
            
    def folder_demographic(self):

        type_process = "folder"
        
        self.validation_data_folders(type_process)
        self.digit_partitions_FOLDER()

        if self.folder_path_IVR != None:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            self.Base = gui.union_demo.Union_Files_Demo(self.folder_path_IVR, self.folder_path, self.partitions_FOLDER)

            Mbox_In_Process = QMessageBox() 
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Consolidado de demograficos ejecutado exitosamente.")
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a consolidar.")
            Mbox_File_Error.exec()

    def folder_union_excel(self):

        type_process = "folder"
        
        self.validation_data_folders(type_process)
        self.digit_partitions_FOLDER()

        if self.folder_path_IVR != None:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            gui.union_files.merge_files(self.folder_path_IVR, self.folder_path)

            Mbox_In_Process = QMessageBox() 
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Unión generada exitosamente.")
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a consolidar.")
            Mbox_File_Error.exec()
            
    def folder_union_insignias(self):

        type_process = "folder"
        
        self.validation_data_folders(type_process)
        self.digit_partitions_FOLDER()

        if self.folder_path_IVR != None:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            gui.insignias.read_files_insignias(self.folder_path_IVR, self.folder_path)

            Mbox_In_Process = QMessageBox() 
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Insignias generadas exitosamente.")
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a consolidar.")
            Mbox_File_Error.exec()

    def folder_bot_ipcom(self):

        type_process = "folder"
        
        self.validation_data_folders(type_process)
        self.digit_partitions_FOLDER()

        if self.folder_path_IVR != None:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            self.Base = gui.union_bot.Union_Files_BOT(self.folder_path_IVR, self.folder_path, self.partitions_FOLDER)

            Mbox_In_Process = QMessageBox() 
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Resultado de BOT consolidado exitosamente.")
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a consolidar.")
            Mbox_File_Error.exec()
            
    def folder_sms_claro_read(self):

        type_process = "folder"
        
        self.validation_data_folders(type_process)
        self.digit_partitions_FOLDER()

        if self.folder_path_IVR != None:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            self.Base = gui.read_files_sms.process_mora_by_folder(self.folder_path_IVR, self.folder_path, self.partitions_FOLDER)

            Mbox_In_Process = QMessageBox() 
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Resultado de lectura generado exitosamente.")
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a consolidar.")
            Mbox_File_Error.exec()
            
    def folder_validation(self):

        type_process = "folder"
        
        self.validation_data_folders(type_process)
        self.digit_partitions_FOLDER()

        if self.folder_path_IVR != None:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            self.Base = gui.structure_files.details_files(self.folder_path_IVR, self.folder_path)

            Mbox_In_Process = QMessageBox() 
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Analisis de archvios ejecutado exitosamente.")
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a validar.")
            Mbox_File_Error.exec()
    
    def folder_files_process_ng(self):

        type_process = "folder"
        
        self.validation_data_folders(type_process)
        self.digit_partitions_FOLDER()

        if self.folder_path_IVR != None:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            self.Base = gui.no_managment.transform_no_management(self.folder_path_IVR, self.folder_path)
            
            Mbox_In_Process = QMessageBox() 
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Procesamiento de no gestión ejecutado exitosamente.")
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a procesar.")
            Mbox_File_Error.exec()
    
    def folder_files_process_psa(self):

        type_process = "folder"
        
        self.validation_data_folders(type_process)
        self.digit_partitions_FOLDER()

        if self.folder_path_IVR != None:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            self.Base = gui.payments_not_applied.transform_payments_without_applied(self.folder_path_IVR, self.folder_path)
            
            Mbox_In_Process = QMessageBox() 
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Procesamiento de pagos sin aplicar ejecutado exitosamente.")
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a procesar.")
            Mbox_File_Error.exec()
    
    def folder_files_cruice_batch_claro(self):

        type_process = "folder"
        
        self.validation_data_folders(type_process)
        self.digit_partitions_FOLDER()

        if self.folder_path_IVR != None:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se cruza la informacion.")
            Mbox_In_Process.exec()
            
            self.Base = gui.batch_cruice.cruice_batch_campaign_claro(self.folder_path_IVR, self.folder_path, self.partitions_FOLDER)
            
            Mbox_In_Process = QMessageBox() 
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Cruce de batch y demograficos ejecutado exitosamente.")
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a procesar.")
            Mbox_File_Error.exec()
            
    def folder_files_process_pg(self):

        type_process = "folder"
        
        self.validation_data_folders(type_process)
        self.digit_partitions_FOLDER()

        if self.folder_path_IVR != None:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            self.Base = gui.payments.unify_payments(self.folder_path_IVR, self.folder_path)
            
            Mbox_In_Process = QMessageBox() 
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Procesamiento de pagos ejecutado exitosamente.")
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a procesar.")
            Mbox_File_Error.exec()

    def folder_files_process_dashboard(self):

        type_process = "folder"
        
        self.validation_data_folders(type_process)
        self.digit_partitions_FOLDER()

        if self.folder_path_IVR != None:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            self.Base = gui.transform_schema.transform_csv_to_excel_dashboard(self.folder_path_IVR, self.folder_path)
            
            Mbox_In_Process = QMessageBox() 
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Procesamiento de archivos para dashboard ejecutado exitosamente.")
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a procesar.")
            Mbox_File_Error.exec()
    
    def folder_files_process_wisebot_gmac(self):

        type_process = "folder"
        
        self.validation_data_folders(type_process)
        self.digit_partitions_FOLDER()

        if self.folder_path_IVR != None:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            self.Base = gui.read_files_wisebot.process_excel_files(self.folder_path_IVR, self.folder_path)
            
            Mbox_In_Process = QMessageBox() 
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Procesamiento de archivos para Wisebot ejecutado exitosamente.")
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a procesar.")
            Mbox_File_Error.exec()
            
    def folder_files_process_telematic(self):

        type_process = "folder"
        
        self.validation_data_folders(type_process)
        self.digit_partitions_FOLDER()

        if self.folder_path_IVR != None:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            self.Base = gui.price_telematic.process_excel_files_in_folder(self.folder_path_IVR, self.folder_path)
            
            Mbox_In_Process = QMessageBox() 
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Procesamiento de telematica pagos ejecutado exitosamente.")
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a procesar.")
            Mbox_File_Error.exec()

    def folder_files_xlsx_to_csv(self):

        type_process = "folder"
        
        self.validation_data_folders(type_process)
        self.digit_partitions_FOLDER()

        if self.folder_path_IVR != None:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            self.Base = gui.conversion_csv.convert_xlsx_to_csv(self.folder_path_IVR)
            
            Mbox_In_Process = QMessageBox() 
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Procesamiento de conversion ejecutado exitosamente.")
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a procesar.")
            Mbox_File_Error.exec()
            
    def folder_files_csv_to_json(self):

        type_process = "folder"
        
        self.validation_data_folders(type_process)
        self.digit_partitions_FOLDER()

        if self.folder_path_IVR != None:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            self.Base = cloud.conversion_csv_to_json.convert_csv_folder_to_json(self.folder_path_IVR, self.folder_path)
            
            Mbox_In_Process = QMessageBox() 
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Procesamiento de conversion ejecutado exitosamente.")
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a procesar.")
            Mbox_File_Error.exec()
            
    def folder_files_csv_to_parquet(self):

        type_process = "folder"
        
        self.validation_data_folders(type_process)
        self.digit_partitions_FOLDER()

        if self.folder_path_IVR != None:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            self.Base = cloud.conversion_csv_to_parquet.convert_csv_to_parquet(self.folder_path_IVR, self.folder_path)
            
            Mbox_In_Process = QMessageBox() 
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Procesamiento de conversion ejecutado exitosamente.")
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a procesar.")
            Mbox_File_Error.exec()
            
    def ivr_folder_read(self):

        type_process = "IVR"        
        self.validation_data_folders(type_process)
        self.digit_partitions_FOLDER()

        list_to_process_IVR = self.list_IVR

        if len(list_to_process_IVR) > 0:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
    
            Search_IVR = list_to_process_IVR[0]
            
            self.Base = gui.search_data.search_values_in_files(self.folder_path_IVR, self.folder_path, Search_IVR, self.process_data)

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Busqueda de informacion ejecutada exitosamente.")
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a consolidar.")
            Mbox_File_Error.exec()

    def ranking_read(self):

        type_process = "folder"   
             
        self.validation_data_folders(type_process)
        self.digit_partitions_FOLDER()

        if self.folder_path_IVR != None:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            self.Base = gui.ranking_read.process_ranking_files(self.folder_path_IVR, self.folder_path)

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Consolidado de Rankings ejecutado exitosamente.")
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos de RANKING a consolidar.")
            Mbox_File_Error.exec()
            
    def show_developers_window(self):
        
        dialog = QDialog()
        dialog.setWindowTitle("Información del Desarrollador")
        dialog.setWindowModality(Qt.WindowModality.ApplicationModal)
        dialog.setWindowState(Qt.WindowState.WindowFullScreen)
        
        Version_Api = "v1.0.13 (Py3.11-Spark3.5)"

        label = QLabel(
            "<h1>Esta aplicación fue desarrollada por:</h1><br>"
            "<b><h2>Juan Méndez – Arquitecto de Software</h2></b><br>"
            "<span style='font-size: 18px;'>Responsable del diseño, implementación y supervisión del desarrollo. "
            "Encargado de la integración de PySpark, transformación de datos, visualización, lógica y conexiones.</span><br><br>"
            
            "<b><h2>Julián Tique – Desarrollador Full-Stack</h2></b><br>"
            "<span style='font-size: 18px;'>Apoyó en la refactorización de componentes RPA y en la implementación de API´s para funcionalidades con conceptos de la web.</span><br><br>"
            
            "<b><h2>Daniel Sierra – Desarrollador Web</h2></b><br>"
            "<span style='font-size: 18px;'>Apoyó en la creación de componentes RPA y en la implementación de funcionalidades de web scraping.</span><br><br>"
            
            "<b><h2>Juan Raigoso – Desarrollador Frontend</h2></b><br>"
            "<span style='font-size: 18px;'>Contribuyó en la parte visual del sistema de una primera versión, trabajando en la estructura y estilización con CSS.</span><br><br>"
            
            "<b><h2>Deiby Gutiérrez – Desarrollador de Soporte en SQL</h2></b><br>"
            "<span style='font-size: 18px;'>Colaboró en la creación y optimización de scripts SQL para validaciones, pruebas y comparativas en el manejo de BIG DATA.</span><br><br>"
            
            "<br><br>" 
            f"<b>Versión {Version_Api}</b>"
        )
        label.setTextFormat(Qt.TextFormat.RichText)
        label.setAlignment(Qt.AlignmentFlag.AlignCenter)

        layout = QVBoxLayout()
        layout.addWidget(label)
        dialog.setLayout(layout)

        dialog.exec()
    
    def show_stakeholders_window(self):
        dialog = QDialog()
        dialog.setWindowTitle("Stakeholders de la Aplicación")
        dialog.setWindowModality(Qt.WindowModality.ApplicationModal)
        dialog.setWindowState(Qt.WindowState.WindowFullScreen)

        Version_Api = "v1.0.13 (Py3.11-Spark3.5)"

        label = QLabel(
            "<h1>Stakeholders:</h1><br>"
            "<b><h2>Raul Barbosa</h2></b><span style='font-size: 18px;'> – Gerente</span><br><br>"
            "<b><h2>Nathaly Lizarazo</h2></b><span style='font-size: 18px;'> – Directora Operativa</span><br><br>"
            "<b><h2>Nayibe Montoya</h2></b><span style='font-size: 18px;'> – Jefe de Operaciones</span><br><br>"
            "<b><h2>Anthony Quiva</h2></b><span style='font-size: 18px;'> – Coordinador Operativo</span><br><br>"
            "<b><h2>Karen Pinilla</h2></b><span style='font-size: 18px;'> – Analista de Inteligencia</span><br><br>"
            "<br><br>"
            f"<b>Versión {Version_Api}</b>"
        )
        label.setTextFormat(Qt.TextFormat.RichText)
        label.setAlignment(Qt.AlignmentFlag.AlignCenter)

        layout = QVBoxLayout()
        layout.addWidget(label)
        dialog.setLayout(layout)

        dialog.exec()
        
    def open_chrome_with_url(self, url):
        chrome_path = 'C:/Program Files/Google/Chrome/Application/chrome.exe %s'
        webbrowser.get(chrome_path).open(url)
        
    def open_firefox_with_url(self, url):
        firefox_path = 'C:/Program Files/Mozilla Firefox/firefox.exe %s'
        import webbrowser
        webbrowser.get(firefox_path).open(url)

    def schedule_shutdown(self):

        hours = self.process_data.spinBox_Partitions_13.value()
        minutes = self.process_data.spinBox_Partitions_14.value()

        total_seconds = hours * 3600 + minutes * 60

        if  total_seconds > 61:
            shutdown_command = f'shutdown /s /t {total_seconds}'
            os.system(shutdown_command)

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Critical)
            Mbox_In_Process.setText(f"El apagado automático está programado para {hours} hora(s) y {minutes} minuto(s).")
            Mbox_In_Process.exec()

        else: 
            pass
    
    def run_bat_excel(self):

        Root_API = self.root_API 
        bat_file_path = f"{Root_API}/files/bat/Excel_Finisher.bat"  
        
        try:
            subprocess.run([bat_file_path])
            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Excel finalizado exitosamente.")
            Mbox_In_Process.exec()

        except subprocess.CalledProcessError as e:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText(f"Error al ejecutar la operación: {e}")
            Mbox_Incomplete.exec()
    
    def run_bat_powerbi(self):

        Root_API = self.root_API 
        bat_file_path = f"{Root_API}/files/bat/PowerBI_Finisher.bat"  
        
        try:
            subprocess.run([bat_file_path])
            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Power BI finalizado exitosamente.")
            Mbox_In_Process.exec()

        except subprocess.CalledProcessError as e:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText(f"Error al ejecutar la operación: {e}")
            Mbox_Incomplete.exec()

    def run_replay_intercom(self):
        Root_API = self.root_API
        bat_file_path = f"{Root_API}/files/bat/Replay IVR-TRANS.exe"

        class WorkerThread(QThread):
            success_signal = pyqtSignal()
            error_signal = pyqtSignal(str)

            def run(self):
                try:
                    subprocess.run([bat_file_path], check=True)
                    self.success_signal.emit()
                except subprocess.CalledProcessError as e:
                    self.error_signal.emit(str(e))
    
    def run_downloads_intercom(self):

        Value = "Run"
        self.Base = utils.IVR_Downloads_List.Function_Download(Value)

    def clean_hive_tmp_dirs(self):
        paths = [r"C:\tmp\hive", r"D:\tmp\hive"]
        for path in paths:
            if os.path.exists(path) and os.path.isdir(path):
                
                for filename in os.listdir(path):
                    file_path = os.path.join(path, filename)
                    try:
                        if os.path.isfile(file_path) or os.path.islink(file_path):
                            os.unlink(file_path)
                        elif os.path.isdir(file_path):
                            shutil.rmtree(file_path)
                            
                        print("Limpiando directorio:", file_path)
                        
                    except Exception as e:
                        
                        print(f"Error eliminando {file_path}: {e}")
                        
    def run_bat_temp(self):

        Root_API = self.root_API 
        bat_file_path = f"{Root_API}/files/bat/Temp.bat"  
        
        try:
            self.clean_hive_tmp_dirs()  # Clean up Hive temporary directories
        except Exception as e:
            print(f"Error al limpiar las carpetas temporales: {e}")
            
        try:
            subprocess.run([bat_file_path], check=True)
            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Limpieza de temporales ejecutada exitosamente.")
            Mbox_In_Process.exec()

        except subprocess.CalledProcessError as e:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText(f"Error al ejecutar la operación: {e}")
            Mbox_Incomplete.exec()
        
    def validation_data_folders(self, type_process):
        
        if type_process == "IVR":
            self.partitions_FOLDER = None
            Search_Data = self.process_data.Searching_Field.text()
            self.list_IVR = [Search_Data]
            
        else:
            self.partitions_FOLDER = None
            Search_Data = self.process_data.Searching_Field.text()
            self.list_IVR = [Search_Data]

    def validation_data_resources(self):
        
        self.partitions_FOLDER = None
        self.digit_partitions_FOLDER()

        Resource_folder = self.process_data.comboBox_Selected_Process_2.currentText()
        
        if self.folder_path_IVR is None:
            
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a consolidar.")
            Mbox_File_Error.exec()
            
        elif "--- Seleccione opción" == Resource_folder:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Debe elegir el tipo de carpeta del recurso a leer o seleccionar todas.")
            Mbox_Incomplete.exec()
            self.process_data.comboBox_Selected_Process_2.setFocus()

        else:
            self.list_Resources = [Resource_folder]
    
    def exec_claro_structure_df(self):

        type_process = "folder"
        
        self.validation_data_folders(type_process)
        self.digit_partitions_FOLDER()

        if self.folder_path_IVR != None:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa las asignaciones.")
            Mbox_In_Process.exec()
            
            folder_path_bg = f"{self.folder_path}----- Bases para BIG DATA ----"
            bigdata.data_ai.claro_structure_df(self.folder_path_IVR, folder_path_bg, self.partitions_FOLDER)

            Mbox_In_Process = QMessageBox() 
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Dataset generado exitosamente.")
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a consolidar.")
            Mbox_File_Error.exec()
    
    def exec_claro_demographic_df(self):

        type_process = "folder"
        
        self.validation_data_folders(type_process)
        self.digit_partitions_FOLDER()
        self.digit_timemap_bigdata()

        if self.folder_path_IVR != None:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesan los demograficos.")
            Mbox_In_Process.exec()
            
            folder_path_bg = f"{self.folder_path}----- Bases para BIG DATA ----"
            bigdata.demos_ai.function_complete_demographic(self.folder_path_IVR, folder_path_bg, self.partitions_FOLDER, self.bigdatamonth, self.bigdatayear)

            Mbox_In_Process = QMessageBox() 
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Dataset generado exitosamente.")
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a consolidar.")
            Mbox_File_Error.exec()
    
    def exec_claro_touch_df(self):

        type_process = "folder"
        
        self.validation_data_folders(type_process)
        self.digit_partitions_FOLDER()
        self.digit_timemap_bigdata()

        if self.folder_path_IVR != None:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesan los toques por telematica.")
            Mbox_In_Process.exec()
            
            folder_path_bg = f"{self.folder_path}----- Bases para BIG DATA ----"
            bigdata.touch_ai.touch_dataframes_bd(self.folder_path_IVR, folder_path_bg, self.partitions_FOLDER, self.bigdatamonth, self.bigdatayear)

            Mbox_In_Process = QMessageBox() 
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Dataset generado exitosamente.")
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a consolidar.")
            Mbox_File_Error.exec()
    
    def exec_claro_bigdata(self):

        type_process = "folder"
        
        self.validation_data_folders(type_process)
        self.digit_partitions_FOLDER()
        self.digit_timemap_bigdata()

        if self.folder_path_IVR != None:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesan los Datasets de bigdata.")
            Mbox_In_Process.exec()
            
            folder_path_bg = f"{self.folder_path}----- Bases para BIG DATA ----"
            bigdata.union_datalakes_claro.read_compilation_datasets(self.folder_path_IVR, folder_path_bg, self.partitions_FOLDER, self.bigdatamonth, self.bigdatayear)

            Mbox_In_Process = QMessageBox() 
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Dataset generado exitosamente.")
            Mbox_In_Process.exec()
        
        else:
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a consolidar.")
            Mbox_File_Error.exec()
            
    def read_folder_resources(self):
        
        self.validation_data_resources()
        list_to_process_Read = self.list_Resources
        if len(list_to_process_Read) > 0:
            Folder_Resource = list_to_process_Read[0]
        else:
            Folder_Resource = None

        if Folder_Resource == "TODOS":

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            Path_Resource = f"{self.folder_path_IVR}/IVR"
            self.Base = skills.count_ivr.function_complete_IVR(Path_Resource, self.folder_path, self.partitions_FOLDER, self.process_data)
            Path_Resource = f"{self.folder_path_IVR}/BOT"
            self.Base = skills.count_bot.function_complete_BOT(Path_Resource, self.folder_path, self.partitions_FOLDER, self.process_data)
            Path_Resource = f"{self.folder_path_IVR}/EMAIL"
            self.Base = skills.count_email.function_complete_EMAIL(Path_Resource, self.folder_path, self.partitions_FOLDER, self.process_data)
            Path_Resource = f"{self.folder_path_IVR}/SMS"
            self.Base = skills.count_sms.function_complete_SMS(Path_Resource, self.folder_path, self.partitions_FOLDER, self.process_data)

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Consolidado de recurso(s) ejecutado exitosamente.")
            Mbox_In_Process.exec()

        elif Folder_Resource == "IVR":

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()

            Path_Resource = f"{self.folder_path_IVR}"
            self.Base = skills.count_ivr.function_complete_IVR(Path_Resource, self.folder_path, self.partitions_FOLDER, self.process_data)

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Consolidado de recurso(s) ejecutado exitosamente.")
            Mbox_In_Process.exec()

        elif Folder_Resource == "BOT":

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            Path_Resource = f"{self.folder_path_IVR}"
            self.Base = skills.count_bot.function_complete_BOT(Path_Resource, self.folder_path, self.partitions_FOLDER, self.process_data)

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Consolidado de recurso(s) ejecutado exitosamente.")
            Mbox_In_Process.exec()

        elif Folder_Resource == "SMS":

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            Path_Resource = f"{self.folder_path_IVR}"
            self.Base = skills.count_sms.function_complete_SMS(Path_Resource, self.folder_path, self.partitions_FOLDER, self.process_data)

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Consolidado de recurso(s) ejecutado exitosamente.")
            Mbox_In_Process.exec()

        elif Folder_Resource == "EMAIL":

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa la carpeta.")
            Mbox_In_Process.exec()
            
            Path_Resource = f"{self.folder_path_IVR}"
            self.Base = skills.count_email.function_complete_EMAIL(Path_Resource, self.folder_path, self.partitions_FOLDER, self.process_data)

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Consolidado de recurso(s) ejecutado exitosamente.")
            Mbox_In_Process.exec()
        
        else:
            pass

    def validation_data_demo(self):
        
        self.partitions_FOLDER = None
        self.digit_partitions_FOLDER()
        
        if self.folder_path_IVR is None:
            
            Mbox_File_Error = QMessageBox()
            Mbox_File_Error.setWindowTitle("Error de procesamiento")
            Mbox_File_Error.setIcon(QMessageBox.Icon.Warning)
            Mbox_File_Error.setText("Debe seleccionar una ruta con los archivos a consolidar.")
            Mbox_File_Error.exec()
            
        else:
            pass
