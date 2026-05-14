from datetime import datetime
from PyQt6.QtCore import QDate
from PyQt6 import QtWidgets, uic
from PyQt6.QtWidgets import QMessageBox
import modules.fragment_dataBase
import modules.telematic.telematics_crediveci
import modules.telematic.telematics_gmfinancial
import modules.telematic.telematics_puntored
import modules.telematic.telematics_yadinero
import modules.telematic.telematics_habi
import modules.telematic.telematics_payjoy
import web.download_saem_reports
import modules.telematic.claro.bot_process
import modules.telematic.claro.email_process
import modules.telematic.claro.sms_process
import modules.telematic.claro.ivr_process
import modules.telematic.claro.phone_order
import modules.fragment_dataBase
import modules.tmo_trial
import modules.exclusions
import modules.telematic.claro.exclusions_file_claro
import modules.task_web

class Process_Data(QtWidgets.QMainWindow):

    def __init__(self, row_count, file_path, folder_path, process_data):
        super().__init__()

        self.partitions_FILES = None
        self.partitions_FILES_PASH = None
        self.partitions = None
        self.partitions_direction = None

        self.file_path = file_path
        self.folder_path = folder_path
        self.process_data = process_data
        self.digit_partitions()
        self.exec_process()
        self.digit_partitions_direction()

    def digit_partitions(self):

        self.partitions_FILES = str(self.process_data.spinBox_Partitions_2.value())
        self.partitions = self.partitions_FILES
    
    def digit_partitions_direction(self):

        self.partitions_direction = str(self.process_data.spinBox_Partitions_4.value())
        self.partitions = self.partitions_direction
    
    def digit_partitions_pash(self):

        self.partitions_FILES_PASH = str(self.process_data.spinBox_Partitions_19.value())
        self.partitions = self.partitions_FILES_PASH

    def exec_process(self):
        
        self.data_to_process = []
        self.today = None
        self.process_data.pushButton_Process.clicked.connect(self.compilation_process)
        self.process_data.pushButton_Graphic.clicked.connect(self.data_tables)
        #self.process_data.pushButton_Process_3.clicked.connect(self.compilation_process_pash)
        #self.process_data.commandLinkButton_35.clicked.connect(self.compilation_process_direction)
        self.process_data.commandLinkButton_35.clicked.connect(self.reorder_phones)
        self.process_data.commandLinkButton_69.clicked.connect(self.payments_bd_filter)
        self.process_data.commandLinkButton_13.clicked.connect(self.bd_exclusions)
        self.process_data.commandLinkButton_70.clicked.connect(self.tmo_converter)

    def bd_exclusions(self):

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

            sheets_str = self.process_data.label_Total_Registers_2.text()
            sheets = int(sheets_str.split()[0])
            
            if sheets < 4:
                modules.exclusions.process_xlsx_file(file, root)
                print("Normal Exclusions")
            elif sheets > 2:
                modules.telematic.claro.exclusions_file_claro.process_xlsx_file(file, root)
                print("High Exclusions")

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Proceso de extraccion de EXCLUSIONES ejecutado exitosamente.")
            Mbox_In_Process.exec()
        else:
            pass
        
    def reorder_phones(self):

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

            modules.telematic.claro.phone_order.Function_Complete(file, root, partitions)

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Proceso de reordenacion de numeros ejecutado exitosamente.")
            Mbox_In_Process.exec()
        else:
            pass
    
    def payments_bd_filter(self):

        list_data = [self.file_path, self.folder_path]
        lenght_list = len(list_data)

        file = list_data[0]
        root = list_data[1]

        if lenght_list >= 2:

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("Procesando")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Por favor espere la ventana de confirmacion, mientras se procesa el archivo.")
            Mbox_In_Process.exec()

            modules.fragment_dataBase.process_csv_file(file, root)

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Proceso de filtro de pagos ejecutado exitosamente.")
            Mbox_In_Process.exec()
        else:
            pass

    def tmo_converter(self):

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

            modules.tmo_trial.Function_Complete(file, root, partitions)

            Mbox_In_Process = QMessageBox()
            Mbox_In_Process.setWindowTitle("")
            Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
            Mbox_In_Process.setText("Proceso de conversion de TMO ejecutado exitosamente.")
            Mbox_In_Process.exec()
        else:
            pass

    def data_tables(self):

        self.validation_data()
        lenght_list = len(self.data_to_process)

        if lenght_list >= 10:
            list_Tables = self.data_to_process
            #self.Tables = Process_Data_Tables(list_Tables)
        else: 
            pass

    def validation_data_pash(self):

        self.digit_partitions_pash()

        ### CheckBox
        list_credit = []
        
        checkbox_names_credit_pash = [
            "checkBox_Brand_3", "checkBox_Brand_33", "checkBox_Brand_74",
            "checkBox_Brand_104", "checkBox_Brand_GROUP_3", "checkBox_Brand_GROUP_63",
            "checkBox_Brand_GROUP_SPECIALS_4", "checkBox_Brand_GROUP_SPECIALS_11", "checkBox_Brand_GROUP_SPECIALS_12",
            "checkBox_Brand_GROUP_SPECIALS_13", "checkBox_Brand_GROUP_6", "checkBox_Brand_GROUP_66",
            "checkBox_Brand_GROUP_SPECIALS_7", "checkBox_Brand_GROUP_SPECIALS_8", "checkBox_Brand_GROUP_SPECIALS_14",
            "checkBox_Brand_GROUP_SPECIALS_15", "checkBox_Brand_GROUP_SPECIALS_16", "checkBox_Brand_GROUP_SPECIALS_17"
        ]

        for checkbox_name in checkbox_names_credit_pash:
            checkbox = getattr(self.process_data, checkbox_name, None)
            if checkbox is not None and checkbox.isChecked():
                text = checkbox.text()
                list_credit.append(text)
    
        ### Lists
        Type_Clients_Pash = self.process_data.comboBox_Benefits_15.currentText()
        V_Min_Contact_Pash = self.process_data.comboBox_Min_Contact_5.currentText()
        V_Benefits_Pash = self.process_data.comboBox_Benefits_14.currentText()
        V_Selected_Process_Pash = self.process_data.comboBox_Selected_Process_3.currentText()

        if V_Selected_Process_Pash == "EMAIL" or V_Selected_Process_Pash == "SMS":
            V_Min_Contact_Filter_Pash = "Todos"

        list_empty = ["--- Seleccione opcion"]
        validation_list_filters = [Type_Clients_Pash, V_Min_Contact_Pash, V_Benefits_Pash, V_Selected_Process_Pash]

        ### Numeric Spaces
        V_Min_Price_Pash = self.process_data.lineEdit_Mod_Init_Min_5.text()
        V_Max_Price_Pash = self.process_data.lineEdit_Mod_Init_Max_5.text()

        ### Validation
        if len(checkbox_names_credit_pash) == 0:

            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Debe seleccionar al menos una opcion de cartera o de grupo.")
            Mbox_Incomplete.exec()

        elif all(item in validation_list_filters for item in list_empty):
            
            if "--- Seleccione opcion" == Type_Clients_Pash:
                Mbox_Incomplete = QMessageBox()
                Mbox_Incomplete.setWindowTitle("Error de procesamiento")
                Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
                Mbox_Incomplete.setText("Debe elegir el tipo de cliente \npara generar el respectivo archivo.")
                Mbox_Incomplete.exec()
                self.process_data.comboBox_Benefits_15.setFocus()

            elif "--- Seleccione opcion" == V_Selected_Process_Pash:
                Mbox_Incomplete = QMessageBox()
                Mbox_Incomplete.setWindowTitle("Error de procesamiento")
                Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
                Mbox_Incomplete.setText("Debe elegir el tipo de proceso que desea realizar\npara generar el respectivo archivo.")
                Mbox_Incomplete.exec()
                self.process_data.comboBox_Selected_Process_3.setFocus()

            elif "--- Seleccione opcion" == V_Benefits_Pash:
                Mbox_Incomplete = QMessageBox()
                Mbox_Incomplete.setWindowTitle("Error de procesamiento")
                Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
                Mbox_Incomplete.setText("Debe completar el campo de filtro de beneficios\npara realizar la transformacion de los datos.")
                Mbox_Incomplete.exec()
                self.process_data.comboBox_Benefits_14.setFocus()
            
            else:
                Mbox_Incomplete = QMessageBox()
                Mbox_Incomplete.setWindowTitle("Error de procesamiento")
                Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
                Mbox_Incomplete.setText("Debe completar el campo de filtro de números\npara realizar la transformacion de los datos.")
                Mbox_Incomplete.exec()    
                self.process_data.comboBox_Min_Contact_5.setFocus()
        
        elif V_Min_Price_Pash.strip() == "" or V_Max_Price_Pash.strip() == "":

            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Ingrese un valor numérico en el rango mínimo y máximo.")
            Mbox_Incomplete.exec()

            if V_Min_Price_Pash.strip() == "" and V_Max_Price_Pash.strip() == "":
                self.process_data.lineEdit_Mod_Init_Min_5.setText("1")
                self.process_data.lineEdit_Mod_Init_Max_5.setText("999999999")
                self.process_data.lineEdit_Mod_Init_Min_5.setFocus()
            elif V_Min_Price_Pash.strip() == "":
                self.process_data.lineEdit_Mod_Init_Min_5.setText("1")
                self.process_data.lineEdit_Mod_Init_Min_5.setFocus()
            else:
                self.process_data.lineEdit_Mod_Init_Max_5.setText("999999999")
                self.process_data.lineEdit_Mod_Init_Max_5.setFocus()

        elif not (V_Min_Price_Pash.isdigit() and V_Max_Price_Pash.isdigit()):

            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Ha digitado valores inválidos, ingrese solo NÚMEROS.")
            Mbox_Incomplete.exec()

            if not (V_Min_Price_Pash.isdigit() or V_Max_Price_Pash.isdigit()):
                self.process_data.lineEdit_Mod_Init_Max_5.setText("")
                self.process_data.lineEdit_Mod_Init_Min_5.setText("")
                self.process_data.lineEdit_Mod_Init_Max_5.setFocus()
            elif V_Min_Price_Pash.isdigit():
                self.process_data.lineEdit_Mod_Init_Max_5.setText("")
                self.process_data.lineEdit_Mod_Init_Max_5.setFocus()
            else:
                self.process_data.lineEdit_Mod_Init_Min_5.setText("")
                self.process_data.lineEdit_Mod_Init_Min_5.setFocus()

        elif int(V_Min_Price_Pash) >= int(V_Max_Price_Pash):

            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Ha digitado valores inválidos, ingrese un rango coherente.")
            Mbox_Incomplete.exec()

            self.process_data.lineEdit_Mod_Init_Max_5.setText("")
            self.process_data.lineEdit_Mod_Init_Min_5.setText("")
            self.process_data.lineEdit_Mod_Init_Max_5.setFocus()

        else:
            
            V_Min_Contact_Filter_Pash = V_Min_Contact_Pash

            self.data_to_process = [Type_Clients_Pash, V_Benefits_Pash, V_Selected_Process_Pash, \
                                    V_Min_Contact_Filter_Pash, list_credit, V_Min_Price_Pash, V_Max_Price_Pash]

            list_data = [self.file_path, self.folder_path, self.partitions]
            list_data.extend(self.data_to_process)
            self.data_to_process = list_data
    
    def validation_data(self):

        self.digit_partitions()

        ### CheckBox
        list_CheckBox_Brands = []
        checkbox_names_brands = [
            "checkBox_Brand_1", "checkBox_Brand_31", "checkBox_Brand_61",
            "checkBox_Brand_91", "checkBox_Brand_121", "checkBox_Brand_151",
            "checkBox_Brand_181", "checkBox_Brand_211", "checkBox_Brand_Potencial_2",
            "checkBox_Brand_Prechurn_2", "checkBox_Brand_Castigo_2", "checkBox_Brand_Churn_2",
            "checkBox_Brand_Preprovision_2", "checkBox_Brand_Prepotencial_2", "checkBox_Brand_Provision_2",
            "checkBox_Brand_Less_Castigo_2", "checkBox_brand_ALL_2", "checkBox_Brand_GROUP_1", 
            "checkBox_Brand_GROUP_61", "checkBox_Brand_GROUP_SPECIALS_2", "checkBox_Brand_Provision_4"

        ]
        for checkbox_name in checkbox_names_brands:
            checkbox = getattr(self.process_data, checkbox_name, None)

            if checkbox is not None and checkbox.isChecked():
                text = checkbox.text()
                if "Marca " in text:
                    text = text.replace("Marca ", "")
                list_CheckBox_Brands.append(text)

        list_CheckBox_Origins = []
        checkbox_names_origins = ["checkBox_Origin_ASCARD_2", "checkBox_Origin_BSCS_2", "checkBox_Origin_RR_2", "checkBox_Origin_SGA_2", "checkBox_Origin_ALL_Origins_2"]
        for checkbox_name in checkbox_names_origins:
            checkbox = getattr(self.process_data, checkbox_name, None)
            if checkbox is not None and checkbox.isChecked():
                text = checkbox.text()
                list_CheckBox_Origins.append(text)

        ### Calendar FLP
        Date_Selection = str(self.process_data.checkBox_ALL_DATES_FLP_2.isChecked())
        Calendar_Date = str(self.process_data.calendarWidget.selectedDate())
        Calendar_Date_ = self.process_data.calendarWidget.selectedDate()
        Today__ = datetime.now().date()
        Today = str(QDate(Today__.year, Today__.month, Today__.day))
        Today_ = QDate(Today__.year, Today__.month, Today__.day)

        formatted_date = Calendar_Date_.toString("yyyy-MM-dd")
        formatted_date_today = Today_.toString("yyyy-MM-dd")
        self.today = formatted_date_today

        if Date_Selection == "True":
            Date_Selection_Filter = "All Dates"

        elif Calendar_Date == Today:
            Date_Selection_Filter = None

        else:
            Date_Selection_Filter = formatted_date
    
        ### Lists
        V_Benefits = self.process_data.comboBox_Benefits.currentText()
        V_Min_Contact = self.process_data.comboBox_Min_Contact.currentText()
        V_Selected_Process = self.process_data.comboBox_Selected_Process.currentText()
        V_Selected_Campaign = self.process_data.comboBox_Selected_Process_3.currentText()

        if V_Selected_Process == "EMAIL" or V_Selected_Process == "SMS":
            V_Min_Contact_Filter = "Todos"

        list_empty = ["--- Seleccione opcion"]
        validation_list_filters = [V_Benefits, V_Min_Contact, V_Selected_Process]

        ### Numeric Spaces
        V_Min_Price = self.process_data.lineEdit_Mod_Init_Min.text()
        V_Max_Price = self.process_data.lineEdit_Mod_Init_Max.text()

        ### Validation
        if V_Selected_Campaign == "Claro":
            
            if len(list_CheckBox_Brands) == 0 or len(list_CheckBox_Origins) == 0:

                Mbox_Incomplete = QMessageBox()
                Mbox_Incomplete.setWindowTitle("Error de procesamiento")
                Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
                Mbox_Incomplete.setText("Debe seleccionar al menos una opcion de marca y origen.")
                Mbox_Incomplete.exec()

            elif Date_Selection_Filter is None:
                Mbox_Incomplete = QMessageBox()
                Mbox_Incomplete.setWindowTitle("Error de procesamiento")
                Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
                Mbox_Incomplete.setText("Debe seleccionar al menos una fecha o elegir todas las FLP.")
                Mbox_Incomplete.exec()

            elif all(item in validation_list_filters for item in list_empty):
                
                if "--- Seleccione opcion" == V_Selected_Process:
                    Mbox_Incomplete = QMessageBox()
                    Mbox_Incomplete.setWindowTitle("Error de procesamiento")
                    Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
                    Mbox_Incomplete.setText("Debe elegir el tipo de proceso que desea realizar\npara generar el respectivo archivo.")
                    Mbox_Incomplete.exec()
                    self.process_data.comboBox_Selected_Process.setFocus()

                elif "--- Seleccione opcion" == V_Benefits:
                    Mbox_Incomplete = QMessageBox()
                    Mbox_Incomplete.setWindowTitle("Error de procesamiento")
                    Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
                    Mbox_Incomplete.setText("Debe completar el campo de filtro de beneficios\npara realizar la transformacion de los datos.")
                    Mbox_Incomplete.exec()
                    self.process_data.comboBox_Benefits.setFocus()
                
                else:
                    Mbox_Incomplete = QMessageBox()
                    Mbox_Incomplete.setWindowTitle("Error de procesamiento")
                    Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
                    Mbox_Incomplete.setText("Debe completar el campo de filtro de números\npara realizar la transformacion de los datos.")
                    Mbox_Incomplete.exec()    
                    self.process_data.comboBox_Min_Contact.setFocus()
            
            elif V_Min_Price.strip() == "" or V_Max_Price.strip() == "":

                Mbox_Incomplete = QMessageBox()
                Mbox_Incomplete.setWindowTitle("Error de procesamiento")
                Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
                Mbox_Incomplete.setText("Ingrese un valor numérico en el rango mínimo y máximo.")
                Mbox_Incomplete.exec()

                if V_Min_Price.strip() == "" and V_Max_Price.strip() == "":
                    self.process_data.lineEdit_Mod_Init_Min.setText("1")
                    self.process_data.lineEdit_Mod_Init_Max.setText("999999999")
                    self.process_data.lineEdit_Mod_Init_Min.setFocus()
                elif V_Min_Price.strip() == "":
                    self.process_data.lineEdit_Mod_Init_Min.setText("1")
                    self.process_data.lineEdit_Mod_Init_Min.setFocus()
                else:
                    self.process_data.lineEdit_Mod_Init_Max.setText("999999999")
                    self.process_data.lineEdit_Mod_Init_Max.setFocus()

            elif not (V_Min_Price.isdigit() and V_Max_Price.isdigit()):

                Mbox_Incomplete = QMessageBox()
                Mbox_Incomplete.setWindowTitle("Error de procesamiento")
                Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
                Mbox_Incomplete.setText("Ha digitado valores inválidos, ingrese solo NÚMEROS.")
                Mbox_Incomplete.exec()

                if not (V_Min_Price.isdigit() or V_Max_Price.isdigit()):
                    self.process_data.lineEdit_Mod_Init_Max.setText("")
                    self.process_data.lineEdit_Mod_Init_Min.setText("")
                    self.process_data.lineEdit_Mod_Init_Max.setFocus()
                elif V_Min_Price.isdigit():
                    self.process_data.lineEdit_Mod_Init_Max.setText("")
                    self.process_data.lineEdit_Mod_Init_Max.setFocus()
                else:
                    self.process_data.lineEdit_Mod_Init_Min.setText("")
                    self.process_data.lineEdit_Mod_Init_Min.setFocus()

            elif int(V_Min_Price) >= int(V_Max_Price):

                Mbox_Incomplete = QMessageBox()
                Mbox_Incomplete.setWindowTitle("Error de procesamiento")
                Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
                Mbox_Incomplete.setText("Ha digitado valores inválidos, ingrese un rango coherente.")
                Mbox_Incomplete.exec()

                self.process_data.lineEdit_Mod_Init_Max.setText("")
                self.process_data.lineEdit_Mod_Init_Min.setText("")
                self.process_data.lineEdit_Mod_Init_Max.setFocus()

            else:
                
                V_Min_Contact_Filter = V_Min_Contact

                self.data_to_process = [list_CheckBox_Brands, list_CheckBox_Origins, Date_Selection_Filter, V_Benefits, V_Min_Contact_Filter, \
                                        V_Selected_Process, V_Min_Price, V_Max_Price]

                list_data = [self.file_path, self.folder_path, self.partitions]
                list_data.extend(self.data_to_process)
                self.data_to_process = list_data
                
        elif "--- Seleccione opción" == V_Selected_Campaign:
            
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Debe elegir el tipo de asignacion que desea tratar\npara generar el respectivo archivo.")
            Mbox_Incomplete.exec()
            self.process_data.comboBox_Selected_Process_3.setFocus()
                
        elif "--- Seleccione opción" == V_Selected_Process:
            
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Debe elegir el tipo de proceso que desea realizar\npara generar el respectivo archivo.")
            Mbox_Incomplete.exec()
            self.process_data.comboBox_Selected_Process.setFocus()
            
        else:
            list_data = [self.file_path, self.folder_path, self.partitions, V_Selected_Process, V_Selected_Campaign]
            self.data_to_process = list_data
            print(f"There are selected campaigns and resource {list_data}")
        
    def validation_data_direction(self):

        self.digit_partitions_direction()

        ### CheckBox
        list_CheckBox_Brands = []
        checkbox_names_brands = [
            "checkBox_Brand_4", "checkBox_Brand_34", "checkBox_Brand_64",
            "checkBox_Brand_94", "checkBox_Brand_124", "checkBox_brand_154",
            "checkBox_Brand_184", "checkBox_Brand_214", "checkBox_Brand_Castigo_5",
            "checkBox_Brand_Churn_5", "checkBox_Brand_Potencial_5", "checkBox_Brand_Prechurn_5",
            "checkBox_Brand_Preprovision_5", "checkBox_Brand_Prepotencial_5", "checkBox_Brand_Provision_5",
            "checkBox_Brand_GROUP_4", "checkBox_Brand_GROUP_64", "checkBox_Brand_GROUP_SPECIALS_5", 
            "checkBox_Brand_Less_Castigo_4", "checkBox_brand_ALL_4"

        ]
        for checkbox_name in checkbox_names_brands:
            checkbox = getattr(self.process_data, checkbox_name, None)
            if checkbox is not None and checkbox.isChecked():
                text = checkbox.text()
                if "Marca " in text:
                    text = text.replace("Marca ", "")
                list_CheckBox_Brands.append(text)
            
        list_CheckBox_Origins = []
        checkbox_names_origins = ["checkBox_Origin_ASCARD_4", "checkBox_Origin_BSCS_4", "checkBox_Origin_RR_4", "checkBox_Origin_SGA_4", "checkBox_Origin_ALL_Origins_4"]
        for checkbox_name in checkbox_names_origins:
            checkbox = getattr(self.process_data, checkbox_name, None)
            if checkbox is not None and checkbox.isChecked():
                text = checkbox.text()
                list_CheckBox_Origins.append(text)

      
        
        ### Calendar FLP
        list_dates_direction = []
        checkbox_dates_direction = [
            "checkBox_Brand_5", "checkBox_Brand_35", "checkBox_Brand_65", "checkBox_Brand_95", "checkBox_Brand_125",\
            "checkBox_brand_155", "checkBox_Brand_185", "checkBox_Brand_215", "checkBox_Brand_6", "checkBox_Brand_36",\
            "checkBox_Brand_66", "checkBox_Brand_96", "checkBox_Brand_126", "checkBox_brand_156", "checkBox_Brand_186",\
            "checkBox_Brand_216", "checkBox_Brand_7", "checkBox_Brand_37", "checkBox_Brand_67", "checkBox_Brand_97",\
            "checkBox_Brand_127", "checkBox_brand_157", "checkBox_Brand_187", "checkBox_Brand_217", "checkBox_Brand_8",\
            "checkBox_Brand_38", "checkBox_Brand_68", "checkBox_Brand_98", "checkBox_Brand_128", "checkBox_brand_158",\
            "checkBox_Brand_188", "checkBox_Brand_189"
        ]

        year_direction = str(self.process_data.spinBox_Partitions_11.value())
        month_direction = str(self.process_data.spinBox_Partitions_12.value())

        if len(month_direction) == 1:
            month_direction = f"0{month_direction}"
        else:
            pass

        for date_direction in checkbox_dates_direction:
            checkbox = getattr(self.process_data, date_direction, None)
            if checkbox is not None and checkbox.isChecked():
                day_text = checkbox.text()

                if day_text == "Todo":
                    date_complete_direction = day_text
                    list_dates_direction.append(str(date_complete_direction))

                else:
                    date_complete_direction = f"{year_direction}-{month_direction}-{day_text}"
                    list_dates_direction.append(str(date_complete_direction))

        if "Todo" in list_dates_direction:
            Date_Selection_Filter = "All Dates"

        elif len(list_dates_direction) < 1:
            Date_Selection_Filter = None

        else:
            Date_Selection_Filter = list_dates_direction

        ### Lists
        V_Benefits = self.process_data.comboBox_Benefits_7.currentText()
        V_Min_Contact = self.process_data.comboBox_Min_Contact_3.currentText()

        list_empty = ["--- Seleccione opcion"]
        validation_list_filters = [V_Benefits, V_Min_Contact]

        ### Numeric Spaces
        V_Min_Price = self.process_data.lineEdit_Mod_Init_Min_3.text()
        V_Max_Price = self.process_data.lineEdit_Mod_Init_Max_3.text()

        ### Validation
        if len(list_CheckBox_Brands) == 0 or len(list_CheckBox_Origins) == 0:

            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Debe seleccionar al menos una opcion de marca y origen.")
            Mbox_Incomplete.exec()

        elif all(item in validation_list_filters for item in list_empty):
            
            if "--- Seleccione opcion" == V_Benefits:
                Mbox_Incomplete = QMessageBox()
                Mbox_Incomplete.setWindowTitle("Error de procesamiento")
                Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
                Mbox_Incomplete.setText("Debe completar el campo de filtro de beneficios\npara realizar la transformacion de los datos.")
                Mbox_Incomplete.exec()
                self.process_data.comboBox_Benefits.setFocus()
            
            else:
                Mbox_Incomplete = QMessageBox()
                Mbox_Incomplete.setWindowTitle("Error de procesamiento")
                Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
                Mbox_Incomplete.setText("Debe completar el campo de filtro de números\npara realizar la transformacion de los datos.")
                Mbox_Incomplete.exec()    
                self.process_data.comboBox_Min_Contact.setFocus()
        
        elif V_Min_Price.strip() == "" or V_Max_Price.strip() == "":

            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Ingrese un valor numérico en el rango mínimo y máximo.")
            Mbox_Incomplete.exec()

            if V_Min_Price.strip() == "" and V_Max_Price.strip() == "":
                self.process_data.lineEdit_Mod_Init_Min_3.setText("1")
                self.process_data.lineEdit_Mod_Init_Max_3.setText("999999999")
                self.process_data.lineEdit_Mod_Init_Min_3.setFocus()
            elif V_Min_Price.strip() == "":
                self.process_data.lineEdit_Mod_Init_Min_3.setText("1")
                self.process_data.lineEdit_Mod_Init_Min_3.setFocus()
            else:
                self.process_data.lineEdit_Mod_Init_Max_3.setText("999999999")
                self.process_data.lineEdit_Mod_Init_Max_3.setFocus()

        elif not (V_Min_Price.isdigit() and V_Max_Price.isdigit()):

            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Ha digitado valores inválidos, ingrese solo NÚMEROS.")
            Mbox_Incomplete.exec()

            if not (V_Min_Price.isdigit() or V_Max_Price.isdigit()):
                self.process_data.lineEdit_Mod_Init_Max_3.setText("")
                self.process_data.lineEdit_Mod_Init_Min_3.setText("")
                self.process_data.lineEdit_Mod_Init_Max_3.setFocus()
            elif V_Min_Price.isdigit():
                self.process_data.lineEdit_Mod_Init_Max_3.setText("")
                self.process_data.lineEdit_Mod_Init_Max_3.setFocus()
            else:
                self.process_data.lineEdit_Mod_Init_Min_3.setText("")
                self.process_data.lineEdit_Mod_Init_Min_3.setFocus()

        elif int(V_Min_Price) >= int(V_Max_Price):

            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Ha digitado valores inválidos, ingrese un rango coherente.")
            Mbox_Incomplete.exec()

            self.process_data.lineEdit_Mod_Init_Max_3.setText("")
            self.process_data.lineEdit_Mod_Init_Min_3.setText("")
            self.process_data.lineEdit_Mod_Init_Max_3.setFocus()

        elif Date_Selection_Filter is None:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Debe seleccionar al menos una fecha o elegir todas las FLP.")
            Mbox_Incomplete.exec()

        else:
            
            V_Min_Contact_Filter = V_Min_Contact

            self.data_to_process = [list_CheckBox_Brands, list_CheckBox_Origins, Date_Selection_Filter, V_Benefits, V_Min_Contact_Filter, \
                                    V_Min_Price, V_Max_Price]

            list_data = [self.file_path, self.folder_path, self.partitions]
            list_data.extend(self.data_to_process)
            self.data_to_process = list_data

    def compilation_process_pash(self):
        
        self.validation_data_pash()
        lenght_list = len(self.data_to_process)

        if lenght_list >= 8:        

            list_data = self.data_to_process

            Type_Clients = list_data[3]
            Benefits_Pash = list_data[4]
            Process_Pash = list_data[5]
            Contact_Pash = list_data[6]
            List_Credit = list_data[7]

            file = list_data[0]
            root = list_data[1]
            partitions = int(list_data[2])

            if "Todos" in List_Credit:
                List_Credit = ["Credicol", "Credielite", "Credimoda"]

            List_Credit = List_Credit

            List_Credit = [Credit.lower() for Credit in List_Credit]
                
            value_min = int(list_data[8])
            value_max = int(list_data[9])

            widget_filter = "Create File"

            if Process_Pash == "BOT":

                Mbox_In_Process = QMessageBox()
                Mbox_In_Process.setWindowTitle("Procesando")
                Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
                Mbox_In_Process.setText("Por favor espere la ventana de confirmacion, mientras se procesa el archivo.")
                Mbox_In_Process.exec()

                modules.bot_process_Pash.Function_Complete(file, root, partitions, Type_Clients, Benefits_Pash, \
                                                      Contact_Pash, List_Credit, value_min, value_max,\
                                                        widget_filter)
                
                Mbox_In_Process = QMessageBox()
                Mbox_In_Process.setWindowTitle("")
                Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
                Mbox_In_Process.setText("Proceso de BOTS ejecutado exitosamente.")
                Mbox_In_Process.exec()

            elif Process_Pash == "EMAIL":

                Mbox_In_Process = QMessageBox()
                Mbox_In_Process.setWindowTitle("Procesando")
                Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
                Mbox_In_Process.setText("Por favor espere la ventana de confirmacion, mientras se procesa el archivo.")
                Mbox_In_Process.exec()

                modules.telematic.claro.email_process.Function_Complete(file, root, partitions, Type_Clients, Benefits_Pash, \
                                                      Contact_Pash, List_Credit, value_min, value_max,\
                                                        widget_filter)
                
                Mbox_In_Process = QMessageBox()
                Mbox_In_Process.setWindowTitle("")
                Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
                Mbox_In_Process.setText("Proceso de EMAIL ejecutado exitosamente.")
                Mbox_In_Process.exec()

            elif Process_Pash == "IVR":

                Mbox_In_Process = QMessageBox()
                Mbox_In_Process.setWindowTitle("Procesando")
                Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
                Mbox_In_Process.setText("Por favor espere la ventana de confirmacion, mientras se procesa el archivo.")
                Mbox_In_Process.exec()

                modules.ivr_process_Pash.Function_Complete(file, root, partitions, Type_Clients, Benefits_Pash, \
                                                      Contact_Pash, List_Credit, value_min, value_max,\
                                                        widget_filter)
                
                Mbox_In_Process = QMessageBox()
                Mbox_In_Process.setWindowTitle("")
                Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
                Mbox_In_Process.setText("Proceso de IVR ejecutado exitosamente.")
                Mbox_In_Process.exec()

            elif Process_Pash == "SMS":

                Mbox_In_Process = QMessageBox()
                Mbox_In_Process.setWindowTitle("Procesando")
                Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
                Mbox_In_Process.setText("Por favor espere la ventana de confirmacion, mientras se procesa el archivo.")
                Mbox_In_Process.exec()

                modules.telematic.claro.sms_process.Function_Complete(file, root, partitions, Type_Clients, Benefits_Pash, \
                                                      Contact_Pash, List_Credit, value_min, value_max,\
                                                        widget_filter)
                
                Mbox_In_Process = QMessageBox()
                Mbox_In_Process.setWindowTitle("")
                Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
                Mbox_In_Process.setText("Proceso de SMS ejecutado exitosamente.")
                Mbox_In_Process.exec()

            else:
                pass

        else:
            pass

    def compilation_process(self):
        
        self.validation_data()
        campaign_selected = self.process_data.comboBox_Selected_Process_3.currentText()
        
        if campaign_selected == "Claro":
            
            lenght_list = len(self.data_to_process)
            widget_filter = "Create_File"

            if lenght_list >= 8:        

                list_data = self.data_to_process
                today_IVR = self.today

                Process = list_data[8]

                file = list_data[0]
                root = list_data[1]
                partitions = int(list_data[2])
                brands = list_data[3]

                brands_all = []
                brands_all_less_castigo = []
                brands_group_0_30 = []
                brands_group_60_210 = []
                brands_specials = []

                if "Todo" in brands:
                    brands_all = ["0", "30", "60", "90", "120", "150", "180", "210", "prechurn", "potencial", "castigo", "churn", "preprovision", "prepotencial", "provision", "otros"]

                if "Todo - Castigo" in brands:
                    brands_all_less_castigo = ["0", "30", "60", "90", "120", "150", "180", "210", "prechurn", "potencial", "churn", "preprovision", "prepotencial", "provision", "otros"]

                if "0 - 30" in brands:
                    brands_group_0_30 = ["0", "30"]

                if "60 - 210" in brands:
                    brands_group_60_210 = ["60", "90", "120", "150", "180", "210"]

                if "Especiales" in brands:
                    brands_specials = ["prechurn", "potencial", "churn", "preprovision", "provision", "prepotencial"]

                brands = brands + brands_all + brands_all_less_castigo + brands_group_0_30 + brands_group_60_210 + brands_specials

                brands_group = [brand_w.lower() for brand_w in brands]

                #####################
                if "potencial" in brands_group:
                    brand_pot = ["Potencial a Castigar"]
                    brands_group = brands + brand_pot
                    brands = brands_group
                else:
                    pass

                brands_group = [brand_w.lower() for brand_w in brands]
                brands = brands_group

                if "prepotencial" in brands_group:
                    brand_pre = ["Prepotencial Especial"]
                    brands_group = brands + brand_pre
                    brands = brands_group
                else:
                    pass

                if "otros" in brands_group:
                    brand_unique = ["Apple Manual"]
                    brands_group = brands + brand_unique
                    brands = brands_group
                else:
                    pass
                #####################

                brands = brands_group
                for brand in brands_group:
                    if brand not in brands:
                        brands.append(brand)

                origins = list_data[4]
                
                if "Todo" in origins:
                    origins =["ASCARD", "BSCS", "RR", "SGA"]

                else:
                    origins = list_data[4]
                    
                date = list_data[5]
                benefits = list_data[6]
                contact = list_data[7]
                value_min = int(list_data[9])
                value_max = int(list_data[10])

                if Process == "BOT":

                    Mbox_In_Process = QMessageBox()
                    Mbox_In_Process.setWindowTitle("Procesando")
                    Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
                    Mbox_In_Process.setText("Por favor espere la ventana de confirmacion, mientras se procesa el archivo.")
                    Mbox_In_Process.exec()

                    modules.telematic.claro.bot_process.Function_Complete(file, root, partitions, brands, origins, date, benefits, contact, \
                                                        value_min, value_max, widget_filter)
                    Mbox_In_Process = QMessageBox()
                    Mbox_In_Process.setWindowTitle("")
                    Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
                    Mbox_In_Process.setText("Proceso de BOTS ejecutado exitosamente.")
                    Mbox_In_Process.exec()

                elif Process == "EMAIL":

                    Mbox_In_Process = QMessageBox()
                    Mbox_In_Process.setWindowTitle("Procesando")
                    Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
                    Mbox_In_Process.setText("Por favor espere la ventana de confirmacion, mientras se procesa el archivo.")
                    Mbox_In_Process.exec()

                    modules.telematic.claro.email_process.Function_Complete(file, root, partitions, brands, origins, date, benefits, \
                                                            value_min, value_max, widget_filter)
                    
                    Mbox_In_Process = QMessageBox()
                    Mbox_In_Process.setWindowTitle("")
                    Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
                    Mbox_In_Process.setText("Proceso de EMAIL ejecutado exitosamente.")
                    Mbox_In_Process.exec()

                elif Process == "IVR":

                    Mbox_In_Process = QMessageBox()
                    Mbox_In_Process.setWindowTitle("Procesando")
                    Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
                    Mbox_In_Process.setText("Por favor espere la ventana de confirmacion, mientras se procesa el archivo.")
                    Mbox_In_Process.exec()

                    modules.telematic.claro.ivr_process.Function_Complete(file, root, partitions, brands, origins, date, today_IVR, benefits, contact, \
                                                        value_min, value_max, widget_filter)
                    
                    Mbox_In_Process = QMessageBox()
                    Mbox_In_Process.setWindowTitle("")
                    Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
                    Mbox_In_Process.setText("Proceso de IVR ejecutado exitosamente.")
                    Mbox_In_Process.exec()

                elif Process == "SMS":

                    Mbox_In_Process = QMessageBox()
                    Mbox_In_Process.setWindowTitle("Procesando")
                    Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
                    Mbox_In_Process.setText("Por favor espere la ventana de confirmacion, mientras se procesa el archivo.")
                    Mbox_In_Process.exec()

                    modules.telematic.claro.sms_process.Function_Complete(file, root, partitions, brands, origins, date, benefits, contact, \
                                                        value_min, value_max, widget_filter)
                    
                    Mbox_In_Process = QMessageBox()
                    Mbox_In_Process.setWindowTitle("")
                    Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
                    Mbox_In_Process.setText("Proceso de SMS ejecutado exitosamente.")
                    Mbox_In_Process.exec()

                else:
                    pass

            else:
                pass
    
        elif campaign_selected == "--- Seleccione opción":
            print("Without campaign selected")
            pass
        
        else:
            
            list_data = self.data_to_process
            print(list_data)
            
            lenght_list = len(self.data_to_process)
            
            if lenght_list >= 3:
                
                file = list_data[0]
                root = list_data[1]
                partitions = int(list_data[2])
                process_resource = list_data[3]
                campaign_selected = list_data[4]
                
                root = f"{root}---- Bases para TELEMATICA ----"
                
                if campaign_selected == "Puntored":

                    Mbox_In_Process = QMessageBox()
                    Mbox_In_Process.setWindowTitle("Procesando")
                    Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
                    Mbox_In_Process.setText("Por favor espere la ventana de confirmacion, mientras se procesa el archivo.")
                    Mbox_In_Process.exec()

                    modules.telematic.telematics_puntored.function_complete_telematics(file, root, partitions, process_resource)
                    
                    Mbox_In_Process = QMessageBox()
                    Mbox_In_Process.setWindowTitle("")
                    Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
                    Mbox_In_Process.setText("Proceso de telematica ejecutado exitosamente.")
                    Mbox_In_Process.exec()
                
                elif campaign_selected == "Crediveci":

                    Mbox_In_Process = QMessageBox()
                    Mbox_In_Process.setWindowTitle("Procesando")
                    Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
                    Mbox_In_Process.setText("Por favor espere la ventana de confirmacion, mientras se procesa el archivo.")
                    Mbox_In_Process.exec()

                    modules.telematic.telematics_crediveci.function_complete_telematics(file, root, partitions, process_resource)
                    
                    Mbox_In_Process = QMessageBox()
                    Mbox_In_Process.setWindowTitle("")
                    Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
                    Mbox_In_Process.setText("Proceso de telematica ejecutado exitosamente.")
                    Mbox_In_Process.exec()
                
                elif campaign_selected == "GM Financial":

                    Mbox_In_Process = QMessageBox()
                    Mbox_In_Process.setWindowTitle("Procesando")
                    Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
                    Mbox_In_Process.setText("Por favor espere la ventana de confirmacion, mientras se procesa el archivo.")
                    Mbox_In_Process.exec()

                    modules.telematic.telematics_gmfinancial.function_complete_telematics(file, root, partitions, process_resource)
                    
                    Mbox_In_Process = QMessageBox()
                    Mbox_In_Process.setWindowTitle("")
                    Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
                    Mbox_In_Process.setText("Proceso de telematica ejecutado exitosamente.")
                    Mbox_In_Process.exec()
                    
                elif campaign_selected == "Ya Dinero":

                    Mbox_In_Process = QMessageBox()
                    Mbox_In_Process.setWindowTitle("Procesando")
                    Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
                    Mbox_In_Process.setText("Por favor espere la ventana de confirmacion, mientras se procesa el archivo.")
                    Mbox_In_Process.exec()

                    modules.telematic.telematics_yadinero.function_complete_telematics(file, root, partitions, process_resource)
                    
                    Mbox_In_Process = QMessageBox()
                    Mbox_In_Process.setWindowTitle("")
                    Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
                    Mbox_In_Process.setText("Proceso de telematica ejecutado exitosamente.")
                    Mbox_In_Process.exec()
                
                elif campaign_selected == "Habi":

                    Mbox_In_Process = QMessageBox()
                    Mbox_In_Process.setWindowTitle("Procesando")
                    Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
                    Mbox_In_Process.setText("Por favor espere la ventana de confirmacion, mientras se procesa el archivo.")
                    Mbox_In_Process.exec()

                    modules.telematic.telematics_habi.function_complete_telematics(file, root, partitions, process_resource)

                    Mbox_In_Process = QMessageBox()
                    Mbox_In_Process.setWindowTitle("")
                    Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
                    Mbox_In_Process.setText("Proceso de telematica ejecutado exitosamente.")
                    Mbox_In_Process.exec()
                    
                elif campaign_selected == "Payjoy":

                    Mbox_In_Process = QMessageBox()
                    Mbox_In_Process.setWindowTitle("Procesando")
                    Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
                    Mbox_In_Process.setText("Por favor espere la ventana de confirmacion, mientras se procesa el archivo.")
                    Mbox_In_Process.exec()

                    modules.telematic.telematics_payjoy.function_complete_telematics(file, root, partitions, process_resource)

                    Mbox_In_Process = QMessageBox()
                    Mbox_In_Process.setWindowTitle("")
                    Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
                    Mbox_In_Process.setText("Proceso de telematica ejecutado exitosamente.")
                    Mbox_In_Process.exec()
                    
                else:
                    print("Without campaign validation")
                
            else:
                print("Without data to process")