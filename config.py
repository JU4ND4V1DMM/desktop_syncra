from PyQt6.QtWidgets import QApplication
from PyQt6 import uic
import os
import sys
import traceback

Version_Api = "v1.0.13 (Py3.11-Spark3.5)"
API = "Syncra"


class main_window:

    def __init__(self):

        self.app = QApplication(sys.argv)

        try:
            from gui.starter import Init_APP
            self.project = Init_APP()

        except Exception as e:
            script_path = os.path.abspath(__file__)
            root_API = os.path.dirname(script_path)
            
            self.process_data = uic.loadUi(
                f"{root_API}/gui/warnsparksession.ui")
            self.process_data.label_Version_Control_Version.setText(
                f"{API} - {Version_Api}")
            self.process_data.label_Version_Detail.setText(
                f"Error: {str(e)}")
            self.process_data.show()

        self.app.exec()