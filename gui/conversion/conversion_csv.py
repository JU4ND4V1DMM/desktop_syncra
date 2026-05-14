import pandas as pd
import os
from zipfile import BadZipFile

def convert_xlsx_to_csv(folder_path):
    """
    Convierte archivos .xlsx a .csv en una carpeta y sus subcarpetas
    """
    
    if not os.path.exists(folder_path):
        print(f"❌ La carpeta {folder_path} no existe.")
        return
    
    print(f"🔍 Buscando archivos en: {folder_path}")
    
    # 📂 Recorre todas las subcarpetas recursivamente
    for root, dirs, files in os.walk(folder_path):
        print(f"📂 Explorando carpeta: {root}")
        
        for filename in files:
            
            if filename.endswith(".xlsx"):
                print(f"📊 Procesando archivo: {filename}")
                
                file_path = os.path.join(root, filename)
                csv_filename = filename.replace(".xlsx", ".csv")
                csv_path = os.path.join(root, csv_filename)
                
                try:
                    # 📖 Leyendo archivo Excel
                    df = pd.read_excel(file_path, engine='openpyxl')
                    
                    # 💾 Guardando como CSV
                    df.to_csv(csv_path, index=False, sep=';')
                    print(f"✅ Convertido: {filename} → {csv_filename}")
                    
                except BadZipFile:
                    print(f"❌ Error: {filename} no es un archivo Excel válido")
                    continue

                except Exception as e:
                    print(f"⚠️ Error convirtiendo {filename}: {e}")
                    continue
            else:
                print(f"⏭️ Saltando: {filename} (no es .xlsx)")
    
    print("🎉 ¡Conversión completada!")