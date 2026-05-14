import pandas as pd
import os
from datetime import datetime

def process_excel_files(input_folder, output_folder):
    """
    Processes XLSX files in an input folder, standardizing their headers
    and saving the results in an output folder.

    Args:
        input_folder (str): Path to the folder containing the XLSX files to be processed.
        output_folder (str): Path to the folder where the processed files will be saved.
    """
    time_value = datetime.now().strftime("%Y-%m-%d")
    processed_output_folder = os.path.join(output_folder, f'Procesamiento {time_value} WISEBOT')
    
    # Create the output folder if it doesn't exist
    os.makedirs(processed_output_folder, exist_ok=True)

    # Define the input and output header structures
    input_headers_1 = ['campaña', 'fecha_estado_final', 'rut', 'nombre', 'apellido', 'telefono', 'estado_llamada', 'desea_beneficios', 'tiempo_llamada']
    input_headers_2 = ['campaña', 'fecha_llamada', 'rut', 'nombre', 'apellido', 'telefono', 'desea_beneficios', 'estado_llamada', 'tiempo_llamada']
    input_headers_3 = ['campaña', 'fecha_llamada', 'rut', 'nombre', 'apellido', 'telefono', 'estado_llamada', 'tiempo_llamada']
    output_headers = ['campaña', 'fecha_llamada', 'rut', 'nombre', 'apellido', 'telefono', 'estado_llamada', 'desea_beneficios', 'tiempo_llamada']

    # Iterate over all files in the input folder
    for file_name in os.listdir(input_folder):
        if file_name.endswith('.xlsx'):
            input_file_path = os.path.join(input_folder, file_name)
            output_file_path = os.path.join(processed_output_folder, file_name)

            print(f"Processing file: {file_name}")

            try:
                # Read all sheets from the Excel file
                xl = pd.ExcelFile(input_file_path)
                consolidated_df = pd.DataFrame()

                # Iterate over each sheet
                for sheet_name in xl.sheet_names:
                    df = pd.read_excel(xl, sheet_name=sheet_name)
                    
                    # Standardize column names to lowercase
                    df.columns = [col.strip().lower() for col in df.columns]
                    
                    columns = df.columns.tolist()
                    
                    # Identify the header structure
                    if columns == input_headers_1:
                        print(f"File {file_name} - Sheet {sheet_name}: Match with header 1")
                        df_temp = pd.DataFrame(columns=output_headers)
                        df_temp['campaña'] = df['campaña']
                        df_temp['fecha_llamada'] = df['fecha_estado_final']
                        df_temp['rut'] = df['rut']
                        df_temp['telefono'] = df['telefono']
                        df_temp['estado_llamada'] = df['estado_llamada']
                        df_temp['tiempo_llamada'] = df['tiempo_llamada']
                        consolidated_df = pd.concat([consolidated_df, df_temp], ignore_index=True)
                        
                    elif columns == input_headers_2:
                        print(f"File {file_name} - Sheet {sheet_name}: Match with header 2")
                        df_temp = pd.DataFrame(columns=output_headers)
                        df_temp['campaña'] = df['campaña']
                        df_temp['fecha_llamada'] = df['fecha_llamada']
                        df_temp['rut'] = df['rut']
                        df_temp['telefono'] = df['telefono']
                        df_temp['estado_llamada'] = df['estado_llamada']
                        df_temp['tiempo_llamada'] = df['tiempo_llamada']
                        consolidated_df = pd.concat([consolidated_df, df_temp], ignore_index=True)

                    elif columns == input_headers_3:
                        print(f"File {file_name} - Sheet {sheet_name}: Match with header 3")
                        df_temp = pd.DataFrame(columns=output_headers)
                        df_temp['campaña'] = df['campaña']
                        df_temp['fecha_llamada'] = df['fecha_llamada']
                        df_temp['rut'] = df['rut']
                        df_temp['telefono'] = df['telefono']
                        df_temp['estado_llamada'] = df['estado_llamada']
                        df_temp['tiempo_llamada'] = df['tiempo_llamada']
                        consolidated_df = pd.concat([consolidated_df, df_temp], ignore_index=True)
                    
                    elif columns == output_headers:
                        print(f"File {file_name} - Sheet {sheet_name}: Match with the output format, saving without changes.")
                        consolidated_df = pd.concat([consolidated_df, df], ignore_index=True)
                    
                    else:
                        print(f"Columns {columns} - File {file_name} - Sheet {sheet_name}: No matching header structure found, skipping.")

                if not consolidated_df.empty:
                    with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
                        consolidated_df.to_excel(writer, sheet_name='Detalle', index=False)
                    print(f"File {file_name} processed and saved to {processed_output_folder}\n")
                else:
                    print(f"File {file_name} contains no matching sheets, not saved.\n")

            except Exception as e:
                print(f"Error processing file {file_name}: {e}\n")