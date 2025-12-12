import os
from PyQt6.QtWidgets import QMessageBox
from datetime import datetime
import pandas as pd

def process_ranking_files(input_folder, output_file):
    """
    Processes Excel files in a folder, filters and transforms data, and saves the result to a CSV file.

    Args:
        input_folder (str): Path to the folder containing the Excel files.
        output_file (str): Path to save the resulting CSV file.
    """
    print(f"🚀 Starting Ranking Files Processing...")
    print(f"📂 Input folder: {input_folder}")
    print(f"💾 Output file: {output_file}")
    
    all_data = []
    all_data_detail = []
    unprocessed_files = []  # List to track files with 0 records
    processed_files_count = 0
    
    # Define possible column names for dynamic handling
    cuenta_columns = ["raiz", "cuenta"]
    estado_columns = ["gestion", "recuperada"]
    filter_columns = ["aliado", "casa", "casacobro", "agencia"]
    servicios_column = "nservicios"
    pago_column = ["pago"]
    datepayment_column = ["fechadepago"]
    concept_column = ["concepto", "estadoactual"]

    # Iterate through all files in the input folder
    print(f"\n🔍 Scanning folder for Excel files...")
    excel_files = [f for f in os.listdir(input_folder) if f.endswith(".xlsx") or f.endswith(".xls")]
    print(f"📊 Found {len(excel_files)} Excel file(s)")

    for file in excel_files:
        file_path = os.path.join(input_folder, file)
        print(f"\n📄 Processing: {file}")
        
        try:
            excel_data = pd.ExcelFile(file_path)
            print(f"   📑 Sheets found: {len(excel_data.sheet_names)}")
            
            file_has_data = False
            sheet_count = 0

            # Iterate through all sheets in the Excel file
            for sheet_name in excel_data.sheet_names:
                sheet_count += 1
                print(f"   📊 Processing sheet {sheet_count}: {sheet_name[:20]}...", end=" ")
                
                df = excel_data.parse(sheet_name)

                # Normalize column names: strip spaces, convert to lowercase, and replace special characters
                df.columns = (
                    df.columns.str.strip()
                    .str.lower()
                    .str.replace(" ", "")
                    .str.replace(r"[^\w\s]", "", regex=True)
                )

                # Filter rows dynamically based on filter_columns
                filter_column = next((col for col in filter_columns if col in df.columns), None)
                if filter_column:
                    initial_count = len(df)
                    df = df[df[filter_column].str.contains("RECUPERA", case=False, na=False)]
                    filtered_count = len(df)
                    if filtered_count == 0:
                        print(f"⚠️ 0 records after filtering")
                        continue
                    print(f"✅ {filtered_count} records (filtered from {initial_count})")
                else:
                    print(f"❌ No filter column found")
                    continue

                # Handle "cuenta" columns dynamically
                cuenta_column = next((col for col in cuenta_columns if col in df.columns), None)
                if cuenta_column:
                    df["cuenta"] = (df[cuenta_column]
                                    .astype(str)
                                    .str.strip()
                                    .str.replace(r"\.0$", "", regex=True)
                                    .str.replace(".", "", regex=False))

                    # Count occurrences of each "cuenta" and add a "servicios" column
                    if servicios_column in df.columns:
                        df["servicios"] = df[servicios_column]
                        df["tipo"] = "fija"
                    else:
                        df["servicios"] = df.groupby("cuenta")["cuenta"].transform("count")
                        df["tipo"] = "movil"
                else:
                    print(f"❌ No cuenta column found")
                    continue

                # Add "pago" column dynamically
                payment_column = next((col for col in pago_column if col in df.columns), None)
                if payment_column:
                    df["pago"] = df[payment_column]
                else:
                    df["pago"] = None

                # Add "fecha" column dynamically
                date_column = next((col for col in datepayment_column if col in df.columns), None)
                if date_column:
                    df["fecha"] = df[date_column]
                else:
                    df["fecha"] = None

                # Add "concepto" column dynamically
                concepto_column = next((col for col in concept_column if col in df.columns), None)
                if concepto_column:
                    df["concepto"] = df[concepto_column]
                else:
                    df["concepto"] = None

                # Add "estado" column dynamically
                estado_column = next((col for col in estado_columns if col in df.columns), None)
                if estado_column:
                    df["estado"] = df[estado_column]
                else:
                    df["estado"] = df["concepto"]

                # Create a new column "llave" based on "estado" and "tipo"
                df["llave"] = (df["estado"].astype(str) + df["tipo"].astype(str)).str.upper()

                # Update "estado" based on "llave"
                df["estado"] = df["llave"].apply(
                    lambda x: "NO RECUPERADA" if x == "NOFIJA" else
                              "RECUPERADA" if x == "SIFIJA" else
                              "NO GESTIONAR" if x == "NOMOVIL" else
                              "GESTIONAR" if x == "AJUSTEMOVIL" else
                              "GESTIONAR" if x == "PENDIENTEMOVIL" else
                              "NO GESTIONAR" if x == "PAGO TOTAL NO RXMOVIL" else
                              "NO GESTIONAR" if x == "PAGO TOTAL NO_RXMOVIL" else
                              "NO GESTIONAR" if x == "PAGO TOTAL NO-RX" else
                              "NO GESTIONAR" if x == "PAGO TOTAL SI RXMOVIL" else
                              "NO GESTIONAR" if x == "PAGO TOTAL SI_RXMOVIL" else
                              "NO GESTIONAR" if x == "PAGO TOTAL SI-RX" else
                              "GESTIONAR" if x == "SIMOVIL" else
                              "GESTION RECAUDO" if "GESTION RECAUDO" in x else
                              "GESTION RECAUDO" if "GESTION_RECAUDO" in x else
                              "GESTION RECAUDO" if "GESTIÓN_RECAUDO" in x else
                              "GESTION RECAUDO" if "GESTIÓN RECAUDO" in x else
                              x
                )
                
                df["archivo"] = file
                
                # Select relevant columns if they exist
                df.columns = df.columns.str.upper()
                required_columns = ["CUENTA", "SERVICIOS", "ESTADO"]
                required_columns_detail = ["CUENTA", "SERVICIOS", "ESTADO", "PAGO", "FECHA", "CONCEPTO", "ARCHIVO"]
                
                df_detail = df[[col for col in required_columns_detail if col in df.columns]]
                df_detail["PAGO"] = df_detail["PAGO"].fillna(0).astype(int)
                df = df[[col for col in required_columns if col in df.columns]]

                # Remove duplicates
                initial_dup_count = len(df)
                df = df.drop_duplicates()
                df_detail = df_detail.drop_duplicates()
                dup_removed = initial_dup_count - len(df)
                if dup_removed > 0:
                    print(f"   🧹 Removed {dup_removed} duplicates")

                # Append to the list of all data
                if len(df) > 0:
                    all_data.append(df)
                    all_data_detail.append(df_detail)
                    file_has_data = True
                    
            if file_has_data:
                processed_files_count += 1
                print(f"   ✅ File processed successfully")
            else:
                unprocessed_files.append(file)
                print(f"   ⚠️  File added to unprocessed list (no valid data)")
                
        except Exception as e:
            unprocessed_files.append(file)
            print(f"   ❌ Error processing file: {str(e)[:50]}...")
    
    print(f"\n{'='*50}")
    print(f"📊 PROCESSING SUMMARY")
    print(f"{'='*50}")
    print(f"✅ Successfully processed: {processed_files_count} file(s)")
    print(f"⚠️  Unprocessed files: {len(unprocessed_files)}")
    
    # Concatenate all data and save to CSV
    if all_data:
        print(f"\n💾 Saving results...")
        
        # Save for CARGUE
        folder = f"---- Bases para CARGUE ----"
        output_directory = os.path.join(output_file, folder)
        os.makedirs(output_directory, exist_ok=True)
        output_file_ranking = os.path.join(output_directory, f"Cargue Rankings {datetime.now().strftime('%Y-%m-%d')}.csv")

        final_df = pd.concat(all_data, ignore_index=True)
        final_df.to_csv(output_file_ranking, sep=";", index=False, encoding="utf-8")
        print(f"📁 CARGUE file saved: {output_file_ranking}")
        print(f"   📊 Total records: {len(final_df)}")
        
        # Save for CRUCE
        folder = f"---- Bases para CRUCE ----"
        output_directory = os.path.join(output_file, folder)
        os.makedirs(output_directory, exist_ok=True)
        output_file_detail = os.path.join(output_directory, f"Detalle Rankings {datetime.now().strftime('%Y-%m-%d')}.csv")

        final_detail_df = pd.concat(all_data_detail, ignore_index=True)
        final_detail_df.to_csv(output_file_detail, sep=";", index=False, encoding="utf-8")
        print(f"📁 CRUCE file saved: {output_file_detail}")
        print(f"   📊 Total detailed records: {len(final_detail_df)}")
        
        print(f"\n🎉 PROCESSING COMPLETED SUCCESSFULLY!")
        print(f"⏰ Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    else:
        print(f"\n❌ No data found to process.")
        
    # Log unprocessed files
    if unprocessed_files:
        unprocessed_files_str = "\n".join(unprocessed_files)
        print(f"\n⚠️  Showing warning for {len(unprocessed_files)} unprocessed file(s)")
        Mbox_In_Process = QMessageBox()
        Mbox_In_Process.setWindowTitle("📄 Unprocessed Files")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Warning)
        Mbox_In_Process.setText(f"✅ Processing completed with {processed_files_count} successful files.\n\n⚠️  The following {len(unprocessed_files)} file(s) could not be processed:\n\n" + unprocessed_files_str)
        Mbox_In_Process.setStandardButtons(QMessageBox.StandardButton.Ok)
        Mbox_In_Process.exec()
    
    print(f"{'='*50}")
    return processed_files_count