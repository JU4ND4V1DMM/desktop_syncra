import os
from PyQt6.QtWidgets import QMessageBox
from datetime import datetime
import pandas as pd

def process_ranking_files(input_folder, output_file):
    print(f"🚀 Starting Ranking Files Processing...")
    print(f"📂 Input folder: {input_folder}")
    print(f"💾 Output file: {output_file}")
    
    all_data = []
    all_data_detail = []
    unprocessed_files = []
    processed_files_count = 0
    
    cuenta_columns = ["raiz", "cuenta", "raíz"]
    estado_columns = ["gestion", "recuperada", "estado"]
    filter_columns = ["aliado", "casa", "casas", "casacobro", "agencia"]
    servicios_column = "nservicios"
    pago_column = ["pago", "pago total"]
    datepayment_column = ["fechadepago"]
    concept_column = ["concepto", "estadoactual", "estado"]

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

            for sheet_name in excel_data.sheet_names:
                sheet_count += 1
                print(f"   📊 Processing sheet {sheet_count}: {sheet_name[:20]}...", end=" ")
                
                df = excel_data.parse(sheet_name)

                original_columns = df.columns.str.lower().tolist()
                
                df.columns = (
                    df.columns.str.strip()
                    .str.lower()
                    .str.replace(" ", "")
                    .str.replace(r"[^\w\s]", "", regex=True)
                )

                is_special_estado_file = "estado" in original_columns
                
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

                cuenta_column = next((col for col in cuenta_columns if col in df.columns), None)
                if cuenta_column:
                    df["cuenta"] = (df[cuenta_column]
                                    .astype(str)
                                    .str.strip()
                                    .str.replace(r"\.0$", "", regex=True)
                                    .str.replace(".", "", regex=False))

                    if servicios_column in df.columns:
                        df["servicios"] = df[servicios_column]
                        df["tipo"] = "fija"
                    else:
                        df["servicios"] = df.groupby("cuenta")["cuenta"].transform("count")
                        df["tipo"] = "movil"
                else:
                    print(f"❌ No cuenta column found")
                    continue

                payment_column = next((col for col in pago_column if col in df.columns), None)
                if payment_column:
                    df["pago"] = df[payment_column]
                else:
                    df["pago"] = None

                date_column = next((col for col in datepayment_column if col in df.columns), None)
                if date_column:
                    df["fecha"] = df[date_column]
                else:
                    df["fecha"] = None

                concepto_column = next((col for col in concept_column if col in df.columns), None)
                if concepto_column:
                    df["concepto"] = df[concepto_column]
                else:
                    df["concepto"] = None

                estado_column = next((col for col in estado_columns if col in df.columns), None)
                if estado_column:
                    df["estado"] = df[estado_column]
                else:
                    df["estado"] = df["concepto"]

                # 🔽 SOLO MODIFICADO ESTA PARTE 🔽
                if is_special_estado_file:
                    if 'concepto' in df.columns:
                        df['concepto_real'] = df['concepto']
                    elif 'estado' in original_columns and 'estado' in df.columns:
                        df['concepto_real'] = df['estado']
                    
                    # Guardamos el estado original para mostrarlo
                    estado_original = df['estado'].copy()
                    
                    # Nueva lógica: GESTIONAR - CAIDO o NO GESTIONAR - estado_original
                    df['estado'] = df['estado'].apply(
                        lambda x: f"GESTIONAR - CAIDO" if str(x).strip().upper() == "CAIDO" 
                        else f"NO GESTIONAR - {str(x).strip()}"
                    )
                    print(f"   🔄 Aplicada lógica especial: CAIDO → 'GESTIONAR - CAIDO', otros → 'NO GESTIONAR - estado_original'")
                # 🔼 FIN DE LA MODIFICACIÓN 🔼
                else:
                    df["llave"] = (df["estado"].astype(str) + df["tipo"].astype(str)).str.upper()

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
                                  "GESTION RECAUDO" if "GESTION RECAUDO" in str(x) else
                                  "GESTION RECAUDO" if "GESTION_RECAUDO" in str(x) else
                                  "GESTIÓN RECAUDO" if "GESTIÓN_RECAUDO" in str(x) else
                                  "GESTIÓN RECAUDO" if "GESTIÓN RECAUDO" in str(x) else
                                  str(x)
                    )
                
                df["archivo"] = file
                
                df.columns = df.columns.str.upper()
                
                required_columns = ["CUENTA", "SERVICIOS", "ESTADO"]
                required_columns_detail = ["CUENTA", "SERVICIOS", "ESTADO", "PAGO", "FECHA", "CONCEPTO", "ARCHIVO"]
                
                if 'CONCEPTO_REAL' in df.columns:
                    df['CONCEPTO'] = df['CONCEPTO_REAL']
                
                df_detail = df[[col for col in required_columns_detail if col in df.columns]]
                if 'PAGO' in df_detail.columns:
                    df_detail["PAGO"] = df_detail["PAGO"].fillna(0).astype(int)
                
                df = df[[col for col in required_columns if col in df.columns]]

                initial_dup_count = len(df)
                df = df.drop_duplicates()
                df_detail = df_detail.drop_duplicates()
                dup_removed = initial_dup_count - len(df)
                if dup_removed > 0:
                    print(f"   🧹 Removed {dup_removed} duplicates")

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
    
    if all_data:
        print(f"\n💾 Saving results...")
        
        folder = f"---- Bases para CARGUE ----"
        output_directory = os.path.join(output_file, folder)
        os.makedirs(output_directory, exist_ok=True)
        output_file_ranking = os.path.join(output_directory, f"Cargue Rankings {datetime.now().strftime('%Y-%m-%d')}.csv")

        final_df = pd.concat(all_data, ignore_index=True)
        final_df.to_csv(output_file_ranking, sep=";", index=False, encoding="utf-8")
        print(f"📁 CARGUE file saved: {output_file_ranking}")
        print(f"   📊 Total records: {len(final_df)}")
        
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