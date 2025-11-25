import polars as pl
from pathlib import Path
from datetime import datetime

def convert_csv_to_parquet_optimized(input_folder: str, output_base_path: str) -> None:
    """
    CSV to Parquet converter with robust encoding handling.
    """
    separator = ";"
    output_folder = Path(output_base_path) / f"PARQUET_{datetime.now().strftime('%Y%m%d')} {Path(input_folder).name}"
    output_folder.mkdir(parents=True, exist_ok=True)
    
    print(f"🔄 Converting: {input_folder} -> {output_folder}")
    
    for file_path in Path(input_folder).glob('*.csv'):
        try:
            print(f"📖 Processing: {file_path.name}")
            
            # Configuración robusta para lectura
            read_options = {
                "separator": separator,
                "truncate_ragged_lines": True,  # ✅ FIX: Para líneas con campos inconsistentes
                "ignore_errors": True,
                "infer_schema_length": 1000,
                "low_memory": True,
                "rechunk": False,
            }
            
            # Intentar diferentes encodings
            encodings = ['utf-8', 'latin1', 'iso-8859-1', 'windows-1252', 'cp1252']
            df = None
            
            for encoding in encodings:
                try:
                    # Probar encoding con pocas filas primero
                    df_test = pl.read_csv(
                        file_path,
                        n_rows=5,
                        **{**read_options, "encoding": encoding}
                    )
                    # Si funciona, leer archivo completo
                    df = pl.read_csv(
                        file_path,
                        **{**read_options, "encoding": encoding}
                    )
                    print(f"✅ {file_path.name}: Encoding {encoding} successful")
                    break
                except Exception as e:
                    continue
            
            if df is None:
                print(f"❌ {file_path.name}: Could not read with any encoding")
                # Intentar sin especificar encoding
                try:
                    df = pl.read_csv(file_path, **read_options)
                    print(f"✅ {file_path.name}: Read without explicit encoding")
                except Exception as e:
                    print(f"❌ {file_path.name}: Final attempt failed - {str(e)[:100]}")
                    continue
            
            # Escribir archivo Parquet
            output_file = output_folder / f"{file_path.stem}.parquet"
            df.write_parquet(output_file, compression="zstd")
            
            print(f"✅ {file_path.name} -> {len(df):,} rows, {len(df.columns)} cols")
            
        except Exception as e:
            print(f"❌ {file_path.name}: {str(e)[:100]}...")
    
    print("🎉 Conversion completed")

# Versión con scan_csv para archivos muy grandes
def convert_csv_lazy_optimized(input_folder: str, output_base_path: str) -> None:
    """
    Usando evaluación perezosa para archivos grandes.
    """
    separator = ";"
    output_folder = Path(output_base_path) / f"PARQUET_{datetime.now().strftime('%Y%m%d')} {Path(input_folder).name}"
    output_folder.mkdir(parents=True, exist_ok=True)
    
    print(f"🔄 Lazy converting: {input_folder} -> {output_folder}")
    
    for file_path in Path(input_folder).glob('*.csv'):
        try:
            print(f"📖 Lazy processing: {file_path.name}")
            
            encodings = ['utf-8', 'latin1', 'iso-8859-1', 'windows-1252']
            lf = None
            
            for encoding in encodings:
                try:
                    lf = pl.scan_csv(
                        file_path,
                        separator=separator,
                        encoding=encoding,
                        infer_schema_length=1000,
                        low_memory=True,
                        truncate_ragged_lines=True  # ✅ FIX
                    )
                    # Probar el schema
                    lf.schema
                    break
                except Exception:
                    lf = None
                    continue
            
            if lf is None:
                print(f"❌ {file_path.name}: Could not read with any encoding")
                continue
            
            output_file = output_folder / f"{file_path.stem}.parquet"
            lf.sink_parquet(output_file, compression="zstd")
            
            # Contar filas eficientemente
            row_count = lf.select(pl.len()).collect().item()
            print(f"✅ {file_path.name} -> {row_count:,} rows")
            
        except Exception as e:
            print(f"❌ {file_path.name}: {str(e)[:100]}...")
    
    print("🎉 Lazy conversion completed")

# ✅ FUNCIÓN WRAPPER CORREGIDA - sin recursividad
def convert_csv_to_parquet(input_folder: str, output_base_path: str) -> None:
    """
    Wrapper function para conversión CSV to Parquet.
    """
    use_lazy=False
    if use_lazy:
        convert_csv_lazy_optimized(input_folder, output_base_path)
    else:
        convert_csv_to_parquet_optimized(input_folder, output_base_path)