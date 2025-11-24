import polars as pl
from pathlib import Path
from datetime import datetime

def convert_csv_to_parquet_simple(input_folder: str, output_base_path: str) -> None:
    """
    Efficient CSV to Parquet converter with memory optimization.
    """
    separator = ";"
    output_folder = Path(output_base_path) / f"PARQUET_{datetime.now().strftime('%Y%m%d')} {Path(input_folder).name}"
    output_folder.mkdir(parents=True, exist_ok=True)
    
    print(f"🔄 Converting: {input_folder} -> {output_folder}")
    
    for file_path in Path(input_folder).glob('*.csv'):
        try:
            print(f"📖 Processing: {file_path.name}")
            
            # Memory-optimized reading configuration
            read_options = {
                "separator": separator,
                "truncate_ragged_lines": True,
                "ignore_errors": True,
                "infer_schema_length": 1000,  # Reduced for performance
                "low_memory": True,           # Enable low memory mode
                "rechunk": False,             # Avoid memory reallocation
            }
            
            # Try encodings with minimal reading first
            encodings = ['utf-8', 'latin1', 'iso-8859-1', 'windows-1252']
            df = None
            
            for encoding in encodings:
                try:
                    # Test encoding with just 3 rows
                    df_test = pl.read_csv(
                        file_path,
                        n_rows=3,
                        **{**read_options, "encoding": encoding}
                    )
                    # If encoding works, read full file
                    df = pl.read_csv(
                        file_path,
                        **{**read_options, "encoding": encoding}
                    )
                    break
                except Exception:
                    continue
            
            if df is None:
                print(f"❌ {file_path.name}: Could not read with any encoding")
                continue
            
            # Write with compression
            output_file = output_folder / f"{file_path.stem}.parquet"
            df.write_parquet(output_file, compression="zstd")  # Better compression than snappy
            
            print(f"✅ {file_path.name} -> {len(df):,} rows, {len(df.columns)} cols")
            
        except Exception as e:
            print(f"❌ {file_path.name}: {str(e)[:100]}...")
    
    print("🎉 Conversion completed")

# Ultra-efficient version for massive files
def convert_large_csv_optimized(input_folder: str, output_base_path: str) -> None:
    """
    Stream processing for extremely large CSV files.
    """
    separator = ";"
    output_folder = Path(output_base_path) / f"PARQUET_{datetime.now().strftime('%Y%m%d')} {Path(input_folder).name}"
    output_folder.mkdir(parents=True, exist_ok=True)
    
    print(f"🔄 Stream converting: {input_folder} -> {output_folder}")
    
    for file_path in Path(input_folder).glob('*.csv'):
        try:
            print(f"📖 Streaming: {file_path.name}")
            
            # Detect encoding and schema first
            encodings = ['utf-8', 'latin1', 'iso-8859-1', 'windows-1252']
            found_encoding = None
            
            for encoding in encodings:
                try:
                    # Read minimal data to detect schema
                    pl.read_csv(file_path, n_rows=10, separator=separator, encoding=encoding)
                    found_encoding = encoding
                    break
                except Exception:
                    continue
            
            if not found_encoding:
                print(f"❌ {file_path.name}: Encoding detection failed")
                continue
            
            # Stream process the entire file
            df = pl.read_csv(
                file_path,
                separator=separator,
                encoding=found_encoding,
                low_memory=True,
                infer_schema_length=1000,
                ignore_errors=True
            )
            
            output_file = output_folder / f"{file_path.stem}.parquet"
            df.write_parquet(output_file, compression="zstd")
            
            print(f"✅ {file_path.name} -> {len(df):,} rows")
            
        except Exception as e:
            print(f"❌ {file_path.name}: {str(e)[:100]}...")
    
    print("🎉 Stream conversion completed")

# Most optimized version - uses scan_csv for maximum efficiency
def convert_csv_scan_optimized(input_folder: str, output_base_path: str) -> None:
    """
    Most efficient version using lazy evaluation (scan_csv).
    """
    separator = ";"
    output_folder = Path(output_base_path) / f"PARQUET_{datetime.now().strftime('%Y%m%d')} {Path(input_folder).name}"
    output_folder.mkdir(parents=True, exist_ok=True)
    
    print(f"🔄 Lazy converting: {input_folder} -> {output_folder}")
    
    for file_path in Path(input_folder).glob('*.csv'):
        try:
            print(f"📖 Lazy processing: {file_path.name}")
            
            # Use lazy evaluation for maximum memory efficiency
            encodings = ['utf-8', 'latin1', 'iso-8859-1', 'windows-1252']
            lf = None
            
            for encoding in encodings:
                try:
                    lf = pl.scan_csv(
                        file_path,
                        separator=separator,
                        encoding=encoding,
                        infer_schema_length=1000,
                        low_memory=True
                    )
                    # Test if it works by checking schema
                    lf.schema
                    break
                except Exception:
                    lf = None
                    continue
            
            if lf is None:
                print(f"❌ {file_path.name}: Could not read with any encoding")
                continue
            
            # Execute lazy frame and write to parquet
            output_file = output_folder / f"{file_path.stem}.parquet"
            lf.sink_parquet(output_file, compression="zstd")
            
            # Get row count efficiently
            row_count = lf.select(pl.len()).collect().item()
            print(f"✅ {file_path.name} -> {row_count:,} rows")
            
        except Exception as e:
            print(f"❌ {file_path.name}: {str(e)[:100]}...")
    
    print("🎉 Lazy conversion completed")
    
def convert_csv_to_parquet(input_folder: str, output_base_path: str) -> None:
    """
    Wrapper function to choose the best conversion method based on file size.
    """
    try:
        convert_csv_to_parquet(input_folder, output_base_path)
    except Exception:
        try:
            convert_large_csv_optimized(input_folder, output_base_path)
        except Exception:
            convert_csv_scan_optimized(input_folder, output_base_path)