import os
from web.pyspark_session import get_spark_session
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from datetime import datetime
from web.save_files import save_to_csv
 
spark = None
sqlContext = None
def get_lazy_spark():
    """Inicializa Spark solo si no existe una sesión previa."""
    global spark, sqlContext
    if spark is None:
        from pyspark.sql import SQLContext # Import local para optimizar
        spark = get_spark_session()
        sqlContext = SQLContext(spark)
    return spark, sqlContext

def read_file_with_delimiter(file_path: str) -> DataFrame:
    """📖 Read file and detect delimiter automatically"""
    print(f"   🔍 Detecting delimiter for: {os.path.basename(file_path)}")
    with open(file_path, 'r') as f:
        first_line = f.readline()
        if ',' in first_line:
            delimiter = ','
            print(f"   ✅ Detected delimiter: COMMA (,)")
        elif ';' in first_line:
            delimiter = ';'
            print(f"   ✅ Detected delimiter: SEMICOLON (;)")
        elif '\t' in first_line:
            delimiter = '\t'
            print(f"   ✅ Detected delimiter: TAB (\\t)")
        else:
            raise ValueError(f"❌ No delimiter detected in file: {os.path.basename(file_path)}")
    
    print(f"   📊 Reading CSV file...")
    df = spark.read.csv(file_path, sep=delimiter, header=True, inferSchema=True)
    print(f"   ✅ File loaded - Shape: ({df.count()} rows, {len(df.columns)} columns)")
    return df, delimiter

def merge_files(input_directory: str, output_directory: str):
    get_lazy_spark()
    """🔄 Merge multiple CSV/TXT files into a single file"""
    
    print("=" * 70)
    print("🚀 STARTING FILE MERGE PROCESS")
    print("=" * 70)
    print(f"📁 Input directory: {input_directory}")
    print(f"📁 Output directory: {output_directory}")
    print("-" * 70)
    
    # 📂 Get all CSV and TXT files
    file_paths = [os.path.join(input_directory, f) for f in os.listdir(input_directory) 
                  if f.endswith('.csv') or f.endswith('.txt')]
    
    print(f"🔍 Found {len(file_paths)} file(s) to process:")
    for i, file_path in enumerate(file_paths, 1):
        print(f"   {i}. {os.path.basename(file_path)}")
    
    if not file_paths:
        print("❌ No CSV or TXT files found in the input directory!")
        return
    
    print("-" * 70)
    print("📖 Reading files...")
    
    merged_df = None
    found_delimiter = None
    total_files_processed = 0
    total_rows = 0
    merged_with_origin_df = None

    for i, file_path in enumerate(file_paths, 1):
        print(f"\n📊 Processing file {i}/{len(file_paths)}: {os.path.basename(file_path)}")
        
        try:
            df, delimiter = read_file_with_delimiter(file_path)
            found_delimiter = delimiter
            file_rows = df.count()
            total_rows += file_rows
            
            df_with_origin = df.withColumn("origen", lit(os.path.basename(file_path)))
            if merged_with_origin_df is None:
                merged_with_origin_df = df_with_origin
            else:
                merged_with_origin_df = merged_with_origin_df.unionByName(df_with_origin, allowMissingColumns=True)
            
            if merged_df is None:
                merged_df = df
                print(f"   🔄 Created initial DataFrame")
            else:
                merged_df = merged_df.unionByName(df, allowMissingColumns=True)
                print(f"   🔄 Merged with existing DataFrame")
            
            total_files_processed += 1
            print(f"   ✅ SUCCESS - Added {file_rows:,} rows")
            
        except Exception as e:
            print(f"   ❌ ERROR reading file: {e}")
            continue

    print("-" * 70)
    
    if merged_df is not None:
        print(f"📊 MERGE COMPLETED:")
        print(f"   • Files processed: {total_files_processed}/{len(file_paths)}")
        print(f"   • Total rows before deduplication: {total_rows:,}")
        print(f"   • Current DataFrame shape: ({merged_df.count():,} rows, {len(merged_df.columns)} columns)")
        
        # 🧹 Remove duplicates
        print(f"\n🧹 Removing duplicates...")
        initial_count = merged_df.count()
        merged_df = merged_df.dropDuplicates()
        final_count = merged_df.count()
        duplicates_removed = initial_count - final_count
        
        print(f"   • Rows before: {initial_count:,}")
        print(f"   • Rows after: {final_count:,}")
        print(f"   • Duplicates removed: {duplicates_removed:,}")
        
        # 🔧 Determine process type based on column names
        columns = merged_df.columns
        print(f"\n🔍 Analyzing columns: {columns}")
        
        if "2_" in columns and "3_" in columns:
            Type_Process = "Conversion"
            print(f"   🏷️ Process type: CONVERSION (special columns detected)")
        else:
            Type_Process = "Union_Archivos"
            print(f"   🏷️ Process type: FILE MERGE")
        
        # 💾 Set saving parameters
        Partitions = 1
        delimiter = ";"
        
        print(f"\n💾 Saving parameters:")
        print(f"   • Process type: {Type_Process}")
        print(f"   • Partitions: {Partitions}")
        print(f"   • Delimiter: '{delimiter}'")
        print(f"   • Output directory: {output_directory}")
        
        # 💽 Save the merged file
        print(f"\n📤 Saving merged file...")
        save_to_csv(merged_df, output_directory, Type_Process, Partitions, delimiter)
        
        if merged_with_origin_df is not None:
            print(f"\n📤 Saving merged file with origin...")
            save_to_csv(merged_with_origin_df, output_directory, "Union_Archivos_Con_Origen", Partitions, delimiter)
        
        print(f"\n✅ FINAL STATISTICS:")
        print(f"   • Total files merged: {total_files_processed}")
        print(f"   • Final unique rows: {final_count:,}")
        print(f"   • Total columns: {len(merged_df.columns)}")
        print(f"   • Process completed successfully!")
        
    else:
        print("❌ No files could be processed successfully!")
        print("💡 Please check that files contain valid data and delimiters.")
    
    print("=" * 70)
    print("🏁 FILE MERGE PROCESS COMPLETED")
    print("=" * 70)