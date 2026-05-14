import os
import shutil
import polars as pl
from web.save_files import save_to_csv

def process_data(directory, output_directory, selected_columns, return_matches, join_column_cruce, partitions):
    
    # Read original data with Polars and ensure consistent data types
    original_data = pl.read_csv(directory, separator=";", infer_schema_length=0)
    
    # Convert column names to lowercase
    original_data = original_data.rename({col: col.lower() for col in original_data.columns})
    
    # Rename 'document' column to 'ID' if exists
    if "document" in original_data.columns:
        original_data = original_data.rename({"document": "ID"})
        print("Column 'document' found in original data. Renaming to 'ID'.")
    else:
        print("Column 'document' not found in original data. Using first column as join key.")
    
    join_column_original = original_data.columns[0]
    
    # Copy and read demographic files
    src_folder = r"\\172.128.10.200\4. Gestion de Operaciones\2. Claro\Data compartida\NO BORRAR CONEXIÓN API\Demos Unificados"
    cruce_path = copy_demos_files(src_folder)
    unions_files = read_and_union_files(cruce_path)
    cruce_data = unions_files
    
    # Ensure join columns have the same data type
    print(f"Left join column: {join_column_original} (type: {original_data[join_column_original].dtype})")
    print(f"Right join column: {join_column_cruce} (type: {cruce_data[join_column_cruce].dtype})")
    
    # Convert join columns to string to ensure compatibility
    original_data = original_data.with_columns([
        pl.col(join_column_original).cast(pl.Utf8)
    ])
    
    cruce_data = cruce_data.with_columns([
        pl.col(join_column_cruce).cast(pl.Utf8)
    ])
    
    # Rename the left join column to 'document' to match the desired output
    original_data = original_data.rename({join_column_original: "document"})
    
    # Perform the join
    joined_data = original_data.join(
        cruce_data, 
        left_on="document", 
        right_on=join_column_cruce, 
        how="left"
    )
    
    # Debug: print available columns after join
    print(f"Available columns after join: {joined_data.columns}")
    
    # Filter based on the return_matches flag
    if return_matches:
        result_data = joined_data.filter(pl.col(join_column_cruce).is_not_null())
        print("Return matches is True")
    else:
        result_data = joined_data.filter(pl.col(join_column_cruce).is_null())
        print("Return matches is False")
    
    # Select the specified columns
    result_data = result_data.select(selected_columns)
    result_data = result_data.unique()
    
    # Save the result to the specified output directory
    Type_File = f"demograficos_cruzados"
    delimiter = ";"
    save_to_csv(result_data, output_directory, Type_File, partitions, delimiter)
    
    delete_temp_folder()
    
    print(f"Data processing complete. Results saved to: {output_directory}")

def delete_temp_folder():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    temp_folder = os.path.join(script_dir, "temp_spark_30042000")
    if os.path.exists(temp_folder):
        shutil.rmtree(temp_folder)
        print("🗑️ Temporary folder deleted")

def copy_demos_files(src_folder):
    """
    Copy parquet files from source folder to temporary directory
    """
    # Get current script directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Create temporary folder
    temp_folder = os.path.join(script_dir, "temp_spark_30042000")
    os.makedirs(temp_folder, exist_ok=True)
    print(f"📁 Temporary folder created: {temp_folder}")

    # Copy Parquet files
    copied_files = 0
    for root, _, files in os.walk(src_folder):
        for file in files:
            if file.lower().endswith('.parquet'):
                src_file = os.path.join(root, file)
                dst_file = os.path.join(temp_folder, file)
                print(f"📩 Copying {src_file} to {dst_file}")
                shutil.copy2(src_file, dst_file)
                copied_files += 1
    
    print(f"✅ Copied {copied_files} files to temporary folder")
    return temp_folder

def read_and_union_files(cruce_path):
    """
    Read and union parquet files from directory
    """
    df = pl.read_parquet(f"{cruce_path}/*.parquet")
    df = df.unique()
    return df

########################################
########################################
########################################

# Example Usage in Claro

def search_demographic_claro(filepath, output_directory, partitions, process_data_):
    
    selected_columns = ["document", "demographic", "type"]  # Now 'document' will be available
    return_matches = True  # Set to False if you want non-matching records
    join_column_cruce = "document"  # The column from Data para Cruce to join on

    process_data(filepath, output_directory, selected_columns, return_matches, join_column_cruce, partitions)