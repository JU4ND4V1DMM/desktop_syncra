import polars as pl
from pathlib import Path
from datetime import datetime

def process_demographics_fast(input_folder: str, output_folder: str, num_partitions: int = None, ui_process_data=None) -> None:
    """
    🚀 High-performance function to read CSV and Parquet files, process them,
    and save partitioned by specified number of partitions.
    
    Args:
        input_folder: 📁 Path to input folder containing CSV and Parquet files
        output_folder: 📁 Path to output folder for saving partitioned data
        num_partitions: 🔢 Number of partitions to split the data (optional)
    """
    input_path = Path(input_folder)
    output_path = Path(output_folder)
    
    print(f"🔍 Processing demographics data...")
    print(f"📂 Input folder: {input_folder}")
    print(f"📂 Output folder: {output_folder}")
    if num_partitions:
        print(f"🔢 Number of partitions: {num_partitions}")
    print("=" * 60)
    
    # Ensure output directory exists
    output_path.mkdir(parents=True, exist_ok=True)
    print(f"✅ Output directory created/verified: {output_path}")
    
    # Read all Parquet files in parallel
    parquet_files = list(input_path.glob("*.parquet"))
    parquet_df = None
    if parquet_files:
        print(f"📊 Found {len(parquet_files)} Parquet file(s)")
        print(f"📥 Reading Parquet files: {[f.name for f in parquet_files[:3]]}{'...' if len(parquet_files) > 3 else ''}")
        parquet_df = pl.read_parquet(parquet_files)
        
        # Convert ALL columns to string
        parquet_df = parquet_df.cast(pl.Utf8)
        print(f"✅ Parquet data loaded:")
        print(f"   • Columns: {len(parquet_df.columns)}")
        print(f"   • Shape: {parquet_df.shape}")
        print(f"   • Memory usage: {parquet_df.estimated_size('mb'):.2f} MB")
    else:
        print("ℹ️ No Parquet files found")
    
    # Read all CSV files with optimized settings
    csv_files = list(input_path.glob("*.csv"))
    csv_df = None
    if csv_files:
        print(f"\n📊 Found {len(csv_files)} CSV file(s)")
        
        # Read CSV files one by one and concatenate
        csv_dfs = []
        for csv_file in csv_files:
            print(f"📥 Reading CSV: {csv_file.name}")
            try:
                df = pl.read_csv(
                    str(csv_file),
                    separator=';',
                    infer_schema_length=10000,
                    low_memory=True,
                    rechunk=True,
                    try_parse_dates=True
                )
                # Convert ALL columns to string
                df = df.cast(pl.Utf8)
                csv_dfs.append(df)
                print(f"   ✅ Success - Shape: {df.shape}")
            except Exception as e:
                print(f"   ❌ Error reading {csv_file.name}: {e}")
        
        if csv_dfs:
            csv_df = pl.concat(csv_dfs, how="diagonal")
            
            # Convert again to ensure all are string after concat
            csv_df = csv_df.cast(pl.Utf8)
            
            # Rename CSV columns to match Parquet structure
            csv_df = csv_df.rename({
                "identificacion": "document",
                "cuenta": "account", 
                "dato": "demographic"
            })
            
            print(f"✅ CSV data loaded and transformed:")
            print(f"   • Columns: {len(csv_df.columns)}")
            print(f"   • Shape: {csv_df.shape}")
            print(f"   • Memory usage: {csv_df.estimated_size('mb'):.2f} MB")
        else:
            print("❌ No CSV files could be read successfully")
    else:
        print("ℹ️ No CSV files found")
    
    # Create unified dataframe
    unified_df = None
    
    print("\n🔄 Creating unified dataframe...")
    
    if parquet_df is not None and csv_df is not None:
        print("📊 Merging Parquet and CSV data...")
        
        # Select only the common columns from both dataframes
        parquet_selected = parquet_df.select(["document", "account", "demographic"])
        csv_selected = csv_df.select(["document", "account", "demographic"])
        
        # Combine both dataframes
        combined_df = pl.concat([parquet_selected, csv_selected], how="vertical")
        
        # Remove duplicates based on the key (account + demographic)
        unified_df = combined_df.unique(subset=["account", "demographic"])
        
        print(f"✅ Data merged successfully:")
        print(f"   • Total rows before deduplication: {combined_df.height}")
        print(f"   • Total rows after deduplication: {unified_df.height}")
        print(f"   • Duplicates removed: {combined_df.height - unified_df.height}")
        
    elif parquet_df is not None:
        print("📊 Using only Parquet data")
        unified_df = parquet_df.select(["document", "account", "demographic"]).unique(subset=["account", "demographic"])
        print(f"✅ Parquet data processed - Shape: {unified_df.shape}")
        
    elif csv_df is not None:
        print("📊 Using only CSV data") 
        unified_df = csv_df.select(["document", "account", "demographic"]).unique(subset=["account", "demographic"])
        print(f"✅ CSV data processed - Shape: {unified_df.shape}")
    
    if unified_df is not None:
        print(f"\n🏷️ Classifying data types based on demographics...")
        
        # Classify type based on demographic
        unified_df = unified_df.with_columns(
            type=pl.when(
                (pl.col("demographic").str.starts_with("6")) & 
                (pl.col("demographic").str.len_chars() == 10)
            ).then(pl.lit("landline"))
            .when(
                (pl.col("demographic").str.starts_with("3")) & 
                (pl.col("demographic").str.len_chars() == 10)
            ).then(pl.lit("mobile"))
            .when(
                pl.col("demographic").str.contains("@")
            ).then(pl.lit("email"))
            .otherwise(pl.lit("error"))
        )
        
        # Count types for reporting
        type_counts = unified_df.group_by("type").agg(pl.count().alias("count"))
        print("📈 Type distribution:")
        for row in type_counts.iter_rows():
            print(f"   • {row[0]}: {row[1]:,} rows")
        
        # Generate filename with current date
        current_date = datetime.now().strftime("%Y-%m-%d")
        base_filename = f"demographics_unified_{current_date}"
        
        print(f"\n💾 Saving processed data...")
        print(f"📅 Date stamp: {current_date}")
        print(f"📁 Base filename: {base_filename}")
        
        # Save unified data with partitions
        if num_partitions and num_partitions > 1:
            print(f"🔪 Splitting data into {num_partitions} partitions...")
            
            # Calculate partition size
            total_rows = unified_df.height
            partition_size = total_rows // num_partitions
            
            print(f"📊 Total rows: {total_rows:,}")
            print(f"📊 Partition size: ~{partition_size:,} rows")
            
            # Split and save partitions
            for i in range(num_partitions):
                start_idx = i * partition_size
                end_idx = (i + 1) * partition_size if i < num_partitions - 1 else total_rows
                
                partition_df = unified_df.slice(start_idx, end_idx - start_idx)
                partition_path = output_path / f"{base_filename}_part_{i+1:03d}.parquet"
                partition_df.write_parquet(partition_path, use_pyarrow=False)
                print(f"✅ Saved partition {i+1}/{num_partitions}: {partition_path.name} ({partition_df.height:,} rows)")
        else:
            # Save as single file
            output_file = output_path / f"{base_filename}.parquet"
            unified_df.write_parquet(output_file, use_pyarrow=False)
            print(f"✅ Saved unified data: {output_file.name} ({unified_df.height:,} rows)")
        
        print(f"\n🎉 Processing completed successfully!")
        print(f"📈 Final statistics:")
        print(f"   • Total unique records: {unified_df.height:,}")
        print(f"   • Data size: {unified_df.estimated_size('mb'):.2f} MB")
        print(f"   • Output location: {output_path}")
    else:
        print("\n❌ No data available to process")
        print("💡 Check if input folder contains valid CSV or Parquet files")
    
    print("\n" + "=" * 60)
    print("✅ Final processing completed!")