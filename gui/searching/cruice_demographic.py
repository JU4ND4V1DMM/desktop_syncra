import polars as pl
import os
from pathlib import Path
from datetime import datetime

def demographic_cross(input_folder: str, output_folder: str) -> str:
    Path(output_folder).mkdir(parents=True, exist_ok=True)
    
    all_files = [f for f in os.listdir(input_folder) if f.lower().endswith('.csv')]
    
    if not all_files:
        return "❌ No CSV files found"
    
    demographic_files = []
    main_file = None
    
    for file in all_files:
        file_path = os.path.join(input_folder, file)
        try:
            sample = pl.read_csv(file_path, separator=';', n_rows=100, infer_schema_length=0)
            cols = [c.lower().strip() for c in sample.columns]
            
            if all(col in cols for col in ['identificacion', 'cuenta', 'ciudad', 'depto', 'dato', 'tipodato', 'marca']):
                demographic_files.append(file)
            elif 'cuenta_next' in cols:
                main_file = file
        except:
            continue
    
    if not demographic_files:
        return "❌ No demographic files found"
    
    if not main_file:
        return "❌ No main file with 'cuenta_next' found"
    
    demographic_dfs = []
    for file in demographic_files:
        file_path = os.path.join(input_folder, file)
        df = pl.read_csv(file_path, separator=';', encoding='utf8', infer_schema_length=0, ignore_errors=True)
        df = df.rename({col: col.lower().strip() for col in df.columns})
        demographic_dfs.append(df.select(['identificacion', 'cuenta', 'ciudad', 'depto', 'dato', 'tipodato', 'marca']))
        print(f"✅ Loaded demographic {file}: {df.height:,} rows")
    
    demographic_data = pl.concat(demographic_dfs, how='vertical').unique(subset=['cuenta'], keep='first')
    
    main_data = pl.read_csv(os.path.join(input_folder, main_file), separator=';', encoding='utf8', infer_schema_length=0, ignore_errors=True)
    main_data = main_data.rename({col: col.lower().strip() for col in main_data.columns})
    print(f"✅ Loaded main {main_file}: {main_data.height:,} rows")
    
    demographic_data = demographic_data.with_columns([
        pl.col('cuenta').cast(pl.Utf8).str.replace_all(r'[-.\s]', '').str.to_lowercase().alias('join_key')
    ])
    
    main_data = main_data.with_columns([
        pl.col('cuenta_next').cast(pl.Utf8).str.replace_all(r'[-.\s]', '').str.to_lowercase().alias('join_key'),
        (pl.col('cuenta_next').cast(pl.Utf8).str.replace_all(r'[-.\s]', '') + pl.lit('-')).alias('cuenta_next_format')
    ])
    
    demographic_data = demographic_data.with_columns([
        (pl.col('cuenta').cast(pl.Utf8).str.replace_all(r'[-.\s]', '') + pl.lit('-')).alias('cuenta')
    ])
    
    matched = demographic_data.join(main_data, left_on='join_key', right_on='join_key', how='inner')
    unmatched = demographic_data.join(main_data, left_on='join_key', right_on='join_key', how='anti')
    
    main_with_status = main_data.with_columns([
        pl.when(pl.col('join_key').is_in(demographic_data['join_key']))
        .then(pl.lit('CRUZA'))
        .otherwise(pl.lit('NO CRUZA'))
        .alias('tipologia')
    ]).select([
        'cuenta_next_format',
        'tipologia'
    ]).unique()
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    matched_path = os.path.join(output_folder, f'cruce_demograficos_match_{timestamp}.csv')
    unmatched_path = os.path.join(output_folder, f'cruce_demograficos_nomatch_{timestamp}.csv')
    summary_path = os.path.join(output_folder, f'cruce_demograficos_summary_{timestamp}.csv')
    
    matched.select([c for c in matched.columns if c != 'join_key']).write_csv(matched_path, separator=';')
    unmatched.select([c for c in unmatched.columns if c != 'join_key']).write_csv(unmatched_path, separator=';')
    main_with_status.write_csv(summary_path, separator=';')
    
    print(f"✅ Matched: {matched.height:,} rows")
    print(f"✅ Unmatched: {unmatched.height:,} rows")
    print(f"✅ Summary: {main_with_status.height:,} rows")
    print(f"💾 Saved: {matched_path}")
    print(f"💾 Saved: {unmatched_path}")
    print(f"💾 Saved: {summary_path}")
    
    return f"✅ Files saved in {output_folder}"