import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

def process_management_files(input_path, output_path, partitions, process_data):
    print("="*80)
    print("🚀 STARTING MANAGEMENT FILES PROCESSING")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    if not os.path.exists(input_path):
        print(f"❌ ERROR: Input path does not exist: {input_path}")
        return
    
    if not os.path.exists(output_path):
        os.makedirs(output_path)
        print(f"📁 Created output directory")
    
    # STEP 1: Load management files
    management_files = []
    for ext in ['*.csv', '*.parquet', '*.parq']:
        management_files.extend(glob.glob(os.path.join(input_path, '**', ext), recursive=True))
    
    df_management_total = None
    for file in management_files:
        try:
            if file.lower().endswith('.csv'):
                df = pd.read_csv(file, delimiter=';', encoding='utf-8', low_memory=False)
            else:
                df = pd.read_parquet(file)
            
            if all(col in df.columns for col in ['campana', 'cuenta_promesa', 'perfil']):
                if df_management_total is None:
                    df_management_total = df[['campana', 'cuenta_promesa', 'perfil']].copy()
                else:
                    df_management_total = pd.concat([df_management_total, df[['campana', 'cuenta_promesa', 'perfil']]], ignore_index=True)
        except Exception as e:
            continue
    
    if df_management_total is None:
        print("❌ No valid management data found")
        return
    
    print(f"\n📊 Structure 1: {len(df_management_total):,} records")
    
    # STEP 2: Load mapping files
    mapping_files = []
    for ext in ['*.csv', '*.parquet', '*.parq']:
        mapping_files.extend(glob.glob(os.path.join(input_path, '**', ext), recursive=True))
    
    df_mapping_total = None
    for file in mapping_files:
        try:
            if file.lower().endswith('.csv'):
                df_sample = pd.read_csv(file, delimiter=';', encoding='utf-8', nrows=100)
            else:
                df_sample = pd.read_parquet(file)
            
            if 'Cuenta_Next' in df_sample.columns:
                if file.lower().endswith('.csv'):
                    df_full = pd.read_csv(file, delimiter=';', encoding='utf-8', low_memory=False)
                else:
                    df_full = pd.read_parquet(file)
                
                cols = [c for c in ['Cuenta_Next', 'Cuenta', 'Marca_Asignada'] if c in df_full.columns]
                if df_mapping_total is None:
                    df_mapping_total = df_full[cols].copy()
                else:
                    df_mapping_total = pd.concat([df_mapping_total, df_full[cols]], ignore_index=True)
        except Exception as e:
            continue
    
    if df_mapping_total is None:
        print("❌ No mapping files found")
        return
    
    print(f"📊 Structure 2: {len(df_mapping_total):,} records")
    
    # STEP 3: Prepare data
    if 'Cuenta' in df_mapping_total.columns:
        df_mapping_total['Cuenta_Real'] = df_mapping_total['Cuenta'].astype(str)
    else:
        df_mapping_total['Cuenta_Real'] = ''
    
    df_mapping_total['Cuenta_Next_Clean'] = df_mapping_total['Cuenta_Next'].astype(str).str.replace('-', '', regex=False)
    df_management_total['cuenta_promesa_clean'] = df_management_total['cuenta_promesa'].astype(str).str.replace('-', '', regex=False)
    
    df_mapping_unique = df_mapping_total.drop_duplicates(subset=['Cuenta_Next_Clean'])
    
    # STEP 4: Merge data
    df_merged = pd.merge(
        df_management_total,
        df_mapping_unique,
        left_on='cuenta_promesa_clean',
        right_on='Cuenta_Next_Clean',
        how='inner'
    )
    
    print(f"🔗 Records matched: {len(df_merged):,} ({len(df_merged)/len(df_management_total)*100:.1f}%)")
    
    if len(df_merged) == 0:
        print("❌ No matches found")
        return
    
    # STEP 5: Classify profiles
    df_merged['perfil_upper'] = df_merged['perfil'].astype(str).str.upper()
    
    conditions = [
        df_merged['perfil_upper'].str.contains('CORREO|EMAIL|MAIL', na=False),
        df_merged['perfil_upper'].str.contains('BLASTER|IVR', na=False),
        df_merged['perfil_upper'].str.contains('MENSAJ|SMS|TEXTO', na=False)
    ]
    choices = ['EMAIL', 'IVR', 'SMS']
    df_merged['Recurso'] = np.select(conditions, choices, default=df_merged['perfil'])
    
    marca_col = 'Marca_Asignada' if 'Marca_Asignada' in df_merged.columns else 'Marca'
    if 'Marca_Asignada' not in df_merged.columns:
        df_merged['Marca_Asignada'] = '0'
        marca_col = 'Marca_Asignada'
    
    # STEP 6: Aggregate results
    df_result = df_merged.groupby(['Recurso', 'campana', 'Cuenta_Real', 'Cuenta_Next_Clean', marca_col]).size().reset_index(name='Cantidad')
    
    df_result = df_result.rename(columns={
        'Cuenta_Next_Clean': 'Cuenta_Sin_Punto',
        marca_col: 'Marca'
    })
    
    df_result = df_result[['Cuenta_Real', 'Cuenta_Sin_Punto', 'Marca', 'Recurso', 'Cantidad']]
    
    print(f"\n📊 Unique accounts by resource:")
    for profile in df_result['Recurso'].unique():
        df_profile = df_result[df_result['Recurso'] == profile]
        unique_accounts = df_profile['Cuenta_Sin_Punto'].nunique()
        total_qty = df_profile['Cantidad'].sum()
        print(f"   {profile}: {unique_accounts:,} unique accounts, {total_qty:,} total messages")
    
    # STEP 7: Generate output files
    current_date = datetime.now().strftime('%Y%m%d')
    profiles = df_result['Recurso'].unique()
    
    print(f"\n💾 Generating files:")
    for profile in profiles:
        df_profile = df_result[df_result['Recurso'] == profile].copy()
        total = df_profile['Cantidad'].sum()
        filename = f"{profile}_{current_date}.csv"
        filepath = os.path.join(output_path, filename)
        df_profile.to_csv(filepath, sep=';', index=False, encoding='utf-8')
        print(f"   ✅ {profile}: {len(df_profile):,} rows, {total:,} total -> {filename}")
    
    print("\n" + "="*80)
    print("✅ PROCESSING COMPLETED SUCCESSFULLY")
    print(f"📁 Files saved in: {output_path}")
    print("="*80)
    
    return df_result