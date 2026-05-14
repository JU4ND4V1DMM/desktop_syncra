import os
import pandas as pd
from datetime import datetime, time

def extract_date_time(filename: str):
    base = os.path.splitext(filename)[0]
    first_part = base.split(' ')[0]
    parts = first_part.split('_')
    if len(parts) < 2:
        return None, None
    date_part = parts[-2]
    time_part = parts[-1]
    return date_part, time_part

def process_mora_by_folder(input_folder: str, output_folder: str, gui: None):
    results = []

    files = [f for f in os.listdir(input_folder) if f.lower().endswith(('.xls', '.xlsx'))]

    for file in files:
        print("✅ Processing file:", file)
        file_path = os.path.join(input_folder, file)
        try:
            xls = pd.ExcelFile(file_path)
        except Exception as e:
            print(f"Skipping file {file} due to read error: {e}")
            continue

        mora_series_list = []

        for sheet in xls.sheet_names:
            try:
                df = pd.read_excel(xls, sheet_name=sheet)
            except Exception as e:
                print(f"Skipping sheet {sheet} in {file} due to read error: {e}")
                continue

            mora_col = next((col for col in df.columns if 'edad_mora' in str(col).lower()), None)
            if mora_col is None:
                continue

            series = df[mora_col].dropna().astype(str).str.strip()

            def transform_value(v):
                v_upper = v.upper()
                if v.isdigit():
                    return f"MORA {v}"
                try:
                    num = int(float(v))
                    return f"MORA {num}"
                except:
                    pass
                return v_upper

            transformed = series.map(transform_value)
            mora_series_list.append(transformed)

        if not mora_series_list:
            continue

        all_values = pd.concat(mora_series_list)

        count_df = all_values.value_counts().reset_index()
        count_df.columns = ['Mora', 'Quantity']

        date_part, time_part = extract_date_time(file)

        # Convert date_part 'ddmmyyyy' to datetime.date
        if date_part and len(date_part) == 8:
            try:
                date_obj = datetime.strptime(date_part, '%d%m%Y').date()
            except:
                date_obj = None
        else:
            date_obj = None

        # Convert time_part 'HHMM' to datetime.time
        if time_part and len(time_part) == 4:
            try:
                time_obj = time(int(time_part[:2]), int(time_part[2:]))
            except:
                time_obj = None
        else:
            time_obj = None

        count_df.insert(0, 'File', file)
        count_df.insert(1, 'Date', date_obj)
        count_df.insert(2, 'Time', time_obj)
        count_df.insert(3, 'Type', 'SMS SAEM')
        count_df.insert(3, 'Campaign', 'CLARO')

        results.append(count_df)

    if not results:
        print("No 'Edad_Mora' data found in the files.")
        return

    df_results = pd.concat(results, ignore_index=True)
    desired_order = ['File', 'Date', 'Time', 'Mora', 'Type', 'Quantity', 'Campaign']
    df_results = df_results[desired_order]
    
    timeday = datetime.now().strftime('%Y%m%d')
    output_file = os.path.join(output_folder, f'schema_claro_sms_{timeday}.xlsx')

    with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
        df_results.to_excel(writer, index=False, sheet_name='Summary')

        workbook = writer.book
        worksheet = writer.sheets['Summary']

        header_format = workbook.add_format({'bold': True})
        worksheet.set_row(0, None, header_format)
        worksheet.freeze_panes(1, 0)

        # Define Excel formats for date and time
        date_format = workbook.add_format({'num_format': 'dd/mm/yyyy'})
        time_format = workbook.add_format({'num_format': 'hh:mm'})

        # Apply formats to Date and Time columns (assumed to be columns 1 and 2)
        worksheet.set_column(1, 1, 12, date_format)  # Date column width + format
        worksheet.set_column(2, 2, 8, time_format)   # Time column width + format

        # Auto-adjust other columns widths
        for i, col in enumerate(df_results.columns):
            if i in (1, 2):
                continue  # Already set width and format
            max_len = max(df_results[col].astype(str).map(len).max(), len(col))
            worksheet.set_column(i, i, max_len + 2)

    print(f"Summary file saved at: {output_file}")