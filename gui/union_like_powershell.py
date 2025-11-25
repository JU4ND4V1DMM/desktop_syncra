import os
import glob
from pathlib import Path
from datetime import datetime
from typing import List, Dict

def merge_files_by_subfolder(
    input_folder: str,
    output_folder: str,
    encoding: str = "utf-8"
) -> str:
    """
    🚀 CLEAN SUBFOLDER-BASED FILE MERGER 🚀
    No extra line breaks between files!
    """
    
    # 📁 Create output folder if needed
    os.makedirs(output_folder, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d %H%M")
    
    def find_files_by_subfolder(root_folder: str, patterns: List[str]) -> Dict[str, Dict[str, List[str]]]:
        folder_files = {}
        
        for pattern in patterns:
            search_pattern = os.path.join(root_folder, "**", pattern)
            files = glob.glob(search_pattern, recursive=True)
            
            for file_path in files:
                if os.path.isfile(file_path):
                    folder_path = os.path.dirname(file_path)
                    file_type = pattern.replace('*.', '').upper()
                    
                    if folder_path not in folder_files:
                        folder_files[folder_path] = {"CSV": [], "TXT": []}
                    
                    folder_files[folder_path][file_type].append(file_path)
        
        return folder_files
    
    # 📊 Find files
    file_patterns = ["*.csv", "*.txt"]
    folder_files = find_files_by_subfolder(input_folder, file_patterns)
    
    if not folder_files:
        return f"❌ No CSV or TXT files found in: {input_folder}"
    
    merge_results = {}
    
    # 🔄 Process each subfolder
    for folder_path, file_types in folder_files.items():
        folder_name = os.path.basename(os.path.normpath(folder_path))
        
        if folder_path == input_folder:
            display_name = folder_name + " (ROOT)"
        else:
            relative_path = os.path.relpath(folder_path, input_folder)
            display_name = relative_path
        
        print(f"\n📁 Processing folder: {display_name}")
        
        for file_type, files in file_types.items():
            if not files:
                continue
            
            # 🎯 Generate output filename
            if folder_path == input_folder:
                output_filename = f"{folder_name} {timestamp}.{file_type.lower()}"
            else:
                safe_folder_name = relative_path.replace(os.sep, '_')
                output_filename = f"{safe_folder_name} {timestamp}.{file_type.lower()}"
            
            output_path = os.path.join(output_folder, output_filename)
            
            print(f"   🔄 Merging {len(files)} {file_type} files...")
            
            try:
                with open(output_path, 'w', encoding=encoding) as outfile:
                    for i, filename in enumerate(files, 1):
                        file_basename = os.path.basename(filename)
                        print(f"      📄 ({i}/{len(files)}) {file_basename}")
                        
                        with open(filename, 'r', encoding=encoding) as infile:
                            content = infile.read().rstrip('\n')  # Remove trailing newlines
                            
                            if i > 1:  # If not first file, add newline before content
                                outfile.write('\n')
                            
                            outfile.write(content)
                
                # Store results
                key = f"{display_name} - {file_type}"
                merge_results[key] = {
                    'output_file': output_path,
                    'files_processed': len(files)
                }
                
                print(f"   ✅ {file_type} merge completed!")
                
            except Exception as e:
                print(f"   ❌ Error: {str(e)}")
    
    # 🎉 Generate summary
    if not merge_results:
        return "❌ No files were processed!"
    
    success_msg = f"🎊 MERGE COMPLETED! Processed {sum(r['files_processed'] for r in merge_results.values())} files across {len(folder_files)} folders."
    return success_msg