import os
import glob
from pathlib import Path
from datetime import datetime
from typing import List, Dict

def detect_encoding_fast(file_path: str) -> str:
    """
    Detección rápida de encoding - solo verifica los primeros bytes
    """
    try:
        # Leer solo los primeros 4KB para detección rápida
        with open(file_path, 'rb') as file:
            raw_data = file.read(4096)
            
        # Verificaciones rápidas basadas en BOM (Byte Order Mark)
        if raw_data.startswith(b'\xff\xfe'):
            return 'utf-16'
        elif raw_data.startswith(b'\xfe\xff'):
            return 'utf-16-be'
        elif raw_data.startswith(b'\xef\xbb\xbf'):
            return 'utf-8-sig'
        
        # Búsqueda rápida de caracteres comunes en diferentes encodings
        if b'\x00' in raw_data:
            return 'utf-16'
            
        # Para archivos con caracteres latinos (comunes en CSV)
        if any(char in raw_data for char in [b'\xd1', b'\xf1', b'\xe1', b'\xe9', b'\xed', b'\xf3', b'\xfa']):
            return 'latin-1'
            
        return 'utf-8'
    except Exception:
        return 'utf-8'

def read_file_fast(filename: str, fallback_encoding: str = "utf-8") -> str:
    """
    Lectura rápida de archivo con manejo inteligente de encoding
    """
    # Primero intentar con encoding por defecto
    try:
        with open(filename, 'r', encoding=fallback_encoding) as infile:
            return infile.read().rstrip('\n')
    except UnicodeDecodeError:
        pass
    
    # Si falla, probar encodings comunes en orden de probabilidad
    encodings_to_try = ['latin-1', 'iso-8859-1', 'cp1252', 'utf-8-sig', 'utf-16']
    
    for encoding in encodings_to_try:
        try:
            with open(filename, 'r', encoding=encoding) as infile:
                content = infile.read().rstrip('\n')
                print(f"      ✅ Usado encoding: {encoding}")
                return content
        except UnicodeDecodeError:
            continue
    
    # Último recurso: reemplazar caracteres problemáticos
    try:
        with open(filename, 'r', encoding=fallback_encoding, errors='replace') as infile:
            content = infile.read().rstrip('\n')
            print(f"      ⚠️  Reemplazados caracteres problemáticos")
            return content
    except Exception as e:
        print(f"      ❌ Error crítico leyendo archivo: {str(e)}")
        return ""

def merge_files_by_subfolder(
    input_folder: str,
    output_folder: str,
    encoding: str = "utf-8"
) -> str:
    """
    🚀 CLEAN SUBFOLDER-BASED FILE MERGER - OPTIMIZADO 🚀
    Versión rápida sin dependencias externas
    """
    
    # 📁 Crear carpeta de salida si es necesario
    os.makedirs(output_folder, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    
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
    
    # 📊 Encontrar archivos
    file_patterns = ["*.csv", "*.txt"]
    folder_files = find_files_by_subfolder(input_folder, file_patterns)
    
    if not folder_files:
        return f"❌ No se encontraron archivos CSV o TXT en: {input_folder}"
    
    merge_results = {}
    total_files = 0
    
    # 🔄 Procesar cada subcarpeta
    for folder_path, file_types in folder_files.items():
        folder_name = os.path.basename(os.path.normpath(folder_path))
        
        if folder_path == input_folder:
            display_name = folder_name + " (ROOT)"
        else:
            relative_path = os.path.relpath(folder_path, input_folder)
            display_name = relative_path
        
        print(f"\n📁 Procesando carpeta: {display_name}")
        
        for file_type, files in file_types.items():
            if not files:
                continue
            
            # 🎯 Generar nombre de archivo de salida
            if folder_path == input_folder:
                output_filename = f"{folder_name}_{timestamp}.{file_type.lower()}"
            else:
                safe_folder_name = relative_path.replace(os.sep, '_')
                output_filename = f"{safe_folder_name}_{timestamp}.{file_type.lower()}"
            
            output_path = os.path.join(output_folder, output_filename)
            
            print(f"   🔄 Fusionando {len(files)} archivos {file_type}...")
            
            try:
                with open(output_path, 'w', encoding=encoding) as outfile:
                    header_written = False
                    
                    for i, filename in enumerate(files, 1):
                        file_basename = os.path.basename(filename)
                        print(f"      📄 ({i}/{len(files)}) {file_basename}")
                        
                        # Lectura rápida del archivo
                        content = read_file_fast(filename, encoding)
                        
                        if not content:
                            continue
                        
                        # Para CSV, manejar headers (opcional - remover si no es necesario)
                        if file_type == "CSV" and i > 1 and header_written:
                            # Saltar header si no es el primer archivo
                            lines = content.split('\n')
                            if len(lines) > 1:
                                content = '\n'.join(lines[1:])
                            elif len(lines) == 1:
                                content = lines[0]
                        
                        if content:
                            if i > 1 and not content.startswith('\n'):
                                outfile.write('\n')
                            outfile.write(content)
                            
                            if file_type == "CSV" and not header_written:
                                header_written = True
                
                # Almacenar resultados
                key = f"{display_name} - {file_type}"
                merge_results[key] = {
                    'output_file': output_path,
                    'files_processed': len(files)
                }
                total_files += len(files)
                
                print(f"   ✅ Fusión {file_type} completada! → {output_path}")
                
            except Exception as e:
                print(f"   ❌ Error: {str(e)}")
    
    # 🎉 Generar resumen
    if not merge_results:
        return "❌ No se procesaron archivos!"
    
    success_msg = f"🎊 FUSIÓN COMPLETADA! {total_files} archivos procesados en {len(merge_results)} grupos."
    print(f"\n{success_msg}")
    
    # Mostrar archivos generados
    print("\n📁 Archivos generados:")
    for key, result in merge_results.items():
        print(f"   📄 {os.path.basename(result['output_file'])}")
    
    return success_msg