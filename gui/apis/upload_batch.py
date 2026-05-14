import os
import requests
from ..apis.login_api1_intercom import login_intercom

def upload_batch_file(file_path):
    """
    Upload a specific CSV file to the API
    
    Args:
        file_path: Full path of the file to upload
    
    Returns:
        dict: API response with jobId or None if error
    """
    print("🚀 Starting API upload for single file")
    
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return None
    
    file_name = os.path.basename(file_path)
    print(f"📁 File to upload: {file_name}")
    
    # Get token
    token = login_intercom()
    if not token:
        print("❌ Failed to obtain token")
        return None
    
    # API endpoint
    url = "https://recupera.controlnextapp.com:10000/upload-csv-batch"
    headers = {"Authorization": f"Bearer {token}"}
    
    try:
        # Upload file
        with open(file_path, 'rb') as f:
            files = {'archivo': (file_name, f, 'text/csv')}
            print("📤 Uploading file... (this may take a few seconds)")
            response = requests.post(url, headers=headers, files=files, timeout=300)
        
        response.raise_for_status()
        result = response.json()
        
        job_id = result.get('jobId')
        message = result.get('message', 'No message')
        
        print(f"✅ Upload successful")
        print(f"   📨 Message: {message}")
        print(f"   🆔 Job ID: {job_id}")
        
        return result
        
    except requests.exceptions.Timeout:
        print("❌ Timeout: Upload is taking too long")
        return None
    except requests.exceptions.RequestException as e:
        print(f"❌ Upload error: {e}")
        if hasattr(e, 'response') and e.response:
            try:
                error_detail = e.response.json()
                print(f"   Details: {error_detail}")
            except:
                print(f"   Status code: {e.response.status_code}")
        return None
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return None