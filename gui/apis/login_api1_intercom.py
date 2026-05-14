import requests
import json

def login_intercom():
    """
    Realiza login en la API de intercom y retorna el token
    """
    url = "https://recupera.controlnextapp.com:10000/login"
    
    payload = {
        "user": "apirecupera",
        "password": "apiRecuperaNuvero"
    }
    
    headers = {
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        token = data.get("token")
        
        if token:
            print("✅ Login successful - Token obtained")
            return token
        else:
            print("❌ Token not found in response")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Login error: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"❌ Error decoding response: {e}")
        return None