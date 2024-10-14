import requests
import json
# URL do endpoint da API
url = "http://api.olhovivo.sptrans.com.br/v2.1/Posicao"

# Token de autenticação
token = "1c73d8a0b84a67931a936eaf370b425f85a0027674fc73ee4b0b18ea93034ad6"

# Cabeçalhos da requisição
headers = {
    "Authorization": f"Bearer {token}"
}

# Fazendo a requisição GET
response = requests.get(url, headers=headers)
print(response.status_code)
# data = response.json()
# j = json.dumps(data, ensure_ascii = False)
# data = json.loads(j)
# print(data)

