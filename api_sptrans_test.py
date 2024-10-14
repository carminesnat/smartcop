import requests
import json

url = 'http://api.olhovivo.sptrans.com.br/v2.1/'
mytoken = '1c73d8a0b84a67931a936eaf370b425f85a0027674fc73ee4b0b18ea93034ad6'

session = requests.Session()
res = session.post(f"{url}Login/Autenticar?token={mytoken}")

response = session.get(url + 'Posicao')
data = response.json()
j = json.dumps(data, ensure_ascii = False)
data = json.loads(j)
print(data)
