import requests
import json
import time

session = requests.Session()

mytoken = '1c73d8a0b84a67931a936eaf370b425f85a0027674fc73ee4b0b18ea93034ad6'
url = 'http://api.olhovivo.sptrans.com.br/v2.1/'
res = session.post(url + 'Login/Autenticar?token=' + mytoken)

def _get(path):
    response = session.get(url + path)
    data = response.json()
    return data

line = _get('Posicao')
j = json.dumps(line, ensure_ascii = False)
data = json.loads(j)
print(data)

