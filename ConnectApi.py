import requests, json, time, socket


def connect(str, param=None):

    try:
        url = 'https://villaboxsupermercado.varejofacil.com/api'+str
        headers = {'content-type': 'application/json', 'x-api-key': '9cbaf052036606a6edab877389f96cca'}
        z = 0
        print(url, ' - ', param)
        while (z == 0):
            r = requests.get(url, headers=headers, params=param)
            dados = json.loads(r.text)
            #print(dados)
            if ('message' in dados):
                print('Aguarde 60 segundos...')
                time.sleep(61)

            else:
                z = 1

        return dados
    except ValueError:
        print('Erro ao conectar na API!!!', dados)


confiaveis = ['www.google.com', 'www.yahoo.com', 'www.bb.com.br']

def check_host():
   for host in confiaveis:
     a=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
     a.settimeout(.5)
     try:
       b=a.connect_ex((host, 80))
       if b==0: #ok, conectado
         return True
     except:
       pass
     a.close()
   return False

print(check_host())
