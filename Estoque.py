import ConnectApi as api, ConnectionFactory as db
from datetime import date, datetime

urlSaldo = '/v1/estoque/saldos'


def getEstoqueSaldo():
    try:
        db.truncateTable('truncate table fatoestoquesaldo')
        sql = 'insert into fatoEstoqueSaldo values (?,?,?,?,?,?)'
        id = 0
        limite = 1
        while (id <= limite):
            param = {'sort': 'id', 'q': 'saldo =gt= 0', 'count': 500, 'start': id}
            dados = api.connect(urlSaldo,param)
            id = dados['start'] + 500
            limite = dados['total']
            Lista = []
            for v in dados['items']:
                data = date.today()
                idSaldo = v.get('id')
                cdLoja = v.get('lojaId')
                cdProduto = v.get('produtoId')
                cdLocal = v.get('localId')
                saldo = v.get('saldo')
                Lista.append([data, idSaldo, cdLoja, cdProduto, cdLocal, saldo])
            db.insertLote(sql, Lista)
            print('Dados inseridos com sucesso!!!', id)
    except ValueError:
        print('Erro...')



def getRequisicoes():
    try:
        urlRequisicoes = '/v1/estoque/requisicoes-mercadorias'
        db.truncateTable('truncate table fatoRequisicoes')
        sqlInsertReq = 'insert into fatoRequisicoes values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        getIdReq = 'select isnull(max(id),0) as id from fatoRequisicoes'
        hash = True
        while (hash == True):
            id = db.getId(getIdReq)
            param = {'count': 300, 'sort': 'id', 'q': f'id =gt= {id}'}
            dados = api.connect(urlRequisicoes,param)

            if dados['count'] == 0:
                hash = False
            else:
                print(dados['total'])
                Lista = []
                for v in dados['items']:
                    for v1 in v['itens']:
                        c = [   v.get('id'),
                                v.get('dataTransferencia'),
                                v.get('dataRegistro'),
                                v.get('dataRecebimento'),
                                v.get('tipo'),
                                v.get('status'),
                                v.get('modelo'),
                                v.get('lojaId'),
                                v.get('localOrigemId'),
                                v.get('localDestinoId'),
                                v.get('setorId'),
                                v.get('solicitanteId'),
                                v.get('motivoRequisicaoId'),
                                v.get('observacaoGeral'),
                                v.get('total'),
                                v1.get('id'),
                                v1.get('quantidadeTransferida'),
                                v1.get('observacao'),
                                v1.get('produtoId'),
                                v1.get('custoMedio'),
                                v1.get('custo'),
                                v1.get('custoReposicao'),
                                v1.get('custoFiscal')]
                    Lista.append(c)
                db.insertLote(sqlInsertReq, Lista)
            print('Dados inseridos com sucesso!!!', id)
    except ValueError:
        print('Erro...')



def execEstoque():
    print('Inicio...\n',datetime.now())
    print('Inserindo saldo de estoque!!!')
    getEstoqueSaldo()
    print('Inserindo Requisições!!!')
    getRequisicoes()
    print('Fim...\n',datetime.now())


def updateDimData():
    sql = 'exec sp_data'
    db.truncateTable(sql)

