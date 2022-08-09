import ConnectApi as api, ConnectionFactory as db
# from VarejoFacilAPI.ContasReceber import mContasReceber as cr, dbContasReceber as db
from datetime import datetime

url = '/v1/financeiro/lancamentos-balancetes'


def getBalancete():
    try:
        sqlInsert = 'set dateformat ymd insert into fatobalancete values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'

        lista = [92, 112, 1501, 2062]

        for i in lista:
            sqlUltimoId = f'SELECT isnull(max(id),0) as id FROM fatobalancete where categoriaFinanceiraId = {i}'

            hash = True
            while (hash == True):
                id = db.getId(sqlUltimoId)
                param = {'count': 100, 'q': f'id =gt= {str(id)} ; categoriaFinanceiraId == {i}', 'sort': 'id'}
               # param = {'count': 100, 'q': 'categoriaFinanceiraId == 2062', 'sort': 'id'}

                dados = api.connect(url, param)
                if dados['count'] == 0:
                    hash = False
                else:
                    for v in dados['items']:
                        print(v)
                        c = (
                            [
                                v.get('id'),
                                v.get('lojaId'),
                                v.get('categoriaFinanceiraId'),
                                v.get('valor', 0),
                                v.get('codigoOrigem'),
                                v.get('tipo'),
                                v.get('natureza'),
                                v.get('documento'),
                                v.get('historico'),
                                str(v.get('dataLancamento')).replace('T', ' ')[0:23] if v.get('dataLancamento') else None,
                                str(v.get('dataEmissao')).replace('T', ' ')[0:23] if v.get('dataEmissao') else None,
                                str(v.get('dataVencimento')).replace('T', ' ')[0:23] if v.get('dataVencimento') else None,
                                str(v.get('dataPagamento' )).replace('T', ' ')[0:23] if v.get('dataPagamento') else None,
                                v.get('codigoPessoa', 0),
                                v.get('efetivo'),
                                str(v.get('criadoEm')).replace('T', ' ')[0:23]
                            ])
                        db.insert(sqlInsert, c)
    except ValueError as ex:
        ex.args


def execBalancete():
    getBalancete()

