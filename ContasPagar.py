import ConnectApi as api, ConnectionFactory as db
# from VarejoFacilAPI.ContasReceber import mContasReceber as cr, dbContasReceber as db
from datetime import datetime

urlContasAPagar = '/v1/financeiro/contas-pagar'
urlFinPagamentoPDV = '/v1/financeiro/pagamentos-pdv'
urlPagamentoPDV = '/v1/pdv/pagamentos'
sqlDuplicidade = """
                                  delete c from FatoContasApagar c
                        join (
                                select 
                                    id,
                                    lojaId,
                                    dtEmissao,
                                    count(distinct dtAlteracao) contagem,
                                    max(dtAlteracao) data
                                from FatoContasApagar
                                -- where id = 39142
                                group by 
                                    id,
                                    lojaId,
                                    dtEmissao
                                having count(distinct dtAlteracao) > 1
                        ) d on d.id = c.id and d.lojaId = C.lojaId and d.data <> c.dtAlteracao

                """
sqlDuplicidadeMov = """

								delete m from FatoContasAPagarMov m
								join (
									select 
										tituloPagarId,
										max(id) as id
									from FatoContasAPagarMov
									group by 
										tituloPagarId
									having count(1) > 1
								) d on d.tituloPagarId = m.tituloPagarId and d.id <> m.id
"""


def getContasApagar():
    try:
        # db.truncateTable('truncate table FatoContasApagar')
        sqlInsert = 'set dateformat ymd insert into FatoContasApagar values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        sqlUltimoId = 'SELECT max(dtAlteracao) as id FROM FatoContasApagar'

        hash = True
        while (hash == True):
            id = str(db.getId(sqlUltimoId)).replace(' ', 'T')
            param = {'count': 500, 'q': 'dataAlteracao =gt= ' + str(id), 'sort': 'dataAlteracao'}
            # param = {'q': 'id =gt= '+str(id), 'sort': 'id'}
            dados = api.connect(urlContasAPagar, param)
            if dados['count'] == 0:
                hash = False
            else:
                for v in dados['items']:

                    listCat = []
                    for v2 in v.get('categorias'):
                        listCat.append({'codigo': v2.get('categoriaFinanceiraId'), 'valor': v2.get('valor')})

                    for v1 in v.get('titulos'):

                        categoria = None
                        for i in listCat:
                            valorteste = round((float(v.get('valorBruto', 0)) - float(v.get('valorDesconto', 0))) + float(v.get('valorAcrescimo', 0)), 2)
                            if i['valor'] == v.get('valorBruto') or i['valor'] == valorteste:
                                categoria = i['codigo']
                                break

                        c = ([v.get('id'),
                              v.get('numeroDocumento'),
                              v.get('dataEmissao'),
                              str(v.get('dataAlteracao')).replace('T', ' ')[0:23],
                              v.get('dataCompetencia'),
                              v.get('valorBruto'),
                              v.get('valorDesconto'),
                              v.get('valorAcrescimo'),
                              v.get('condicaoDePagamento'),
                              v.get('numeroDeTitulos'),
                              v.get('primeiroVencimento'),
                              v.get('efetivo'),
                              v.get('lojaId'),
                              v.get('especieDocumentoId'),
                              v.get('fornecedorId'),
                              v.get('agenteFinanceiroId'),
                              categoria,
                              v.get('centroCustoId'),
                              v1.get('id'),
                              v1.get('ordem'),
                              v1.get('vencimento'),
                              v1.get('liquidacao'),
                              str(v1.get('dataAlteracao')).replace('T', ' ')[0:23],
                              v1.get('valor'),
                              v1.get('valorLiquido'),
                              v1.get('status')])
                        db.insert(sqlInsert, c)
                        db.query(f"delete from FatoContasAPagarMov where tituloPagarId = '{v1.get('id')}' ")
        print('Removendo regitros duplidados...')
        db.query(sqlDuplicidade)
    except ValueError as ex:
        ex.args


def getContasApagarId(id):
    try:
        sqlInsert = 'set dateformat ymd insert into FatoContasApagar values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        sqlUltimoId = 'SELECT max(dtAlteracao) as id FROM FatoContasApagar'

        param = {'q': 'id == ' + str(id), 'sort': 'id'}
        dados = api.connect(urlContasAPagar, param)
        if dados['count'] == 0:
            pass
        else:
            for v in dados['items']:
                listCat = []
                for v2 in v.get('categorias'):
                    listCat.append({'codigo': v2.get('categoriaFinanceiraId'), 'valor': v2.get('valor')})

                for v1 in v.get('titulos'):

                    categoria = None
                    for i in listCat:
                        valorteste =  round(( float(v.get('valorBruto',0)) - float(v.get('valorDesconto',0)) ) + float(v.get('valorAcrescimo',0)),2)
                        if i['valor'] == v.get('valorBruto') or i['valor'] == valorteste:
                            categoria = i['codigo']
                            break

                    c = ([v.get('id'),
                          v.get('numeroDocumento'),
                          v.get('dataEmissao'),
                          str(v.get('dataAlteracao')).replace('T', ' ')[0:23],
                          v.get('dataCompetencia'),
                          v.get('valorBruto'),
                          v.get('valorDesconto'),
                          v.get('valorAcrescimo'),
                          v.get('condicaoDePagamento'),
                          v.get('numeroDeTitulos'),
                          v.get('primeiroVencimento'),
                          v.get('efetivo'),
                          v.get('lojaId'),
                          v.get('especieDocumentoId'),
                          v.get('fornecedorId'),
                          v.get('agenteFinanceiroId'),
                          categoria,
                          v.get('centroCustoId'),
                          v1.get('id'),
                          v1.get('ordem'),
                          v1.get('vencimento'),
                          v1.get('liquidacao'),
                          str(v1.get('dataAlteracao')).replace('T', ' ')[0:23],
                          v1.get('valor'),
                          v1.get('valorLiquido'),
                          v1.get('status')])
                    db.insert(sqlInsert, c)
                    db.query(f"delete from FatoContasAPagarMov where tituloPagarId = {v1.get('id')} ")
        print('Removendo regitros duplidados...')
        db.query(sqlDuplicidade)
        getMovimentacoesInicial()
    except ValueError as ex:
        ex.args


def getMovimentacoes(id):
    try:
        sqlInsertMov = 'set dateformat ymd insert into FatoContasApagar values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        urlMoviemntacao = f'/v1/financeiro/contas-pagar/titulos/{id}/movimentacoes'

        hash = True
        while (hash == True):

            param = {'q': "operacao == LIQUIDACAO_DE_TITULO ", 'sort': 'id'}
            dados = api.connect(urlMoviemntacao, param)
            if dados['count'] == 0:
                hash = False
            else:
                for v in dados['items']:
                    c = ([v.get('id'),
                          v.get('operacao'),
                          v.get('tituloPagarId'),
                          v.get('diasEmAtraso'),
                          v.get('juros'),
                          v.get('multa'),
                          v.get('desconto'),
                          v.get('acrescimo'),
                          v.get('valorDoMovimento'),
                          v.get('usuarioId'),
                          str(v.get('data')).replace('T', ' '),
                          str(v.get('dataRegistro')).replace('T', ' '),
                          v.get('saldo'),
                          v.get('amortizado')])
                    db.insert(sqlInsertMov, c)
        db.query(sqlDuplicidadeMov)
    except ValueError as ex:
        ex.args


def getMovimentacoesInicial():
    try:
        sqlInsertMov = 'set dateformat ymd insert into FatoContasApagarMov values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        sql = "select	titulo_id from FatoContasApagar cp where titulo_status = 'LIQUIDADO' and not exists ( select * from FatoContasAPagarMov cpm where cpm.tituloPagarId = cp.titulo_id   ) group by titulo_id oRDER BY titulo_id"
        data = db.getAll(sql)

        for i in data:
            urlMoviemntacao = f'/v1/financeiro/contas-pagar/titulos/{i[0]}/movimentacoes'

            param = {'q': "operacao == LIQUIDACAO_DE_TITULO ", 'sort': 'id'}
            dados = api.connect(urlMoviemntacao, param)
            if dados['count'] == 0:
                hash = False
            else:
                for v in dados['items']:
                    c = ([v.get('id'),
                          v.get('operacao'),
                          v.get('tituloAPagarId'),
                          v.get('diasEmAtraso'),
                          v.get('juros'),
                          v.get('multa'),
                          v.get('desconto'),
                          v.get('acrescimo'),
                          v.get('valorDoMovimento'),
                          v.get('usuarioId'),
                          str(v.get('data')).replace('T', ' '),
                          str(v.get('dataDeRegistro')).replace('T', ' '),
                          v.get('saldo'),
                          v.get('amortizado')])
                    db.insert(sqlInsertMov, c)
        db.query(sqlDuplicidadeMov)
    except ValueError as ex:
        ex.args


def pagamentoPDV():
    try:
        # db.truncateTable('truncate table FatoPagamentoPDV')
        sqlInsert = 'set dateformat ymd insert into FatoPagamentoPDV values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'

        for i in range(2):
            i = i + 1
            sqlUltimoId = f'SELECT isnull(max(identificadorId),0) as id FROM FatoPagamentoPDV where lojaid = {i}'
            #sqlUltimoId = f"SELECT isnull(max(dataHoraAberturaPagamento),'2020-01-01') as id FROM FatoPagamentoPDV where lojaid = {i}"

            hash = True
            while (hash == True):
                id = db.getId(sqlUltimoId)
                param = {'q': f'identificadorId =gt= {id}; lojaId=={i}', 'sort': 'identificadorId'}
                # param = {'q': f"dataHoraAberturaPagamento =gt= '{id}'; lojaId=={i}", "sort": "'dataHoraAberturaPagamento'"}

                dados = api.connect(urlPagamentoPDV, param)

                if dados['count'] == 0:
                    hash = False
                else:
                    print(dados['total'])
                    for v in dados['items']:
                        for v1 in v.get('itensPagamento'):
                            c = ([v.get('id'),
                                  v.get('identificadorId'),
                                  v.get('sequencial'),
                                  v.get('numeroCaixa'),
                                  v.get('data'),
                                  v.get('hora'),
                                  v.get('lojaId'),
                                  str(v.get('dataHoraAberturaPagamento').replace('T', ' '))[0:19],
                                  str(v.get('dataHoraFechamentoPagamento').replace('T', ' '))[0:19],
                                  v.get('funcionarioId'),
                                  v.get('autorizadorId'),
                                  v.get('codigoImpressora'),
                                  v.get('contadorDocumento'),
                                  v.get('coo'),
                                  v.get('numeroEquipamento'),
                                  v.get('sequencialOperador'),
                                  v.get('serieEquipamento'),
                                  v.get('valor'),
                                  v1.get('tipoPagamentoId'),
                                  v1.get('valor'),
                                  ])
                            db.insert(sqlInsert, c)
    except ValueError as ex:
        ex.args


def getFinPagamentoPDV():
    try:
        db.truncateTable('truncate table FatoFinPagamentoPDV')
        sqlInsert = 'set dateformat ymd insert into FatoFinPagamentoPDV values (?,?,?,?,?)'
        sqlUltimoId = 'SELECT isnull(max(id),0) as id FROM FatoFinPagamentoPDV'

        hash = True
        while (hash == True):
            id = db.getId(sqlUltimoId)
            param = {'q': f'id =gt= {id}', 'sort': 'id'}
            dados = api.connect(urlFinPagamentoPDV, param)

            if dados['total'] == 0:
                hash = False
            else:
                for v in dados['items']:

                    for v1 in v.get('lojas'):
                        c = ([v.get('id'),
                              v.get('descricao'),
                              v.get('categoriaId'),
                              v1.get('lojaId'),
                              v1.get('valorMaximo')
                              ])
                        db.insert(sqlInsert, c)
    except ValueError as ex:
        ex.args


def execContasPagar():
    dtInicio = datetime.now().time()
    print('Inicio...\n', dtInicio)
    print('Inserindo Contas a Pagar...')
    getContasApagar()
    print('Inserindo Movimentos...')
    getMovimentacoesInicial()
    print('Inserindo Pagamentos PDV...')
    pagamentoPDV
    print('Inserindo Mapeamento do pagamentos PDV...')
    getFinPagamentoPDV()
    print('Fim...\n', datetime.now().time())

# getContasApagarId(51218)
# getContasApagar()
# getMovimentacoesInicial()
# getContasApagar()
#getContasApagar()
# pagamentoPDV()

