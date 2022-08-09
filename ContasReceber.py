import ConnectApi as api, ConnectionFactory as db
# from VarejoFacilAPI.ContasReceber import mContasReceber as cr, dbContasReceber as db
from datetime import datetime

url = '/v1/financeiro/contas-receber'


def getContasReceber():
    try:
        sqlInsert = 'set dateformat ymd insert into FatoContasAReceber values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        sqlUltimoData = 'SELECT max(dtAlteracao) as id FROM FatoContasAReceber'
        # sqlUltimoId = 'SELECT max(id) as id FROM FatoContasAReceber'
        sqlDuplicidade = """
                         delete c from FatoContasAReceber c
                        join (
                                select 
                                    id,
									titulo_id,
                                    lojaId,
                                    dtEmissao,
                                    count(titulo_dtAlteracao) contagem,
                                    max(titulo_dtAlteracao) data,
									max(rowid) as rowId
                                from FatoContasAReceber
                                 --where id = 795640
                                group by 
                                    id,
									titulo_id,
                                    lojaId,
                                    dtEmissao
                                having count(titulo_dtAlteracao) > 1
                        ) d on d.id = c.id and d.titulo_id = c.titulo_id and  d.lojaId = C.lojaId and c.rowId <> d.rowId

        """
        # limite = ultimoIdContasReceber()
        hash = True
        while (hash == True):
            id = str(db.getId(sqlUltimoData)).replace(' ', 'T')
            param = {'count': 100, 'q': 'dataAlteracao =gt= ' + str(id), 'sort': 'dataAlteracao'}
            # param = {'count': 300,'q': 'id =gt= '+str(id), 'sort': 'id'}
            # param = {'count': 500,'q': 'id =gt= 0', 'sort': 'id'}
            dados = api.connect(url, param)
            if dados['count'] == 0:
                hash = False
            else:
                for v in dados['items']:

                    cartao_id = None
                    numeroAutorizacao = None
                    modalidade = None
                    bandeiraId = None
                    redeAdquirenteId = None
                    nsu = None
                    nsuAutorizacao = None
                    taxa = None
                    numeroCartao = None
                    contaCorrenteId = None
                    finalizadoraId = None
                    formaDeCaptura = None

                    if v.get('recebimentoCartao'):
                        d2 = v.get('recebimentoCartao')
                        cartao_id = d2.get('id')
                        numeroAutorizacao = d2.get('numeroAutorizacao')
                        modalidade = d2.get('modalidade')
                        bandeiraId = d2.get('bandeiraId')
                        redeAdquirenteId = d2.get('redeAdquirenteId')
                        nsu = d2.get('nsu', 0)
                        nsuAutorizacao = d2.get('nsuAutorizacao', 0)
                        taxa = d2.get('taxa', 0)
                        numeroCartao = 1
                        contaCorrenteId = d2.get('contaCorrenteId', 0)
                        finalizadoraId = d2.get('finalizadoraId', 0)
                        formaDeCaptura = d2.get('formaDeCaptura', 0)

                    for v1 in v.get('titulos'):
                        c = ([v.get('id'),
                              v.get('dataEmissao'),
                              str(v.get('dataAlteracao')).replace('T', ' ')[0:23],
                              v.get('valorBruto'),
                              v.get('valorDesconto'),
                              v.get('valorAcrescimo'),
                              v.get('condicaoDePagamento'),
                              v.get('numeroDeTitulos'),
                              v.get('primeiroVencimento'),
                              v.get('efetivo'),
                              v.get('lojaId'),
                              v.get('especieDocumentoId'),
                              v.get('agenteFinanceiroId'),
                              v.get('categoriaFinanceiraId'),
                              v.get('centroCustoId'),
                              v.get('intervaloDeDiasEntreOsTitulos'),
                              v.get('geraTitulos'),
                              v1.get('id'),
                              v1.get('ordem'),
                              v1.get('vencimento'),
                              v1.get('liquidacao'),
                              str(v1.get('dataAlteracao')).replace('T', ' ')[0:23],
                              v1.get('valor'),
                              v1.get('valorLiquido'),
                              v1.get('status'),
                              v.get('origem'),
                              v.get('formaPagamentoId'),
                              v.get('diasParaIncidenciaDeJuros'),
                              v.get('moraDiariaPorAtraso'),
                              v.get('diasParaIncidenciaDeMulta'),
                              v.get('valorMultaPorAtraso'),
                              v.get('tipoValorMoraDiaria'),
                              v.get('tipoValorMulta'),
                              # Informações dos cartões
                              cartao_id,
                              numeroAutorizacao,
                              modalidade,
                              bandeiraId,
                              redeAdquirenteId,
                              nsu,
                              nsuAutorizacao,
                              taxa,
                              numeroCartao,
                              contaCorrenteId,
                              finalizadoraId,
                              formaDeCaptura
                              ])

                        db.insert(sqlInsert, c)
        print('Removendo registros duplicados...')
        db.query(sqlDuplicidade)
    except ValueError as ex:
        ex.args


def getContasReceberId(ID = None):
    try:

        sqlInsert = 'set dateformat ymd insert into FatoContasAReceber values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'

        param = {'q': f'numeroDocumento == {ID} '}
        param = {'q': f"dataEmissao =gt= '2021-11-01T00:00:00' ,  "}

        # param = {'count': 500,'q': 'id =gt= 0', 'sort': 'id'}
        dados = api.connect(url, param)

        for v in dados['items']:

            cartao_id = None
            numeroAutorizacao = None
            modalidade = None
            bandeiraId = None
            redeAdquirenteId = None
            nsu = None
            nsuAutorizacao = None
            taxa = None
            numeroCartao = None
            contaCorrenteId = None
            finalizadoraId = None
            formaDeCaptura = None

            if v.get('recebimentoCartao'):
                d2 = v.get('recebimentoCartao')
                cartao_id = d2.get('id')
                numeroAutorizacao = d2.get('numeroAutorizacao')
                modalidade = d2.get('modalidade')
                bandeiraId = d2.get('bandeiraId')
                redeAdquirenteId = d2.get('redeAdquirenteId')
                nsu = d2.get('nsu', 0)
                nsuAutorizacao = d2.get('nsuAutorizacao', 0)
                taxa = d2.get('taxa', 0)
                numeroCartao = 1
                contaCorrenteId = d2.get('contaCorrenteId', 0)
                finalizadoraId = d2.get('finalizadoraId', 0)
                formaDeCaptura = d2.get('formaDeCaptura', 0)

            for v1 in v.get('titulos'):
                c = ([v.get('id'),
                      v.get('observacao'),
                      v.get('numeroDocumento'),
                      v.get('dataEmissao'),
                      str(v.get('dataAlteracao')).replace('T', ' ')[0:23],
                      v.get('valorBruto'),
                      v.get('valorDesconto'),
                      v.get('valorAcrescimo'),
                      v.get('condicaoDePagamento'),
                      v.get('numeroDeTitulos'),
                      v.get('primeiroVencimento'),
                      v.get('efetivo'),
                      v.get('lojaId'),
                      v.get('especieDocumentoId'),
                      v.get('agenteFinanceiroId'),
                      v.get('categoriaFinanceiraId'),
                      v.get('centroCustoId'),
                      v.get('intervaloDeDiasEntreOsTitulos'),
                      v.get('geraTitulos'),
                      v1.get('id'),
                      v1.get('ordem'),
                      v1.get('vencimento'),
                      v1.get('liquidacao'),
                      str(v1.get('dataAlteracao')).replace('T', ' ')[0:23],
                      v1.get('valor'),
                      v1.get('valorLiquido'),
                      v1.get('status'),
                      v.get('origem'),
                      v.get('formaPagamentoId'),
                      v.get('diasParaIncidenciaDeJuros'),
                      v.get('moraDiariaPorAtraso'),
                      v.get('diasParaIncidenciaDeMulta'),
                      v.get('valorMultaPorAtraso'),
                      v.get('tipoValorMoraDiaria'),
                      v.get('tipoValorMulta'),
                      # Informações dos cartões
                      cartao_id,
                      numeroAutorizacao,
                      modalidade,
                      bandeiraId,
                      redeAdquirenteId,
                      nsu,
                      nsuAutorizacao,
                      taxa,
                      numeroCartao,
                      contaCorrenteId,
                      finalizadoraId,
                      formaDeCaptura
                      ])
                db.insert(sqlInsert, c)
        print('Removendo registros duplicados...')
    except ValueError as ex:
        ex.args


def ultimoIdContasReceber():
    try:
        param = {'sort': '-id', 'count': 1}
        dados = api.connect(url, param)
        for v in dados['items']:
            c = v.get('id')
            return c

    except ValueError:
        print('Erro...')


def execContasReceber():
    dtInicio = datetime.now().time()
    print('Inicio...\n', dtInicio)
    print('Inserindo Contas a Receber...')
    getContasReceber()
    print('Fim...\n', datetime.now().time())



