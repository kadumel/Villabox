import ConnectApi as api
import ConnectionFactory as db
from datetime import datetime, timedelta
import traceback

urlLoja = '/v1/pessoa/lojas'
urlProduto = '/v1/produto/produtos'
urlSecao = '/v1/produto/secoes'
urlGrupo = '/v1/produto/secoes/{0}/grupos'
urlSubGrupo = '/v1/produto/secoes/{0}/grupos/{1}/subgrupos'
urlFornecedor = '/v1/pessoa/fornecedores'
urlPordutoCusto = '/v1/produto/custos'
urlCategoriaFinanceira = '/v1/financeiro/categorias'
urlFormaPagamento = '/v1/financeiro/formas-pagamento'
urlContaCorrente = '/v1/financeiro/contas-correntes'
urlAgenteFinanceiro = '/v1/pessoa/agentes-financeiros'
urlMarcas = "/v1/produto/marcas"

def getLojas():
    try:
        db.truncateTable('truncate table dimLoja')
        sql = 'insert into dimLoja values (?,?,?)'
        param = {'sort': 'id'}
        dados = api.connect(urlLoja,param)
        list = []
        for v in dados['items']:
            id = v.get('id')
            nome = v.get('nome')
            loja = 'LOJA - 0'+str(id)
            list.append([id, nome, loja])
        db.insertLote(sql, list)
    except:
        print('Erro...')


def getProduto():
    try:
        #Consultas
        sqlGetData = "SELECT isnull(max(dateadd(hour, -3, dtAlteração)), '2000-01-01T00:00:00' ) as id FROM dimProduto"
        #sqlGetId = "SELECT isnull(max(cdproduto), 0 ) as id FROM dimProduto"
        sqlInsert = 'set dateformat ymd insert into dimProduto values (?,?,?,?,?,?,?,?)'
        sqlDuplicate = """
                DELETE
                from DimProduto 
                where 
                    convert(varchar(10), cdproduto)+'|'+convert(varchar(24), dtalteração) in (
                        select 
                            convert(varchar(10), cdproduto)+'|'+convert(varchar(24),min(dtAlteração))
                        from DimProduto
                        group by 
                            cdProduto
                        having 
                            count(1) > 1
                        )
            """
        sqlDuplicateDtImportacao = """
                        DELETE
                        from DimProduto 
                        where 
                            convert(varchar(10), cdproduto)+'|'+convert(varchar(24), dtImportação,20) in (
                                select 
                                    convert(varchar(10), cdproduto)+'|'+convert(varchar(24),min(dtImportação),20)
                                from DimProduto
                                group by 
                                    cdProduto
                                having 
                                    count(1) > 1
                                )
                    """

        hash = True

        while (hash == True):

            id = str(db.getId(sqlGetData))

            id2 = str(id).replace(' ', 'T')[0:19]
            id3 = str(datetime.strptime(str(id)[0:19], '%Y-%m-%d %H:%M:%S') + timedelta(hours=3)).replace("T"," ")

            param = {'q': f'dataAlteracao=gt={id2}','sort': 'dataAlteracao', 'count': 500}
            #param = {'q': 'id =gt= '+str(id),'sort': 'id', 'count': 500}
            # param = {'q': f'id == 15379'}

            dados = api.connect(urlProduto,param)
            if dados['count'] == 0 :
                print('Fim da transação.')
                break

            list = []
            for v in dados['items']:

                dtUpdate = str(v.get('dataAlteracao'))[0:23].replace('T',' ')
                if dtUpdate[0:19] == id3 and dados['count'] == 1:
                    print('Fim da transação.')
                    hash = False
                    break
                else:
                    cdProduto = v.get('id')
                    nome = v.get('descricao')
                    idSecao = v.get('secaoId')
                    idGrupo = v.get('grupoId')
                    idSubGrupo = v.get('subgrupoId')
                    idMarca = v.get('marcaId')
                    dtImportacao = datetime.now()
                    list.append([cdProduto, nome, idSecao, idGrupo, idSubGrupo, idMarca ,dtUpdate, str(dtImportacao)[0:19]])
            db.insertLote(sqlInsert, list)
        db.query(sqlDuplicate)
        db.query(sqlDuplicateDtImportacao)
    except Exception:
        traceback.print_exc()


def getProdutoID(codigo):
    try:
        #Consultas
        sqlGetId = "SELECT isnull(max(cdproduto), 0 ) as id FROM dimProduto"
        sqlInsert = 'set dateformat ymd insert into dimProduto values (?,?,?,?,?,?,?)'
        sqlDuplicate = """
                       DELETE
                       from DimProduto 
                       where 
                           convert(varchar(10), cdproduto)+'|'+convert(varchar(24), dtalteração) in (
                               select 
                                   convert(varchar(10), cdproduto)+'|'+convert(varchar(24),min(dtAlteração))
                               from DimProduto
                               group by 
                                   cdProduto
                               having 
                                   count(1) > 1
                               )
                   """
        global hash
        hash = True

        while (hash == True):

            id = db.getId(sqlGetId)
            param = {'q': f'id =gt= {str}','sort': 'id', 'count': 500}
            #param = {'q': f'id == {codigo}'}

            dados = api.connect(urlProduto,param)
            if dados['count'] == 0:
                print('Fim da transação.')
                hash = False
                break
            list = []
            for v in dados['items']:
                cdProduto = v.get('id')
                nome = v.get('descricao')
                idSecao = v.get('secaoId')
                idGrupo = v.get('grupoId')
                idSubGrupo = v.get('subgrupoId')
                dtUpdate = str(v.get('dataAlteracao')).replace('T',' ')[0:23]
                dtImportacao = str(datetime.now())[0:19]
                list.append([cdProduto, nome, idSecao, idGrupo, idSubGrupo, dtUpdate, dtImportacao])
            db.insertLote(sqlInsert, list)
            hash = False
        db.query(sqlDuplicate)
    except Exception:
        traceback.print_exc()



def getProdutoIDnotFound():
    try:
        #Consultas
        sqlGetId = """select distinct f.produtoId from fatovendas f
                        where 
                            not exists ( select * from DimProduto p where p.cdProduto = f.produtoId) """
        sqlInsert =  'set dateformat ymd insert into dimProduto values (?,?,?,?,?,?,?,?)'
        id = db.getAll(sqlGetId)
        for x in id:
            param = {'q': f'id == {x[0]}'}
            dados = api.connect(urlProduto,param)
            for v in dados['items']:
                cdProduto = v.get('id')
                nome = v.get('descricao')
                idSecao = v.get('secaoId')
                idGrupo = v.get('grupoId')
                idSubGrupo = v.get('subgrupoId')
                idMarca = v.get('marcaId')
                dtUpdate = str(v.get('dataAlteracao'))[0:23].replace('T', ' ')
                dtImportacao = datetime.now()
                list = [cdProduto, nome, idSecao, idGrupo, idSubGrupo, idMarca ,dtUpdate, str(dtImportacao)[0:19]]
                db.insert(sqlInsert, list)
                print(list)
    except Exception:
        traceback.print_exc()



def getProdutoCusto():
    try:
        #Consultas
        sqlGetId = 'SELECT isnull(max(id),0) as id FROM DimProdutoCusto'
        sqlGetDataReajuste = 'SELECT isnull(max(dataUltimoReajustePreco1),0) as id FROM DimProdutoCusto'
        sqlInsert = 'set dateformat ymd insert into DimProdutoCusto values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        sqlDelteDuplicate = """
                            delete from DimProdutoCusto 
                            where 
                                convert(varchar(10),id) +'|'+convert(varchar(19), dataUltimoReajustePreco1) 
                                in (
                                    select 
                                        convert(varchar(10),id) +'|'+convert(varchar(19),min(dataUltimoReajustePreco1))
                                    from DimProdutoCusto
                                    group by 
                                        id
                                    having 
                                        count(1) > 1)
                            """

        # limite = ultimoIdProduto(urlPordutoCusto)
        # id = db.getId(sqlGetId)

        global hash
        hash = True

        while (hash == True):

            id = str(db.getId(sqlGetDataReajuste)).replace(' ', 'T')
            # param = {'q': f'id =gt= {str(id)}', 'sort': 'id', 'count': 500}
            param = {'q': f'dataUltimoReajustePreco1=gt={id}', 'sort': 'dataUltimoReajustePreco1', 'count': 500}

            dados = api.connect(urlPordutoCusto,param)

            if dados['count'] == 0:
                print('Não a dados para a consulta excutada.')
                hash = False
                break
            # print(dados)

            list = []
            for v in dados['items']:

                dataAjustePreco = None
                if v.get('dataUltimoReajustePreco1'):
                    dataAjustePreco = v.get('dataUltimoReajustePreco1')[0:19]

                c = [
                    v.get('id'),
                    v.get('lojaId'),
                    v.get('produtoId'),
                    v.get('precoVenda1'),
                    v.get('precoOferta1'),
                    v.get('margemPreco1'),
                    dataAjustePreco,
                    v.get('precoVenda2'),
                    v.get('precoOferta2'),
                    v.get('margemPreco2'),
                    v.get('quantidadeMinimaPreco2'),
                    v.get('precoVenda3'),
                    v.get('precoOferta3'),
                    v.get('margemPreco3'),
                    v.get('quantidadeMinimaPreco3'),
                    v.get('permiteDesconto'),
                    v.get('custoProduto'),
                    v.get('precoMedioDeReposicao'),
                    v.get('precoFiscalDeReposicao'),
                    v.get('origem'),
                    datetime.today()
                ]

                list.append(c)
            db.insertLote(sqlInsert, list)
        db.query(sqlDelteDuplicate)
    except Exception:
        traceback.print_exc()




def ultimoIdProduto(url):
    try:
        param = {'sort': '-id', 'count': 1 }
        dados = api.connect(url,param)
        for v in dados['items']:
            c = v.get('id')
            return c
    except:
        print('Erro...')


def getSecao():
    try:
        db.truncateTable('truncate table DimSecao')
        sql = 'insert into dimSecao values (?,?)'
        param = {'sort': 'id'}
        dados = api.connect(urlSecao,param)
        list = []
        for v in dados['items']:
            id = v.get('id')
            nome = v.get('descricao')
            list.append([id, nome])
        db.insertLote(sql, list)
    except:
        print('Erro...')


def getGrupo():
    try:
        db.truncateTable('truncate table DimGrupo')
        sql = 'insert into DimGrupo values (?,?,?)'
        listSecao = db.getAll('select cdSecao from DimSecao')
        list = []
        for i in listSecao:
            param = {'sort': 'id', 'count': 500}
            dados = api.connect(urlGrupo.format(i[0]), param)
            for v in dados['items']:
                id = v.get('id')
                nome = v.get('descricao')
                cdSecao = v.get('secaoId')
                list.append([id, nome, cdSecao])
        db.insertLote(sql, list)
    except:
        print('Erro...')


def getSubGrupo():
    try:
        db.truncateTable('truncate table DimSubGrupo')
        sql = 'insert into DimSubGrupo values (?,?,?,?)'
        listGrupo = db.getAll('select cdSecao, cdGrupo from DimGrupo')
        list = []
        for i in listGrupo:
            param = {'sort': 'id', 'count': 500}
            dados = api.connect(urlSubGrupo.format(i[0], i[1]), param)
            for v in dados['items']:
                id = v.get('id')
                nome = v.get('descricao')
                cdSecao = v.get('secaoId')
                cdGrupo = v.get('grupoId')
                list.append([id, nome, cdSecao, cdGrupo])
        db.insertLote(sql, list)
    except Exception:
        traceback.print_exc()


def getFornecedor():
    try:
        db.truncateTable('truncate table dimFornecedor')
        sql = 'insert into dimFornecedor values (?,?,?,?,?)'
        param = {'sort': 'id', 'count': 500}
        dados = api.connect(urlFornecedor,param)
        list = []
        for v in dados['items']:
            id = v.get('id')
            nome = v.get('nome')
            fantasia = v.get('fantasia')
            cnpj = v.get('numeroDoDocumento')
            prazo = v.get('prazo')
            list.append([id, nome, fantasia, cnpj, prazo])
        db.insertLote(sql, list)
    except ValueError as e:
        print('Erro... ' , e)


def getCategorias():
    try:
        db.truncateTable('truncate table DimCategorias')
        sql = 'insert into DimCategorias values (?,?,?,?,?,?,?)'
        param = {'sort': 'id', 'count': 500}
        dados = api.connect(urlCategoriaFinanceira, param)
        list = []
        for v in dados['items']:
            id = v.get('id')
            nome = v.get('descricao')
            idPai = v.get('codigoDaCategoriaPai')
            inativa = v.get('inativa')
            posicao = v.get('posicao')
            classificacao = v.get('classificacaoDaCategoria')
            tipo = v.get('tipoDeCategoria')
            list.append([id, nome, idPai, inativa, posicao, classificacao, tipo])
        db.insertLote(sql, list)
    except ValueError:
        print('Erro...')


def getFormaPagamento():
    try:
        db.truncateTable('truncate table dimformapagamento')
        sql = 'insert into dimformapagamento values (?,?,?,?,?,?,?,?,?)'
        param = {'sort': 'id', 'count': 500}
        dados = api.connect(urlFormaPagamento, param)
        list = []
        for v in dados['items']:

            id = v.get('id')
            nome = v.get('descricao')
            idespecieDocPai = v.get('especieDeDocumentoId')
            agenteFinanceiro = v.get('agenteFinanceiroId')
            catFinId = v.get('categoriaFinanceiraId')
            controleCartao = v.get('controleDeCartao')
            movContaCorrente = v.get('movimentaContaCorrente')
            idLoja = None
            idContaCorrente = None

            for v1 in v['lojas']:
                idLoja = v1.get('lojaId')
                idContaCorrente = v1.get('contaCorrenteId')

            list.append(
                [id, nome, idespecieDocPai, agenteFinanceiro, catFinId, controleCartao, movContaCorrente, idLoja,
                 idContaCorrente])
        db.insertLote(sql, list)
    except ValueError:
        print('Erro...')


def getContaCorrente():
    try:
        sqlTruncate = db.truncateTable('truncate table dimContaCorrente')
        sqlInsert = 'set dateformat ymd insert into dimcontaCorrente values (?,?,?,?,?,?,?)'

        param = {'sort': 'id'}
        dados = api.connect(urlContaCorrente, param)
        for v in dados['items']:
            c = ([v.get('id'),
                  v.get('descricao'),
                  v.get('tipo'),
                  v.get('lojaId'),
                  v.get('agenteFinanceiroId'),
                  v.get('agencia'),
                  v.get('conta')
                  ])
            db.insert(sqlInsert, c)

    except ValueError as ex:
        ex.args


def getAgenteFinanceiro():
    pass


def getMarca():
    try:
        # sqlTruncate = db.truncateTable('truncate table DimMarca')
        sqlInsert = 'set dateformat ymd insert into DimMarca values (?,?)'
        sqlUltimoId = 'select isnull(max(id),0) as id from dimMarca'

        hash = True

        while hash == True:
            id = db.getId(sqlUltimoId)
            param = {'sort': 'id', 'q': f'id =gt= {id} '}
            dados = api.connect(urlMarcas, param)

            if dados['count'] == 0:
                hash = False
            else:
                for v in dados['items']:
                    c = ([v.get('id'),
                          v.get('descricao')
                          ])
                    db.insert(sqlInsert, c)

    except ValueError as ex:
        ex.args


def execTabAux():
    dtInicio = datetime.now().time()
    print('Inicio...\n',dtInicio)
    print('Inserindo Lojas...')
    getLojas()
    print('Inserindo Produto...')
    getProduto()
    # getProdutoID()
    print('Inserindo getProdutoCusto...')
    getProdutoCusto()
    print('Inserindo Seção...')
    getSecao()
    print('Inserindo Grupo...')
    getGrupo()
    print('Inserindo SubGrupo...')
    getSubGrupo()
    getProdutoIDnotFound()
    print('Inserindo Fornecedores...')
    getFornecedor()
    print('Inserindo Categoria Financeira...')
    getCategorias()
    print('Inserindo Forma de pagamento...')
    getFormaPagamento()
    print('Inserindo Conta Corrente...')
    getContaCorrente()
    print('Inserindo Agente Financeiro...')
    getAgenteFinanceiro()
    print('Inserindo Marcas...')
    getMarca()
    print('Fim...\n',datetime.now().time())









