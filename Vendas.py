import ConnectApi as api
import ConnectionFactory as db
from datetime import datetime

url = '/v1/venda/cupons-fiscais'
urlTrocasDevolucoes = '/v1/pdv/trocas-devolucoes'

def vendas():
    try:
        sqlInsert = 'set dateformat ymd insert into FatoVendas values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        # sqlUltimoData = 'select max(criadoEm) as id from fatovendasTeste'
        sqlUltimoData = "select max(identificadorid) as id from fatovendas"
        duplicados = """ 
                      delete from fatovendas 
                      where id+'|'+convert(varchar(30),iditem)+'|'+convert(varchar(30),dtImportacao,120)  in (select 
                                                                                                                  id+'|'+convert(varchar(30),iditem)+'|'+convert(varchar(30),min(dtImportacao),120)
                                                                                                              from fatovendas
                                                                                                              group by 
                                                                                                                  id,
                                                                                                                  idItem
                                                                                                              having 
                                                                                                                  count(1) > 1)
          """

        limite = 1  
        count = 0
        while (count < limite ):
            id = db.getId(sqlUltimoData)
            # param = {'count': 100, 'q': f'criadoEm=gt="{updateDate[0:19]}"', 'sort': 'criadoEm'}
            # if updateDate == '0':
            param = {'count': 100, 'q': f'identificadorId=gt={id}', 'sort': 'identificadorId'}
            # else:
            #     param = {'count': 100, 'q': f'lojaId==2;criadoem=gt="{updateDate[0:19]}', 'sort': 'criadoem'}

            dados = api.connect(url, param)
            if limite == 1:
                limite = dados['total']
            count = count + 100
            print(f'start: {count} total: {limite}')
            list = []
            for v in dados['items']:
                for v1 in v.get('itensVenda'):
                    c = [v.get('id'),
                         v.get('numeroCaixa'),
                         v.get('lojaId'),
                         v.get('funcionarioId'),
                         v.get('dataVenda'),
                         str(v.get('dataHoraAberturaCupom').replace('T',' '))[0:19],
                         str(v.get('dataHoraFechamentoCupom').replace('T', ' '))[0:19],
                         v1.get('id'),
                         v1.get('produtoId'),
                         v1.get('quantidadeVenda'),
                         v1.get('valorUnidade'),
                         v1.get('valorAcrescimo'),
                         v1.get('valorDesconto'),
                         v1.get('valorTotal'),
                         v1.get('precoVenda'),
                         v1.get('precoCusto'),
                         v1.get('precoCustoMedio'),
                         v1.get('precoCustoFiscal'),
                         v1.get('tributacaoAliquota'),
                         v1.get('tributacaoValorReducao'),
                         v1.get('aliquotaPIS'),
                         v1.get('aliquotaCOFINS'),
                         datetime.now(),
                         v.get('identificadorId'),
                         v.get('cancelada'),
                         v1.get('tipo'),
                         v1.get('taxaEntrega'),
                         v1.get('participouPromocaoDesconto'),
                         v1.get('codigoAuxiliarId'),
                         v1.get('funcionarioVendedorId'),
                         v1.get('funcionarioAutorizadorId'),
                         v1.get('funcionarioCaptacaoPrevendaId'),
                         v1.get('funcionarioProducaoId'),
                         v1.get('valorServico'),
                         v1.get('fatorBonificacao'),
                         v1.get('valorDoDescontoMegaCaixa'),
                         v1.get('tipoBonificacao'),
                         v1.get('serieProduto'),
                         v1.get('tributacao'),
                         v1.get('tributacaoSimbologia'),
                         v1.get('tipoPreco'),
                         v1.get('tipoDeDescontoAplicado'),
                         v1.get('csosn'),
                         v1.get('cstPIS'),
                         v1.get('cstCOFINS'),
                         v1.get('natureza'),
                         v1.get('ncm'),
                         v1.get('ncmExcecao'),
                         v1.get('tabelaA'),
                         v1.get('tabelaB'),
                         v1.get('cfop'),
                         v1.get('setorDeProducaoId'),
                         v1.get('valorFecop'),
                         v1.get('tributacaoAliquotaFecop'),
                         v1.get('localVendaId'),
                         v1.get('valorICMSDesonerado'),
                         v1.get('foiVendidoEmOferta'),
                         v.get('temItensVendidoEmOferta'),
                         str(v.get('criadoEm').replace('T', ' '))[0:23]]
                    list.append(c)
            db.insertLote(sqlInsert,list)
        db.query(duplicados)
        print(f'Dados Inseridos - {count}')
    except ValueError:
        print('Erro...')



def vendasID(id):
    try:
        sqlInsert = 'set dateformat ymd insert into FatoVendas values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        # sqlUltimoData = 'select max(criadoEm) as id from fatovendasTeste'
        sqlUltimoData = "select max(identificadorid) as id from fatovendas"
        duplicados = """ 
                      delete from fatovendas 
                      where id+'|'+convert(varchar(30),iditem)+'|'+convert(varchar(30),dtImportacao,120)  in (select 
                                                                                                                  id+'|'+convert(varchar(30),iditem)+'|'+convert(varchar(30),min(dtImportacao),120)
                                                                                                              from fatovendas
                                                                                                              group by 
                                                                                                                  id,
                                                                                                                  idItem
                                                                                                              having 
                                                                                                                  count(1) > 1)
          """

        limite = 1
        count = 0
        while (count < limite ):

            # param = {'count': 100, 'q': f'criadoEm=gt="{updateDate[0:19]}"', 'sort': 'criadoEm'}
            # if updateDate == '0':
            param = {'count': 100, 'q': f'identificadorId=={id}', 'sort': 'identificadorId'}
            # else:
            #     param = {'count': 100, 'q': f'lojaId==2;criadoem=gt="{updateDate[0:19]}', 'sort': 'criadoem'}

            dados = api.connect(url, param)
            if limite == 1:
                limite = dados['total']
            count = count + 100
            print(f'start: {count} total: {limite}')
            list = []
            for v in dados['items']:
                for v1 in v.get('itensVenda'):
                    c = [v.get('id'),
                         v.get('numeroCaixa'),
                         v.get('lojaId'),
                         v.get('funcionarioId'),
                         v.get('dataVenda'),
                         str(v.get('dataHoraAberturaCupom').replace('T',' '))[0:19],
                         str(v.get('dataHoraFechamentoCupom').replace('T', ' '))[0:19],
                         v1.get('id'),
                         v1.get('produtoId'),
                         v1.get('quantidadeVenda'),
                         v1.get('valorUnidade'),
                         v1.get('valorAcrescimo'),
                         v1.get('valorDesconto'),
                         v1.get('valorTotal'),
                         v1.get('precoVenda'),
                         v1.get('precoCusto'),
                         v1.get('precoCustoMedio'),
                         v1.get('precoCustoFiscal'),
                         v1.get('tributacaoAliquota'),
                         v1.get('tributacaoValorReducao'),
                         v1.get('aliquotaPIS'),
                         v1.get('aliquotaCOFINS'),
                         datetime.now(),
                         v.get('identificadorId'),
                         v.get('cancelada'),
                         v1.get('tipo'),
                         v1.get('taxaEntrega'),
                         v1.get('participouPromocaoDesconto'),
                         v1.get('codigoAuxiliarId'),
                         v1.get('funcionarioVendedorId'),
                         v1.get('funcionarioAutorizadorId'),
                         v1.get('funcionarioCaptacaoPrevendaId'),
                         v1.get('funcionarioProducaoId'),
                         v1.get('valorServico'),
                         v1.get('fatorBonificacao'),
                         v1.get('valorDoDescontoMegaCaixa'),
                         v1.get('tipoBonificacao'),
                         v1.get('serieProduto'),
                         v1.get('tributacao'),
                         v1.get('tributacaoSimbologia'),
                         v1.get('tipoPreco'),
                         v1.get('tipoDeDescontoAplicado'),
                         v1.get('csosn'),
                         v1.get('cstPIS'),
                         v1.get('cstCOFINS'),
                         v1.get('natureza'),
                         v1.get('ncm'),
                         v1.get('ncmExcecao'),
                         v1.get('tabelaA'),
                         v1.get('tabelaB'),
                         v1.get('cfop'),
                         v1.get('setorDeProducaoId'),
                         v1.get('valorFecop'),
                         v1.get('tributacaoAliquotaFecop'),
                         v1.get('localVendaId'),
                         v1.get('valorICMSDesonerado'),
                         v1.get('foiVendidoEmOferta'),
                         v.get('temItensVendidoEmOferta'),
                         str(v.get('criadoEm').replace('T', ' '))[0:23]]
                    list.append(c)
            db.insertLote(sqlInsert,list)
        db.query(duplicados)
        print(f'Dados Inseridos - {count}')
    except ValueError:
        print('Erro...')






def ultimoIdDataHora():
    try:
        param = {'sort': '-dataHoraAberturaCupom', 'count': 1 }
        dados = api.connect(url,param)
        for v in dados['items']:
            c = v.get('dataHoraAberturaCupom')
            return c
    except:
        print('Erro na importação na ultima data e hora de abertura de cupom fiscal')


def getTrocasDevolucoes():
    try:
        sqlInsert = 'set dateformat ymd insert into fatoTrocasDevolucoes values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        # sqlUltimoData = 'select max(criadoEm) as id from fatovendasTeste'
        sqlUltimoData = "select isnull(max(id),0)  as id from fatoTrocasDevolucoes"

        hash = True

        while (hash == True):

            id = db.getId(sqlUltimoData)
            param = {'count': 500, 'q': f'id =gt= {id}', 'sort': 'id'}
            dados = api.connect(urlTrocasDevolucoes, param)
            if dados['count'] == 0:
                hash = False
            else:
                print(f"start: {dados['count']} total: {dados['total']}")
                list = []
                for v in dados['items']:
                    for v1 in v.get('itens'):
                        c = [v.get('id'),
                             v.get('localId'),
                             v.get('lojaId'),
                             v.get('lojaDestinoId'),
                             v.get('caixaId'),
                             v.get('clienteId'),
                             v.get('cooVenda'),
                             str(v.get('dataVenda').replace('T', ' '))[0:10],
                             v.get('valorTotal'),
                             v.get('emitiuNotaFiscal'),
                             v.get('finalidade'),
                             v.get('caixaDaTransacao'),
                             v.get('sequencialDaTransacao'),
                             str(v.get('dataDaTransacao', '1899-12-31T00:00:00').replace('T', ' '))[0:19],
                             v1.get('id'),
                             v1.get('produtoId'),
                             v1.get('vendedorId'),
                             v1.get('quantidadeDeItensNoCupom'),
                             v1.get('quantidadeDeItensDevolvidos'),
                             v1.get('numeroDeSerie'),
                             v1.get('valorUnitario'),
                             v1.get('percentualDeDesconto'),
                             v1.get('valorTotalDevolvido')]
                        print(c)
                        list.append(c)
                db.insertLote(sqlInsert, list)
        # db.query(duplicados)
    except ValueError:
        print('Erro...')

def execVendas():
    print('Inicio...\n', datetime.now())
    print('Inserindo vendas!!!')
    vendas()

    getTrocasDevolucoes()
    print('Fim...\n', datetime.now())



