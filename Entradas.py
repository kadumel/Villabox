import ConnectApi as api
import ConnectionFactory as db
from datetime import datetime

url = '/v1/compra/notas-fiscais'

def entradas():
    try:
        sqlInsert = 'set dateformat ymd insert into FatoCompras values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        sqlUltimoId = "select isnull(max(dataAlteracao),0) as id from FatoCompras "
        sqlDuplicidade = """
                        delete c from FatoCompras c
                        join (
                                select 
                                    id,
                                    lojaId,
                                    localId,
                                    dataEmissao,
                                    count(distinct dataAlteracao) contagem,
                                    max(dataalteracao) data
                                from FatoCompras
                                -- where id = 39142
                                group by 
                                    id,
                                    lojaId,
                                    localId,
                                    dataEmissao
                                having count(distinct dataalteracao) > 1
                        ) d on d.id = c.id and d.lojaId = C.lojaId and d.data <> c.dataAlteracao
            """
        limite = 1
        id = 0
        count = 0
        hash = True
        while (hash == True):

            id = db.getId(sqlUltimoId)[0:23]
            param = {'count': 300, 'q': f"dataAlteracao=gt={id}", 'sort': 'dataAlteracao'}
            # param = {'count': 300, 'q': f'id==2285', 'sort': '-id'}
            dados = api.connect(url, param)

            if dados['count'] ==0:
                hash = False
            else:
            # if limite == 1:
                limite = dados['total']
                count = count + 300
                print(f'start: {count} total: {limite}')
                list = []
                for v in dados['items']:
                    for v1 in v.get('itens'):
                        c = [v.get('id'),
                            v.get('lojaId'),
                            v.get('localId'),
                            v.get('cfopId'),
                            v.get('fornecedorId'),
                            v.get('clienteId'),
                            v.get('operacaoId'),
                            v.get('dataEmissao'),
                            v.get('dataOperacao'),
                            v.get('dataExclusao'),#.replace('T',' ')[0:19],
                            v.get('dataAlteracao'),#replace('T',' ')[0:19],
                            v.get('dataPostoFiscal'),#.replace('T',' ')[0:19],
                            v.get('numeroNota'),
                            v.get('serie'),
                            v.get('chaveDaNfe'),
                            None, #v.get('observacao'),
                            v.get('funcionarioCompradorId'),
                            v.get('funcionarioEmissorId'),
                            v.get('tipoDeFrete'),
                            v.get('condicaoDePagamento'),
                            v.get('processoDeEmissao'),
                            v.get('tipoDeDocumentoFiscal'),
                            v.get('modalidade'),
                            v.get('tipoDeOperacao'),
                            v.get('atualizaCusto'),
                            v.get('atualizaEstoque'),
                            v.get('geraFiscal'),
                            v.get('compoeABC'),
                            v.get('situacao'),
                            v.get('tipoDeGeracao'),
                            v.get('classificacao'),
                            v.get('baseDeCalculoDoICMS'),
                            v.get('valorDoICMS'),
                            v.get('baseDeCalculoDoICMSSubstituicaoTributaria'),
                            v.get('valorDoICMSSubstituicaoTributaria'),
                            v.get('valorDoIPI'),
                            v.get('valorDoFrete'),
                            v.get('valorDeOutrasDespesas'),
                            v.get('valorDoSeguro'),
                            v.get('valorDoDesconto'),
                            v.get('valorDoDAE'),
                            v.get('valorTotalDosItens'),
                            v.get('valorDoDocumento'),
                            v.get('valorDoPIS'),
                            v.get('valorDoCOFINS'),
                            v.get('valorDoICMSDesonerado'),
                            v.get('baseDeCalculoFecop'),
                            v.get('valorFecop'),
                            v.get('baseDeCalculoFecopSubstituicaoTributaria'),
                            v.get('valorFecopSubstituicaoTributaria'),
                            v1.get('id'),
                            v1.get('produtoId'),
                            v1.get('sequencial'),
                            v1.get('compoeTotalDaNota'),
                            v1.get('cfopId'),
                            v1.get('unidadeDeMedida'),
                            v1.get('quantidadeDeItensNaUnidade'),
                            v1.get('quantidade'),
                            v1.get('quantidadeCompleta'),
                            v1.get('valorDaEmbalagem'),
                            v1.get('tipoDeEntradaDesconto'),
                            v1.get('valorDoDescontoNaoTributado'),
                            v1.get('valorDoDescontoTributado'),
                            v1.get('percentualDoDesconto'),
                            v1.get('tipoDeEntradaFrete'),
                            v1.get('valorDoFrete'),
                            v1.get('percentualDoFrete'),
                            v1.get('tipoDeEntradaSeguro'),
                            v1.get('valorDoSeguro'),
                            v1.get('percentualDoSeguro'),
                            v1.get('tipoDeEntradaOutrasDespesas'),
                            v1.get('valorOutrasDespesas'),
                            v1.get('percentualOutrasDespesas'),
                            v1.get('tipoDeEntradaDAE'),
                            v1.get('valorDoDAE'),
                            v1.get('percentualDoDAE'),
                            v1.get('valorTotalDoItem'),
                            v1.get('situacaoFiscalId'),
                            v1.get('percentualTributado'),
                            v1.get('csosn'),
                            v1.get('custoReposicao'),
                            v1.get('custoMedio'),
                            v1.get('custoFiscal'),
                            v1.get('dataValidade'),
                            v1.get('tributacao'),
                            v1.get('numeroPedido'),
                            v1.get('sequencialItemPedido'),
                            v1.get('ncm'),
                            v1.get('cest'),
                            v1.get('aliquotaNacional'),
                            v1.get('aliquotaImportado'),
                            v1.get('aliquotaEstadual'),
                            v1.get('modalidadeDaBaseDeCalculo'),
                            v1.get('percentualICMSDeCompra'),
                            v1.get('valorDoICMS'),
                            v1.get('baseDeCalculoDoICMS'),
                            v1.get('baseDeCalculoDoICMSComSubstituicaoTributaria'),
                            v1.get('aliquotaDoICMSComSubstituicaoTributaria'),
                            v1.get('valorDoICMSComSubstituicaoTributaria'),
                            v1.get('percentualDeAgregacao'),
                            v1.get('percentualDeReducaoDASubstituicaoTributaria'),
                            v1.get('aliquotaDoICMS'),
                            v1.get('aliquotaDoICMSDeVenda'),
                            v1.get('aliquotaDoICMSAntecipado'),
                            v1.get('valorDoICMSAntecipado'),
                            v1.get('valorDoICMSNoSimples'),
                            v1.get('aliquotaNoSimples'),
                            v1.get('cstDoIPI'),
                            v1.get('baseDeCalculoDoIPI'),
                            v1.get('aliquotaDoIPI'),
                            v1.get('tipoDeEntradaIPI'),
                            v1.get('valorDoIPI'),
                            v1.get('percentualDoIPI'),
                            v1.get('cstDoPISId'),
                            v1.get('baseDeCalculoDoPIS'),
                            v1.get('aliquotaDoPIS'),
                            v1.get('valorDoPIS'),
                            v1.get('cstDoCOFINSId'),
                            v1.get('baseDeCalculoDoCOFINS'),
                            v1.get('aliquotaDoCOFINS'),
                            v1.get('valorDoCOFINS'),
                            v1.get('codigoNaturezaDeImpostoFederal'),
                            v1.get('baseDeCalculoDoFecop'),
                            v1.get('aliquotaDoFecop'),
                            v1.get('valorDoFecop'),
                            v1.get('baseDeCalculoDoFecopSubstituto'),
                            v1.get('aliquotaDoFecopSubstituto'),
                            v1.get('valorDoFecopSubstituto'),
                            v1.get('valorDoICMSDesonerado'),
                            v1.get('motivoDesoneracao'),
                            v1.get('codigoBeneficioFiscal'),
                            v1.get('percentualDiferimento'),
                            v1.get('valorICMSDiferimento')]
                        list.append(c)
                db.insertLote(sqlInsert, list)
                db.query(sqlDuplicidade)
                print(f'Dados Inseridos - {count}')
    except ValueError:
        print('Erro...')




def entradasExcCan(tipo=None):
    try:
        sqlInsert = 'set dateformat ymd insert into FatoCompras values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        sqlUltimoId = "select max(id) as id from FatoCompras "
        sqlExclusao = "select max(dataExclusao) id from fatocompras where situacao in ('EXCLUIDA', 'CANCELADA')"
        sqlUpdate = f'update fatocompras set situacao = ?, dataExclusao = ? where id = ?'

        limite = 1
        id = 0
        count = 0

        while (count < limite ):

            dtExclusao = db.getId(sqlExclusao)
            # param = {'count': 300, 'q': f"id=gt={id} ; situacao==EXCLUIDA , situacao==CANCELADA", 'sort': 'id'}
            param = {'count': 300, 'q': f"dataExclusao=gt={dtExclusao[0:19]}", 'sort': 'id'}
            dados = api.connect(url, param)

            if limite == 1:
                limite = dados['total']
            count = count + 300

            list = []
            for v in dados['items']:
                c = [
                    v.get('situacao'),
                    v.get('dataExclusao'),
                    v.get('id'),
                ]
                list.append(c)
            db.insertLote(sqlUpdate, list)
        print(f'Dados Inseridos - {count}')
    except ValueError:
        print('Erro...')



def execEntradas():
    print('Inicio...\n', datetime.now())
    print('\n Inserindo novas compras!!!')
    entradas()
    print('Fim...\n', datetime.now())

