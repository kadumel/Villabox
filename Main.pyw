import sys
import TabAuxiliar as ta, Vendas as vd, Estoque as et

if __name__=='__main__':
    for i in sys.argv:
        if (i == 'a'): # Tabela Auxilares
            ta.processamentoTabAux()
        elif (i == 'v'): # Tabela de vendas
           vd.vendas();
        elif (i == 'e'): # Saldo do estoque
            et.processamentoEstoque()
            #et.updateDimData()
        elif (i == 't'): # Tudo
            ta.processamentoTabAux()
            et.processamentoEstoque()
            vd.vendas()
            #et.updateDimData()
