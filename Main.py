import sys
import TabAuxiliar as ta, Vendas as vd, Estoque as et, Entradas as en, ContasReceber as cr, ContasPagar as cp, Balancete as b

if __name__=='__main__':
    for i in sys.argv:
        if (i == 'a'): # Tabela Auxilares
            ta.execTabAux()
        elif (i == 'v'): # Tabela de vendas
           vd.execVendas();
        elif (i == 'e'): # Saldo do estoque
            et.execEstoque()
        elif (i == 'en'): # Saldo do estoque
            et.execEntradas()
        elif (i == 'cr'):  # Contas a receber
            cr.execContasReceber()
        elif (i == 'cp'):  # Contas a pagar
            cp.execContasPagar()
        elif (i == 'b'):  # Balancete
            b.execBalancete()
        elif (i == 't'): # Tudo
            ta.execTabAux()
            en.execEntradas()
            et.execEstoque()
            vd.execVendas()
            cr.execContasReceber()
            cp.execContasPagar()
            b.execBalancete()



