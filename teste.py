import ConnectApi as api, ConnectionFactory as db

import ContasPagar as cp
#with open('C:\\Users\\CPD02\\Documents\\Script contas a receber2.sql' , 'r', encoding="UTF-16") as f:
#    data = f.readlines()
count = 0
#for i in data:
 #   inicio = "SET IDENTITY_INSERT [dbo].[FatoContasAReceber] ON"
  #  fim = "SET IDENTITY_INSERT [dbo].[FatoContasAReceber] OFF"

   # if i[0:6] == 'INSERT':
    #    db.query( f"{inicio}; {i}; {fim}; ")




lista = [51053,	51218,	51502,	51727,	51735,	52694,	52975,	52986,	53037,	53040,	53083,	53320]


for i in lista:
    cp.getContasApagarId(i)
