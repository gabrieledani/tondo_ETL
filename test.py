import os

dir = os.getcwd()
dir_files = os.path.join(dir,'files')
print(dir_files)
dir_cad = os.path.join(dir_files,'cadastros')
print(dir_cad)

for filename in os.listdir(dir_cad):
    f = os.path.join(dir_cad, filename)
    # checking if it is a file
    if os.path.isfile(f) and f.endswith('.csv'):
        print(filename)


#valor com "?" trocar pra nulo

#embalagem.item-caixa.qt-item - tem virgula

#tratar cidade dentro da ETL de nota, pedido, orçamento a partir do arquivo de cidades (exportação completa do cadastro de cidades do TOTVS)

#nota-item: campos de quantidade de valor - retirar o ponto de separador de milhar e trocar a vírgula por ponto
#trocar ponto e vírgula no meio para # e trocar separador para ponto e virgula

#rsbi01: campo descrição db tem ponto-virgula, trocar para # e separador trocar para ponto e vírgula