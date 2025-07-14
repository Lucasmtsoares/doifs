from app.models.maps import INSTITUTES

print("Funcionando!!")

# Função que processa os nomes de um instituto
def processa_instituto(nomes):
    for nome in nomes:
        print(f"Nome encontrado: {nome}")

# Iterando por todos os institutos do dicionário
for sigla, nomes in INSTITUTES.items():
    print(f"\nProcessando {sigla}:")
    processa_instituto(nomes)
