from motor.motor_asyncio import AsyncIOMotorClient
import asyncio

# Conectando ao MongoDB Atlas
client = AsyncIOMotorClient("mongodb+srv://doifaplicacao:65RfPHZHWrZEtf9n@cluster0.lxzu3pv.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["publications_dou"]

# Lista com os nomes antigos
colecoes_antigas = [
    "IF Baiano", "IF Goiano", "IF Sertão PE", "IF Sudeste MG",
    "IFAC", "IFAL", "IFAM", "IFAP", "IFB", "IFBA", "IFC", "IFCE",
    "IFES", "IFF", "IFFar", "IFG", "IFMA", "IFMG", "IFMS", "IFMT",
    "IFNMG", "IFPA", "IFPB", "IFPE", "IFPI", "IFPR", "IFRJ", "IFRN",
    "IFRO", "IFRR", "IFRS", "IFS", "IFSC", "IFSP", "IFSUL",
    "IFSULDEMINAS", "IFMT", "IFTO"
]

async def renomear_colecoes():
    for nome_antigo in colecoes_antigas:
        nome_novo = nome_antigo.replace(" ", "_") + "_flat"
        
        colecao_antiga = db[nome_antigo]
        colecao_nova = db[nome_novo]

        # Copiar documentos
        documentos = colecao_antiga.find()
        async for doc in documentos:
            await colecao_nova.insert_one(doc)

        # Apagar a coleção antiga (se quiser)
        # await colecao_antiga.drop()

        print(f"Renomeado: {nome_antigo} -> {nome_novo}")

asyncio.run(renomear_colecoes())
client.close()