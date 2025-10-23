from motor.motor_asyncio import AsyncIOMotorClient

uri = "mongodb+srv://doifaplicacao:65RfPHZHWrZEtf9n@cluster0.lxzu3pv.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

class Connection:
    def __init__(self):
        print("Iniciando conexão...")
        self.client = AsyncIOMotorClient(uri, tls=True)

    def connection(self):
        print("Conectado!!")
        return self.client