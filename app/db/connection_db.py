from typing import Optional
from motor.motor_asyncio import AsyncIOMotorClient

MONGODB_URI = "mongodb+srv://doifaplicacao:65RfPHZHWrZEtf9n@cluster0.lxzu3pv.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

class Connection:
    def __init__(self, uri: str):
        print("Iniciando conexão...")
        self.client: Optional[AsyncIOMotorClient] = None
        self.uri = uri
        
    def connection(self) -> AsyncIOMotorClient:
        
        if self.client is None:
            print("Conectando...")
            self.client = AsyncIOMotorClient(
                self.uri,
                tls=True,
                serverSelectionTimeoutMS=5000
            )
            return self.client
        
    def close(self):
        if self.client:
            print("Fechando conexão...")
            self.client.close()
            self.client = None
            
mongo_client_manager = Connection(MONGODB_URI)