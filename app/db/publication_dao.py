# app/db/schema.py
from app.db.connection_db import Connection
import asyncio
from app.models.publication import Publication

class PublicationDAO:
    def __init__(self):
        connection = Connection()
        self.client = connection.connection()
        self.db = self.client['publications_dou']

    async def get_publication(self, publication):
        print("Buscando...")
        
        #publication = Publicaton("REINALDO RAFAEL DE ALBUQUERQUE PEREIRA JUNIOR", "IFAL", type=None, year=None)
        
        filter = {
            **({"content": {"$regex": publication.name , "$options": "i"}} if publication.name else {}),
            **({"type": publication.type} if publication.type else {}),
            **({"year": publication.year} if publication.year else {})
        }
        if not publication.institute:
            return {"erro": "Instituto é obrigatório"}
        
        colletion = self.db.list_collections()
        print("SAIDA >>>> ", colletion)
        
        collection = self.db[publication.institute]
        cursor = collection.find(filter, {"_id": 0, "institute": 1, "type": 1, "concierge": 1, "date": 1, "url": 1})
        res = await cursor.to_list(length=100)
        
        #print(publication.name, publication.institute, publication.type, publication.year)
        
        print("Achei!!")
        print(res)
        self.close()
        return res

    def close(self):
        self.client.close()

# execução




"""
if __name__ == "__main__":
    test = PublicationDAO()
    asyncio.run(test.list())



_id: ObjectId('66bc177782c5dedf846094b4')
year: 2018
months: Object
    janeiro: Array (24)
        0: Object
            publication: Object
                type: "Nomeação"
                orgao: "Reitoria"
                content: "O reitor do Instituto absc..."
                concierge: "Portaria Nº 23, DE 4 JANEIRO DE 2018"
                date: "08/01/2018"
                responsible: "Carlos GUEDES DE LACERDA"
                url: "https://www.in.gov.br/web/dou/-/portaria-n-23-de-4-de-janeiro-de-2018-1652192"
        1: Object >
        2: Object >
        .
        .
        .

"""
