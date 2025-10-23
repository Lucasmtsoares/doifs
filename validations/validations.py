#name = "REINALDO RAFAEL DE ALBUQUERQUE PEREIRA JUNIOR"
institute = "IFAL"
type = "Nomeação"
year = 2018

class Validate:
    def __init__(self, name, institute, type, year):
        self.name = name
        self.institute = institute
        self.type = type
        self.year = year
        
    def validate(self):
        filtro = {}

        if self.name:
            filtro["content"] = {"$regex": self.name, "$options": "i"}
        if self.type:
            filtro["type"] = self.type
        if self.institute:
            collection = db[instituto]
        else:
            return {"erro": "Instituto é obrigatório"}
        if ano:
            filtro["year"] = ano

        resultado = await collection.find_one(filtro, {"_id": 0, "url": 1})