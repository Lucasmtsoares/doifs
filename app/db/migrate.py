# Exemplo: convertendo documentos atuais para modelo flat

from pymongo import MongoClient
from datetime import datetime

client = MongoClient("mongodb+srv://doifaplicacao:65RfPHZHWrZEtf9n@cluster0.lxzu3pv.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")

db = client["publications_dou"]
collection_old = db["IFMS_flat"]
collection_new = db["IFMS"]

docs = collection_old.find()

for doc in docs:
    year = doc["year"]
    for month, entries in doc["months"].items():
        for item in entries:
            pub = item["publication"]
            # extrair dia do campo "date"
            date = datetime.strptime(pub["date"], "%d/%m/%Y")
            flat_doc = {
                "institute": "IFMS",
                "type": pub["type"],
                "orgao": pub["organ"],
                "content": pub["content"],
                "concierge": pub["concierge"],
                "date": date.strftime("%Y-%m-%d"),
                "year": date.year,
                "month": month,
                "day": date.day,
                "responsible": pub["responsible"],
                "url": pub["url"]
            }
            collection_new.insert_one(flat_doc)
client.close()


"""IF Baiano,
IF Goiano,
IF Sertão PE,
IF Sudeste MG,
IFAC,
IFAL,
IFAM,
IFAP,
IFB,
IFBA,
IFC,
IFCE,
IFES,
IFF,
IFFar,
IFG,
IFMA,
IFMG,
IFMS,
IFMT,
IFNMG,
IFPA,
IFPB,
IFPE,
IFPI,
IFPR,
IFRJ,
IFRN,
IFRO,
IFRR,
IFRS,
IFS,
IFSC,
IFSP,
IFSUL,
IFSULDEMINAS,
IFMT,
IFTO"""