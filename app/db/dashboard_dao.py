# app/db/schema.py
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from bson import SON
from app.db.connection_db import Connection
import asyncio
from app.models.publication import Publication
import json 

class DashboardDAO:
    def __init__(self):
        connection = Connection()
        self.client = connection.connection()
        self.db = self.client['publications_dou']

    async def get_total_types(self):
        print("Buscando...")
        
        today = datetime.now()
        month_previous = today - timedelta(days=30) 
        
        # Otimização do pipeline
        pipeline = [
            {
                "$match": {
                    "type": {"$in": ["Nomeação", "Exoneração"]},
                    "$expr": {
                        "$and": [
                            {"$gte": [{"$toDate": "$date"}, month_previous]},
                            {"$lte": [{"$toDate": "$date"}, today]}
                        ]
                    }
                }
            },
            {
                "$group": {
                    "_id": None, # Agrupa todos os documentos em um único grupo
                    "nomeacoes": {
                        "$sum": {
                            "$cond": [{"$eq": ["$type", "Nomeação"]}, 1, 0]
                        }
                    },
                    "exoneracoes": {
                        "$sum": {
                            "$cond": [{"$eq": ["$type", "Exoneração"]}, 1, 0]
                        }
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "nomeacoes": "$nomeacoes",
                    "exoneracoes": "$exoneracoes"
                }
            }
        ]
        
        resultado = await self.db['IFAL'].aggregate(pipeline).to_list(None)
        
        self.close()
        
        # Retorna o primeiro (e único) item da lista, ou um padrão se vazio
        return resultado[0] if resultado else {"nomeacoes": 0, "exoneracoes": 0}   
    
    async def get_total_aggregate(self):
        print("Buscando resultados dos ultimos 90 dias...")
        
        today = datetime.now()
        month_previous_ = today - timedelta(days=90) 
        month_previous = month_previous_.strftime('%Y-%m-%d')
        #print("")
        #print(f"Saida date today > {today}")
        print(f"Saida date > {month_previous}")
        #print("")

        pipeline = [
           {
                "$match": {
                "type": { "$in": ["Nomeação", "Exoneração"] },
                "date": {"$gte": month_previous}
                }
            },
            {
                "$group": {
                "_id": {
                   "date": "$date",
                   "type": "$type"
                },
                "count": { "$sum": 1 }
                }
            },
            {
                "$group": {
                "_id": "$_id.date",
                "nomeacoes": {
                    "$sum": {
                    "$cond": [{ "$eq": ["$_id.type", "Nomeação"] }, "$count", 0]
                    }
                },
                "exoneracoes": {
                    "$sum": {
                    "$cond": [{ "$eq": ["$_id.type", "Exoneração"] }, "$count", 0]
                    }
                }
                }
            },
            {
                "$sort": { "_id": 1 }
            },
            {
                "$project": {
                "_id": 0,
                "date": "$_id",
                "nomeacoes": "$nomeacoes",
                "exoneracoes": "$exoneracoes"
                }
            }
        ]

    
        #total_type_period = {}
        resultado = await self.db["IFAL"].aggregate(pipeline).to_list(None)
        
        self.close() 
        return resultado

    async def get_total_types_period(self):
        print("Buscando...")
        
        try:
            # Gera a lista completa de dias com valores zerados
            types_bory = generate_days_dic(90) 
            
            # Obtém os dados agregados do MongoDB
            types_bory_aggregate = await self.get_total_aggregate()
            
            # Cria um dicionário para acesso rápido aos dados do MongoDB
            # A chave será a data e o valor será o dicionário completo do MongoDB
            datas_mongodb_dict = {item['date']: item for item in types_bory_aggregate}

            # Cria a lista final que será retornada
            result = []
            
            # Itera APENAS sobre a lista de dias zerados
            for days_set in types_bory:
                # Verifica se a data do dia zerado existe nos dados do MongoDB
                data_today = days_set['date']
                if data_today in datas_mongodb_dict:
                    # Se existir, adiciona o dicionário completo do MongoDB
                    result.append(datas_mongodb_dict[data_today])
                else:
                    # Se não existir, adiciona o dicionário com zeros
                    result.append(days_set)

            self.close()
            return result
        
        except TypeError as e:
            print(f"Error: {e}")
            return []
            
    async def get_top_responsibles(self):
        
        today = datetime.now()
        year_previous_ = today - relativedelta(years=1) 

        # Data formatada
        year_previous = year_previous_.strftime('%Y-%m-%d')
                
        pipeline = [
            {
                "$match": {
                    "type": { "$in": ["Nomeação", "Exoneração"] },
                    "date": {"$gt": year_previous}
                }
            },
            
            {
                "$project": {
                    "_id": 0,
                    "type": 1,
                    "institute": 1,
                    "responsible": 1
                }
            },
            
            {
                "$group": {
                    "_id": "$responsible",
                    "institute": { "$first": "$institute" },
                    "nomeacoes": {
                    "$sum": {
                    "$cond": [{ "$eq": ["$type", "Nomeação"] }, 1, 0]
                    }
                    },
                    "exoneracoes": {
                        "$sum": {
                        "$cond": [{ "$eq": ["$type", "Exoneração"] }, 1, 0]
                        }
                    },
                    "total": { "$sum": 1},
                        
                    }
            },
            {
                "$sort": { "total": -1 }
            },
            
            {
                "$project": {
                    "_id": 0,
                    "responsible": "$_id",
                    "institute": "$institute",
                    "responsible_institute": { "$concat": [ "$_id", " - ", "$institute" ] },
                    "total_acts": "$total",
                    "nomeacoes": "$nomeacoes",
                    "exoneracoes": "$exoneracoes",
                }
            }
            
        ]
        
        res = await self.db['IFAL'].aggregate(pipeline).to_list(10)
        self.close()
        return res
       
    async def get_overview_institutes(self):
        
        pipeline = [
                {
                "$addFields": {
                # Cria um campo numérico para ordenar o mês corretamente (necessário!)
                "month_num": {
                    "$switch": {
                    "branches": [
                        { "case": { "$eq": ["$month", "Jan"] }, "then": 1 },
                        { "case": { "$eq": ["$month", "Fev"] }, "then": 2 },
                        { "case": { "$eq": ["$month", "Mar"] }, "then": 3 },
                        { "case": { "$eq": ["$month", "Abr"] }, "then": 4 },
                        { "case": { "$eq": ["$month", "Mai"] }, "then": 5 },
                        { "case": { "$eq": ["$month", "Jun"] }, "then": 6 },
                        { "case": { "$eq": ["$month", "Jul"] }, "then": 7 },
                        { "case": { "$eq": ["$month", "Ago"] }, "then": 8 },
                        { "case": { "$eq": ["$month", "Set"] }, "then": 9 },
                        { "case": { "$eq": ["$month", "Out"] }, "then": 10 },
                        { "case": { "$eq": ["$month", "Nov"] }, "then": 11 },
                        { "case": { "$eq": ["$month", "Dez"] }, "then": 12 }
                    ],
                    "default": 0
                    }
                }
                }
            },
            {
                # Opcional: Filtra por um intervalo de anos se a coleção for muito grande
                "$match": {
                "institute": { "$in": ["IFAC", "IFAL", "IFAP", "IFAM", "IFBA", "IF Baiano", "IFCE", "IFB",
                                      "IFG", "IF Goiano", "IFES", "IFMA", "IFMG", "IFNMG", "IF Sudeste MG",
                                       "IFSULDEMINAS", "IFTM", "IFMT", "IFMS", "IFPA", "IFPB", "IFPE",
                                       "IF Sertão PE", "IFPI", "IFPR", "IFRJ", "IFF", "IFRN", "IFRS",
                                       "IFFar", "IFSUL", "IFRO", "IFRR", "IFSC", "IFC", "IFSP", "IFS", "IFTO"] }, # Exemplo: restringe a dois institutos
                # year: { $gte: 2020 }
                }
            },

            # Estágio 2: Agrupamento Mensal e Contagem
            {
                "$group": {
                #Chave de Agrupamento: Instituto, Ano e Mês (numérico para ordenação)
                "_id": {
                    "institute": "$institute",
                    "year": "$year",
                    "month": "$month",
                    "month_num": "$month_num"
                },
                
                # Acumulador de Nomeações
                "nomeacoes": {
                    "$sum": {
                    "$cond": [{ "$eq": ["$type", "Nomeação"] }, 1, 0]
                    }
                },
                
                # Acumulador de Exonerações
                "exoneracoes": {
                    "$sum": {
                    "$cond": [{ "$eq": ["$type", "Exoneração"] }, 1, 0]
                    }
                }
                }
            },

                # Estágio 3: Ordenação Final (Obrigatório para a série temporal)
            {
                "$sort": {
                "_id.institute": 1,
                "_id.year": 1,
                "_id.month_num": 1 # Garante a ordem Jan, Fev, Mar...
                }
            },

            # Estágio 4: Projeção Final (Formato da Saída)
            {
                "$project": {
                "_id": 0,
                "institute": "$_id.institute",
                "year": "$_id.year",
                "month": "$_id.month",
                "nomeacoes": "$nomeacoes",
                "exoneracoes": "$exoneracoes"
                }
            }
        ]
        
        res = await self.db['IFAL'].aggregate(pipeline).to_list(None)
        
        print("Teste")
        self.close()
        return res 
        
    async def get_publications_last(self):
        print("Útima publicação")
        res = await self.db['IFAL'].find({}, {"_id": 0, "institute": 1, "type": 1, "date": 1}).sort("date", -1).limit(1).to_list(1)
        self.close()
        return res
        
    async def get_total_publications(self):
        print("Total de publicações...")
        res = await self.db['IFAL'].count_documents({})
        self.close()
        return res
      
      
    def close(self):
        print("Fechando conexão...")
        self.client.close()
        
    
        

# execução

def generate_days_dic(todays_last=90):
    today = datetime.now()
    months = []
    
    for i in range(todays_last, 0, -1):
        date = today - timedelta(days=i)
        date_formate = date.strftime('%Y-%m-%d')
        months.append({
            "date": date_formate,
            "nomeacoes": 0,
            "exoneracoes": 0
        })
    return months




if __name__ == "__main__":
    test = DashboardDAO()
    a = asyncio.run(test.get_publications_last())
    print(a)
    
    



#[{'date': '2025-06-30', 'nomeacoes': 1, 'exoneracoes': 0}, {'date': '2025-08-09', 'nomeacoes': 1, 'exoneracoes': 0}]


"""
    { "region": "norte",        "name": "Norte",        "nomeacoes": 3500,  "exoneracoes": 800 },
    { "region": "nordeste",     "name": "Nordeste",     "nomeacoes": 8000,  "exoneracoes": 2000 },
    { "region": "centro_oeste", "name": "Centro-Oeste", "nomeacoes": 4500,  "exoneracoes": 1200 },
    { "region": "sudeste",      "name": "Sudeste",      "nomeacoes": 10500, "exoneracoes": 2800 },
    { "region": "sul",          "name": "Sul",          "nomeacoes": 5500,  "exoneracoes": 1500 }

"""

"""

    { "uf": "SP", "state_name": "São Paulo", "nomeacoes": 98, "exoneracoes": 24, "year": 2025 },
    { "uf": "MG", "state_name": "Minas Gerais", "nomeacoes": 85, "exoneracoes": 21, "year": 2025 },
    { "uf": "RJ", "state_name": "Rio de Janeiro", "nomeacoes": 79, "exoneracoes": 19, "year": 2025 },
    { "uf": "ES", "state_name": "Espírito Santo", "nomeacoes": 45, "exoneracoes": 12, "year": 2025 },
    
    // Sul
    { "uf": "PR", "state_name": "Paraná", "nomeacoes": 70, "exoneracoes": 17, "year": 2025 },
    { "uf": "RS", "state_name": "Rio Grande do Sul", "nomeacoes": 65, "exoneracoes": 16, "year": 2025 },
    { "uf": "SC", "state_name": "Santa Catarina", "nomeacoes": 55, "exoneracoes": 14, "year": 2025 },
    
    // Nordeste
    { "uf": "BA", "state_name": "Bahia", "nomeacoes": 50, "exoneracoes": 13, "year": 2025 },
    { "uf": "PE", "state_name": "Pernambuco", "nomeacoes": 48, "exoneracoes": 12, "year": 2025 },
    { "uf": "CE", "state_name": "Ceará", "nomeacoes": 46, "exoneracoes": 11, "year": 2025 },
    { "uf": "MA", "state_name": "Maranhão", "nomeacoes": 38, "exoneracoes": 9, "year": 2025 },
    { "uf": "RN", "state_name": "Rio Grande do Norte", "nomeacoes": 35, "exoneracoes": 8, "year": 2025 },
    { "uf": "PB", "state_name": "Paraíba", "nomeacoes": 33, "exoneracoes": 8, "year": 2025 },
    { "uf": "PI", "state_name": "Piauí", "nomeacoes": 28, "exoneracoes": 7, "year": 2025 },
    { "uf": "AL", "state_name": "Alagoas", "nomeacoes": 26, "exoneracoes": 6, "year": 2025 },
    { "uf": "SE", "state_name": "Sergipe", "nomeacoes": 24, "exoneracoes": 6, "year": 2025 },
    
    // Centro-Oeste
    { "uf": "GO", "state_name": "Goiás", "nomeacoes": 60, "exoneracoes": 15, "year": 2025 },
    { "uf": "DF", "state_name": "Distrito Federal", "nomeacoes": 58, "exoneracoes": 14, "year": 2025 },
    { "uf": "MT", "state_name": "Mato Grosso", "nomeacoes": 52, "exoneracoes": 13, "year": 2025 },
    { "uf": "MS", "state_name": "Mato Grosso do Sul", "nomeacoes": 40, "exoneracoes": 10, "year": 2025 },
    
    // Norte
    { "uf": "PA", "state_name": "Pará", "nomeacoes": 42, "exoneracoes": 10, "year": 2025 },
    { "uf": "AM", "state_name": "Amazonas", "nomeacoes": 37, "exoneracoes": 9, "year": 2025 },
    { "uf": "RO", "state_name": "Rondônia", "nomeacoes": 30, "exoneracoes": 7, "year": 2025 },
    { "uf": "TO", "state_name": "Tocantins", "nomeacoes": 29, "exoneracoes": 7, "year": 2025 },
    { "uf": "AC", "state_name": "Acre", "nomeacoes": 22, "exoneracoes": 5, "year": 2025 },
    { "uf": "AP", "state_name": "Amapá", "nomeacoes": 21, "exoneracoes": 5, "year": 2025 },
    { "uf": "RR", "state_name": "Roraima", "nomeacoes": 19, "exoneracoes": 4, "year": 2025 },

    // =========================================================================
    // --- DADOS DO ANO 2024 (Escala de Dezenas) ---
    // =========================================================================
    // (Valores ligeiramente menores que 2025)
    // Sudeste
    { "uf": "SP", "state_name": "São Paulo", "nomeacoes": 90, "exoneracoes": 20, "year": 2024 },
    { "uf": "MG", "state_name": "Minas Gerais", "nomeacoes": 78, "exoneracoes": 18, "year": 2024 },
    { "uf": "RJ", "state_name": "Rio de Janeiro", "nomeacoes": 70, "exoneracoes": 16, "year": 2024 },
    { "uf": "ES", "state_name": "Espírito Santo", "nomeacoes": 40, "exoneracoes": 10, "year": 2024 },
    
    // Sul
    { "uf": "PR", "state_name": "Paraná", "nomeacoes": 65, "exoneracoes": 15, "year": 2024 },
    { "uf": "RS", "state_name": "Rio Grande do Sul", "nomeacoes": 60, "exoneracoes": 14, "year": 2024 },
    { "uf": "SC", "state_name": "Santa Catarina", "nomeacoes": 50, "exoneracoes": 12, "year": 2024 },
    
    // Nordeste
    { "uf": "BA", "state_name": "Bahia", "nomeacoes": 45, "exoneracoes": 11, "year": 2024 },
    { "uf": "PE", "state_name": "Pernambuco", "nomeacoes": 43, "exoneracoes": 10, "year": 2024 },
    { "uf": "CE", "state_name": "Ceará", "nomeacoes": 41, "exoneracoes": 9, "year": 2024 },
    { "uf": "MA", "state_name": "Maranhão", "nomeacoes": 34, "exoneracoes": 8, "year": 2024 },
    { "uf": "RN", "state_name": "Rio Grande do Norte", "nomeacoes": 30, "exoneracoes": 7, "year": 2024 },
    { "uf": "PB", "state_name": "Paraíba", "nomeacoes": 28, "exoneracoes": 6, "year": 2024 },
    { "uf": "PI", "state_name": "Piauí", "nomeacoes": 24, "exoneracoes": 6, "year": 2024 },
    { "uf": "AL", "state_name": "Alagoas", "nomeacoes": 22, "exoneracoes": 5, "year": 2024 },
    { "uf": "SE", "state_name": "Sergipe", "nomeacoes": 20, "exoneracoes": 5, "year": 2024 },
    
    // Centro-Oeste
    { "uf": "GO", "state_name": "Goiás", "nomeacoes": 55, "exoneracoes": 13, "year": 2024 },
    { "uf": "DF", "state_name": "Distrito Federal", "nomeacoes": 53, "exoneracoes": 12, "year": 2024 },
    { "uf": "MT", "state_name": "Mato Grosso", "nomeacoes": 47, "exoneracoes": 11, "year": 2024 },
    { "uf": "MS", "state_name": "Mato Grosso do Sul", "nomeacoes": 35, "exoneracoes": 9, "year": 2024 },
    
    // Norte
    { "uf": "PA", "state_name": "Pará", "nomeacoes": 38, "exoneracoes": 9, "year": 2024 },
    { "uf": "AM", "state_name": "Amazonas", "nomeacoes": 33, "exoneracoes": 8, "year": 2024 },
    { "uf": "RO", "state_name": "Rondônia", "nomeacoes": 26, "exoneracoes": 6, "year": 2024 },
    { "uf": "TO", "state_name": "Tocantins", "nomeacoes": 25, "exoneracoes": 6, "year": 2024 },
    { "uf": "AC", "state_name": "Acre", "nomeacoes": 18, "exoneracoes": 4, "year": 2024 },
    { "uf": "AP", "state_name": "Amapá", "nomeacoes": 17, "exoneracoes": 4, "year": 2024 },
    { "uf": "RR", "state_name": "Roraima", "nomeacoes": 15, "exoneracoes": 3, "year": 2024 },

"""




"""

"IFAC", "IFAL", "IFAP", "IFAM", "IFBA", "IF Baiano", "IFCE", "IFB",
  "IFG", "IF Goiano", "IFES", "IFMA", "IFMG", "IFNMG", "IF Sudeste MG",
  "IFSULDEMINAS", "IFTM", "IFMT", "IFMS", "IFPA", "IFPB", "IFPE",
  "IF Sertão PE", "IFPI", "IFPR", "IFRJ", "IFF", "IFRN", "IFRS",
  "IFFar", "IFSUL", "IFRO", "IFRR", "IFSC", "IFC", "IFSP", "IFS", "IFTO"

"""