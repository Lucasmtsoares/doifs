# app/db/schema.py
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from bson import SON
from app.db.connection_db import Connection
import asyncio
from app.models.publication import Publication
import json 
from motor.motor_asyncio import AsyncIOMotorDatabase, AsyncIOMotorCollection

DB_NAME = "publications_dou"
COLLETION_NAME = "IFAL"

class DashboardDAO:
    def __init__(self, db: AsyncIOMotorDatabase):
        #connection = Connection()
        #self.client = connection.connection()
        #self.db = self.client['publications_dou']
        self.colletion: AsyncIOMotorCollection = db[COLLETION_NAME]

    async def get_type_counts(self):
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
        
        resultado = await self.colletion.aggregate(pipeline).to_list(None)
        
        # Retorna o primeiro (e único) item da lista, ou um padrão se vazio
        return resultado[0] if resultado else {"nomeacoes": 0, "exoneracoes": 0}   
    
    async def get_overall_summary(self):
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
        resultado = await self.colletion.aggregate(pipeline).to_list(None)
        
        return resultado

    async def get_periodic_type_counts(self):
        print("Buscando...")
        
        try:
            # Gera a lista completa de dias com valores zerados
            types_bory = generate_days_dic(90) 
            
            # Obtém os dados agregados do MongoDB
            types_bory_aggregate = await self.get_overall_summary()
            
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

            return result
        
        except TypeError as e:
            print(f"Error: {e}")
            return []
            
    async def get_top_personnel(self):
        
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
        
        res = await self.colletion.aggregate(pipeline).to_list(10)
        
        return res
       
    async def get_institutes_overview(self):
        
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
        
        res = await self.colletion.aggregate(pipeline).to_list(None)
        
        print("Teste")
        return res 
        
    async def get_latest_publications(self):
        print("Útima publicação")
        res = await self.colletion.find({}, {"_id": 0, "institute": 1, "type": 1, "date": 1}).sort("date", -1).limit(1).to_list(1)
        return res
        
    async def get_publication_count(self):
        print("Total de publicações...")
        res = await self.colletion.count_documents({})
        return res
    
    async def get_region_totals(self):
        print("Teste...jjjj")
        
        today = datetime.now()
        year_previous_ = today - relativedelta(years=1) 

        # Data formatada
        year_previous = year_previous_.strftime('%Y-%m-%d')
        
        pipeline = [
            {
                "$addFields": {
                    "region_name": {
                        "$switch": {
                            "branches": [
                                {"case": {"$in": ["$institute", ["IFAC", "IFAP", "IFAM", "IFPA", "IFRO", "IFRR", "IFTO"]]}, "then": "Norte"},
                                {"case": {"$in": ["$institute", ["IFAL", "IFBA", "IF Baiano", "IFCE", "IFMA", "IFPB", "IFPE", "IF Sertão PE", "IFPI", "IFRN", "IFS"]]}, "then": "Nordeste"},
                                {"case": {"$in": ["$institute", ["IFB", "IFG", "IF Goiano", "IFMT", "IFMS"]]}, "then": "Centro-Oeste"},
                                {"case": {"$in": ["$institute", ["IFES", "IFMG", "IFNMG", "IF Sudeste MG", "IFSULDEMINAS", "IFTM", "IFRJ", "IFF", "IFSP"]]}, "then": "Sudeste"},
                                {"case": {"$in": ["$institute", ["IFPR", "IFRS", "IFFar", "IFSUL", "IFSC", "IFC"]]}, "then": "Sul"},
                            ],
                            "default": "Outros"
                        }
                    }
                }
            },
            
            {
                "$match": {
                    "region_name": {"$ne": "Outros"},
                    "date": {"$gt": year_previous}
                }
            },
            
            {
                "$group": {
                    "_id": "$region_name",
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
                "$sort": {
                    "_id": 1
                }
            },
            
            {
                "$project": {
                    "_id": 0,
                    "name": "$_id",
                    "region": {"$toLower": "$_id"},
                    "nomeacoes": "$nomeacoes",
                    "exoneracoes": "$exoneracoes"
                }
            }
        ]
        
        res = await self.colletion.aggregate(pipeline).to_list(None)
        return res
  
    async def get_state_totals(self):
        print("Teste...")
        
        pipeline = [
            {
                "$addFields": {
                    "state_info": {
                        "$switch": {
                            "branches": [
                                {"case": {"$eq": ["$institute", "IFAC"]}, "then": {"uf": "AC", "state_name": "Acre"}},
                                {"case": {"$eq": ["$institute", "IFAL"]}, "then": {"uf": "AL", "state_name": "Alagoas"}},                             
                                {"case": {"$eq": ["$institute", "IFAP"]}, "then": {"uf": "AP", "state_name": "Amapá"}},                            
                                {"case": {"$eq": ["$institute", "IFAM"]}, "then": {"uf": "AM", "state_name": "Amazonas"}},                            
                                {"case": {"$eq": ["$institute", "IFBA"]}, "then": {"uf": "BA", "state_name": "Bahia"}},                      
                                {"case": {"$eq": ["$institute", "IF Baiano"]}, "then": {"uf": "BA", "state_name": "Bahia"}},                         
                                {"case": {"$eq": ["$institute", "IFCE"]}, "then": {"uf": "CE", "state_name": "Ceará"}},                          
                                {"case": {"$eq": ["$institute", "IFB"]}, "then": {"uf": "DF", "state_name": "Distrito Federal"}},                          
                                {"case": {"$eq": ["$institute", "IFES"]}, "then": {"uf": "ES", "state_name": "Espírito Santo"}},                           
                                {"case": {"$eq": ["$institute", "IFG"]}, "then": {"uf": "GO", "state_name": "Goiás"}},                            
                                {"case": {"$eq": ["$institute", "IF Goiano"]}, "then": {"uf": "GO", "state_name": "Goiás"}},                            
                                {"case": {"$eq": ["$institute", "IFMA"]}, "then": {"uf": "MA", "state_name": "Maranhão"}},                            
                                {"case": {"$eq": ["$institute", "IFMT"]}, "then": {"uf": "MT", "state_name": "Mato Grosso"}},   
                                {"case": {"$eq": ["$institute", "IFMS"]}, "then": {"uf": "MS", "state_name": "Mato Grosso do Sul"}}, 
                                {"case": {"$eq": ["$institute", "IFMG"]}, "then": {"uf": "MG", "state_name": "Minas Gerais"}},   
                                {"case": {"$eq": ["$institute", "IFNMG"]}, "then": {"uf": "MG", "state_name": "Minas Gerais"}},   
                                {"case": {"$eq": ["$institute", "IFSULDEMINAS"]}, "then": {"uf": "MG", "state_name": "Minas Gerais"}},   
                                {"case": {"$eq": ["$institute", "IF Sudeste MG"]}, "then": {"uf": "MG", "state_name": "Minas Gerais"}},   
                                {"case": {"$eq": ["$institute", "IFTM"]}, "then": {"uf": "MG", "state_name": "Minas Gerais"}},   
                                {"case": {"$eq": ["$institute", "IFPA"]}, "then": {"uf": "PA", "state_name": "Pará"}},   
                                {"case": {"$eq": ["$institute", "IFPB"]}, "then": {"uf": "PB", "state_name": "Paraíba"}},   
                                {"case": {"$eq": ["$institute", "IFPR"]}, "then": {"uf": "PR", "state_name": "Paraná"}},   
                                {"case": {"$eq": ["$institute", "IFPE"]}, "then": {"uf": "PE", "state_name": "Pernambuco"}},   
                                {"case": {"$eq": ["$institute", "IF Sertão PE"]}, "then": {"uf": "PE", "state_name": "Pernambuco"}},   
                                {"case": {"$eq": ["$institute", "IFPI"]}, "then": {"uf": "PI", "state_name": "Piauí"}},
   
                                {"case": {"$eq": ["$institute", "IFFluminense"]}, "then": {"uf": "RJ", "state_name": "Rio de Janeiro"}},
                                
                                {"case": {"$eq": ["$institute", "IFRJ"]}, "then": {"uf": "RJ", "state_name": "Rio de Janeiro"}},
                                
                                {"case": {"$eq": ["$institute", "IFRN"]}, "then": {"uf": "RN", "state_name": "Rio Grande do Norte"}},
                                
                                {"case": {"$eq": ["$institute", "IFFarroupilha"]}, "then": {"uf": "RS", "state_name": "Rio Grande do Sul"}},
                                
                                {"case": {"$eq": ["$institute", "IFRS"]}, "then": {"uf": "RS", "state_name": "Rio Grande do Sul"}},
                                
                                {"case": {"$eq": ["$institute", "IFSul"]}, "then": {"uf": "RS", "state_name": "Rio Grande do Sul"}},
                                
                                {"case": {"$eq": ["$institute", "IFRO"]}, "then": {"uf": "RO", "state_name": "Rondônia"}},
                                
                                {"case": {"$eq": ["$institute", "IFRR"]}, "then": {"uf": "RR", "state_name": "Roraima"}},
                                
                                {"case": {"$eq": ["$institute", "IFC"]}, "then": {"uf": "SC", "state_name": "Santa Catarina"}},
                                
                                {"case": {"$eq": ["$institute", "IFSC"]}, "then": {"uf": "SC", "state_name": "Santa Catarina"}},
                                
                                {"case": {"$eq": ["$institute", "IFSP"]}, "then": {"uf": "SP", "state_name": "São Paulo"}},
                                
                                {"case": {"$eq": ["$institute", "IFS"]}, "then": {"uf": "SE", "state_name": "Sergipe"}},
                                
                                {"case": {"$eq": ["$institute", "IFTO"]}, "then": {"uf": "TO", "state_name": "Tocantins"}},
                            ],
                            "default": "Outros"
                        }
                    }
                }
            },
            
            {
                "$match": {
                    "state_info": {"$ne": "Outros"}
                }
            },
            
            {
                "$group": {
                    "_id": {
                        "year": "$year",
                        "uf": "$state_info.uf",
                        "state_name": "$state_info.state_name"
                    },
                    
                    "nomeacoes": {
                        "$sum": {
                            "$cond": [{"$eq": ["$type", "Nomeação"]}, 1, 0]
                        }
                    },
                    "exoneracoes": {
                        "$sum": {
                            "$cond": [{"$eq": ["$type", "Exoneração"]}, 1, 0]
                        }
                    },
                   
                }
            },
            
            {
                "$sort": {
                    "_id.year": 1,
                    "_id.uf": 1
                }
            },
            
            {
                "$project": {
                    "_id": 0,
                    "uf": "$_id.uf",
                    "state_name": "$_id.state_name",
                    "year": "$_id.year",
                    "nomeacoes": "$nomeacoes",  
                    "exoneracoes": "$exoneracoes",
                }
            }
        ]
        
        res = await self.colletion.aggregate(pipeline).to_list(None)
        return res
    
    async def get_available_years(self):
        print("Oiii")
        
        pipeline = [
            {
                "$group": {
                    "_id": "$year",
                }
            }, 
            
            {
                "$sort": {
                    "_id": 1
                }
            },
            
            {
                "$group": {
                    "_id":None,
                    "single_years": { "$push": "$_id" }
                }
            },
            
            {
                "$project": {
                    "_id": 0,
                    "years": "$single_years"
                }
            }
                
        ]
        
        res = await self.colletion.aggregate(pipeline).to_list(None)
        return res

        
    
        

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




"""if __name__ == "__main__":
    test = DashboardDAO()
    a = asyncio.run(test.get())
    print(a)"""
    
    



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

"""
{'state_name': 'Acre', 'nomeacoes': 3, 'exoneracoes': 0}, 
{'state_name': 'Alagoas', 'nomeacoes': 544, 'exoneracoes': 213}, 
{'state_name': 'Amazonas', 'nomeacoes': 0, 'exoneracoes': 1}, 
{'state_name': 'Bahia', 'nomeacoes': 1, 'exoneracoes': 0}, 
{'state_name': 'Ceará', 'nomeacoes': 1, 'exoneracoes': 0}, 
{'state_name': 'Distrito Federal', 'nomeacoes': 2, 'exoneracoes': 0}, 
{'state_name': 'Espírito Santo', 'nomeacoes': 1, 'exoneracoes': 1}, 
{'state_name': 'Goiás', 'nomeacoes': 2, 'exoneracoes': 1}, 
{'state_name': 'Maranhão', 'nomeacoes': 1, 'exoneracoes': 0}, 
{'state_name': 'Mato Grosso', 'nomeacoes': 2, 'exoneracoes': 2}, 
{'state_name': 'Mato Grosso do Sul', 'nomeacoes': 2, 'exoneracoes': 0}, 
{'state_name': 'Minas Gerais', 'nomeacoes': 7, 'exoneracoes': 1}, 
{'state_name': 'Paraná', 'nomeacoes': 1, 'exoneracoes': 3}, 
{'state_name': 'Paraíba', 'nomeacoes': 3, 'exoneracoes': 0}, 
{'state_name': 'Pará', 'nomeacoes': 2, 'exoneracoes': 0}, 
{'state_name': 'Pernambuco', 'nomeacoes': 2, 'exoneracoes': 0}, 
{'state_name': 'Piauí', 'nomeacoes': 1, 'exoneracoes': 0}, 
{'state_name': 'Rio Grande do Norte', 'nomeacoes': 1, 'exoneracoes': 0}, 
{'state_name': 'Rio Grande do Sul', 'nomeacoes': 5, 'exoneracoes': 1}, 
{'state_name': 'Rio de Janeiro', 'nomeacoes': 4, 'exoneracoes': 0}, 
{'state_name': 'Rondônia', 'nomeacoes': 1, 'exoneracoes': 0}, 
{'state_name': 'Roraima', 'nomeacoes': 1, 'exoneracoes': 0}, 
{'state_name': 'Santa Catarina', 'nomeacoes': 3, 'exoneracoes': 0}, 
{'state_name': 'Sergipe', 'nomeacoes': 1, 'exoneracoes': 0}, 
{'state_name': 'São Paulo', 'nomeacoes': 1, 'exoneracoes': 0}, 
{'state_name': 'Tocantins', 'nomeacoes': 1, 'exoneracoes': 0}


"""

"""


{'uf': 'AL', 'state_name': 'Alagoas',             'year': 2018, 'nomeacoes': 20, 'exoneracoes': 10}, 
{'uf': 'AM', 'state_name': 'Amazonas',            'year': 2018, 'nomeacoes': 0, 'exoneracoes': 1}, 
{'uf': 'BA', 'state_name': 'Bahia',               'year': 2018, 'nomeacoes': 1, 'exoneracoes': 0}, 
{'uf': 'CE', 'state_name': 'Ceará',               'year': 2018, 'nomeacoes': 1, 'exoneracoes': 0}, 
{'uf': 'DF', 'state_name': 'Distrito Federal',    'year': 2018, 'nomeacoes': 1, 'exoneracoes': 0}, 
{'uf': 'ES', 'state_name': 'Espírito Santo',      'year': 2018, 'nomeacoes': 1, 'exoneracoes': 1}, 
{'uf': 'GO', 'state_name': 'Goiás',               'year': 2018, 'nomeacoes': 2, 'exoneracoes': 1}, 
{'uf': 'MA', 'state_name': 'Maranhão',            'year': 2018, 'nomeacoes': 1, 'exoneracoes': 0}, 
{'uf': 'MG', 'state_name': 'Minas Gerais',        'year': 2018, 'nomeacoes': 6, 'exoneracoes': 1}, 
{'uf': 'MS', 'state_name': 'Mato Grosso do Sul',  'year': 2018, 'nomeacoes': 2, 'exoneracoes': 0}, 
{'uf': 'MT', 'state_name': 'Mato Grosso',         'year': 2018, 'nomeacoes': 2, 'exoneracoes': 2}, 
{'uf': 'PA', 'state_name': 'Pará',                'year': 2018, 'nomeacoes': 2, 'exoneracoes': 0}, 

{'uf': 'PB', 'state_name': 'Paraíba',             'year': 2018, 'nomeacoes': 2, 'exoneracoes': 0}, 
{'uf': 'PE', 'state_name': 'Pernambuco',          'year': 2018, 'nomeacoes': 2, 'exoneracoes': 0}, 
{'uf': 'PI', 'state_name': 'Piauí',               'year': 2018, 'nomeacoes': 1, 'exoneracoes': 0}, 
{'uf': 'PR', 'state_name': 'Paraná',              'year': 2018, 'nomeacoes': 1, 'exoneracoes': 3}, 
{'uf': 'RJ', 'state_name': 'Rio de Janeiro',      'year': 2018, 'nomeacoes': 4, 'exoneracoes': 0}, 
{'uf': 'RN', 'state_name': 'Rio Grande do Norte', 'year': 2018, 'nomeacoes': 1, 'exoneracoes': 0}, 
{'uf': 'RO', 'state_name': 'Rondônia',            'year': 2018, 'nomeacoes': 1, 'exoneracoes': 0}, 
{'uf': 'RR', 'state_name': 'Roraima',             'year': 2018, 'nomeacoes': 1, 'exoneracoes': 0}, 
{'uf': 'RS', 'state_name': 'Rio Grande do Sul',   'year': 2018, 'nomeacoes': 5, 'exoneracoes': 1}, 
{'uf': 'SC', 'state_name': 'Santa Catarina',      'year': 2018, 'nomeacoes': 3, 'exoneracoes': 0}, 
{'uf': 'SE', 'state_name': 'Sergipe',             'year': 2018, 'nomeacoes': 1, 'exoneracoes': 0}, 
{'uf': 'SP', 'state_name': 'São Paulo',           'year': 2018, 'nomeacoes': 1, 'exoneracoes': 0}, 
{'uf': 'TO', 'state_name': 'Tocantins',           'year': 2018, 'nomeacoes': 1, 'exoneracoes': 0}, 

{'uf': 'AL', 'state_name': 'Alagoas',             'year': 2019, 'nomeacoes': 120, 'exoneracoes': 27},
 
{'uf': 'AL', 'state_name': 'Alagoas',             'year': 2020, 'nomeacoes': 62, 'exoneracoes': 20}, 

{'uf': 'AL', 'state_name': 'Alagoas',             'year': 2021, 'nomeacoes': 77, 'exoneracoes': 29}, 

{'uf': 'AL', 'state_name': 'Alagoas',             'year': 2022, 'nomeacoes': 90, 'exoneracoes': 24}, 

{'uf': 'AL', 'state_name': 'Alagoas',             'year': 2023, 'nomeacoes': 144, 'exoneracoes': 73},
 
{'uf': 'AC', 'state_name': 'Acre',                'year': 2024, 'nomeacoes': 3, 'exoneracoes': 0}, 
{'uf': 'AL', 'state_name': 'Alagoas',             'year': 2024, 'nomeacoes': 29, 'exoneracoes': 29}, 
{'uf': 'DF', 'state_name': 'Distrito Federal',    'year': 2024, 'nomeacoes': 1, 'exoneracoes': 0}, 
{'uf': 'MG', 'state_name': 'Minas Gerais',        'year': 2024, 'nomeacoes': 1, 'exoneracoes': 0}, 

{'uf': 'AL', 'state_name': 'Alagoas',             'year': 2025, 'nomeacoes': 2, 'exoneracoes': 1}, 
{'uf': 'PB', 'state_name': 'Paraíba',             'year': 2025, 'nomeacoes': 1, 'exoneracoes': 0}

"""