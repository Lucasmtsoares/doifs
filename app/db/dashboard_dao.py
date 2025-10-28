# app/db/schema.py
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from bson import SON
from app.db.connection_db import Connection
import asyncio
from app.models.publication import Publication

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
    a = asyncio.run(test.get_top_responsibles())
    print(a)
    
    



#[{'date': '2025-06-30', 'nomeacoes': 1, 'exoneracoes': 0}, {'date': '2025-08-09', 'nomeacoes': 1, 'exoneracoes': 0}]
"""
const chartData = [
    {
        'director': 'Dr. João Silva de Oliveira Filho',
        'institute': 'IFAL',
        'director_institute': 'Dr. João S. de O. Filho - IFAL', 
        'total_acts': 987,
        'nomeacoes': 438,
        'exoneracoes': 549
    },
    {
        'director': 'Dra. Maria Aparecida Rodrigues',
        'institute': 'IFCE',
        'director_institute': 'Dra. M. Aparecida R. - IFCE',
        'total_acts': 852,
        'nomeacoes': 441,
        'exoneracoes': 411
    },
    {
        'director': 'Eng. Pedro Henrique Vasconcelos',
        'institute': 'IFSP',
        'director_institute': 'Eng. Pedro H. Vasconcelos - IFSP',
        'total_acts': 745,
        'nomeacoes': 418,
        'exoneracoes': 327
    },
    {
        'director': 'Prof. Ana Cláudia Pereira Santos',
        'institute': 'IFRJ',
        'director_institute': 'Prof. Ana C. P. Santos - IFRJ',
        'total_acts': 698,
        'nomeacoes': 280,
        'exoneracoes': 418
    },
    {
        'director': 'Sr. Carlos Alberto Guimarães',
        'institute': 'IFMG',
        'director_institute': 'Sr. Carlos A. Guimarães - IFMG',
        'total_acts': 610,
        'nomeacoes': 342,
        'exoneracoes': 268
    },
    {
        'director': 'Sra. Fernanda Lima Azevedo',
        'institute': 'IFRS',
        'director_institute': 'Sra. Fernanda L. Azevedo - IFRS',
        'total_acts': 550,
        'nomeacoes': 296,
        'exoneracoes': 254
    },
    {
        'director': 'Dr. Roberto Gomes da Costa',
        'institute': 'IFPE',
        'director_institute': 'Dr. Roberto G. da Costa - IFPE',
        'total_acts': 490,
        'nomeacoes': 229,
        'exoneracoes': 261
    },
    {
        'director': 'Msc. Patrícia Mendes Rocha',
        'institute': 'IFPR',
        'director_institute': 'Msc. Patrícia M. Rocha - IFPR',
        'total_acts': 412,
        'nomeacoes': 177,
        'exoneracoes': 235
    },
    {
        'director': 'Sr. José Paulo Medeiros',
        'institute': 'IFPB',
        'director_institute': 'Sr. José P. Medeiros - IFPB',
        'total_acts': 365,
        'nomeacoes': 215,
        'exoneracoes': 150
    },
    {
        'director': 'Dra. Juliana Ferreira Campos',
        'institute': 'IFTO',
        'director_institute': 'Dra. Juliana F. Campos - IFTO',
        'total_acts': 290,
        'nomeacoes': 135,
        'exoneracoes': 155
    }
];

"""