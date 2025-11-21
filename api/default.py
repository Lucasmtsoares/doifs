from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query
from typing import AsyncGenerator, Optional
from app.models.publication import Publication
from app.db.publication_dao import PublicationDAO
from app.db.dashboard_dao import DashboardDAO, DB_NAME

#Importações de config do database
from app.db.connection_db import mongo_client_manager

from api.controllers.controller import (
    PublicationController,
    PeriodicController,
    PersonnelController,
    InstituteController,
    RegionController,
    StatesController
)

#Função declarativa de tempo de vida
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    print("Iniciando server e API...")
    #Startup
    mongo_client = mongo_client_manager.connection()
    
    try:
        await mongo_client.admin.command('ping')
        db = mongo_client[DB_NAME]
        
        dashboard_dao = DashboardDAO(db)
        
        # 2. Inicializa TODOS os Controllers, INJETANDO o DAO
        app.state.publication_controller = PublicationController(dashboard_dao)
        app.state.periodic_controller = PeriodicController(dashboard_dao)
        app.state.personnel_controller = PersonnelController(dashboard_dao)
        app.state.institute_controller = InstituteController(dashboard_dao)
        app.state.region_controller = RegionController(dashboard_dao)
        app.state.state_controller = StatesController(dashboard_dao)
        
        app.state.db_client = mongo_client
        
        print(f"Serviços da API e DB ({DB_NAME}) inicializados com sucesso")
    except Exception as e:
        print(f"Erro crítico no startup do DB: ({e}). A API pode estar indisponível")
        app.state.db_client = None
        app.state.publication_controller = None
    yield
    
    #shutdown
    mongo_client_manager.close()
    
    
#Inicializando a FastAPI com LIFESPAN    
app = FastAPI(title = "DOIFs API", lifespan=lifespan)

#função de checagem de disponibilidade
def get_controller(controller_name: str):
    #Retorna o controller ou erro 503 se o serviço não estiver pronto
    controller = getattr(app.state, controller_name, None)
    if not controller:
        raise HTTPException(status_code=503, detail="Serviços de database indisponíveis")
    return controller

#Rotas

@app.get("/buscar")
async def get_publication(
    name: Optional[str] = Query(None),
    institute: Optional[str] = Query(None),
    type: Optional[str] = Query(None),
    year: Optional[int] = Query(None)
):
    publication = Publication(name, institute, type, year)
    dao = PublicationDAO()
    result = await dao.get_publication(publication)
    return {"publications": result}


@app.get("/get-totals")
async def get_totals():
    controller = get_controller('publication_controller')
    return await controller.get_totals_controller()

@app.get("/periodic-types")
async def get_periodic_types_data():
    controller = get_controller('periodic_controller')
    return await controller.get_periodic_type_controller()
   

@app.get("institutes-overview")
async def get_institutes_overview_data():
    controller = get_controller('institute_controller')
    return await controller.get_institutes_overview_controller()


@app.get("/top-personnel")
async def get_top_personnel_data():
    controller = get_controller('personnel_controller')
    return await controller.get_top_personnel_contrller()

@app.get("/region-totals")
async def get_region_totals_data():
    controller = get_controller('region_controller')
    return await controller.get_region_totals_controller()

@app.get("/states-totals")
async def get_state_totals_data():
    controller = get_controller('state_controller')
    return await controller.get_state_totals_controller()



"""

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))  # usa o valor do Render ou 8000 como fallback
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
    
    
    -no render -> python api/default.py --no do star command
"""