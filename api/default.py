from fastapi import FastAPI, Query
from typing import Optional
from app.models.publication import Publication
from app.db.publication_dao import PublicationDAO
from app.db.dashboard_dao import DashboardDAO

app = FastAPI()

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


@app.get("/dashboard")
async def test():
    dash = DashboardDAO()
    count_types = await dash.get_total_types()
    
    
    return {
        'count_types': count_types
        }

"""

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))  # usa o valor do Render ou 8000 como fallback
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
    
    
    -no render -> python api/default.py --no do star command
"""