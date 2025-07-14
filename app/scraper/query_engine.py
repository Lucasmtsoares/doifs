import asyncio
from dou_parser import run_scraping
from app.models.maps import INSTITUTES

async def extract_scraping():
    
    try:
        institute = await run_scraping()
        print(institute)
    except:
        print("Deu erro")
        
        
if __name__=="__main__":
    asyncio.run(extract_scraping())