from app.db.dashboard_dao import DashboardDAO


class PublicationController:
    def __init__(self, dashboard_dao: DashboardDAO):
        self.dash = dashboard_dao
    
    async def get_totals_controller(self):
        #retorna totais
        types_counts = await self.dash.get_type_counts()
        latest_pubs = await self.dash.get_latest_publications()
        total_count = await self.dash.get_publication_count()
        
        return {
            "types_counts": types_counts,
            "latest_pubs": latest_pubs,
            "total_count": total_count
        } 
        
            
class PeriodicController:
    
    def __init__(self, dashboard_dao: DashboardDAO):
        self.dash = dashboard_dao
        
    async def get_periodic_type_controller(self):
        periodic_types = await self.dash.get_periodic_type_counts()
        
        return {
            "periodic_types": periodic_types
        }
        
        
class PersonnelController:
    
    def __init__(self, dashboard_dao: DashboardDAO):
        self.dash = dashboard_dao
        
    async def get_top_personnel_controller(self):
        tops_personnel = await self.dash.get_top_personnel()
        
        return {
            "tops_personnel": tops_personnel
        }
        
        
class InstituteController:
    
    def __init__(self, dashboard_dao: DashboardDAO):
        self.dash = dashboard_dao
        
    async def get_institutes_overview_controller(self):
        institutes_overview = await self.dash.get_institutes_overview()
        
        return {
            "institutes_overview": institutes_overview
        }
        
        
class RegionController:
    
    def __init__(self, dashboard_dao: DashboardDAO):
        self.dash = dashboard_dao
        
    async def get_region_totals_controller(self):
        region_totals = await self.dash.get_region_totals()
    
        return {
            "region_totals": region_totals
        }
        
        
class StatesController:
    
    def __init__(self, dashboard_dao: DashboardDAO):
        self.dash = dashboard_dao
        
    async def get_state_totals_controller(self):
        state_totals = await self.dash.get_state_totals()
        
        return {
            "state_totals": state_totals
        }