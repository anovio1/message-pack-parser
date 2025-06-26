"""
Defines the Pydantic schema for validating the structure of the unitdefs.json file.
"""
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any

class UnitDef(BaseModel):
    """
    A Pydantic model that captures the essential fields from a single unit definition.
    It will ignore any extra fields not defined here.
    """
    metalcost: float = 0.0
    energycost: float = 0.0
    health: float = 0.0
    buildtime: float = 0.0
    # The unit's in-game name (e.g., "armcom") will be added programmatically.
    unit_name: Optional[str] = None
    # We can add other fields here as needed for future stats.
    
    class Config:
        extra = 'ignore' # Tell Pydantic to ignore fields not defined in this model.

# This model represents the entire top-level structure of the JSON file.
class UnitDefsFile(BaseModel):
    root: Dict[str, UnitDef]

    def iter_unitdefs(self):
        return iter(self.root.items())

    def __getitem__(self, item):
        return self.root[item]