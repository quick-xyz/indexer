# indexer/types/configs/source.py

from typing import Dict, Optional, Any

from msgspec import Struct


class SourceConfig(Struct):
    name: str           
    path: str                  
    source_type: str = 'quicknode_stream'    
    status: str = 'active'    
    format: Optional[str] = None        
    description: Optional[str] = None 
    configuration: Optional[Dict[str, Any]] = None                   