# indexer/contracts/abi_loader.py

import json
from pathlib import Path
from typing import Optional, List, Dict, Any
from ..core.logging import LoggingMixin, INFO, DEBUG, WARNING, ERROR, CRITICAL


class ABILoader(LoggingMixin):
    """Loads contract ABIs from filesystem with caching"""
    
    def __init__(self, abi_base_path: Optional[Path] = None):
        if abi_base_path is None:
            # Default to config/abis relative to project root
            project_root = Path(__file__).parent.parent.parent
            abi_base_path = project_root / "config" / "abis"
        
        self.abi_base_path = abi_base_path
        self._abi_cache: Dict[str, Optional[List[Dict[str, Any]]]] = {}
        
        self.log_debug("ABI loader initialized", abi_base_path=str(self.abi_base_path))
    
    def load_abi(self, abi_dir: str, abi_file: str) -> Optional[List[Dict[str, Any]]]:
        """Load ABI from filesystem with caching"""
        if not abi_dir or not abi_file:
            return None
        
        cache_key = f"{abi_dir}/{abi_file}"
        
        # Check cache first
        if cache_key in self._abi_cache:
            return self._abi_cache[cache_key]
        
        try:
            abi_path = self.abi_base_path / abi_dir / abi_file
            
            if not abi_path.exists():
                self.log_warning("ABI file not found", 
                               abi_path=str(abi_path),
                               abi_dir=abi_dir, 
                               abi_file=abi_file)
                self._abi_cache[cache_key] = None
                return None
            
            with open(abi_path, 'r') as f:
                abi_data = json.load(f)
            
            # Handle different ABI file formats
            if isinstance(abi_data, list):
                # Direct ABI array
                abi = abi_data
            elif isinstance(abi_data, dict) and 'abi' in abi_data:
                # Wrapped in object with 'abi' key
                abi = abi_data['abi']
            else:
                self.log_error("Unexpected ABI file format", 
                             abi_path=str(abi_path),
                             data_type=type(abi_data).__name__)
                self._abi_cache[cache_key] = None
                return None
            
            # Validate ABI format
            if not isinstance(abi, list):
                self.log_error("ABI is not a list", 
                             abi_path=str(abi_path),
                             abi_type=type(abi).__name__)
                self._abi_cache[cache_key] = None
                return None
            
            # Cache and return
            self._abi_cache[cache_key] = abi
            
            self.log_debug("ABI loaded successfully", 
                         abi_path=str(abi_path),
                         abi_functions=len([item for item in abi if item.get('type') == 'function']),
                         abi_events=len([item for item in abi if item.get('type') == 'event']))
            
            return abi
            
        except json.JSONDecodeError as e:
            self.log_error("Invalid JSON in ABI file", 
                         abi_path=str(abi_path),
                         error=str(e))
            self._abi_cache[cache_key] = None
            return None
            
        except Exception as e:
            self.log_error("Failed to load ABI", 
                         abi_path=str(abi_path),
                         error=str(e),
                         exception_type=type(e).__name__)
            self._abi_cache[cache_key] = None
            return None
    
    def clear_cache(self):
        """Clear the ABI cache"""
        self._abi_cache.clear()
        self.log_debug("ABI cache cleared")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        total_entries = len(self._abi_cache)
        successful_loads = len([k for k, v in self._abi_cache.items() if v is not None])
        failed_loads = total_entries - successful_loads
        
        return {
            "total_entries": total_entries,
            "successful_loads": successful_loads,
            "failed_loads": failed_loads,
            "cache_hit_ratio": successful_loads / total_entries if total_entries > 0 else 0
        }