# src\message_pack_parser\core\stats\__init__.py
"""
This package contains all individual statistic calculation modules.

It dynamically discovers and registers them, creating a plug-and-play system for analytics.
To add a new stat, simply create a new .py file in this directory and define
a STAT_DEFINITION variable of type Stat.
"""
import pkgutil
import importlib
import logging
from typing import Dict, Callable

from .types import Stat
from .unaggregated import get_detailed_command_log

logger = logging.getLogger(__name__)

STATS_REGISTRY: Dict[str, Stat] = {}

def _discover_and_register_stats():
    """Dynamically imports all modules in this package to register their stats."""
    package_path = __path__
    package_name = __name__
    
    for _, module_name, _ in pkgutil.iter_modules(package_path, package_name + '.'):
        try:
            module = importlib.import_module(module_name)
            
            # Look for single STAT_DEFINITION
            if hasattr(module, 'STAT_DEFINITION'):
                stat_def = getattr(module, 'STAT_DEFINITION')
                key_name = module_name.split('.')[-1] # e.g., 'player_economic_efficiency'
                if isinstance(stat_def, Stat):
                    STATS_REGISTRY[key_name] = stat_def
                    
            # Look for multiple definitions, e.g., in combat_engagement_summary
            # Convention: STAT_DEFINITION_SUFFIX
            for attr_name in dir(module):
                if attr_name.startswith('STAT_DEFINITION_'):
                    stat_def = getattr(module, attr_name)
                    # Key is suffix: e.g., STAT_DEFINITION_GLOBAL -> 'global'
                    # Or combine module name and suffix
                    key_suffix = attr_name.replace('STAT_DEFINITION_', '').lower()
                    key_name = f"{module_name.split('.')[-1]}_{key_suffix}"
                    if isinstance(stat_def, Stat):
                        STATS_REGISTRY[key_name] = stat_def

        except Exception as e:
            logger.error(f"Failed to load or register stats from module {module_name}: {e}")

# Run discovery at import time
_discover_and_register_stats()
logger.info(f"Dynamically registered {len(STATS_REGISTRY)} stats: {list(STATS_REGISTRY.keys())}")


# For now, the unaggregated stream registry can remain simple and static here.
UNAGGREGATED_STREAM_REGISTRY: Dict[str, Callable] = {
    "command_log": get_detailed_command_log
}