# src/message_pack_parser/schemas/output_contracts.py
"""
Defines the transformation contracts for stats before output serialization.

This is the single source of truth for how to prepare dataframes for specific
consumers, like a high-performance frontend.

The 'output_transformer' module consumes this configuration to apply the rules.
"""
from typing import Dict, Any

# Contract for the 'unaggregated' stream, which we want in a row-major layout
# with mixed data types.
unaggregated_contract = {
    "columns": {
        # Individual column transformations (like quantization) would be defined
        # here if needed. The RowMajor strategy will use the *final* dtypes
        # after these transforms are applied.
    },
    # This key tells the RowMajor strategy to process this stream.
    "table_options": {
        "layout": "row-major-mixed"
    }
}


# Add other contracts that require this layout...
army_value_timeline_contract = {
    "columns": {
        "frame": {
            "transform": "cast",
            "to_type": "UInt32", #type for consumer
        },
        "teamid": {
            "transform": "cast",
            "to_type": "UInt32", #type for consumer
        },
        "army_value": {
            "transform": "quantize",
            "to_type": "UInt32", #type for consumer
            "params": {
                "type": "static",
                "scale": 0.1   # divide by 10 by consumer
            }
        }
    },
    "table_options": {
        "layout": "row-major-mixed"
    }
}


OUTPUT_CONTRACTS: Dict[str, Dict[str, Any]] = {
    "unaggregated": unaggregated_contract,
    "army_value_timeline": army_value_timeline_contract,
    # ... other existing contracts
}