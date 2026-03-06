"""Owlbear — feathers and claws for your data lake."""

from .athena import AthenaClient
from .trino import TrinoClient

# Backward compat alias
OwlbearClient = AthenaClient

__version__ = "0.1.0"
__all__ = ["AthenaClient", "TrinoClient", "OwlbearClient"]
