"""Owlbear — feathers and claws for your data lake."""

from .athena import AthenaClient
from .trino import TrinoClient

__version__ = "0.3.0"
__all__ = ["AthenaClient", "TrinoClient"]
