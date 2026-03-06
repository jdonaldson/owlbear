"""Backward compatibility — use owlbear.athena instead."""

from .athena import AthenaClient as OwlbearClient

__all__ = ["OwlbearClient"]
