from .reader import ScidIngestor, IngestConfig

# Back-compat alias so older references still work if any:
SCIDReader = ScidIngestor

__all__ = ["ScidIngestor", "IngestConfig", "SCIDReader"]
