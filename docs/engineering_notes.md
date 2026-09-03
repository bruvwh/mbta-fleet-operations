# MBTA Fleet Operations Engineering Notes

This document records major engineering decisions, bottlenecks, migrations, validation results, and performance observations made while developing the MBTA Fleet Operations Data Platform.

The current production-style architecture is documented in the repository README.

## Architecture Evolution

The project began as a local PostgreSQL-based data platform and later migrated to a cloud analytical architecture.

### Initial Architecture

```text
MBTA / historical sources
        |
        v
Local Python ingestion
        |
        v
PostgreSQL
        |
        v
Incremental SQL transformations
        |
        v
Streamlit