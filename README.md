# Wago Extractor

ETL utility for denormalizing and extracting World of Warcraft DB2 data via Wago.tools.

## Purpose

This utility was developed specifically to streamline the workflow for World of Warcraft addon development. By programmatically fetching, joining, and converting raw Blizzard game data into accessible formats (CSV and Lua), it eliminates the manual overhead of parsing DB2 files. The output is optimized for direct inclusion into Lua-based addons.

## Technical Architecture

The utility performs an in-memory relational join across disparate game data schemas to produce filtered datasets for downstream consumption.

### Pipeline Stages

1. **Ingestion**: Stream-buffered retrieval of CSV datasets from the `wago.tools` DB2 API.

2. **Denormalization**: Multi-stage hash joins:

   * `ItemSparse` (Primary) ↔ `Item` (Class Definitions)

   * `Item` ↔ `ItemXItemEffect` ↔ `ItemEffect` ↔ `SpellCategory`

3. **Filtering**: Predicate-based extraction on `ClassID`, `SubclassID`, and joining on `SpellCategoryID`.

4. **Serialization**: Exports to RFC 4180 CSV or Lua table structures.

## Setup

Target environment: **Python 3.10+**.
Dependency management: [uv](https://github.com/astral-sh/uv).

```bash
git clone <repository-url>
cd wago-extractor
uv sync --all-extras