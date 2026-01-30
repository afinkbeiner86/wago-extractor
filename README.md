# Wago Extractor

ETL utility for denormalizing and extracting World of Warcraft DB2 data via Wago.tools.

## Purpose

This utility streamlines the workflow for World of Warcraft addon development by programmatically fetching, joining, and converting raw Blizzard game data into optimized CSV and Lua formats. It eliminates the manual overhead of parsing DB2 files and provides filtered datasets ready for direct integration into Lua-based projects.

## Technical Architecture

The utility performs an in-memory relational join across disparate game data schemas to produce denormalized datasets.

### Pipeline Stages

1. **Ingestion**: Stream-buffered retrieval of CSV datasets from the `wago.tools` API.
2. **Denormalization**: Multi-stage hash joins:
    * `ItemSparse` (Primary) $\leftrightarrow$ `Item` (Class Definitions)
    * `Item` $\leftrightarrow$ `ItemXItemEffect` $\leftrightarrow$ `ItemEffect` $\leftrightarrow$ `SpellCategory`
3. **Filtering**: Predicate-based extraction on `ClassID`, `SubclassID`, and semantic matching on `SpellCategoryID`.
4. **Serialization**: Exports to RFC 4180 CSV or Lua table structures.

## Setup

**Environment**: Python 3.10+  
**Package Manager**: [uv](https://github.com/astral-sh/uv)

```powershell
git clone <repository-url>
cd wago-extractor
uv sync --all-extras
```

## Usage

The utility is invoked via the `wago-extract` entry point. Use the `-c` or `--categories` flag to specify target datasets.

### PowerShell

```powershell
# Extract specific categories to CSV (default)
uv run wago-extract -c potions food drinks

# Extract weapons and armor with Lua serialization
uv run wago-extract -c weapon armor --lua --split-lua
```

## Development

**Testing**:  
Run `uv run pytest` to execute the unit test suite.

**Linting**:  
Use `uv run ruff check .` for static analysis.
