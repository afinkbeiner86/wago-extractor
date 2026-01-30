"""Command-line interface for the Wago WoW Data Extractor."""

import argparse
import sys
import textwrap

from rich.console import Console
from rich.table import Table

from .core import WagoExtractor
from .models import ItemClass, ItemSubClass

console = Console()


def list_categories() -> None:
    """Prints a formatted table of all available item classes and subclasses."""
    table = Table(title="Available Extraction Identifiers")
    table.add_column("Type", style="cyan")
    table.add_column("Identifier (use with -c)", style="green")
    table.add_column("Value", style="dim")

    table.add_row("Semantic", "food", "-")
    table.add_row("Semantic", "drinks", "-")
    table.add_section()

    for item_class in ItemClass:
        table.add_row("Class", item_class.name.lower(), str(item_class.value))

    table.add_section()

    for subclass in ItemSubClass:
        table.add_row("Sub-Class", subclass.name.lower(), str(subclass.value))

    console.print(table)
    console.print(
        "\n[info]Use these with [bold]-c/--categories[/bold]. Plural/Singular is supported automatically.[/info]"
    )


def _normalize_category_name(category_input: str) -> str:
    """Standardizes user input to match internal Enum keys or semantic overrides.

    This function handles common human-input variations such as trailing spaces,
    casing differences, and pluralizations.

    Args:
        category_input: The raw string provided by the user via the CLI.

    Returns:
        A standardized string (e.g., "weapon", "food") compatible with core logic.
    """
    normalized_input = category_input.lower().strip().replace(" ", "_")

    # Handle hardcoded semantic overrides
    if normalized_input in ["drink", "drinks"]:
        return "drinks"
    if normalized_input in ["food", "foods"]:
        return "food"

    # Map all possible valid singular identifiers from our data models
    # We combine Class and SubClass names for a single lookup set
    valid_class_names = {item.name.lower() for item in ItemClass}
    valid_subclass_names = {sub.name.lower() for sub in ItemSubClass}
    all_valid_identifiers = valid_class_names | valid_subclass_names

    if normalized_input in all_valid_identifiers:
        return normalized_input

    if normalized_input.endswith("s"):
        singular_version = normalized_input[:-1]
        if singular_version in all_valid_identifiers:
            return singular_version

    return normalized_input


def main() -> None:
    """Parses command-line arguments and initializes the extraction pipeline."""
    parser = argparse.ArgumentParser(
        description="Wago WoW Data Extractor: ETL for Blizzard DB2 data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""
            Usage Examples:
              # Plural and Singular support are handled automatically
              python -m wago_extractor.cli -c weapons armors plates

              # Mix and match identifiers
              python -m wago_extractor.cli -c food potion "2h axe" --lua

              # List all available identifiers
              python -m wago_extractor.cli --list
        """),
    )

    parser.add_argument(
        "-o",
        "--output-dir",
        default="data/processed",
        help="Target directory for the processed CSV/Lua files.",
    )
    parser.add_argument(
        "-r",
        "--raw-dir",
        default="data/raw",
        help="Local cache directory for raw upstream CSV datasets.",
    )
    parser.add_argument(
        "--lua",
        action="store_true",
        help="Enable generation of Lua data tables for WoW Addons.",
    )
    parser.add_argument(
        "-n",
        "--namespace",
        default="MyAddon",
        help="The global table name used in the generated Lua output.",
    )
    parser.add_argument(
        "-s",
        "--split-lua",
        action="store_true",
        help="Save each category to an individual .lua file instead of a merged file.",
    )
    parser.add_argument(
        "-l",
        "--list",
        action="store_true",
        help="List all valid category identifiers and exit.",
    )
    parser.add_argument(
        "-c",
        "--categories",
        nargs="+",
        help="List of categories to extract (e.g., weapons, armor, potions).",
    )

    args = parser.parse_args()

    if args.list:
        list_categories()
        sys.exit(0)

    if not args.categories:
        console.print("[bold red]Error:[/bold red] No categories were selected for extraction.")
        parser.print_help()
        sys.exit(1)

    normalized_categories = {_normalize_category_name(category) for category in args.categories}

    extractor = WagoExtractor(
        output_dir=args.output_dir, raw_dir=args.raw_dir, addon_namespace=args.namespace
    )

    extractor.run(
        target_categories=list(normalized_categories), export_lua=args.lua, split_lua=args.split_lua
    )


if __name__ == "__main__":
    main()
