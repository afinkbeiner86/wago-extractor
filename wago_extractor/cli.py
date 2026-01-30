"""Command-line interface for the Wago WoW Data Extractor."""

import argparse
import sys

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
    table.add_row("Semantic", "potions", "-")
    table.add_section()

    for item_class in ItemClass:
        table.add_row("Class", item_class.name.lower(), str(item_class.value))

    table.add_section()

    for subclass in ItemSubClass:
        table.add_row("Sub-Class", subclass.name.lower(), str(subclass.value))

    console.print(table)
    console.print(
        "\n[info]Use these with [bold]-c/--categories[/bold]. Semantic keys use custom spell logic.[/info]"
    )


def main() -> None:
    """Parses command-line arguments and initializes the extraction pipeline."""
    parser = argparse.ArgumentParser(
        description="Wago WoW Data Extractor: ETL for Blizzard DB2 data."
    )

    # Path configurations
    parser.add_argument(
        "-o",
        "--output-dir",
        default="data/processed",
        help="Target directory for processed artifacts.",
    )
    parser.add_argument(
        "-r",
        "--raw-dir",
        default="data/raw",
        help="Local cache directory for upstream CSV datasets.",
    )

    # Lua serialization options
    parser.add_argument("-l", "--lua", action="store_true", help="Enable Lua module generation.")
    parser.add_argument(
        "-n", "--namespace", default="MyAddon", help="Global table identifier used in Lua output."
    )
    parser.add_argument(
        "--split-lua",
        action="store_true",
        help="Save each category to a separate .lua file instead of merging.",
    )

    # Discovery
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all available categories and subclasses, then exit.",
    )

    # Dynamic Category filters
    parser.add_argument(
        "-c",
        "--categories",
        nargs="+",
        help=(
            "List of categories to extract. Supports semantic keys (food, drinks, potions), "
            "Item Classes (e.g., WEAPON, ARMOR), or Sub-Classes (e.g., PLATE, AXE1H)."
        ),
    )

    # Legacy/Shortcut flags for convenience
    parser.add_argument("--food", action="store_true", help="Shortcut for --categories food")
    parser.add_argument("--drinks", action="store_true", help="Shortcut for --categories drinks")
    parser.add_argument("--potions", action="store_true", help="Shortcut for --categories potions")

    args = parser.parse_args()

    # Handle --list request
    if args.list:
        list_categories()
        sys.exit(0)

    # Build predicate list from both dynamic list and shortcut flags
    selected: set[str] = set()
    if args.categories:
        selected.update(c.lower() for c in args.categories)

    if args.food:
        selected.add("food")
    if args.drinks:
        selected.add("drinks")
    if args.potions:
        selected.add("potions")

    # Default to standard consumables if no specific flags or categories are provided
    final_selection = list(selected) if selected else ["food", "drinks", "potions"]

    extractor = WagoExtractor(
        output_dir=args.output_dir, raw_dir=args.raw_dir, addon_namespace=args.namespace
    )

    extractor.run(target_categories=final_selection, export_lua=args.lua, split_lua=args.split_lua)


if __name__ == "__main__":
    main()
