"""Core logic: Downloader, Extractor, and Exporter with native Rich progress tracking."""

import csv
import logging
import time
from collections import defaultdict
from collections.abc import Generator
from pathlib import Path
from typing import Any

import requests
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    ProgressColumn,
    SpinnerColumn,
    Task,
    TaskProgressColumn,
    TextColumn,
    TransferSpeedColumn,
)
from rich.table import Table
from rich.text import Text

from .models import Expansion, ItemClass, ItemSubClass, WoWItem

console = Console()


class SmartProgressColumn(ProgressColumn):
    """Dynamic renderer for progress tracking supporting indeterminate states."""

    def render(self, task: Task) -> Text:
        """Renders row counts for indeterminate tasks or percentage for completions.

        Args:
            task: The current Rich Task instance.

        Returns:
            Formatted Text object for the progress column.
        """
        if task.total is None:
            return Text(f"{int(task.completed):,} rows", style="blue")
        completion_percentage = task.percentage if task.percentage is not None else 0
        return Text(f"{completion_percentage:>3.0f}%", style="green")


class WagoExtractor:
    """ETL engine for denormalizing and filtering WoW DB2 datasets.

    Attributes:
        BASE_URL: Root endpoint for Wago tools DB2 CSV exports.
        REQUIRED_TABLES: Tables required to fulfill the relational schema.
    """

    BASE_URL = "https://wago.tools/db2"
    REQUIRED_TABLES = [
        "Item",
        "ItemSparse",
        "ItemXItemEffect",
        "ItemEffect",
        "SpellCategory",
    ]

    def __init__(
        self,
        output_dir: str = "data/processed",
        raw_dir: str = "data/raw",
        addon_namespace: str = "MyAddon",
    ):
        """Initializes storage paths and logging configurations.

        Args:
            output_dir: Destination for final CSV and Lua artifacts.
            raw_dir: Cache directory for ingested upstream CSVs.
            addon_namespace: Target global table for Lua serialization.
        """
        self.output_directory = Path(output_dir)
        self.raw_directory = Path(raw_dir)
        self.output_directory.mkdir(parents=True, exist_ok=True)
        self.raw_directory.mkdir(parents=True, exist_ok=True)
        self.addon_namespace = addon_namespace
        logging.basicConfig(level=logging.ERROR)

    def run(
        self,
        target_categories: list[str],
        export_lua: bool = False,
        split_lua: bool = False,
    ) -> None:
        """Executes the end-to-end extraction pipeline.

        Args:
            target_categories: List of identifiers to extract (e.g., 'potions').
            export_lua: Toggle for generating a consolidated Lua module.
            split_lua: Toggle for creating individual .lua files per category.
        """
        start_timestamp = time.time()
        console.print(
            Panel.fit("[bold blue]Wago WoW Data Extractor[/bold blue]", border_style="blue")
        )

        downloaded_table_paths = self._fetch_raw_data()

        console.print("\n[bold]2. Processing Relational Data[/bold]")
        extracted_items_by_category = self._process_data(downloaded_table_paths, target_categories)

        console.print(f"\n[bold]3. Saving to [green]{self.output_directory}[/green][/bold]")
        for category_name, item_list in extracted_items_by_category.items():
            self._export_to_csv(item_list, category_name)

        if export_lua:
            self._export_to_lua(extracted_items_by_category, split_lua=split_lua)

        execution_duration = time.time() - start_timestamp
        self._display_summary(
            extracted_items_by_category, export_lua, split_lua, execution_duration
        )

    def _fetch_raw_data(self) -> dict[str, Path]:
        """Orchestrates stream-buffered ingestion of required datasets.

        Returns:
            Mapping of table names to local filesystem paths.
        """
        console.print(
            f"\n[bold]1. Downloading tables to [green]{self.raw_directory}[/green][/bold]"
        )
        table_path_map = {}
        with Progress(
            TextColumn("  [blue]{task.description:25}"),
            BarColumn(bar_width=40, style="grey37", complete_style="slate_blue1"),
            TaskProgressColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            console=console,
        ) as progress_context:
            for table_name in self.REQUIRED_TABLES:
                task_id = progress_context.add_task(f"Fetching {table_name}", total=None)
                table_path_map[table_name] = self._download_table_with_progress(
                    table_name, progress_context, task_id
                )
        return table_path_map

    def _process_data(
        self, table_paths: dict[str, Path], target_categories: list[str]
    ) -> dict[str, list[WoWItem]]:
        """Performs multi-pass relational joins and predicate filtering.

        Args:
            table_paths: Mapping of raw CSV locations.
            target_categories: Extraction filters requested by caller.

        Returns:
            Denormalized item data grouped by category.
        """
        items_by_category = defaultdict(list)
        total_item_rows = self._count_csv_rows(table_paths["ItemSparse"])

        with Progress(
            SpinnerColumn(spinner_name="dots", style="blue"),
            TextColumn("[blue]{task.description:25}"),
            BarColumn(bar_width=40, style="grey37", complete_style="slate_blue1"),
            SmartProgressColumn(),
            console=console,
            transient=True,
        ) as progress_context:
            indexing_task = progress_context.add_task("Indexing & Joining", total=4)
            item_metadata_map, item_to_spell_category_map = self._build_relational_indices(
                table_paths, progress_context, indexing_task
            )

            filtering_task = progress_context.add_task("Filtering Items", total=total_item_rows)
            for row_data in self._read_csv_generator(table_paths["ItemSparse"]):
                self._evaluate_and_map_row(
                    row_data,
                    item_metadata_map,
                    item_to_spell_category_map,
                    target_categories,
                    items_by_category,
                )
                progress_context.update(filtering_task, advance=1)

        self._print_completion_bar("Indexing & Joining")
        self._print_completion_bar("Filtering Items")

        return items_by_category

    def _evaluate_and_map_row(
        self,
        row_data: dict,
        item_metadata: dict,
        spell_category_map: dict,
        targets: list[str],
        results_accumulator: dict,
    ) -> None:
        """Helper to evaluate a single row against target categories."""
        item_id = int(row_data["ID"])
        if item_id in item_metadata:
            metadata_row = item_metadata[item_id]
            spell_category_name = spell_category_map.get(item_id, "")
            self._apply_category_filters(
                row_data, metadata_row, spell_category_name, targets, results_accumulator
            )

    def _print_completion_bar(self, label: str) -> None:
        """Renders a static 100% progress bar for completed transient tasks."""
        bar_string = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        console.print(
            f"  [blue]{label:20}[/blue][slate_blue1]{bar_string}[/slate_blue1] [green]100%"
        )

    def _build_relational_indices(
        self, table_paths: dict[str, Path], progress_tracker: Progress, task_id: Any
    ) -> tuple[dict, dict]:
        """Constructs in-memory lookup indices for relational joins.

        Returns:
            Tuple of (Item Metadata Map, Item ID to Spell Category Map).
        """
        item_metadata = {
            int(row["ID"]): row for row in self._read_csv_generator(table_paths["Item"])
        }
        progress_tracker.update(task_id, advance=1)

        effect_to_spell_cat_id = {
            int(row["ID"]): int(row["SpellCategoryID"])
            for row in self._read_csv_generator(table_paths["ItemEffect"])
            if row.get("SpellCategoryID")
        }
        progress_tracker.update(task_id, advance=1)

        spell_category_names = {
            int(row["ID"]): row.get("Name_lang", "")
            for row in self._read_csv_generator(table_paths["SpellCategory"])
        }
        progress_tracker.update(task_id, advance=1)

        item_to_category_name = {}
        for row in self._read_csv_generator(table_paths["ItemXItemEffect"]):
            effect_id = int(row["ItemEffectID"])
            spell_category_id = effect_to_spell_cat_id.get(effect_id)
            if spell_category_id:
                item_to_category_name[int(row["ItemID"])] = spell_category_names.get(
                    spell_category_id, ""
                )
        progress_tracker.update(task_id, advance=1)

        return item_metadata, item_to_category_name

    def _apply_category_filters(
        self,
        sparse_row: dict,
        metadata_row: dict,
        spell_category: str,
        target_categories: list[str],
        results: dict,
    ) -> None:
        """Matches items against requested categories and appends to results."""
        class_id = int(metadata_row["ClassID"])
        subclass_id = int(metadata_row["SubclassID"])

        for category_key in target_categories:
            if self._check_category_match(category_key, class_id, subclass_id, spell_category):
                try:
                    wow_item = WoWItem.from_rows(sparse_row, metadata_row, spell_category)
                    results[category_key].append(wow_item)
                except ValueError:
                    continue

    def _check_category_match(
        self, category_key: str, class_id: int, subclass_id: int, spell_category: str
    ) -> bool:
        """Boolean check to see if IDs match the requested category logic."""
        key_constant = category_key.upper().replace("-", "_")

        if category_key == "food" and "Food" in spell_category:
            return True
        if category_key == "drinks" and "Drink" in spell_category:
            return True

        if key_constant in ItemClass.__members__:
            return class_id == ItemClass[key_constant].value

        if key_constant in ItemSubClass.__members__:
            target_sub_id = ItemSubClass[key_constant].value
            if subclass_id != target_sub_id:
                return False

            if key_constant in ["POTION", "ELIXIR", "FLASK", "FOOD_AND_DRINK"]:
                return class_id == ItemClass.CONSUMABLE.value
            if key_constant in ["CLOTH", "LEATHER", "MAIL", "PLATE", "SHIELD"]:
                return class_id == ItemClass.ARMOR.value
            if class_id == ItemClass.WEAPON.value:
                return True

        return False

    def _display_summary(
        self, items_map: dict, exported_lua: bool, split_lua: bool, duration: float
    ) -> None:
        """Renders a tabular summary of extraction results."""
        summary_table = Table(show_header=True, header_style="bold magenta")
        summary_table.add_column("File Type", style="dim")
        summary_table.add_column("Filename")
        summary_table.add_column("Items", justify="right")

        for category_name, items in items_map.items():
            summary_table.add_row("CSV Data", f"{category_name}.csv", str(len(items)))
            if exported_lua and split_lua:
                summary_table.add_row(
                    "Lua Module", f"{category_name}.lua", "[green]Extracted[/green]"
                )

        if exported_lua and not split_lua:
            summary_table.add_row("Lua Module", "data.lua", "[green]Merged[/green]")

        console.print(summary_table)
        console.print(f"\n[bold green]✨ Done![/bold green] [white]{duration:.2f}s[/white]\n")

    def _count_csv_rows(self, file_path: Path) -> int:
        """Counts rows in a CSV file efficiently."""
        with open(file_path, "rb") as csv_file:
            return sum(1 for _ in csv_file) - 1

    def _read_csv_generator(self, file_path: Path) -> Generator[dict[str, Any], None, None]:
        """Reads CSV rows as dictionaries via generator."""
        with open(file_path, encoding="utf-8") as csv_file:
            yield from csv.DictReader(csv_file)

    def _download_table_with_progress(
        self, table_name: str, progress_bar: Progress, task_id: Any
    ) -> Path:
        """Downloads a DB2 table with progress tracking."""
        target_path = self.raw_directory / f"{table_name}.csv"
        request_url = f"{self.BASE_URL}/{table_name}/csv"
        http_response = requests.get(request_url, stream=True, timeout=60)
        http_response.raise_for_status()

        total_bytes = int(http_response.headers.get("content-length", 0))
        progress_bar.update(task_id, total=total_bytes if total_bytes > 0 else None)

        with open(target_path, "wb") as output_file:
            for byte_chunk in http_response.iter_content(chunk_size=8192):
                output_file.write(byte_chunk)
                progress_bar.update(task_id, advance=len(byte_chunk))
        return target_path

    def _export_to_csv(self, item_list: list[WoWItem], category_name: str) -> None:
        """Exports WoWItem objects to a CSV file."""
        if not item_list:
            return
        sorted_items = sorted(item_list, key=lambda item: item.id)
        file_output_path = self.output_directory / f"{category_name}.csv"
        with open(file_output_path, "w", newline="", encoding="utf-8") as csv_file:
            csv_headers = list(sorted_items[0].to_dict().keys())
            csv_writer = csv.DictWriter(csv_file, fieldnames=csv_headers)
            csv_writer.writeheader()
            csv_writer.writerows([item.to_dict() for item in sorted_items])

    def _export_to_lua(
        self, category_data: dict[str, list[WoWItem]], split_lua: bool = False
    ) -> None:
        """Exports data as a Lua table for WoW Addons."""
        merged_lua_buffer = [f"{self.addon_namespace} = {self.addon_namespace} or {{}}"]

        for category_name, items in category_data.items():
            category_lua_string = self._generate_category_lua_content(
                category_name, items, include_header=split_lua
            )

            if split_lua:
                (self.output_directory / f"{category_name}.lua").write_text(
                    category_lua_string, encoding="utf-8"
                )
            else:
                merged_lua_buffer.append(category_lua_string)

        if not split_lua:
            (self.output_directory / "data.lua").write_text(
                "\n".join(merged_lua_buffer), encoding="utf-8"
            )

    def _generate_category_lua_content(
        self, category_name: str, items: list[WoWItem], include_header: bool
    ) -> str:
        """Generates the Lua string representation for a single category."""
        lua_lines = []
        if include_header:
            lua_lines.append(f"{self.addon_namespace} = {self.addon_namespace} or {{}}")

        lua_lines.append(f"{self.addon_namespace}.{category_name.upper()} = {{")

        items_by_expansion = defaultdict(list)
        for item in items:
            items_by_expansion[int(item.expansion)].append(item)

        for expansion_id in sorted(items_by_expansion.keys()):
            expansion_label = Expansion.get_name(expansion_id)
            lua_lines.append(f"   [{expansion_id}] = {{ -- {expansion_label}")
            for item in sorted(items_by_expansion[expansion_id], key=lambda i: i.id):
                lua_lines.append(f'     [{item.id}] = "{item.name}",')
            lua_lines.append("   },")

        lua_lines.append("}\n")
        return "\n".join(lua_lines)
