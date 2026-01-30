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

from .models import CATEGORY_MAP, Expansion, ItemClass, ItemSubClass, WoWItem

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
        percent = task.percentage if task.percentage is not None else 0
        return Text(f"{percent:>3.0f}%", style="green")


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
        self.output_dir = Path(output_dir)
        self.raw_dir = Path(raw_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.raw_dir.mkdir(parents=True, exist_ok=True)
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
        start_time = time.time()
        console.print(
            Panel.fit("[bold blue]Wago WoW Data Extractor[/bold blue]", border_style="blue")
        )

        table_paths = self._fetch_raw_data()

        console.print("\n[bold]2. Processing Relational Data[/bold]")
        items_by_category = self._process_data(table_paths, target_categories)

        console.print(f"\n[bold]3. Saving to [green]{self.output_dir}[/green][/bold]")
        for category_name, items in items_by_category.items():
            self._export_csv(items, category_name)

        if export_lua:
            self._export_lua(items_by_category, split_lua=split_lua)

        self._display_summary(items_by_category, export_lua, split_lua, time.time() - start_time)

    def _fetch_raw_data(self) -> dict[str, Path]:
        """Orchestrates stream-buffered ingestion of required datasets.

        Returns:
            Mapping of table names to local filesystem paths.
        """
        console.print(f"\n[bold]1. Downloading tables to [green]{self.raw_dir}[/green][/bold]")
        paths = {}
        with Progress(
            TextColumn("  [blue]{task.description:25}"),
            BarColumn(bar_width=40, style="grey37", complete_style="slate_blue1"),
            TaskProgressColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            console=console,
        ) as progress:
            for table_name in self.REQUIRED_TABLES:
                task_id = progress.add_task(f"Fetching {table_name}", total=None)
                paths[table_name] = self._download_table_rich(table_name, progress, task_id)
        return paths

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
        items_map = defaultdict(list)
        total_rows = self._count_csv_rows(table_paths["ItemSparse"])

        with Progress(
            SpinnerColumn(spinner_name="dots", style="blue"),
            TextColumn("[blue]{task.description:25}"),
            BarColumn(bar_width=40, style="grey37", complete_style="slate_blue1"),
            SmartProgressColumn(),
            console=console,
            transient=True,
        ) as progress:
            indexing_task = progress.add_task("Indexing & Joining", total=4)
            item_metadata, item_to_spell_category = self._build_relation_maps(
                table_paths, progress, indexing_task
            )

            filtering_task = progress.add_task("Filtering Items", total=total_rows)
            for row in self._read_csv(table_paths["ItemSparse"]):
                item_id = int(row["ID"])
                if item_id in item_metadata:
                    metadata = item_metadata[item_id]
                    spell_category_name = item_to_spell_category.get(item_id, "")

                    self._filter_and_map_item(
                        row, metadata, spell_category_name, target_categories, items_map
                    )

                progress.update(filtering_task, advance=1)

        bar_visual = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        for label in ["Indexing & Joining", "Filtering Items"]:
            msg = f"  [blue]{label:20}[/blue][slate_blue1]{bar_visual}[/slate_blue1] [green]100%"
            console.print(msg)

        return items_map

    def _build_relation_maps(
        self, table_paths: dict[str, Path], progress: Progress, task_id: Any
    ) -> tuple[dict, dict]:
        """Constructs in-memory lookup indices for O(1) relational joins.

        Args:
            table_paths: Mapping of raw CSV locations.
            progress: Progress context manager instance.
            task_id: Rich task ID for the indexing phase.

        Returns:
            Tuple containing item metadata and spell category mappings.
        """
        item_metadata = {int(row["ID"]): row for row in self._read_csv(table_paths["Item"])}
        progress.update(task_id, advance=1)

        effect_to_spell_category_id = {
            int(row["ID"]): int(row["SpellCategoryID"])
            for row in self._read_csv(table_paths["ItemEffect"])
            if row.get("SpellCategoryID")
        }
        progress.update(task_id, advance=1)

        spell_category_names = {
            int(row["ID"]): row.get("Name_lang", "")
            for row in self._read_csv(table_paths["SpellCategory"])
        }
        progress.update(task_id, advance=1)

        item_to_category_name = {}
        for row in self._read_csv(table_paths["ItemXItemEffect"]):
            item_id = int(row["ItemID"])
            effect_id = int(row["ItemEffectID"])
            spell_cat_id = effect_to_spell_category_id.get(effect_id)
            if spell_cat_id:
                item_to_category_name[item_id] = spell_category_names.get(spell_cat_id, "")
        progress.update(task_id, advance=1)

        return item_metadata, item_to_category_name

    def _filter_and_map_item(
        self,
        sparse_row: dict,
        meta_row: dict,
        spell_category: str,
        targets: list[str],
        results: dict,
    ) -> None:
        """Filters and groups items into requested categories with context enforcement.

        Args:
            sparse_row: Row dictionary from ItemSparse.
            meta_row: Row dictionary from Item (relational metadata).
            spell_category: Resolved spell category name.
            targets: List of category keys requested by the user.
            results: Accumulator dictionary for filtered WoWItem objects.
        """
        class_id = int(meta_row["ClassID"])
        subclass_id = int(meta_row["SubclassID"])

        for category_key in targets:
            is_match = False
            key_upper = category_key.upper().replace("-", "_")

            # 1. Semantic Overrides (excluding potions which are now subclass-based)
            if category_key == "food" and "Food" in spell_category:
                is_match = True
            elif category_key == "drinks" and "Drink" in spell_category:
                is_match = True

            # 2. Dynamic Match against ItemClass
            elif key_upper in ItemClass.__members__:
                if class_id == ItemClass[key_upper].value:
                    is_match = True

            # 3. Dynamic Match against ItemSubClass with STRICT context
            elif key_upper in ItemSubClass.__members__:
                target_subclass_id = ItemSubClass[key_upper].value

                if subclass_id == target_subclass_id:
                    # Specific Context Enforcement for overloaded IDs
                    if key_upper in ["POTION", "ELIXIR", "FLASK", "FOOD_AND_DRINK"]:
                        if class_id == ItemClass.CONSUMABLE.value:
                            is_match = True
                    elif key_upper in ["CLOTH", "LEATHER", "MAIL", "PLATE", "SHIELD"]:
                        if class_id == ItemClass.ARMOR.value:
                            is_match = True
                    elif class_id == ItemClass.WEAPON.value:
                        is_match = True

            # 4. Fallback to CATEGORY_MAP
            elif (
                category_key in CATEGORY_MAP
                and class_id == CATEGORY_MAP[category_key].value
                and category_key not in ["food", "drinks"]
            ):
                is_match = True

            if is_match:
                try:
                    results[category_key].append(
                        WoWItem.from_rows(sparse_row, meta_row, spell_category)
                    )
                except ValueError:
                    continue

    def _display_summary(
        self, items_map: dict, exported_lua: bool, split_lua: bool, elapsed_time: float
    ) -> None:
        """Renders a tabular summary of extraction results.

        Args:
            items_map: Grouped extraction results.
            exported_lua: Flag indicating if Lua artifacts were generated.
            split_lua: Flag indicating if Lua artifacts were split.
            elapsed_time: Execution duration in seconds.
        """
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
        console.print(f"\n[bold green]✨ Done![/bold green] [white]{elapsed_time:.2f}s[/white]\n")

    def _count_csv_rows(self, file_path: Path) -> int:
        """Counts rows in a CSV file efficiently.

        Args:
            file_path: Path to the CSV file.

        Returns:
            Number of rows excluding the header.
        """
        with open(file_path, "rb") as f:
            return sum(1 for _ in f) - 1

    def _read_csv(self, file_path: Path) -> Generator[dict[str, Any], None, None]:
        """Reads CSV rows as dictionaries via generator.

        Args:
            file_path: Path to the CSV file.

        Yields:
            Dictionary representing a CSV row.
        """
        with open(file_path, encoding="utf-8") as f:
            yield from csv.DictReader(f)

    def _download_table_rich(self, table_name: str, progress_bar: Progress, task_id: Any) -> Path:
        """Downloads a DB2 table with progress tracking.

        Args:
            table_name: Name of the DB2 table.
            progress_bar: Rich Progress instance.
            task_id: ID of the progress task.

        Returns:
            Path to the downloaded file.
        """
        local_path = self.raw_dir / f"{table_name}.csv"
        url = f"{self.BASE_URL}/{table_name}/csv"
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()

        content_length = int(response.headers.get("content-length", 0))
        progress_bar.update(task_id, total=content_length if content_length > 0 else None)

        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                progress_bar.update(task_id, advance=len(chunk))
        return local_path

    def _export_csv(self, items: list[WoWItem], category_name: str) -> None:
        """Exports WoWItem objects to a CSV file.

        Args:
            items: List of items to export.
            category_name: Filename prefix.
        """
        if not items:
            return

        sorted_items = sorted(items, key=lambda item: item.id)
        output_path = self.output_dir / f"{category_name}.csv"

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            headers = list(sorted_items[0].to_dict().keys())
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows([item.to_dict() for item in sorted_items])

    def _export_lua(self, category_data: dict[str, list[WoWItem]], split_lua: bool = False) -> None:
        """Exports data as a Lua table for WoW Addons.

        Args:
            category_data: Filtered items grouped by category.
            split_lua: Toggle for individual .lua files per category.
        """
        merged_lines = [f"{self.addon_namespace} = {self.addon_namespace} or {{}}"]

        for category_name, items in category_data.items():
            category_lines = []
            if split_lua:
                category_lines.append(f"{self.addon_namespace} = {self.addon_namespace} or {{}}")

            category_lines.append(f"{self.addon_namespace}.{category_name.upper()} = {{")

            items_by_expansion = defaultdict(list)
            for item in items:
                items_by_expansion[int(item.expansion)].append(item)

            for expansion_id in sorted(items_by_expansion.keys()):
                expansion_name = Expansion.get_name(expansion_id)
                category_lines.append(f"   [{expansion_id}] = {{ -- {expansion_name}")

                for item in sorted(items_by_expansion[expansion_id], key=lambda i: i.id):
                    category_lines.append(f'     [{item.id}] = "{item.name}",')

                category_lines.append("   },")
            category_lines.append("}\n")

            if split_lua:
                (self.output_dir / f"{category_name}.lua").write_text(
                    "\n".join(category_lines), encoding="utf-8"
                )
            else:
                merged_lines.extend(category_lines)

        if not split_lua:
            (self.output_dir / "data.lua").write_text("\n".join(merged_lines), encoding="utf-8")
