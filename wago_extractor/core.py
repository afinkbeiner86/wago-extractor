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

from .models import CATEGORY_MAP, Expansion, WoWItem

console = Console()


class SmartProgressColumn(ProgressColumn):
    """Custom renderer for indeterminate row counts or percentages."""

    def render(self, task: Task) -> Text:
        if task.total is None:
            return Text(f"{int(task.completed):,} rows", style="blue")
        percent = task.percentage if task.percentage is not None else 0
        return Text(f"{percent:>3.0f}%", style="green")


class WagoExtractor:
    BASE_URL = "https://wago.tools/db2"

    def __init__(
        self,
        output_dir: str = "data/processed",
        raw_dir: str = "data/raw",
        addon_namespace: str = "MyAddon",
    ):
        self.output_dir = Path(output_dir)
        self.raw_dir = Path(raw_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.addon_namespace = addon_namespace
        logging.basicConfig(level=logging.ERROR)

    def _count_csv_rows(self, path: Path) -> int:
        with open(path, "rb") as f:
            return sum(1 for _ in f) - 1

    def _read_csv(self, path: Path) -> Generator[dict[str, Any], None, None]:
        with open(path, encoding="utf-8") as f:
            yield from csv.DictReader(f)

    def run(self, target_cats: list[str], export_lua: bool = False) -> None:
        start_time = time.time()
        console.print(
            Panel.fit("[bold blue]Wago WoW Data Extractor[/bold blue]", border_style="blue")
        )

        console.print(f"\n[bold]1. Downloading to [green]{self.raw_dir}[/green][/bold]")
        tables = ["Item", "ItemSparse", "ItemXItemEffect", "ItemEffect", "SpellCategory"]
        paths = {}

        with Progress(
            TextColumn("  [blue]{task.description:25}"),
            BarColumn(bar_width=40, style="grey37", complete_style="slate_blue1"),
            TaskProgressColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            console=console,
        ) as progress:
            for name in tables:
                tid = progress.add_task(f"Fetching {name}", total=None)
                paths[name] = self._download_table_rich(name, progress, tid)

        console.print("\n[bold]2. Processing Relational Data[/bold]")
        items_map = defaultdict(list)
        total_rows = self._count_csv_rows(paths["ItemSparse"])

        with Progress(
            SpinnerColumn(spinner_name="dots", style="blue"),
            TextColumn("[blue]{task.description:25}"),
            BarColumn(bar_width=40, style="grey37", complete_style="slate_blue1"),
            SmartProgressColumn(),
            console=console,
            transient=True,
        ) as progress:
            t_idx = progress.add_task("Indexing & Joining", total=4)

            meta = {int(r["ID"]): r for r in self._read_csv(paths["Item"])}
            progress.update(t_idx, advance=1)

            eff_scat = {
                int(r["ID"]): int(r["SpellCategoryID"])
                for r in self._read_csv(paths["ItemEffect"])
                if r.get("SpellCategoryID")
            }
            progress.update(t_idx, advance=1)

            scat_names = {
                int(r["ID"]): r.get("Name_lang", "") for r in self._read_csv(paths["SpellCategory"])
            }
            progress.update(t_idx, advance=1)

            item_to_cat = {}
            for r in self._read_csv(paths["ItemXItemEffect"]):
                i_id, e_id = int(r["ItemID"]), int(r["ItemEffectID"])
                s_id = eff_scat.get(e_id)
                if s_id:
                    item_to_cat[i_id] = scat_names.get(s_id, "")
            progress.update(t_idx, advance=1)

            f_task = progress.add_task("Filtering Items", total=total_rows)
            for row in self._read_csv(paths["ItemSparse"]):
                i_id = int(row["ID"])
                if i_id in meta:
                    m = meta[i_id]
                    cid, scid, scat = (
                        int(m["ClassID"]),
                        int(m["SubclassID"]),
                        item_to_cat.get(i_id, ""),
                    )
                    for cat in target_cats:
                        if (
                            (cat == "food" and "Food" in scat)
                            or (cat == "drinks" and "Drink" in scat)
                            or (cat == "potions" and cid == 0 and scid == 1)
                            or (
                                CATEGORY_MAP.get(cat)
                                and cid == CATEGORY_MAP[cat].value
                                and cat not in ["food", "drinks", "potions"]
                            )
                        ):
                            items_map[cat].append(WoWItem.from_rows(row, m, scat))
                progress.update(f_task, advance=1)

        bar = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        for label in ["Indexing & Joining", "Filtering Items"]:
            console.print(
                f"  [blue]{label:25}[/blue][slate_blue1]{bar}[/slate_blue1] [green]100%[/green]"
            )

        console.print(f"\n[bold]3. Saving to [green]{self.output_dir}[/green][/bold]")
        summary = Table(show_header=True, header_style="bold magenta")
        summary.add_column("File Type", style="dim")
        summary.add_column("Filename")
        summary.add_column("Items", justify="right")

        for cat, items in items_map.items():
            self._export_csv(items, cat)
            summary.add_row("CSV Data", f"{cat}.csv", str(len(items)))
        if export_lua:
            self._export_lua(items_map)
            summary.add_row("Lua Module", "data.lua", "[green]Merged[/green]")

        console.print(summary)
        elapsed = time.time() - start_time
        console.print(f"\n[bold green]✨ Done![/bold green] [white]{elapsed:.2f}s[/white]\n")

    def _download_table_rich(self, name: str, progress: Progress, tid: Any) -> Path:
        p = self.raw_dir / f"{name}.csv"
        r = requests.get(f"{self.BASE_URL}/{name}/csv", stream=True, timeout=60)
        r.raise_for_status()
        size = int(r.headers.get("content-length", 0))
        progress.update(tid, total=size if size > 0 else None)
        with open(p, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
                progress.update(tid, advance=len(chunk))
        return p

    def _export_csv(self, items: list[WoWItem], name: str) -> None:
        if not items:
            return
        items = sorted(items, key=lambda x: x.id)
        out = self.output_dir / f"{name}.csv"
        with open(out, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(items[0].to_dict().keys()))
            writer.writeheader()
            writer.writerows([i.to_dict() for i in items])

    def _export_lua(self, data: dict[str, list[WoWItem]]) -> None:
        content = [f"{self.addon_namespace} = {self.addon_namespace} or {{}}"]
        for cat, items in data.items():
            content.append(f"{self.addon_namespace}.{cat.upper()} = {{")
            by_exp = defaultdict(list)
            for i in items:
                by_exp[int(i.expansion)].append(i)
            for eid in sorted(by_exp.keys()):
                ename = Expansion.get_name(eid)
                content.append(f"   [{eid}] = {{ -- {ename}")
                for i in sorted(by_exp[eid], key=lambda x: x.id):
                    content.append(f'     [{i.id}] = "{i.name}",')
                content.append("   },")
            content.append("}\n")
        (self.output_dir / "data.lua").write_text("\n".join(content), encoding="utf-8")
