"""Command-line interface"""

import argparse

from .core import WagoExtractor


def main() -> None:
    parser = argparse.ArgumentParser(description="Wago WoW Data Extractor")
    parser.add_argument("-o", "--output-dir", default="data/processed")
    parser.add_argument("-r", "--raw-dir", default="data/raw")
    parser.add_argument("-l", "--lua", action="store_true")
    parser.add_argument("-n", "--namespace", default="MyAddon")

    parser.add_argument("--food", action="store_true")
    parser.add_argument("--drinks", action="store_true")
    parser.add_argument("--potions", action="store_true")
    parser.add_argument("--weapons", action="store_true")
    parser.add_argument("--armor", action="store_true")

    args = parser.parse_args()

    selected: list[str] = []
    if args.food:
        selected.append("food")
    if args.drinks:
        selected.append("drinks")
    if args.potions:
        selected.append("potions")
    if args.weapons:
        selected.append("weapon")
    if args.armor:
        selected.append("armor")

    if not selected:
        selected = ["food", "drinks", "potions"]

    extractor = WagoExtractor(args.output_dir, args.raw_dir, args.namespace)
    extractor.run(selected, args.lua)


if __name__ == "__main__":
    main()
