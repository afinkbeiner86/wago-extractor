"""Data models and Enums for WoW Item Extractor"""

from dataclasses import dataclass
from enum import Enum, IntEnum
from typing import Any


class Expansion(IntEnum):
    """World of Warcraft Expansion IDs"""

    UNKNOWN_ID = -3
    CLASSIC = 0
    THE_BURNING_CRUSADE = 1
    WRATH_OF_THE_LICH_KING = 2
    CATACLYSM = 3
    MISTS_OF_PANDARIA = 4
    WARLORDS_OF_DRAENOR = 5
    LEGION = 6
    BATTLE_FOR_AZEROTH = 7
    SHADOWLANDS = 8
    DRAGONFLIGHT = 9
    THE_WAR_WITHIN = 10
    MIDNIGHT = 11

    @classmethod
    def get_name(cls, value: int) -> str:
        try:
            return cls(value).name
        except ValueError:
            return f"UNKNOWN_{value}"


class ItemClass(Enum):
    """Item class IDs from WoW game data"""

    CONSUMABLE = 0
    CONTAINER = 1
    WEAPON = 2
    GEM = 3
    ARMOR = 4
    REAGENT = 5
    PROJECTILE = 6
    TRADESKILL = 7
    RECIPE = 9
    QUIVER = 11
    QUEST = 12
    KEY = 13
    MISCELLANEOUS = 15
    GLYPH = 16


class ItemQuality(Enum):
    """Item quality levels (rarity)"""

    POOR = 0
    COMMON = 1
    UNCOMMON = 2
    RARE = 3
    EPIC = 4
    LEGENDARY = 5
    ARTIFACT = 6
    HEIRLOOM = 7


CATEGORY_MAP = {
    "consumable": ItemClass.CONSUMABLE,
    "weapon": ItemClass.WEAPON,
    "armor": ItemClass.ARMOR,
    "gem": ItemClass.GEM,
    "reagent": ItemClass.REAGENT,
    "glyph": ItemClass.GLYPH,
    "food": ItemClass.CONSUMABLE,
    "drinks": ItemClass.CONSUMABLE,
    "potions": ItemClass.CONSUMABLE,
}


@dataclass
class WoWItem:
    """Represents a fully merged item (Metadata + Details)"""

    id: int
    name: str
    class_id: ItemClass
    subclass_id: int
    quality: ItemQuality
    item_level: int
    required_level: int
    stackable: int
    sell_price: int
    expansion: Expansion | int
    description: str = ""
    spell_category_name: str = ""

    @classmethod
    def from_rows(
        cls, sparse_row: dict[str, Any], item_row: dict[str, Any], spell_cat: str = ""
    ) -> "WoWItem":
        def clean_str(text: str) -> str:
            if not text:
                return ""
            return text.replace("\r", "").replace("\n", " ").replace('"', "").strip()

        raw_exp_id = int(sparse_row.get("ExpansionID", 0))
        expansion_val: Expansion | int
        try:
            expansion_val = Expansion(raw_exp_id)
        except ValueError:
            expansion_val = raw_exp_id

        return cls(
            id=int(sparse_row["ID"]),
            name=clean_str(sparse_row.get("Display_lang", "")),
            class_id=ItemClass(int(item_row["ClassID"])),
            subclass_id=int(item_row["SubclassID"]),
            quality=ItemQuality(int(sparse_row.get("OverallQualityID", 0))),
            item_level=int(sparse_row.get("ItemLevel", 0)),
            required_level=int(sparse_row.get("RequiredLevel", 0)),
            stackable=int(sparse_row.get("Stackable", 1)),
            sell_price=int(sparse_row.get("SellPrice", 0)),
            expansion=expansion_val,
            description=clean_str(sparse_row.get("Description_lang", "")),
            spell_category_name=spell_cat,
        )

    def to_dict(self) -> dict[str, Any]:
        exp_label = (
            self.expansion.name
            if isinstance(self.expansion, Expansion)
            else Expansion.get_name(self.expansion)
        )

        return {
            "ID": self.id,
            "Name": self.name,
            "Class": self.class_id.name,
            "SubclassID": self.subclass_id,
            "Quality": self.quality.name,
            "ItemLevel": self.item_level,
            "ReqLevel": self.required_level,
            "Expansion": exp_label,
            "SpellCategory": self.spell_category_name,
            "Description": self.description,
        }
