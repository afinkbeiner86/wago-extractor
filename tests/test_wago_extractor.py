"""Unit and integration tests for the Wago WoW Data Extractor."""

import sys
from unittest.mock import MagicMock, patch

import pytest

from wago_extractor.cli import main
from wago_extractor.core import WagoExtractor
from wago_extractor.models import Expansion, ItemQuality, WoWItem


@pytest.fixture
def mock_directories(tmp_path):
    """Provides temporary paths for raw and processed data."""
    raw_dir = tmp_path / "raw"
    processed_dir = tmp_path / "processed"
    raw_dir.mkdir()
    processed_dir.mkdir()
    return raw_dir, processed_dir


# --- MODELS TESTS ---


def test_expansion_logic():
    """Tests the Expansion enum and its safety helper."""
    assert Expansion.get_name(0) == "CLASSIC"
    assert Expansion.get_name(10) == "THE_WAR_WITHIN"
    assert Expansion.get_name(99) == "UNKNOWN_99"


def test_wow_item_transformation():
    """Tests the conversion from CSV rows to WoWItem objects."""
    sparse_row = {
        "ID": "117",
        "Display_lang": "Tough Jerky",
        "OverallQualityID": "1",
        "ExpansionID": "0",
        "ItemLevel": "5",
    }
    metadata_row = {"ClassID": "0", "SubclassID": "5"}

    item = WoWItem.from_rows(sparse_row, metadata_row, "Food")

    assert item.id == 117
    assert item.name == "Tough Jerky"
    assert item.quality == ItemQuality.COMMON

    item_dict = item.to_dict()
    assert item_dict["Expansion"] == "CLASSIC"
    assert item_dict["Class"] == "CONSUMABLE"


# --- CORE TESTS ---


def test_csv_row_counting(mock_directories):
    """Tests the internal CSV line counter."""
    raw_dir, _ = mock_directories
    test_csv = raw_dir / "test.csv"
    test_csv.write_text("id,name\n1,item1\n2,item2\n3,item3")

    extractor = WagoExtractor(raw_dir=str(raw_dir))
    assert extractor._count_csv_rows(test_csv) == 3


@patch("requests.get")
def test_downloader(mock_get, mock_directories):
    """Tests the downloader with a mocked network response."""
    raw_dir, processed_dir = mock_directories

    mock_response = MagicMock()
    mock_response.headers = {"content-length": "12"}
    mock_response.iter_content.return_value = [b"chunk1", b"chunk2"]
    mock_get.return_value = mock_response

    extractor = WagoExtractor(output_dir=str(processed_dir), raw_dir=str(raw_dir))
    progress_mock = MagicMock()

    file_path = extractor._download_table_with_progress("Item", progress_mock, "task1")

    assert file_path.exists()
    mock_get.assert_called_once()


def test_relational_join_logic(mock_directories):
    """Tests the complex indexing logic that joins ItemEffect to SpellCategory."""
    raw_dir, processed_dir = mock_directories

    item_path = raw_dir / "Item.csv"
    item_path.write_text("ID,ClassID,SubclassID\n500,0,1")

    sparse_path = raw_dir / "ItemSparse.csv"
    sparse_path.write_text(
        "ID,Display_lang,ExpansionID,OverallQualityID,ItemLevel\n500,Super Pot,9,1,1"
    )

    effect_path = raw_dir / "ItemEffect.csv"
    effect_path.write_text("ID,SpellCategoryID\n10,88")

    spell_cat_path = raw_dir / "SpellCategory.csv"
    spell_cat_path.write_text("ID,Name_lang\n88,Healing Potion")

    join_path = raw_dir / "ItemXItemEffect.csv"
    join_path.write_text("ItemID,ItemEffectID\n500,10")

    extractor = WagoExtractor(output_dir=str(processed_dir), raw_dir=str(raw_dir))

    table_path_map = {
        "Item": item_path,
        "ItemSparse": sparse_path,
        "ItemEffect": effect_path,
        "SpellCategory": spell_cat_path,
        "ItemXItemEffect": join_path,
    }

    with patch.object(
        WagoExtractor,
        "_download_table_with_progress",
        side_effect=lambda name, prog, tid: table_path_map[name],
    ):
        extractor.run(["potion"])

    processed_file = processed_dir / "potion.csv"

    assert processed_file.exists(), f"Output file not found at {processed_file}"
    file_content = processed_file.read_text()
    assert "Healing Potion" in file_content
    assert "Super Pot" in file_content


# --- CLI TESTS ---


def test_cli_normalization_logic():
    """Tests that CLI arguments are correctly normalized (singular/plural)."""
    test_args = ["wago-extract", "-c", "weapons", "potions", "PLATE"]

    with patch.object(sys, "argv", test_args):
        with patch("wago_extractor.cli.WagoExtractor") as mock_extractor:
            main()

            called_categories = mock_extractor.return_value.run.call_args[1]["target_categories"]

            assert "weapon" in called_categories
            assert "potion" in called_categories
            assert "plate" in called_categories


def test_cli_lua_flag():
    """Tests that the --lua flag is correctly passed to the extractor."""
    test_args = ["wago-extract", "-c", "food", "--lua"]

    with patch.object(sys, "argv", test_args):
        with patch("wago_extractor.cli.WagoExtractor") as mock_extractor:
            main()
            mock_extractor.return_value.run.assert_called()
            kwargs = mock_extractor.return_value.run.call_args[1]
            assert kwargs["export_lua"] is True
