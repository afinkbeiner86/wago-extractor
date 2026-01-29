import sys
from unittest.mock import MagicMock, patch

import pytest

from wago_extractor.cli import main
from wago_extractor.core import WagoExtractor
from wago_extractor.models import Expansion, ItemQuality, WoWItem


@pytest.fixture
def mock_dirs(tmp_path):
    """Provides temporary paths for raw and processed data."""
    raw = tmp_path / "raw"
    proc = tmp_path / "processed"
    raw.mkdir()
    proc.mkdir()
    return raw, proc


# --- MODELS TESTS ---


def test_expansion_logic():
    """Tests the Expansion enum and its safety helper."""
    assert Expansion.get_name(0) == "CLASSIC"
    assert Expansion.get_name(10) == "THE_WAR_WITHIN"
    assert Expansion.get_name(99) == "UNKNOWN_99"


def test_wow_item_transformation():
    """Tests the conversion from CSV rows to WoWItem objects."""
    sparse = {
        "ID": "117",
        "Display_lang": "Tough Jerky",
        "OverallQualityID": "1",
        "ExpansionID": "0",
        "ItemLevel": "5",
    }
    meta = {"ClassID": "0", "SubclassID": "5"}

    item = WoWItem.from_rows(sparse, meta, "Food")

    assert item.id == 117
    assert item.name == "Tough Jerky"
    assert item.quality == ItemQuality.COMMON

    d = item.to_dict()
    assert d["Expansion"] == "CLASSIC"
    assert d["Class"] == "CONSUMABLE"


# --- CORE TESTS ---


def test_csv_row_counting(mock_dirs):
    """Tests the internal CSV line counter."""
    raw_dir, _ = mock_dirs
    test_csv = raw_dir / "test.csv"
    test_csv.write_text("id,name\n1,item1\n2,item2\n3,item3")

    extractor = WagoExtractor(raw_dir=str(raw_dir))
    assert extractor._count_csv_rows(test_csv) == 3


@patch("requests.get")
def test_downloader(mock_get, mock_dirs):
    """Tests the downloader with a mocked network response."""
    raw_dir, proc_dir = mock_dirs

    mock_resp = MagicMock()
    mock_resp.headers = {"content-length": "12"}
    mock_resp.iter_content.return_value = [b"chunk1", b"chunk2"]
    mock_get.return_value = mock_resp

    extractor = WagoExtractor(output_dir=str(proc_dir), raw_dir=str(raw_dir))
    progress = MagicMock()

    path = extractor._download_table_rich("Item", progress, "task1")

    assert path.exists()
    mock_get.assert_called_once()


def test_relational_join_logic(mock_dirs):
    """Tests the complex indexing logic that joins ItemEffect to SpellCategory."""
    raw_dir, proc_dir = mock_dirs

    # 1. Create the dummy files
    item_path = raw_dir / "Item.csv"
    item_path.write_text("ID,ClassID,SubclassID\n500,0,1")

    sparse_path = raw_dir / "ItemSparse.csv"
    sparse_path.write_text("ID,Display_lang,ExpansionID\n500,Super Pot,9")

    effect_path = raw_dir / "ItemEffect.csv"
    effect_path.write_text("ID,SpellCategoryID\n10,88")

    scat_path = raw_dir / "SpellCategory.csv"
    scat_path.write_text("ID,Name_lang\n88,Healing Potion")

    join_path = raw_dir / "ItemXItemEffect.csv"
    join_path.write_text("ItemID,ItemEffectID\n500,10")

    extractor = WagoExtractor(output_dir=str(proc_dir), raw_dir=str(raw_dir))

    # 2. Map table names to the paths we created
    path_map = {
        "Item": item_path,
        "ItemSparse": sparse_path,
        "ItemEffect": effect_path,
        "SpellCategory": scat_path,
        "ItemXItemEffect": join_path,
    }

    # 3. Use side_effect to return the correct Path for each call
    with patch.object(
        WagoExtractor, "_download_table_rich", side_effect=lambda name, prog, tid: path_map[name]
    ):
        extractor.run(["potions"])

    processed_file = proc_dir / "potions.csv"
    assert processed_file.exists()
    content = processed_file.read_text()
    assert "Healing Potion" in content
    assert "Super Pot" in content


# --- CLI TESTS ---


def test_cli_flags():
    """Tests that CLI arguments correctly influence the extractor run."""
    args = ["wago-extract", "--lua", "--weapons"]

    with patch.object(sys, "argv", args):
        with patch("wago_extractor.cli.WagoExtractor") as mock_extractor:
            main()
            mock_extractor.return_value.run.assert_called_with(["weapon"], True)


def test_cli_defaults():
    """Tests default category selection."""
    args = ["wago-extract"]

    with patch.object(sys, "argv", args):
        with patch("wago_extractor.cli.WagoExtractor") as mock_extractor:
            main()
            expected_cats = ["food", "drinks", "potions"]
            mock_extractor.return_value.run.assert_called_with(expected_cats, False)
