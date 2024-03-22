import pytest

from subgrounds import Subgrounds

# A market maker with title == None
MARKET_MAKER_ID_TITLE_NULL = "0x0accd621cbf9380019b887caf4e4e35a45dc1b7f"


def sg():
    return Subgrounds()

@pytest.fixture()
def omen_xdai_subgraph(sg):
    OMEN_TRADES_SUBGRAPH = "https://api.thegraph.com/subgraphs/name/protofire/omen-xdai"
    subgraph = sg.load_subgraph(OMEN_TRADES_SUBGRAPH)
    return subgraph


def test_null_filter(sg, omen_xdai_subgraph):
    """
    This test demonstrates the usage of a null-filter, i.e. a condition where
    or "columnA is not None".
    """

    filter_null = omen_xdai_subgraph.FixedProductMarketMaker.title == None
    filter_ids = omen_xdai_subgraph.FixedProductMarketMaker.id == MARKET_MAKER_ID_TITLE_NULL

    trades_by_id_title_null = omen_xdai_subgraph.Query.fixedProductMarketMakers(
        where=[filter_null, filter_ids],
        first=10
    )
    df = sg.query_df([trades_by_id_title_null.id, trades_by_id_title_null.title])  # type: ignore
    assert len(df) == 1

def test_not_null_filter(sg, omen_xdai_subgraph):
    """
    This test demonstrates the usage of a null-filter, i.e. a condition where
    or "columnA is not None".
    """

    filter_not_null = omen_xdai_subgraph.FixedProductMarketMaker.title != None
    filter_ids = omen_xdai_subgraph.FixedProductMarketMaker.id == MARKET_MAKER_ID_TITLE_NULL

    trades_by_id_title_not_null = omen_xdai_subgraph.Query.fixedProductMarketMakers(
        where=[filter_not_null, filter_ids],
        first=10
    )
    df = sg.query_df([trades_by_id_title_not_null.id, trades_by_id_title_not_null.title])  # type: ignore
    assert len(df) == 0

    trades_by_id = omen_xdai_subgraph.Query.fixedProductMarketMakers(
        where=[filter_ids],
        first=10
    )
    df = sg.query_df([trades_by_id.id, trades_by_id.title])  # type: ignore
    assert len(df) == 1
