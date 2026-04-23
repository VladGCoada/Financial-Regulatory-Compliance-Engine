from unittest.mock import MagicMock, patch
from frce.clients.ecb_client import EcbClient


def _mock_ecb_response(ccy: str) -> dict:
    return {
        "dataSets": [{"series": {"0:0:0:0:0": {"observations": {"0": [1.08]}}}}],
        "structure": {
            "dimensions": {
                "observation": [{"values": [{"id": "2024-01-15"}]}]
            }
        },
    }


def test_ecb_client_parses_rate():
    client = EcbClient()
    mock_resp = MagicMock()
    mock_resp.json.return_value = _mock_ecb_response("USD")
    mock_resp.raise_for_status.return_value = None

    with patch("frce.clients.ecb_client.requests.get", return_value=mock_resp):
        rates = client.get_rates(["USD"])

    assert len(rates) == 1
    assert rates[0]["target_currency"] == "USD"
    assert rates[0]["rate"] == 1.08
    assert rates[0]["base_currency"] == "EUR"


def test_ecb_client_handles_network_failure():
    client = EcbClient()
    with patch("frce.clients.ecb_client.requests.get", side_effect=ConnectionError("down")):
        rates = client.get_rates(["GBP"])
    assert rates == []