"""Convert TronGrid records to the canonical CSV row schemas."""

from __future__ import annotations

from decimal import Decimal

from pipeline.utils.address import normalize_address, to_hex
from pipeline.utils.timeutils import format_iso, now_iso, parse_timestamp


def parse_account(raw: dict) -> dict:
    """Normalize an account record; return an empty row for invalid addresses."""
    if not isinstance(raw, dict):
        return {}
    address = normalize_address(raw.get("address"))
    if not address:
        return {}
    first_seen = format_iso(parse_timestamp(raw.get("create_time")))
    last_seen = format_iso(parse_timestamp(raw.get("latest_opration_time")))
    return {
        "address": address,
        "hex_address": to_hex(address),
        "first_seen_timestamp": first_seen,
        "last_seen_timestamp": last_seen,
        "created_date": first_seen[:10],
        "created_at": now_iso(),
    }


def parse_trx_transaction(raw: dict) -> dict | None:
    """Normalize a successful native TransferContract; skip unusable records."""
    if not isinstance(raw, dict):
        return None
    tx_hash = raw.get("txID")
    timestamp = format_iso(parse_timestamp(raw.get("block_timestamp")))
    if not isinstance(tx_hash, str) or not tx_hash.strip() or not timestamp:
        return None
    raw_data = raw.get("raw_data")
    if not isinstance(raw_data, dict):
        return None
    contracts = raw_data.get("contract")
    if not isinstance(contracts, list) or len(contracts) != 1:
        return None
    contract = contracts[0]
    if not isinstance(contract, dict) or contract.get("type") != "TransferContract":
        return None
    parameter = contract.get("parameter")
    if not isinstance(parameter, dict) or not isinstance(parameter.get("value"), dict):
        return None
    value = parameter["value"]
    sender = normalize_address(value.get("owner_address"))
    receiver = normalize_address(value.get("to_address"))
    results = raw.get("ret", [])
    if not isinstance(results, list) or any(not isinstance(item, dict) for item in results):
        return None
    if any(item.get("contractRet", "SUCCESS") != "SUCCESS" for item in results):
        return None
    amount = value.get("amount")
    fee = raw.get("fee", results[0].get("fee", 0) if results else 0)
    for number in (amount, fee):
        if type(number) is int and number >= 0:
            continue
        if isinstance(number, str) and number.isascii() and number.isdigit():
            continue
        return None
    if not sender or not receiver:
        return None
    return {
        "tx_hash": tx_hash,
        "block_number": raw.get("blockNumber", ""),
        "block_timestamp": timestamp,
        "from_address": sender,
        "to_address": receiver,
        "token_symbol": "TRX",
        "contract_address": "",
        "amount": format(Decimal(str(amount)) / 1_000_000, "f"),
        "fee_trx": format(Decimal(str(fee)) / 1_000_000, "f"),
        "created_at": now_iso(),
    }
