from pipeline.storage.schema import build_headers


def test_build_headers_matches_phase_one_data_model():
    assert build_headers() == {
        "seed_wallets.csv": ["wallet_address", "hex_address", "transaction_id", "source_name", "label", "created_at"],
        "accounts.csv": ["address", "hex_address", "first_seen_timestamp", "last_seen_timestamp", "created_date", "created_at"],
        "transactions.csv": ["tx_hash", "block_number", "block_timestamp", "from_address", "to_address", "token_symbol", "contract_address", "amount", "fee_trx", "created_at"],
        "wallet_hops.csv": ["hop_id", "wallet_address", "seed_address", "parent_address", "via_tx_hash", "hop_level", "direction", "discovered_at"],
        "pipeline_crawl_state.csv": ["address", "seed_address", "hop_level", "synced", "expanded", "is_supernode", "created_at", "updated_at"],
    }


def test_build_headers_returns_independent_mutable_values():
    first = build_headers()
    first["accounts.csv"].append("drift")
    assert "drift" not in build_headers()["accounts.csv"]
