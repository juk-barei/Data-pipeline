"""Strict TRON Base58Check address conversion."""

from __future__ import annotations

import hashlib

_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_ALPHABET_INDEX = {char: index for index, char in enumerate(_ALPHABET)}


def _checksum(payload: bytes) -> bytes:
    return hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4]


def _base58_encode(data: bytes) -> str:
    number = int.from_bytes(data, "big")
    encoded = ""
    while number:
        number, remainder = divmod(number, 58)
        encoded = _ALPHABET[remainder] + encoded
    leading_zeroes = len(data) - len(data.lstrip(b"\x00"))
    return (_ALPHABET[0] * leading_zeroes) + encoded


def _base58_decode(value: str) -> bytes:
    number = 0
    for char in value:
        number = number * 58 + _ALPHABET_INDEX[char]
    decoded = number.to_bytes((number.bit_length() + 7) // 8, "big") if number else b""
    leading_zeroes = len(value) - len(value.lstrip(_ALPHABET[0]))
    return (b"\x00" * leading_zeroes) + decoded


def _normalize_hex(hex_addr: str) -> str:
    value = hex_addr.strip().lower()
    if value.startswith("0x"):
        value = value[2:]
    if len(value) == 40:
        value = "41" + value
    if len(value) != 42 or not value.startswith("41"):
        raise ValueError("not a TRON address")
    bytes.fromhex(value)
    return value


def to_base58(hex_addr: str) -> str:
    """Convert TRON hex to Base58Check, returning the input on failure."""

    try:
        normalized = _normalize_hex(hex_addr)
        payload = bytes.fromhex(normalized)
        return _base58_encode(payload + _checksum(payload))
    except (AttributeError, KeyError, TypeError, ValueError):
        return hex_addr


def to_hex(base58_addr: str) -> str:
    """Convert a valid TRON Base58Check address to lowercase hex."""

    try:
        decoded = _base58_decode(base58_addr.strip())
        payload, supplied_checksum = decoded[:-4], decoded[-4:]
        if len(payload) != 21 or payload[0] != 0x41:
            return ""
        if supplied_checksum != _checksum(payload):
            return ""
        return payload.hex()
    except (AttributeError, KeyError, TypeError, ValueError):
        return ""


def normalize_address(addr: str) -> str:
    """Return a canonical TRON Base58Check address or an empty string."""

    if not isinstance(addr, str):
        return ""
    value = addr.strip()
    if value.startswith("T"):
        return value if to_hex(value) else ""
    converted = to_base58(value)
    return converted if converted != value and to_hex(converted) else ""
