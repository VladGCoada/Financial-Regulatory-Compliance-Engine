from __future__ import annotations


class TransparencyRegistry:
    def __init__(self) -> None:
        self._entries: list[dict] = []

    def register(self, entry: dict) -> dict:
        self._entries.append(entry)
        return entry

    def all(self) -> list[dict]:
        return list(self._entries)
