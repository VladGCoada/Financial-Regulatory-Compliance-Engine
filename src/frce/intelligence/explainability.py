from __future__ import annotations


def top_features(feature_importance: dict[str, float], n: int = 3) -> dict[str, float]:
    return dict(sorted(feature_importance.items(), key=lambda item: item[1], reverse=True)[:n])
