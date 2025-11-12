from .pbp import PbP

try:
    from .playertotals import PlayerTotals  # optional (requires scikit-learn)
    from .teamtotals import TeamTotals  # optional (requires scikit-learn)
except Exception:  # pragma: no cover - fallback path when sklearn missing
    PlayerTotals = None
    TeamTotals = None

__all__ = ["PbP", "PlayerTotals", "TeamTotals"]
__version__ = "0.2.2"
