"""Source routing helpers for the unified parsing pipeline."""
from __future__ import annotations

import json
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Union

import pandas as pd

from . import cdn_client, cdn_parser, lineup_builder, v2_parser


class SourceKind(str, Enum):
    CDN_REMOTE = "cdn_remote"
    CDN_LOCAL = "cdn_local"
    V2_LOCAL = "v2_local"
    V2_DICT = "v2_dict"


def load_json(path_or_dict: Union[str, Path, Dict[str, Any]], kind: SourceKind) -> Dict[str, Any]:
    if kind == SourceKind.V2_DICT and isinstance(path_or_dict, dict):
        return path_or_dict
    if kind in {SourceKind.CDN_LOCAL, SourceKind.V2_LOCAL}:
        path = Path(path_or_dict)
        with path.open("r", encoding="utf-8") as fp:
            return json.load(fp)
    raise ValueError(f"Unsupported load_json kind: {kind}")


def parse_any(
    game_ref: Union[str, Tuple[Union[str, Path], Optional[Union[str, Path]]], Dict[str, Any]],
    kind: SourceKind,
    mapping_yaml_path: Optional[str] = None,
) -> pd.DataFrame:
    if kind == SourceKind.CDN_REMOTE:
        if not isinstance(game_ref, str):
            raise TypeError("CDN_REMOTE requires a game id string")
        pbp_json = cdn_client.fetch_pbp(game_ref)
        box_json = None
        try:
            box_json = cdn_client.fetch_box(game_ref)
        except Exception:
            box_json = None
        df = cdn_parser.parse_actions_to_rows(pbp_json, box_json or {}, mapping_yaml_path)
        return lineup_builder.attach_lineups(
            df, box_json=box_json, pbp_json=pbp_json
        )

    if kind == SourceKind.CDN_LOCAL:
        if not isinstance(game_ref, (tuple, list)) or len(game_ref) != 2:
            raise TypeError("CDN_LOCAL requires (pbp_path, box_path)")
        pbp_ref, box_ref = game_ref
        pbp_json = (
            pbp_ref
            if isinstance(pbp_ref, dict)
            else load_json(pbp_ref, SourceKind.CDN_LOCAL)
        )
        if box_ref is None:
            raise TypeError("CDN_LOCAL requires box score path")
        box_json = (
            box_ref
            if isinstance(box_ref, dict)
            else load_json(box_ref, SourceKind.CDN_LOCAL)
        )
        df = cdn_parser.parse_actions_to_rows(pbp_json, box_json, mapping_yaml_path)
        return lineup_builder.attach_lineups(
            df, box_json=box_json, pbp_json=pbp_json
        )

    if kind == SourceKind.V2_LOCAL:
        if not isinstance(game_ref, (str, Path)):
            raise TypeError("V2_LOCAL requires a path")
        v2_json = load_json(game_ref, SourceKind.V2_LOCAL)
        df = v2_parser.parse_v2_to_rows(v2_json, mapping_yaml_path)
        return lineup_builder.attach_lineups(df)

    if kind == SourceKind.V2_DICT:
        if not isinstance(game_ref, dict):
            raise TypeError("V2_DICT requires a dictionary")
        df = v2_parser.parse_v2_to_rows(game_ref, mapping_yaml_path)
        return lineup_builder.attach_lineups(df)

    raise ValueError(f"Unsupported source kind: {kind}")
