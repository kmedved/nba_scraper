# Changelog

## 1.2.1

* Added compatibility columns (`event_team`, `event_type_de`, player team IDs, turnover/steal/block flags, `ft_n`/`ft_m`, `season`) to both CDN and legacy v2 parsers for use with `nba_parser`.
* Populated lineup name columns (`home_player_1`…`away_player_5`) alongside existing ID fields in the lineup builder.
* Provided structured free-throw trip metadata and canonical season detection for all events.
* Bundled an updated `nba_parser` (0.2.2) with resilient scoremargin handling and structured free-throw possession detection.
* Added an integration smoke test to ensure the canonical dataframe round-trips through `nba_parser.PbP`.
* Documented the canonical schema and nba_parser workflow in the README.
