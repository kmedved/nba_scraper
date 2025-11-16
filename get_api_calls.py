'''
script to write api calls to json for unittests
'''
import json
import nba_scraper.scrape_functions as sf


HOME_DICT, AWAY_DICT = sf.get_lineup_api('2017-18', 1610612739, 1610612738,
                                         'Regular+Season', 1, '2017-10-17')
V2_DICT, PBP_DICT = sf.get_pbp_api('201718', '2017',
                                   '0021700001', 'Regular+Season')

with open('home_dict.json', 'w', encoding='utf-8') as home:
    json.dump(HOME_DICT, home, ensure_ascii=False)

with open('away_dict.json', 'w', encoding='utf-8') as away:
    json.dump(AWAY_DICT, away, ensure_ascii=False)

with open('v2_dict.json', 'w', encoding='utf-8') as v2:
    json.dump(V2_DICT, v2, ensure_ascii=False)

with open('pbp_dict.json', 'w', encoding='utf-8') as pbp:
    json.dump(PBP_DICT, pbp, ensure_ascii=False)
