#w_config_mitchell.py - example with all three new fields
API_KEY = "00000000-0000-0000-0000-000000000000"
SECRET_KEY = """-----BEGIN RSA PRIVATE KEY-----
-----END RSA PRIVATE KEY-----"""
BET_SIZE_DOLLARS = 10.00
MAX_PRICE_CENTS = 99
# default bet size for this account|
# default price cap for this account
#=== OPTIONAL - leave any of these out to skip the filter ===
# Whitelist of bet types Mitchell wants to participate in.
# None / unset = ALL bet types allowed.
ALLOWED_BET_TYPES = [
"moneyline", 
"exact_match_no",
"match_total_over",
"match_total_under",
"game_handicap_yes",
"game_handicap_no",
"first_set_winner",
"set_2_winner",
"set_3_winner",
]
# Per-bet-type size override (falls back to BET_SIZE_DOLLARS if not listed)
BET_SIZES_BY_TYPE = {
        "moneyline": 50.00,
        "exact_match_no": 5.00,
        "game_handicap_no": 5.00,
        "match_total_under": 20.00,
}
# Per-bet-type price cap override (falls back to MAX_PRICE_CENTS if not listed)
BET_PRICES_BY_TYPE = {
"moneyline": 99,
"exact_match_no": 95,
# don't pay above 95¢ on these
}