"""Tennis player mappings and league registry for polybot2.

Single source of truth for all tennis configuration:
- PLAYER_MAP_TENNIS: global player map (all players, all tournaments)
- TENNIS_LEAGUES: tournament registry (aliases, PM league code, sets_to_win)

To add a new tournament: add one entry to TENNIS_LEAGUES.
To add a new player: add one entry to PLAYER_MAP_TENNIS.
No other files need to change.
"""

PLAYER_MAP_TENNIS = {
    "aboian, valerio": {
        "polymarket_code": "aboian",
        "provider_aliases": {"kalstrop_v1": ["Aboian, Valerio"]},
        "pm_aliases": ["Valerio Aboian"],
    },
    "added, dan": {
        "polymarket_code": "added",
        "provider_aliases": {"kalstrop_v1": ["Added, Dan"]},
        "pm_aliases": ["Dan Added"],
    },
    "adeshina, esther": {
        "polymarket_code": "adeshin",
        "provider_aliases": {"kalstrop_v1": ["Adeshina, Esther"]},
        "pm_aliases": ["Esther Adeshina"],
    },
    "agamenone, franco": {
        "polymarket_code": "agameno",
        "provider_aliases": {"kalstrop_v1": ["Agamenone, Franco"]},
        "pm_aliases": ["Franco Agamenone"],
    },
    "aguilar cardozo, joaquin": {
        "polymarket_code": "aguil",
        "provider_aliases": {"kalstrop_v1": ["Aguilar Cardozo, Joaquin"]},
        "pm_aliases": ["Joaquin Aguilar"],
    },
    "alexandrova, ekaterina": {
        "polymarket_code": "alexand",
        "provider_aliases": {"kalstrop_v1": ["Alexandrova, Ekaterina"]},
        "pm_aliases": ["Ekaterina Alexandrova"],
    },
    "alkaya, mert": {
        "polymarket_code": "alkaya",
        "provider_aliases": {"kalstrop_v1": ["Alkaya, Mert"]},
        "pm_aliases": ["Mert Alkaya"],
    },
    "altmaier, daniel": {
        "polymarket_code": "altmaie",
        "provider_aliases": {"kalstrop_v1": ["Altmaier, Daniel"]},
        "pm_aliases": ["Daniel Altmaier"],
    },
    "ambrogi, luciano emanuel": {
        "polymarket_code": "ambrogi",
        "provider_aliases": {"kalstrop_v1": ["Ambrogi, Luciano Emanuel"]},
        "pm_aliases": ["Luciano Emanuel Ambrogi"],
    },
    "andreescu, bianca": {
        "polymarket_code": "andrees",
        "provider_aliases": {"kalstrop_v1": ["Andreescu, Bianca"]},
        "pm_aliases": ["Bianca Andreescu"],
    },
    "andreeva, erika": {
        "polymarket_code": "andree",
        "provider_aliases": {"kalstrop_v1": ["Andreeva, Erika"]},
        "pm_aliases": ["Erika Andreeva"],
    },
    "andreeva, mirra": {
        "polymarket_code": "andreev",
        "provider_aliases": {"kalstrop_v1": ["Andreeva, Mirra"]},
        "pm_aliases": ["Mirra Andreeva"],
    },
    "angelini, lorenzo": {
        "polymarket_code": "angelin",
        "provider_aliases": {"kalstrop_v1": ["Angelini, Lorenzo"]},
        "pm_aliases": ["Lorenzo Angelini"],
    },
    "anisimova, amanda": {
        "polymarket_code": "anisimo",
        "provider_aliases": {"kalstrop_v1": ["Anisimova, Amanda"]},
        "pm_aliases": ["Amanda Anisimova"],
    },
    "appleton, emily": {
        "polymarket_code": "appleto",
        "provider_aliases": {"kalstrop_v1": ["Appleton, Emily"]},
        "pm_aliases": ["Emily Appleton"],
    },
    "arango, emiliana": {
        "polymarket_code": "arango",
        "provider_aliases": {"kalstrop_v1": ["Arango, Emiliana"]},
        "pm_aliases": ["Emiliana Arango"],
    },
    "arnaldi, matteo": {
        "polymarket_code": "arnaldi",
        "provider_aliases": {"kalstrop_v1": ["Arnaldi, Matteo"]},
        "pm_aliases": ["Matteo Arnaldi"],
    },
    "astakhova, darya": {
        "polymarket_code": "astakho",
        "provider_aliases": {"kalstrop_v1": ["Astakhova, Darya"]},
        "pm_aliases": ["Darya Astakhova"],
    },
    "atmane, terence": {
        "polymarket_code": "atmane",
        "provider_aliases": {"kalstrop_v1": ["Atmane, Terence"]},
        "pm_aliases": ["Terence Atmane"],
    },
    "auger-aliassime, felix": {
        "polymarket_code": "augeral",
        "provider_aliases": {"kalstrop_v1": ["Auger-Aliassime, Felix"]},
        "pm_aliases": ["Felix Auger-Aliassime"],
    },
    "avanesyan, elina": {
        "polymarket_code": "avanesy",
        "provider_aliases": {"kalstrop_v1": ["Avanesyan, Elina"]},
        "pm_aliases": ["Elina Avanesyan"],
    },
    "badosa, paula": {
        "polymarket_code": "badosa",
        "provider_aliases": {"kalstrop_v1": ["Badosa, Paula"]},
        "pm_aliases": ["Paula Badosa"],
    },
    "baez, sebastian": {
        "polymarket_code": "baez",
        "provider_aliases": {"kalstrop_v1": ["Baez, Sebastian"]},
        "pm_aliases": ["Sebastian Baez"],
    },
    "bai, zhuoxuan": {
        "polymarket_code": "bai",
        "provider_aliases": {"kalstrop_v1": ["Bai, Zhuoxuan"]},
        "pm_aliases": ["Zhuoxuan Bai"],
    },
    "balshaw, felix": {
        "polymarket_code": "balshaw",
        "provider_aliases": {"kalstrop_v1": ["Balshaw, Felix"]},
        "pm_aliases": ["Felix Balshaw"],
    },
    "barrena, alex": {
        "polymarket_code": "barrena",
        "provider_aliases": {"kalstrop_v1": ["Barrena, Alex"]},
        "pm_aliases": ["Alex Barrena"],
    },
    "barrios vera, marcelo tomas": {
        "polymarket_code": "barrios",
        "provider_aliases": {"kalstrop_v1": ["Barrios Vera, Marcelo Tomas"]},
        "pm_aliases": ["Tomas Barrios"],
    },
    "barthel, mona": {
        "polymarket_code": "barthel",
        "provider_aliases": {"kalstrop_v1": ["Barthel, Mona"]},
        "pm_aliases": ["Mona Barthel"],
    },
    "barton, hynek": {
        "polymarket_code": "barton",
        "provider_aliases": {"kalstrop_v1": ["Barton, Hynek"]},
        "pm_aliases": ["Hynek Barton"],
    },
    "bartunkova, nikola": {
        "polymarket_code": "bartunk",
        "provider_aliases": {"kalstrop_v1": ["Bartunkova, Nikola"]},
        "pm_aliases": ["Nikola Bartunkova"],
    },
    "basavareddy, nishesh": {
        "polymarket_code": "basavar",
        "provider_aliases": {"kalstrop_v1": ["Basavareddy, Nishesh"]},
        "pm_aliases": ["Nishesh Basavareddy"],
    },
    "basilashvili, nikoloz": {
        "polymarket_code": "basilas",
        "provider_aliases": {"kalstrop_v1": ["Basilashvili, Nikoloz"]},
        "pm_aliases": ["Nikoloz Basilashvili"],
    },
    "basile, pierluigi": {
        "polymarket_code": "basile",
        "provider_aliases": {"kalstrop_v1": ["Basile, Pierluigi"]},
        "pm_aliases": ["Pierluigi Basile"],
    },
    "basiletti, noemi": {
        "polymarket_code": "basilet",
        "provider_aliases": {"kalstrop_v1": ["Basiletti, Noemi"]},
        "pm_aliases": ["Noemi Basiletti"],
    },
    "basing, max": {
        "polymarket_code": "basing",
        "provider_aliases": {"kalstrop_v1": ["Basing, Max"]},
        "pm_aliases": ["Max Basing"],
    },
    "bassols ribera, marina": {
        "polymarket_code": "ribera",
        "provider_aliases": {"kalstrop_v1": ["Bassols Ribera, Marina"]},
        "pm_aliases": ["Marina Bassols Ribera"],
    },
    "bautista agut, roberto": {
        "polymarket_code": "agut",
        "provider_aliases": {"kalstrop_v1": ["Bautista Agut, Roberto"]},
        "pm_aliases": ["Roberto Bautista Agut"],
    },
    "bax, florent": {
        "polymarket_code": "bax",
        "provider_aliases": {"kalstrop_v1": ["Bax, Florent"]},
        "pm_aliases": ["Florent Bax"],
    },
    "begu, irina-camelia": {
        "polymarket_code": "begu",
        "provider_aliases": {"kalstrop_v1": ["Begu, Irina-Camelia"]},
        "pm_aliases": ["Irina-Camelia Begu"],
    },
    "bejlek, sara": {
        "polymarket_code": "bejlek",
        "provider_aliases": {"kalstrop_v1": ["Bejlek, Sara"]},
        "pm_aliases": ["Sara Bejlek"],
    },
    "belozertsev, nikita": {
        "polymarket_code": "bilozer",
        "provider_aliases": {"kalstrop_v1": ["Belozertsev, Nikita"]},
        "pm_aliases": ["Nikita Bilozertsev"],
    },
    "bellucci, mattia": {
        "polymarket_code": "bellucc",
        "provider_aliases": {"kalstrop_v1": ["Bellucci, Mattia"]},
        "pm_aliases": ["Mattia Bellucci"],
    },
    "bencic, belinda": {
        "polymarket_code": "bencic",
        "provider_aliases": {"kalstrop_v1": ["Bencic, Belinda"]},
        "pm_aliases": ["Belinda Bencic"],
    },
    "bergs, zizou": {
        "polymarket_code": "bergs",
        "provider_aliases": {"kalstrop_v1": ["Bergs, Zizou"]},
        "pm_aliases": ["Zizou Bergs"],
    },
    "berrettini, matteo": {
        "polymarket_code": "berrett",
        "provider_aliases": {"kalstrop_v1": ["Berrettini, Matteo"]},
        "pm_aliases": ["Matteo Berrettini"],
    },
    "bertola, remy": {
        "polymarket_code": "bertola",
        "provider_aliases": {"kalstrop_v1": ["Bertola, Remy"]},
        "pm_aliases": ["Remy Bertola"],
    },
    "birrell, kimberly": {
        "polymarket_code": "birrell",
        "provider_aliases": {"kalstrop_v1": ["Birrell, Kimberly"]},
        "pm_aliases": ["Kimberly Birrell"],
    },
    "blanch, dali": {
        "polymarket_code": "blan",
        "provider_aliases": {"kalstrop_v1": ["Blanch, Dali"]},
        "pm_aliases": ["Dali Blanch"],
    },
    "blanch, darwin": {
        "polymarket_code": "blanc",
        "provider_aliases": {"kalstrop_v1": ["Blanch, Darwin"]},
        "pm_aliases": ["Darwin Blanch"],
    },
    "blanchet, ugo": {
        "polymarket_code": "blanche",
        "provider_aliases": {"kalstrop_v1": ["Blanchet, Ugo"]},
        "pm_aliases": ["Ugo Blanchet"],
    },
    "blinkova, anna": {
        "polymarket_code": "blinkov",
        "provider_aliases": {"kalstrop_v1": ["Blinkova, Anna"]},
        "pm_aliases": ["Anna Blinkova"],
    },
    "blockx, alexander": {
        "polymarket_code": "blockx",
        "provider_aliases": {"kalstrop_v1": ["Blockx, Alexander"]},
        "pm_aliases": ["Alexander Blockx"],
    },
    "boisson, lois": {
        "polymarket_code": "boisson",
        "provider_aliases": {"kalstrop_v1": ["Boisson, Lois"]},
        "pm_aliases": ["Lois Boisson"],
    },
    "boitan, gabi adrian": {
        "polymarket_code": "boitan",
        "provider_aliases": {"kalstrop_v1": ["Boitan, Gabi Adrian"]},
        "pm_aliases": ["Gabi Boitan"],
    },
    "bolkvadze, mariam": {
        "polymarket_code": "bolkvad",
        "provider_aliases": {"kalstrop_v1": ["Bolkvadze, Mariam"]},
        "pm_aliases": ["Mariam Bolkvadze"],
    },
    "bolt, alex": {
        "polymarket_code": "bolt",
        "provider_aliases": {"kalstrop_v1": ["Bolt, Alex"]},
        "pm_aliases": ["Alex Bolt"],
    },
    "bondar, anna": {
        "polymarket_code": "bondar",
        "provider_aliases": {"kalstrop_v1": ["Bondar, Anna"]},
        "pm_aliases": ["Anna Bondar"],
    },
    "bonding, oliver": {
        "polymarket_code": "bonding",
        "provider_aliases": {"kalstrop_v1": ["Bonding, Oliver"]},
        "pm_aliases": ["Oliver Bonding"],
    },
    "bondioli, federico": {
        "polymarket_code": "bondiol",
        "provider_aliases": {"kalstrop_v1": ["Bondioli, Federico"]},
        "pm_aliases": ["Federico Bondioli"],
    },
    "bonzi, benjamin": {
        "polymarket_code": "bonzi",
        "provider_aliases": {"kalstrop_v1": ["Bonzi, Benjamin"]},
        "pm_aliases": ["Benjamin Bonzi"],
    },
    "boogaard, thijs": {
        "polymarket_code": "boogaar",
        "provider_aliases": {"kalstrop_v1": ["Boogaard, Thijs"]},
        "pm_aliases": ["Thijs Boogaard"],
    },
    "borges, nuno": {
        "polymarket_code": "borges",
        "provider_aliases": {"kalstrop_v1": ["Borges, Nuno"]},
        "pm_aliases": ["Nuno Borges"],
    },
    "boscardin dias, pedro": {
        "polymarket_code": "dias",
        "provider_aliases": {"kalstrop_v1": ["Boscardin Dias, Pedro"]},
        "pm_aliases": ["Pedro Boscardin Dias"],
    },
    "bosio, victoria": {
        "polymarket_code": "bosio",
        "provider_aliases": {"kalstrop_v1": ["Bosio, Victoria"]},
        "pm_aliases": ["Victoria Bosio"],
    },
    "boulter, katie": {
        "polymarket_code": "boulter",
        "provider_aliases": {"kalstrop_v1": ["Boulter, Katie"]},
        "pm_aliases": ["Katie Boulter"],
    },
    "bouzas maneiro, jessica": {
        "polymarket_code": "maneiro",
        "provider_aliases": {"kalstrop_v1": ["Bouzas Maneiro, Jessica"]},
        "pm_aliases": ["Jessica Bouzas Maneiro"],
    },
    "bouzkova, marie": {
        "polymarket_code": "bouzkov",
        "provider_aliases": {"kalstrop_v1": ["Bouzkova, Marie"]},
        "pm_aliases": ["Marie Bouzkova"],
    },
    "boyer, tristan": {
        "polymarket_code": "boyer",
        "provider_aliases": {"kalstrop_v1": ["Boyer, Tristan"]},
        "pm_aliases": ["Tristan Boyer"],
    },
    "brace, cadence": {
        "polymarket_code": "brace",
        "provider_aliases": {"kalstrop_v1": ["Brace, Cadence"]},
        "pm_aliases": ["Cadence Brace"],
    },
    "brancaccio, nuria": {
        "polymarket_code": "brancac",
        "provider_aliases": {"kalstrop_v1": ["Brancaccio, Nuria"]},
        "pm_aliases": ["Nuria Brancaccio"],
    },
    "brancaccio, raul": {
        "polymarket_code": "brancac",
        "provider_aliases": {"kalstrop_v1": ["Brancaccio, Raul"]},
        "pm_aliases": ["Raul Brancaccio"],
    },
    "britton, daniella": {
        "polymarket_code": "britton",
        "provider_aliases": {"kalstrop_v1": ["Britton, Daniella"]},
        "pm_aliases": ["Daniella Britton"],
    },
    "broady, liam": {
        "polymarket_code": "broady",
        "provider_aliases": {"kalstrop_v1": ["Broady, Liam"]},
        "pm_aliases": ["Liam Broady"],
    },
    "brockmann, tessa johanna": {
        "polymarket_code": "brockma",
        "provider_aliases": {"kalstrop_v1": ["Brockmann, Tessa Johanna"]},
        "pm_aliases": ["Tessa Brockmann"],
    },
    "bronzetti, lucia": {
        "polymarket_code": "bronzet",
        "provider_aliases": {"kalstrop_v1": ["Bronzetti, Lucia"]},
        "pm_aliases": ["Lucia Bronzetti"],
    },
    "brooksby, jenson": {
        "polymarket_code": "brooksb",
        "provider_aliases": {"kalstrop_v1": ["Brooksby, Jenson"]},
        "pm_aliases": ["Jenson Brooksby"],
    },
    "broom, charles": {
        "polymarket_code": "broom",
        "provider_aliases": {"kalstrop_v1": ["Broom, Charles"]},
        "pm_aliases": ["Charles Broom"],
    },
    "brunclik, petr": {
        "polymarket_code": "bruncli",
        "provider_aliases": {"kalstrop_v1": ["Brunclik, Petr"]},
        "pm_aliases": ["Petr Brunclik"],
    },
    "bu, yunchaokete": {
        "polymarket_code": "bu",
        "provider_aliases": {"kalstrop_v1": ["Bu, Yunchaokete"]},
        "pm_aliases": ["Yunchaokete Bu"],
    },
    "bublik, alexander": {
        "polymarket_code": "bublik",
        "provider_aliases": {"kalstrop_v1": ["Bublik, Alexander"]},
        "pm_aliases": ["Alexander Bublik"],
    },
    "budkov kjaer, nicolai": {
        "polymarket_code": "kjaer",
        "provider_aliases": {"kalstrop_v1": ["Budkov Kjaer, Nicolai"]},
        "pm_aliases": ["Nicolai Budkov Kjaer"],
    },
    "bueno, gonzalo": {
        "polymarket_code": "bueno",
        "provider_aliases": {"kalstrop_v1": ["Bueno, Gonzalo"]},
        "pm_aliases": ["Gonzalo Bueno"],
    },
    "burrage, jodie": {
        "polymarket_code": "burrage",
        "provider_aliases": {"kalstrop_v1": ["Burrage, Jodie"]},
        "pm_aliases": ["Jodie Burrage"],
    },
    "burruchaga, roman andres": {
        "polymarket_code": "burruch",
        "provider_aliases": {"kalstrop_v1": ["Burruchaga, Roman Andres"]},
        "pm_aliases": ["Roman Andres Burruchaga"],
    },
    "buse, ignacio": {
        "polymarket_code": "buse",
        "provider_aliases": {"kalstrop_v1": ["Buse, Ignacio"]},
        "pm_aliases": ["Ignacio Buse"],
    },
    "butvilas, edas": {
        "polymarket_code": "butvila",
        "provider_aliases": {"kalstrop_v1": ["Butvilas, Edas"]},
        "pm_aliases": ["Edas Butvilas"],
    },
    "caniato, carlo alberto": {
        "polymarket_code": "caniato",
        "provider_aliases": {"kalstrop_v1": ["Caniato, Carlo Alberto"]},
        "pm_aliases": ["Carlo Alberto Caniato"],
    },
    "carballes baena, roberto": {
        "polymarket_code": "baena",
        "provider_aliases": {"kalstrop_v1": ["Carballes Baena, Roberto"]},
        "pm_aliases": ["Roberto Carballes Baena"],
    },
    "carle, maria": {
        "polymarket_code": "carle",
        "provider_aliases": {"kalstrop_v1": ["Carle, Maria"]},
        "pm_aliases": ["Maria Lourdes Carle"],
    },
    "carreno busta, pablo": {
        "polymarket_code": "busta",
        "provider_aliases": {"kalstrop_v1": ["Carreno Busta, Pablo"]},
        "pm_aliases": ["Pablo Carreno Busta"],
    },
    "casanova, hernan": {
        "polymarket_code": "casanov",
        "provider_aliases": {"kalstrop_v1": ["Casanova, Hernan"]},
        "pm_aliases": ["Hernan Casanova"],
    },
    "cassone, murphy": {
        "polymarket_code": "cassone",
        "provider_aliases": {"kalstrop_v1": ["Cassone, Murphy"]},
        "pm_aliases": ["Murphy Cassone"],
    },
    "castelnuovo, luca": {
        "polymarket_code": "casteln",
        "provider_aliases": {"kalstrop_v1": ["Castelnuovo, Luca"]},
        "pm_aliases": ["Luca Castelnuovo"],
    },
    "ceban, mark": {
        "polymarket_code": "ceban",
        "provider_aliases": {"kalstrop_v1": ["Ceban, Mark"]},
        "pm_aliases": ["Mark Ceban"],
    },
    "cecchinato, marco": {
        "polymarket_code": "cecchin",
        "provider_aliases": {"kalstrop_v1": ["Cecchinato, Marco"]},
        "pm_aliases": ["Marco Cecchinato"],
    },
    "cerundolo, francisco": {
        "polymarket_code": "cerundo",
        "provider_aliases": {"kalstrop_v1": ["Cerundolo, Francisco"]},
        "pm_aliases": ["Francisco Cerundolo"],
    },
    "cerundolo, juan manuel": {
        "polymarket_code": "cerund",
        "provider_aliases": {"kalstrop_v1": ["Cerundolo, Juan Manuel"]},
        "pm_aliases": ["Juan Manuel Cerundolo"],
    },
    "charaeva, alina": {
        "polymarket_code": "charaev",
        "provider_aliases": {"kalstrop_v1": ["Charaeva, Alina"]},
        "pm_aliases": ["Alina Charaeva"],
    },
    "chidekh, clement": {
        "polymarket_code": "chidekh",
        "provider_aliases": {"kalstrop_v1": ["Chidekh, Clement"]},
        "pm_aliases": ["Clement Chidekh"],
    },
    "choinski, jan": {
        "polymarket_code": "choinsk",
        "provider_aliases": {"kalstrop_v1": ["Choinski, Jan"]},
        "pm_aliases": ["Jan Choinski"],
    },
    "chwalinska, maja": {
        "polymarket_code": "chwalin",
        "provider_aliases": {"kalstrop_v1": ["Chwalinska, Maja"]},
        "pm_aliases": ["Maja Chwalinska"],
    },
    "cigarran, thiago": {
        "polymarket_code": "cigarra",
        "provider_aliases": {"kalstrop_v1": ["Cigarran, Thiago"]},
        "pm_aliases": ["Thiago Cigarran"],
    },
    "cilic, marin": {
        "polymarket_code": "cilic",
        "provider_aliases": {"kalstrop_v1": ["Cilic, Marin"]},
        "pm_aliases": ["Marin Cilic"],
    },
    "cina, federico": {
        "polymarket_code": "cina",
        "provider_aliases": {"kalstrop_v1": ["Cina, Federico"]},
        "pm_aliases": ["Federico Cina"],
    },
    "cirstea, sorana": {
        "polymarket_code": "cirstea",
        "provider_aliases": {"kalstrop_v1": ["Cirstea, Sorana"]},
        "pm_aliases": ["Sorana Cirstea"],
    },
    "clarke, jay": {
        "polymarket_code": "clarke",
        "provider_aliases": {"kalstrop_v1": ["Clarke, Jay"]},
        "pm_aliases": ["Jay Clarke"],
    },
    "cobolli, flavio": {
        "polymarket_code": "cobolli",
        "provider_aliases": {"kalstrop_v1": ["Cobolli, Flavio"]},
        "pm_aliases": ["Flavio Cobolli"],
    },
    "collarini, andrea": {
        "polymarket_code": "collari",
        "provider_aliases": {"kalstrop_v1": ["Collarini, Andrea"]},
        "pm_aliases": ["Andrea Collarini"],
    },
    "collignon, raphael": {
        "polymarket_code": "collign",
        "provider_aliases": {"kalstrop_v1": ["Collignon, Raphael"]},
        "pm_aliases": ["Raphael Collignon"],
    },
    "comesana, francisco": {
        "polymarket_code": "comesan",
        "provider_aliases": {"kalstrop_v1": ["Comesana, Francisco"]},
        "pm_aliases": ["Francisco Comesana"],
    },
    "coppejans, kimmer": {
        "polymarket_code": "coppeja",
        "provider_aliases": {"kalstrop_v1": ["Coppejans, Kimmer"]},
        "pm_aliases": ["Kimmer Coppejans"],
    },
    "coria, federico": {
        "polymarket_code": "coria",
        "provider_aliases": {"kalstrop_v1": ["Coria, Federico"]},
        "pm_aliases": ["Federico Coria"],
    },
    "costoulas, sofia": {
        "polymarket_code": "costoul",
        "provider_aliases": {"kalstrop_v1": ["Costoulas, Sofia"]},
        "pm_aliases": ["Sofia Costoulas"],
    },
    "cristian, jaqueline": {
        "polymarket_code": "cristia",
        "provider_aliases": {"kalstrop_v1": ["Cristian, Jaqueline"]},
        "pm_aliases": ["Jaqueline Cristian"],
    },
    "cross, kayla": {
        "polymarket_code": "cross",
        "provider_aliases": {"kalstrop_v1": ["Cross, Kayla"]},
        "pm_aliases": ["Kayla Cross"],
    },
    "cundom, julian": {
        "polymarket_code": "cundom",
        "provider_aliases": {"kalstrop_v1": ["Cundom, Julian"]},
        "pm_aliases": ["Julian Cundom"],
    },
    "dalla valle, enrico": {
        "polymarket_code": "valle",
        "provider_aliases": {"kalstrop_v1": ["Dalla Valle, Enrico"]},
        "pm_aliases": ["Enrico Dalla Valle"],
    },
    "damas, miguel": {
        "polymarket_code": "damas",
        "provider_aliases": {"kalstrop_v1": ["Damas, Miguel"]},
        "pm_aliases": ["Miguel Damas"],
    },
    "damm jr, martin": {
        "polymarket_code": "damm",
        "provider_aliases": {"kalstrop_v1": ["Damm Jr, Martin"]},
        "pm_aliases": ["Martin Damm Jr"],
    },
    "daniel, taro": {
        "polymarket_code": "daniel",
        "provider_aliases": {"kalstrop_v1": ["Daniel, Taro"]},
        "pm_aliases": ["Taro Daniel"],
    },
    "darderi, luciano": {
        "polymarket_code": "darderi",
        "provider_aliases": {"kalstrop_v1": ["Darderi, Luciano"]},
        "pm_aliases": ["Luciano Darderi"],
    },
    "dart, harriet": {
        "polymarket_code": "dart",
        "provider_aliases": {"kalstrop_v1": ["Dart, Harriet"]},
        "pm_aliases": ["Harriet Dart"],
    },
    "davidovich fokina, alejandro": {
        "polymarket_code": "fokina",
        "provider_aliases": {"kalstrop_v1": ["Davidovich Fokina, Alejandro"]},
        "pm_aliases": ["Alejandro Davidovich Fokina"],
    },
    "day, kayla": {
        "polymarket_code": "day",
        "provider_aliases": {"kalstrop_v1": ["Day, Kayla"]},
        "pm_aliases": ["Kayla Day"],
    },
    "de jong, jesper": {
        "polymarket_code": "jong",
        "provider_aliases": {"kalstrop_v1": ["De Jong, Jesper"]},
        "pm_aliases": ["Jesper De Jong"],
    },
    "de minaur, alex": {
        "polymarket_code": "minaur",
        "provider_aliases": {"kalstrop_v1": ["de Minaur, Alex"]},
        "pm_aliases": ["Alex de Minaur"],
    },
    "dedura-palomero, diego": {
        "polymarket_code": "dedurap",
        "provider_aliases": {"kalstrop_v1": ["Dedura-Palomero, Diego"]},
        "pm_aliases": ["Diego Dedura-Palomero"],
    },
    "del pino, arklon": {
        "polymarket_code": "pin",
        "provider_aliases": {"kalstrop_v1": ["Del Pino, Arklon"]},
        "pm_aliases": ["Arklon Del Pino"],
    },
    "dellien, hugo": {
        "polymarket_code": "dellien",
        "provider_aliases": {"kalstrop_v1": ["Dellien, Hugo"]},
        "pm_aliases": ["Hugo Dellien"],
    },
    "den ouden, guy": {
        "polymarket_code": "ouden",
        "provider_aliases": {"kalstrop_v1": ["Den Ouden, Guy"]},
        "pm_aliases": ["Guy Den Ouden"],
    },
    "diallo, gabriel": {
        "polymarket_code": "diallo",
        "provider_aliases": {"kalstrop_v1": ["Diallo, Gabriel"]},
        "pm_aliases": ["Gabriel Diallo"],
    },
    "diaz acosta, facundo": {
        "polymarket_code": "acosta",
        "provider_aliases": {"kalstrop_v1": ["Diaz Acosta, Facundo"]},
        "pm_aliases": ["Facundo Acosta"],
    },
    "djokovic, novak": {
        "polymarket_code": "djokovi",
        "provider_aliases": {"kalstrop_v1": ["Djokovic, Novak"]},
        "pm_aliases": ["Novak Djokovic"],
    },
    "dodig, matej": {
        "polymarket_code": "dodig",
        "provider_aliases": {"kalstrop_v1": ["Dodig, Matej"]},
        "pm_aliases": ["Matej Dodig"],
    },
    "dodin, oceane": {
        "polymarket_code": "dodin",
        "provider_aliases": {"kalstrop_v1": ["Dodin, Oceane"]},
        "pm_aliases": ["Oceane Dodin"],
    },
    "dolehide, caroline": {
        "polymarket_code": "dolehid",
        "provider_aliases": {"kalstrop_v1": ["Dolehide, Caroline"]},
        "pm_aliases": ["Caroline Dolehide"],
    },
    "donski, alexander": {
        "polymarket_code": "donski",
        "provider_aliases": {"kalstrop_v1": ["Donski, Alexander"]},
        "pm_aliases": ["Alexander Donski"],
    },
    "dougaz, aziz": {
        "polymarket_code": "dougaz",
        "provider_aliases": {"kalstrop_v1": ["Dougaz, Aziz"]},
        "pm_aliases": ["Aziz Dougaz"],
    },
    "draper, jack": {
        "polymarket_code": "draper",
        "provider_aliases": {"kalstrop_v1": ["Draper, Jack"]},
        "pm_aliases": ["Jack Draper"],
    },
    "draxl, liam": {
        "polymarket_code": "draxl",
        "provider_aliases": {"kalstrop_v1": ["Draxl, Liam"]},
        "pm_aliases": ["Liam Draxl"],
    },
    "droguet, titouan": {
        "polymarket_code": "droguet",
        "provider_aliases": {"kalstrop_v1": ["Droguet, Titouan"]},
        "pm_aliases": ["Titouan Droguet"],
    },
    "duckworth, james": {
        "polymarket_code": "duckwor",
        "provider_aliases": {"kalstrop_v1": ["Duckworth, James"]},
        "pm_aliases": ["James Duckworth"],
    },
    "dudeney, alicia": {
        "polymarket_code": "dudeney",
        "provider_aliases": {"kalstrop_v1": ["Dudeney, Alicia"]},
        "pm_aliases": ["Alicia Dudeney"],
    },
    "dunne, katy": {
        "polymarket_code": "dunne",
        "provider_aliases": {"kalstrop_v1": ["Dunne, Katy"]},
        "pm_aliases": ["Katy Dunne"],
    },
    "durasovic, viktor": {
        "polymarket_code": "durasov",
        "provider_aliases": {"kalstrop_v1": ["Durasovic, Viktor"]},
        "pm_aliases": ["Viktor Durasovic"],
    },
    "dutra da silva, daniel": {
        "polymarket_code": "silva",
        "provider_aliases": {"kalstrop_v1": ["Dutra Da Silva, Daniel"]},
        "pm_aliases": ["Daniel Dutra da Silva"],
    },
    "dzumhur, damir": {
        "polymarket_code": "dzumhur",
        "provider_aliases": {"kalstrop_v1": ["Dzumhur, Damir"]},
        "pm_aliases": ["Damir Dzumhur"],
    },
    "eala, alexandra": {
        "polymarket_code": "eala",
        "provider_aliases": {"kalstrop_v1": ["Eala, Alexandra"]},
        "pm_aliases": ["Alexandra Eala"],
    },
    "echargui, moez": {
        "polymarket_code": "echargu",
        "provider_aliases": {"kalstrop_v1": ["Echargui, Moez"]},
        "pm_aliases": ["Moez Echargui"],
    },
    "erjavec, veronika": {
        "polymarket_code": "erjavec",
        "provider_aliases": {"kalstrop_v1": ["Erjavec, Veronika"]},
        "pm_aliases": ["Veronika Erjavec"],
    },
    "estevez, juan": {
        "polymarket_code": "estevez",
        "provider_aliases": {"kalstrop_v1": ["Estevez, Juan"]},
        "pm_aliases": ["Juan Estevez"],
    },
    "etcheverry, tomas martin": {
        "polymarket_code": "etcheve",
        "provider_aliases": {"kalstrop_v1": ["Etcheverry, Tomas Martin"]},
        "pm_aliases": ["Tomas Etcheverry"],
    },
    "evans, daniel": {
        "polymarket_code": "evans",
        "provider_aliases": {"kalstrop_v1": ["Evans, Daniel"]},
        "pm_aliases": ["Daniel Evans"],
    },
    "faria, jaime": {
        "polymarket_code": "faria",
        "provider_aliases": {"kalstrop_v1": ["Faria, Jaime"]},
        "pm_aliases": ["Jaime Faria"],
    },
    "faurel, thomas": {
        "polymarket_code": "faurel",
        "provider_aliases": {"kalstrop_v1": ["Faurel, Thomas"]},
        "pm_aliases": ["Thomas Faurel"],
    },
    "fearnley, jacob": {
        "polymarket_code": "fearnle",
        "provider_aliases": {"kalstrop_v1": ["Fearnley, Jacob"]},
        "pm_aliases": ["Jacob Fearnley"],
    },
    "fernandez, bruno": {
        "polymarket_code": "fernand",
        "provider_aliases": {"kalstrop_v1": ["Fernandez, Bruno"]},
        "pm_aliases": ["Bruno Fernandez"],
    },
    "fernandez, leylah": {
        "polymarket_code": "fernand",
        "provider_aliases": {"kalstrop_v1": ["Fernandez, Leylah"]},
        "pm_aliases": ["Leylah Fernandez"],
    },
    "ferrari, gianmarco": {
        "polymarket_code": "ferrari",
        "provider_aliases": {"kalstrop_v1": ["Ferrari, Gianmarco"]},
        "pm_aliases": ["Gianmarco Ferrari"],
    },
    "fery, arthur": {
        "polymarket_code": "fery",
        "provider_aliases": {"kalstrop_v1": ["Fery, Arthur"]},
        "pm_aliases": ["Arthur Fery"],
    },
    "ficovich, juan pablo": {
        "polymarket_code": "ficovic",
        "provider_aliases": {"kalstrop_v1": ["Ficovich, Juan Pablo"]},
        "pm_aliases": ["Juan Pablo Ficovich"],
    },
    "fita boluda, angela": {
        "polymarket_code": "boluda",
        "provider_aliases": {"kalstrop_v1": ["Fita Boluda, Angela"]},
        "pm_aliases": ["Angela Fita Boluda"],
    },
    "fomin, sergey": {
        "polymarket_code": "fomin",
        "provider_aliases": {"kalstrop_v1": ["Fomin, Sergey"]},
        "pm_aliases": ["Sergey Fomin"],
    },
    "fonseca, joao": {
        "polymarket_code": "fonseca",
        "provider_aliases": {"kalstrop_v1": ["Fonseca, Joao"]},
        "pm_aliases": ["Joao Fonseca"],
    },
    "forejtek, jonas": {
        "polymarket_code": "forejte",
        "provider_aliases": {"kalstrop_v1": ["Forejtek, Jonas"]},
        "pm_aliases": ["Jonas Forejtek"],
    },
    "forti, francesco": {
        "polymarket_code": "forti",
        "provider_aliases": {"kalstrop_v1": ["Forti, Francesco"]},
        "pm_aliases": ["Francesco Forti"],
    },
    "frech, magdalena": {
        "polymarket_code": "frech",
        "provider_aliases": {"kalstrop_v1": ["Frech, Magdalena"]},
        "pm_aliases": ["Magdalena Frech"],
    },
    "friedsam, anna-lena": {
        "polymarket_code": "friedsa",
        "provider_aliases": {"kalstrop_v1": ["Friedsam, Anna-Lena"]},
        "pm_aliases": ["Anna-Lena Friedsam"],
    },
    "fritz, taylor": {
        "polymarket_code": "fritz",
        "provider_aliases": {"kalstrop_v1": ["Fritz, Taylor"]},
        "pm_aliases": ["Taylor Fritz"],
    },
    "fruhvirtova, linda": {
        "polymarket_code": "fruhvir",
        "provider_aliases": {"kalstrop_v1": ["Fruhvirtova, Linda"]},
        "pm_aliases": ["Linda Fruhvirtova"],
    },
    "fucsovics, marton": {
        "polymarket_code": "fucsovi",
        "provider_aliases": {"kalstrop_v1": ["Fucsovics, Marton"]},
        "pm_aliases": ["Marton Fucsovics"],
    },
    "gadamauri, buvaysar": {
        "polymarket_code": "gadamau",
        "provider_aliases": {"kalstrop_v1": ["Gadamauri, Buvaysar"]},
        "pm_aliases": ["Buvaysar Gadamauri"],
    },
    "galan, daniel elahi": {
        "polymarket_code": "galan",
        "provider_aliases": {"kalstrop_v1": ["Galan, Daniel Elahi"]},
        "pm_aliases": ["Daniel Galan"],
    },
    "galarneau, alexis": {
        "polymarket_code": "galarne",
        "provider_aliases": {"kalstrop_v1": ["Galarneau, Alexis"]},
        "pm_aliases": ["Alexis Galarneau"],
    },
    "galfi, dalma": {
        "polymarket_code": "galfi",
        "provider_aliases": {"kalstrop_v1": ["Galfi, Dalma"]},
        "pm_aliases": ["Dalma Galfi"],
    },
    "gao, xinyu": {
        "polymarket_code": "gao",
        "provider_aliases": {"kalstrop_v1": ["Gao, Xinyu"]},
        "pm_aliases": ["Xinyu Gao"],
    },
    "garin, cristian": {
        "polymarket_code": "garin",
        "provider_aliases": {"kalstrop_v1": ["Garin, Cristian"]},
        "pm_aliases": ["Cristian Garin"],
    },
    "garland, joanna": {
        "polymarket_code": "garland",
        "provider_aliases": {"kalstrop_v1": ["Garland, Joanna"]},
        "pm_aliases": ["Joanna Garland"],
    },
    "gasanova, anastasia": {
        "polymarket_code": "gasanov",
        "provider_aliases": {"kalstrop_v1": ["Gasanova, Anastasia"]},
        "pm_aliases": ["Anastasia Gasanova"],
    },
    "gaston, hugo": {
        "polymarket_code": "gaston",
        "provider_aliases": {"kalstrop_v1": ["Gaston, Hugo"]},
        "pm_aliases": ["Hugo Gaston"],
    },
    "gaubas, vilius": {
        "polymarket_code": "gaubas",
        "provider_aliases": {"kalstrop_v1": ["Gaubas, Vilius"]},
        "pm_aliases": ["Vilius Gaubas"],
    },
    "gauff, coco": {
        "polymarket_code": "gauff",
        "provider_aliases": {"kalstrop_v1": ["Gauff, Coco"]},
        "pm_aliases": ["Coco Gauff"],
    },
    "gea, arthur": {
        "polymarket_code": "gea",
        "provider_aliases": {"kalstrop_v1": ["Gea, Arthur"]},
        "pm_aliases": ["Arthur Gea"],
    },
    "gentzsch, tom": {
        "polymarket_code": "gentzsc",
        "provider_aliases": {"kalstrop_v1": ["Gentzsch, Tom"]},
        "pm_aliases": ["Tom Gentzsch"],
    },
    "ghetu, gabriel": {
        "polymarket_code": "ghetu",
        "provider_aliases": {"kalstrop_v1": ["Ghetu, Gabriel"]},
        "pm_aliases": ["Gabriel Ghetu"],
    },
    "ghibaudo, antoine": {
        "polymarket_code": "ghibaud",
        "provider_aliases": {"kalstrop_v1": ["Ghibaudo, Antoine"]},
        "pm_aliases": ["Antoine Ghibaudo"],
    },
    "gibson, talia": {
        "polymarket_code": "gibson",
        "provider_aliases": {"kalstrop_v1": ["Gibson, Talia"]},
        "pm_aliases": ["Talia Gibson"],
    },
    "gill, felix": {
        "polymarket_code": "gill",
        "provider_aliases": {"kalstrop_v1": ["Gill, Felix"]},
        "pm_aliases": ["Felix Gill"],
    },
    "giron, marcos": {
        "polymarket_code": "giron",
        "provider_aliases": {"kalstrop_v1": ["Giron, Marcos"]},
        "pm_aliases": ["Marcos Giron"],
    },
    "goity zapico, segundo": {
        "polymarket_code": "zapico",
        "provider_aliases": {"kalstrop_v1": ["Goity Zapico, Segundo"]},
        "pm_aliases": ["Segundo Goity Zapico"],
    },
    "golubic, viktorija": {
        "polymarket_code": "golubic",
        "provider_aliases": {"kalstrop_v1": ["Golubic, Viktorija"]},
        "pm_aliases": ["Viktorija Golubic"],
    },
    "gombos, norbert": {
        "polymarket_code": "gombos",
        "provider_aliases": {"kalstrop_v1": ["Gombos, Norbert"]},
        "pm_aliases": ["Norbert Gombos"],
    },
    "gorgodze, ekaterine": {
        "polymarket_code": "gorgodz",
        "provider_aliases": {"kalstrop_v1": ["Gorgodze, Ekaterine"]},
        "pm_aliases": ["Ekaterine Gorgodze"],
    },
    "grabher, julia": {
        "polymarket_code": "grabher",
        "provider_aliases": {"kalstrop_v1": ["Grabher, Julia"]},
        "pm_aliases": ["Julia Grabher"],
    },
    "grant, tyra caterina": {
        "polymarket_code": "grant",
        "provider_aliases": {"kalstrop_v1": ["Grant, Tyra Caterina"]},
        "pm_aliases": ["Tyra Caterina Grant"],
    },
    "gray, alastair": {
        "polymarket_code": "_unknown",
        "provider_aliases": {"kalstrop_v1": ["Gray, Alastair"]},
        "pm_aliases": ["Alastair Gray"],
    },
    "grenier, hugo": {
        "polymarket_code": "grenier",
        "provider_aliases": {"kalstrop_v1": ["Grenier, Hugo"]},
        "pm_aliases": ["Hugo Grenier"],
    },
    "griekspoor, tallon": {
        "polymarket_code": "grieksp",
        "provider_aliases": {"kalstrop_v1": ["Griekspoor, Tallon"]},
        "pm_aliases": ["Tallon Griekspoor"],
    },
    "guerrieri, andrea": {
        "polymarket_code": "guerrie",
        "provider_aliases": {"kalstrop_v1": ["Guerrieri, Andrea"]},
        "pm_aliases": ["Andrea Guerrieri"],
    },
    "gueymard wayenburg, sascha": {
        "polymarket_code": "gueymar",
        "provider_aliases": {"kalstrop_v1": ["Gueymard Wayenburg, Sascha"]},
        "pm_aliases": ["Sascha Gueymard-Wayenburg"],
    },
    "guillen meza, alvaro": {
        "polymarket_code": "meza",
        "provider_aliases": {"kalstrop_v1": ["Guillen Meza, Alvaro"]},
        "pm_aliases": ["Alvaro Guillen Meza"],
    },
    "guo, hanyu": {
        "polymarket_code": "guo",
        "provider_aliases": {"kalstrop_v1": ["Guo, Hanyu"]},
        "pm_aliases": ["Hanyu Guo"],
    },
    "haddad maia, beatriz": {
        "polymarket_code": "maia",
        "provider_aliases": {"kalstrop_v1": ["Haddad Maia, Beatriz"]},
        "pm_aliases": ["Beatriz Haddad Maia"],
    },
    "haita, stefan horia": {
        "polymarket_code": "haita",
        "provider_aliases": {"kalstrop_v1": ["Haita, Stefan Horia"]},
        "pm_aliases": ["Stefan Haita"],
    },
    "halys, quentin": {
        "polymarket_code": "halys",
        "provider_aliases": {"kalstrop_v1": ["Halys, Quentin"]},
        "pm_aliases": ["Quentin Halys"],
    },
    "hanfmann, yannick": {
        "polymarket_code": "hanfman",
        "provider_aliases": {"kalstrop_v1": ["Hanfmann, Yannick"]},
        "pm_aliases": ["Yannick Hanfmann"],
    },
    "hardt, nick": {
        "polymarket_code": "hardt",
        "provider_aliases": {"kalstrop_v1": ["Hardt, Nick"]},
        "pm_aliases": ["Nick Hardt"],
    },
    "harris, billy": {
        "polymarket_code": "harris",
        "provider_aliases": {"kalstrop_v1": ["Harris, Billy"]},
        "pm_aliases": ["Billy Harris"],
    },
    "harris, lloyd": {
        "polymarket_code": "harri",
        "provider_aliases": {"kalstrop_v1": ["Harris, Lloyd"]},
        "pm_aliases": ["Lloyd Harris"],
    },
    "hassan, benjamin": {
        "polymarket_code": "hassan",
        "provider_aliases": {"kalstrop_v1": ["Hassan, Benjamin"]},
        "pm_aliases": ["Benjamin Hassan"],
    },
    "havlickova, lucie": {
        "polymarket_code": "havlick",
        "provider_aliases": {"kalstrop_v1": ["Havlickova, Lucie"]},
        "pm_aliases": ["Lucie Havlickova"],
    },
    "herbert, pierre-hugues": {
        "polymarket_code": "herbert",
        "provider_aliases": {"kalstrop_v1": ["Herbert, Pierre-Hugues"]},
        "pm_aliases": ["Pierre-Hugues Herbert"],
    },
    "hercog, polona": {
        "polymarket_code": "hercog",
        "provider_aliases": {"kalstrop_v1": ["Hercog, Polona"]},
        "pm_aliases": ["Polona Hercog"],
    },
    "heredia, samuel": {
        "polymarket_code": "heredia",
        "provider_aliases": {"kalstrop_v1": ["Heredia, Samuel"]},
        "pm_aliases": ["Samuel Heredia"],
    },
    "hibino, nao": {
        "polymarket_code": "hibino",
        "provider_aliases": {"kalstrop_v1": ["Hibino, Nao"]},
        "pm_aliases": ["Nao Hibino"],
    },
    "hijikata, rinky": {
        "polymarket_code": "hijikat",
        "provider_aliases": {"kalstrop_v1": ["Hijikata, Rinky"]},
        "pm_aliases": ["Rinky Hijikata"],
    },
    "holmgren, august": {
        "polymarket_code": "holmgre",
        "provider_aliases": {"kalstrop_v1": ["Holmgren, August"]},
        "pm_aliases": ["August Holmgren"],
    },
    "hon, priscilla": {
        "polymarket_code": "hon",
        "provider_aliases": {"kalstrop_v1": ["Hon, Priscilla"]},
        "pm_aliases": ["Priscilla Hon"],
    },
    "huesler, marc-andrea": {
        "polymarket_code": "huesler",
        "provider_aliases": {"kalstrop_v1": ["Huesler, Marc-Andrea"]},
        "pm_aliases": ["Marc-Andrea Huesler"],
    },
    "huertas del pino, conner": {
        "polymarket_code": "pino",
        "provider_aliases": {"kalstrop_v1": ["Huertas Del Pino, Conner", "Huertas Del Pino, Connor"]},
        "pm_aliases": ["Conner Huertas Del Pino"],
    },
    "humbert, ugo": {
        "polymarket_code": "humbert",
        "provider_aliases": {"kalstrop_v1": ["Humbert, Ugo"]},
        "pm_aliases": ["Ugo Humbert"],
    },
    "hunter, storm": {
        "polymarket_code": "hunter",
        "provider_aliases": {"kalstrop_v1": ["Hunter, Storm"]},
        "pm_aliases": ["Storm Hunter"],
    },
    "hurkacz, hubert": {
        "polymarket_code": "hurkacz",
        "provider_aliases": {"kalstrop_v1": ["Hurkacz, Hubert"]},
        "pm_aliases": ["Hubert Hurkacz"],
    },
    "hussey, giles": {
        "polymarket_code": "hussey",
        "provider_aliases": {"kalstrop_v1": ["Hussey, Giles"]},
        "pm_aliases": ["Giles Hussey"],
    },
    "iatcenko, polina": {
        "polymarket_code": "iatcenk",
        "provider_aliases": {"kalstrop_v1": ["Iatcenko, Polina"]},
        "pm_aliases": ["Polina Iatcenko"],
    },
    "ilagan, andre": {
        "polymarket_code": "ilagan",
        "provider_aliases": {"kalstrop_v1": ["Ilagan, Andre"]},
        "pm_aliases": ["Andre Ilagan"],
    },
    "inglis, maddison": {
        "polymarket_code": "inglis",
        "provider_aliases": {"kalstrop_v1": ["Inglis, Maddison"]},
        "pm_aliases": ["Maddison Inglis"],
    },
    "ishii, sayaka": {
        "polymarket_code": "ishii",
        "provider_aliases": {"kalstrop_v1": ["Ishii, Sayaka"]},
        "pm_aliases": ["Sayaka Ishii"],
    },
    "ito, aoi": {
        "polymarket_code": "ito",
        "provider_aliases": {"kalstrop_v1": ["Ito, Aoi"]},
        "pm_aliases": ["Aoi Ito"],
    },
    "jacquemot, elsa": {
        "polymarket_code": "jacquem",
        "provider_aliases": {"kalstrop_v1": ["Jacquemot, Elsa"]},
        "pm_aliases": ["Elsa Jacquemot"],
    },
    "jacquet, kyrian": {
        "polymarket_code": "jacquet",
        "provider_aliases": {"kalstrop_v1": ["Jacquet, Kyrian"]},
        "pm_aliases": ["Kyrian Jacquet"],
    },
    "jianu, filip cristian": {
        "polymarket_code": "jianu",
        "provider_aliases": {"kalstrop_v1": ["Jianu, Filip Cristian"]},
        "pm_aliases": ["Filip Cristian Jianu"],
    },
    "jimenez kasintseva, victoria": {
        "polymarket_code": "kasints",
        "provider_aliases": {"kalstrop_v1": ["Jimenez Kasintseva, Victoria"]},
        "pm_aliases": ["Victoria Jimenez Kasintseva"],
    },
    "jodar, rafael": {
        "polymarket_code": "jodar",
        "provider_aliases": {"kalstrop_v1": ["Jodar, Rafael"]},
        "pm_aliases": ["Rafael Jodar"],
    },
    "johnson, sofia": {
        "polymarket_code": "johnson",
        "provider_aliases": {"kalstrop_v1": ["Johnson, Sofia"]},
        "pm_aliases": ["Sofia Johnson"],
    },
    "joint, maya": {
        "polymarket_code": "joint",
        "provider_aliases": {"kalstrop_v1": ["Joint, Maya"]},
        "pm_aliases": ["Maya Joint"],
    },
    "jones, emerson": {
        "polymarket_code": "jone",
        "provider_aliases": {"kalstrop_v1": ["Jones, Emerson"]},
        "pm_aliases": ["Emerson Jones"],
    },
    "jones, francesca": {
        "polymarket_code": "jones",
        "provider_aliases": {"kalstrop_v1": ["Jones, Francesca"]},
        "pm_aliases": ["Francesca Jones"],
    },
    "jong, sander": {
        "polymarket_code": "jong",
        "provider_aliases": {"kalstrop_v1": ["Jong, Sander"]},
        "pm_aliases": ["Sander Jong"],
    },
    "jorda sanchis, david": {
        "polymarket_code": "sanchis",
        "provider_aliases": {"kalstrop_v1": ["Jorda Sanchis, David"]},
        "pm_aliases": ["David Jorda Sanchis"],
    },
    "jovic, iva": {
        "polymarket_code": "jovic",
        "provider_aliases": {"kalstrop_v1": ["Jovic, Iva"]},
        "pm_aliases": ["Iva Jovic"],
    },
    "jubb, paul": {
        "polymarket_code": "jubb",
        "provider_aliases": {"kalstrop_v1": ["Jubb, Paul"]},
        "pm_aliases": ["Paul Jubb"],
    },
    "jung, jason": {
        "polymarket_code": "jung",
        "provider_aliases": {"kalstrop_v1": ["Jung, Jason"]},
        "pm_aliases": ["Jason Jung"],
    },
    "justo, guido ivan": {
        "polymarket_code": "justo",
        "provider_aliases": {"kalstrop_v1": ["Justo, Guido Ivan"]},
        "pm_aliases": ["Guido Ivan Justo"],
    },
    "juvan, kaja": {
        "polymarket_code": "juvan",
        "provider_aliases": {"kalstrop_v1": ["Juvan, Kaja"]},
        "pm_aliases": ["Kaja Juvan"],
    },
    "kabbaj, yasmine": {
        "polymarket_code": "kabbaj",
        "provider_aliases": {"kalstrop_v1": ["Kabbaj, Yasmine"]},
        "pm_aliases": ["Yasmine Kabbaj"],
    },
    "kalieva, elvina": {
        "polymarket_code": "kalieva",
        "provider_aliases": {"kalstrop_v1": ["Kalieva, Elvina"]},
        "pm_aliases": ["Elvina Kalieva"],
    },
    "kalinina, anhelina": {
        "polymarket_code": "kalinin",
        "provider_aliases": {"kalstrop_v1": ["Kalinina, Anhelina"]},
        "pm_aliases": ["Anhelina Kalinina"],
    },
    "kalinskaya, anna": {
        "polymarket_code": "kalinsk",
        "provider_aliases": {"kalstrop_v1": ["Kalinskaya, Anna"]},
        "pm_aliases": ["Anna Kalinskaya"],
    },
    "kasatkina, daria": {
        "polymarket_code": "kasatki",
        "provider_aliases": {"kalstrop_v1": ["Kasatkina, Daria"]},
        "pm_aliases": ["Daria Kasatkina"],
    },
    "kasnikowski, maks": {
        "polymarket_code": "kasniko",
        "provider_aliases": {"kalstrop_v1": ["Kasnikowski, Maks"]},
        "pm_aliases": ["Maks Kasnikowski"],
    },
    "kawa, katarzyna": {
        "polymarket_code": "kawa",
        "provider_aliases": {"kalstrop_v1": ["Kawa, Katarzyna"]},
        "pm_aliases": ["Katarzyna Kawa"],
    },
    "kecmanovic, miomir": {
        "polymarket_code": "kecmano",
        "provider_aliases": {"kalstrop_v1": ["Kecmanovic, Miomir"]},
        "pm_aliases": ["Miomir Kecmanovic"],
    },
    "kenin, sofia": {
        "polymarket_code": "kenin",
        "provider_aliases": {"kalstrop_v1": ["Kenin, Sofia"]},
        "pm_aliases": ["Sofia Kenin"],
    },
    "kessler, mccartney": {
        "polymarket_code": "kessler",
        "provider_aliases": {"kalstrop_v1": ["Kessler, McCartney"]},
        "pm_aliases": ["McCartney Kessler"],
    },
    "keys, madison": {
        "polymarket_code": "keys",
        "provider_aliases": {"kalstrop_v1": ["Keys, Madison"]},
        "pm_aliases": ["Madison Keys"],
    },
    "khachanov, karen": {
        "polymarket_code": "khachan",
        "provider_aliases": {"kalstrop_v1": ["Khachanov, Karen"]},
        "pm_aliases": ["Karen Khachanov"],
    },
    "kicker, nicolas": {
        "polymarket_code": "kicker",
        "provider_aliases": {"kalstrop_v1": ["Kicker, Nicolas"]},
        "pm_aliases": ["Nicolas Kicker"],
    },
    "klimovicova, linda": {
        "polymarket_code": "klimovi",
        "provider_aliases": {"kalstrop_v1": ["Klimovicova, Linda"]},
        "pm_aliases": ["Linda Klimovicova"],
    },
    "klugman, hannah": {
        "polymarket_code": "klugman",
        "provider_aliases": {"kalstrop_v1": ["Klugman, Hannah"]},
        "pm_aliases": ["Hannah Klugman"],
    },
    "knutson, gabriela": {
        "polymarket_code": "knutson",
        "provider_aliases": {"kalstrop_v1": ["Knutson, Gabriela"]},
        "pm_aliases": ["Gabriela Knutson"],
    },
    "koevermans, anouk": {
        "polymarket_code": "koeverm",
        "provider_aliases": {"kalstrop_v1": ["Koevermans, Anouk"]},
        "pm_aliases": ["Anouk Koevermans"],
    },
    "kohlmann de freitas, enzo": {
        "polymarket_code": "freitas",
        "provider_aliases": {"kalstrop_v1": ["Kohlmann de Freitas, Enzo"]},
        "pm_aliases": ["Enzo Kohlmann de Freitas"],
    },
    "kokkinakis, thanasi": {
        "polymarket_code": "kokkina",
        "provider_aliases": {"kalstrop_v1": ["Kokkinakis, Thanasi"]},
        "pm_aliases": ["Thanasi Kokkinakis"],
    },
    "kolar, zdenek": {
        "polymarket_code": "kolar",
        "provider_aliases": {"kalstrop_v1": ["Kolar, Zdenek"]},
        "pm_aliases": ["Zdenek Kolar"],
    },
    "kopp, sandro": {
        "polymarket_code": "kopp",
        "provider_aliases": {"kalstrop_v1": ["Kopp, Sandro"]},
        "pm_aliases": ["Sandro Kopp"],
    },
    "kopriva, vit": {
        "polymarket_code": "kopriva",
        "provider_aliases": {"kalstrop_v1": ["Kopriva, Vit"]},
        "pm_aliases": ["Vit Kopriva"],
    },
    "korneeva, alina": {
        "polymarket_code": "korneev",
        "provider_aliases": {"kalstrop_v1": ["Korneeva, Alina"]},
        "pm_aliases": ["Alina Korneeva"],
    },
    "korpatsch, tamara": {
        "polymarket_code": "korpats",
        "provider_aliases": {"kalstrop_v1": ["Korpatsch, Tamara"]},
        "pm_aliases": ["Tamara Korpatsch"],
    },
    "kostovic, teodora": {
        "polymarket_code": "kostovi",
        "provider_aliases": {"kalstrop_v1": ["Kostovic, Teodora"]},
        "pm_aliases": ["Teodora Kostovic"],
    },
    "kostyuk, marta": {
        "polymarket_code": "kostyuk",
        "provider_aliases": {"kalstrop_v1": ["Kostyuk, Marta"]},
        "pm_aliases": ["Marta Kostyuk"],
    },
    "kouame, moise": {
        "polymarket_code": "kouame",
        "provider_aliases": {"kalstrop_v1": ["Kouame, Moise"]},
        "pm_aliases": ["Moise Kouame"],
    },
    "kovacevic, aleksandar": {
        "polymarket_code": "kovacev",
        "provider_aliases": {"kalstrop_v1": ["Kovacevic, Aleksandar"]},
        "pm_aliases": ["Aleksandar Kovacevic"],
    },
    "kraus, sinja": {
        "polymarket_code": "kraus",
        "provider_aliases": {"kalstrop_v1": ["Kraus, Sinja"]},
        "pm_aliases": ["Sinja Kraus"],
    },
    "krejcikova, barbora": {
        "polymarket_code": "krejcik",
        "provider_aliases": {"kalstrop_v1": ["Krejcikova, Barbora"]},
        "pm_aliases": ["Barbora Krejcikova"],
    },
    "krueger, ashlyn": {
        "polymarket_code": "krueger",
        "provider_aliases": {"kalstrop_v1": ["Krueger, Ashlyn"]},
        "pm_aliases": ["Ashlyn Krueger"],
    },
    "krumich, martin": {
        "polymarket_code": "krumich",
        "provider_aliases": {"kalstrop_v1": ["Krumich, Martin"]},
        "pm_aliases": ["Martin Krumich"],
    },
    "kudermetova, polina": {
        "polymarket_code": "kuderme",
        "provider_aliases": {"kalstrop_v1": ["Kudermetova, Polina"]},
        "pm_aliases": ["Polina Kudermetova"],
    },
    "kukushkin, mikhail": {
        "polymarket_code": "kukushk",
        "provider_aliases": {"kalstrop_v1": ["Kukushkin, Mikhail"]},
        "pm_aliases": ["Mikhail Kukushkin"],
    },
    "kwon, soonwoo": {
        "polymarket_code": "kwon",
        "provider_aliases": {"kalstrop_v1": ["Kwon, Soonwoo"]},
        "pm_aliases": ["Soon-Woo Kwon"],
    },
    "kypson, patrick": {
        "polymarket_code": "kypson",
        "provider_aliases": {"kalstrop_v1": ["Kypson, Patrick"]},
        "pm_aliases": ["Patrick Kypson"],
    },
    "kyrgios, nick": {
        "polymarket_code": "kyrgios",
        "provider_aliases": {"kalstrop_v1": ["Kyrgios, Nick"]},
        "pm_aliases": ["Nick Kyrgios"],
    },
    "la serna, juan manuel": {
        "polymarket_code": "serna",
        "provider_aliases": {"kalstrop_v1": ["La Serna, Juan Manuel"]},
        "pm_aliases": ["Juan Manuel La Serna"],
    },
    "lajal, mark": {
        "polymarket_code": "lajal",
        "provider_aliases": {"kalstrop_v1": ["Lajal, Mark"]},
        "pm_aliases": ["Mark Lajal"],
    },
    "lajovic, dusan": {
        "polymarket_code": "lajovic",
        "provider_aliases": {"kalstrop_v1": ["Lajovic, Dusan"]},
        "pm_aliases": ["Dusan Lajovic"],
    },
    "lamens, suzan": {
        "polymarket_code": "lamens",
        "provider_aliases": {"kalstrop_v1": ["Lamens, Suzan"]},
        "pm_aliases": ["Suzan Lamens"],
    },
    "landaluce, martin": {
        "polymarket_code": "landalu",
        "provider_aliases": {"kalstrop_v1": ["Landaluce, Martin"]},
        "pm_aliases": ["Martin Landaluce"],
    },
    "lawlor, rhys": {
        "polymarket_code": "_unknown",
        "provider_aliases": {"kalstrop_v1": ["Lawlor, Rhys"]},
        "pm_aliases": ["Rhys Lawlor"],
    },
    "lazaro garcia, andrea": {
        "polymarket_code": "garcia",
        "provider_aliases": {"kalstrop_v1": ["Lazaro Garcia, Andrea"]},
        "pm_aliases": ["Andrea Lazaro Garcia"],
    },
    "lee, carol young suh": {
        "polymarket_code": "lee",
        "provider_aliases": {"kalstrop_v1": ["Lee, Carol Young Suh"]},
        "pm_aliases": ["Carol Young Suh Lee"],
    },
    "legout, timo": {
        "polymarket_code": "legout",
        "provider_aliases": {"kalstrop_v1": ["Legout, Timo"]},
        "pm_aliases": ["Timo Legout"],
    },
    "lehecka, jiri": {
        "polymarket_code": "lehecka",
        "provider_aliases": {"kalstrop_v1": ["Lehecka, Jiri"]},
        "pm_aliases": ["Jiri Lehecka"],
    },
    "lepchenko, varvara": {
        "polymarket_code": "lepchen",
        "provider_aliases": {"kalstrop_v1": ["Lepchenko, Varvara"]},
        "pm_aliases": ["Varvara Lepchenko"],
    },
    "linette, magda": {
        "polymarket_code": "linette",
        "provider_aliases": {"kalstrop_v1": ["Linette, Magda"]},
        "pm_aliases": ["Magda Linette"],
    },
    "liu, claire": {
        "polymarket_code": "liu",
        "provider_aliases": {"kalstrop_v1": ["Liu, Claire"]},
        "pm_aliases": ["Claire Liu"],
    },
    "llamas ruiz, pablo": {
        "polymarket_code": "ruiz",
        "provider_aliases": {"kalstrop_v1": ["Llamas Ruiz, Pablo"]},
        "pm_aliases": ["Pablo Llamas Ruiz"],
    },
    "machac, tomas": {
        "polymarket_code": "machac",
        "provider_aliases": {"kalstrop_v1": ["Machac, Tomas"]},
        "pm_aliases": ["Tomas Machac"],
    },
    "maestrelli, francesco": {
        "polymarket_code": "maestre",
        "provider_aliases": {"kalstrop_v1": ["Maestrelli, Francesco"]},
        "pm_aliases": ["Francesco Maestrelli"],
    },
    "majchrzak, kamil": {
        "polymarket_code": "majchrz",
        "provider_aliases": {"kalstrop_v1": ["Majchrzak, Kamil"]},
        "pm_aliases": ["Kamil Majchrzak"],
    },
    "mandlik, elizabeth": {
        "polymarket_code": "mandlik",
        "provider_aliases": {"kalstrop_v1": ["Mandlik, Elizabeth"]},
        "pm_aliases": ["Elizabeth Mandlik"],
    },
    "mannarino, adrian": {
        "polymarket_code": "mannari",
        "provider_aliases": {"kalstrop_v1": ["Mannarino, Adrian"]},
        "pm_aliases": ["Adrian Mannarino"],
    },
    "marcinko, petra": {
        "polymarket_code": "marcink",
        "provider_aliases": {"kalstrop_v1": ["Marcinko, Petra"]},
        "pm_aliases": ["Petra Marcinko"],
    },
    "maria, tatjana": {
        "polymarket_code": "maria",
        "provider_aliases": {"kalstrop_v1": ["Maria, Tatjana"]},
        "pm_aliases": ["Tatjana Maria"],
    },
    "marozsan, fabian": {
        "polymarket_code": "marozsa",
        "provider_aliases": {"kalstrop_v1": ["Marozsan, Fabian"]},
        "pm_aliases": ["Fabian Marozsan"],
    },
    "martin, andrej": {
        "polymarket_code": "mar",
        "provider_aliases": {"kalstrop_v1": ["Martin, Andrej"]},
        "pm_aliases": ["Andrej Martin"],
    },
    "martincova, tereza": {
        "polymarket_code": "martinc",
        "provider_aliases": {"kalstrop_v1": ["Martincova, Tereza"]},
        "pm_aliases": ["Tereza Martincova"],
    },
    "martineau, matteo": {
        "polymarket_code": "martin",
        "provider_aliases": {"kalstrop_v1": ["Martineau, Matteo"]},
        "pm_aliases": ["Matteo Martineau"],
    },
    "martinez, pedro": {
        "polymarket_code": "martine",
        "provider_aliases": {"kalstrop_v1": ["Martinez, Pedro"]},
        "pm_aliases": ["Pedro Martinez"],
    },
    "martinez, tomas": {
        "polymarket_code": "tomasma",
        "provider_aliases": {"kalstrop_v1": ["Martinez, Tomas"]},
        "pm_aliases": ["Tomas Martinez"],
    },
    "masarova, rebeka": {
        "polymarket_code": "masarov",
        "provider_aliases": {"kalstrop_v1": ["Masarova, Rebeka"]},
        "pm_aliases": ["Rebeka Masarova"],
    },
    "masur, daniel": {
        "polymarket_code": "_unknown",
        "provider_aliases": {"kalstrop_v1": ["Masur, Daniel"]},
        "pm_aliases": ["Daniel Masur"],
    },
    "matusevich, anton": {
        "polymarket_code": "matusev",
        "provider_aliases": {"kalstrop_v1": ["Matusevich, Anton"]},
        "pm_aliases": ["Anton Matusevich"],
    },
    "mboko, victoria": {
        "polymarket_code": "mboko",
        "provider_aliases": {"kalstrop_v1": ["Mboko, Victoria"]},
        "pm_aliases": ["Victoria Mboko"],
    },
    "mccabe, james": {
        "polymarket_code": "mccabe",
        "provider_aliases": {"kalstrop_v1": ["McCabe, James"]},
        "pm_aliases": ["James McCabe"],
    },
    "mcdonald, ella": {
        "polymarket_code": "mcdonal",
        "provider_aliases": {"kalstrop_v1": ["McDonald, Ella"]},
        "pm_aliases": ["Ella McDonald"],
    },
    "mcdonald, mackenzie": {
        "polymarket_code": "mcdonal",
        "provider_aliases": {"kalstrop_v1": ["McDonald, Mackenzie"]},
        "pm_aliases": ["Mackenzie McDonald"],
    },
    "mcdonald, niels": {
        "polymarket_code": "mcdona",
        "provider_aliases": {"kalstrop_v1": ["McDonald, Niels"]},
        "pm_aliases": ["Niels McDonald"],
    },
    "mcnally, caty": {
        "polymarket_code": "mcnally",
        "provider_aliases": {"kalstrop_v1": ["McNally, Caty"]},
        "pm_aliases": ["Caty McNally"],
    },
    "medjedovic, hamad": {
        "polymarket_code": "medjedo",
        "provider_aliases": {"kalstrop_v1": ["Medjedovic, Hamad"]},
        "pm_aliases": ["Hamad Medjedovic"],
    },
    "medvedev, daniil": {
        "polymarket_code": "medvede",
        "provider_aliases": {"kalstrop_v1": ["Medvedev, Daniil"]},
        "pm_aliases": ["Daniil Medvedev"],
    },
    "mensik, jakub": {
        "polymarket_code": "mensik",
        "provider_aliases": {"kalstrop_v1": ["Mensik, Jakub"]},
        "pm_aliases": ["Jakub Mensik"],
    },
    "merida, daniel": {
        "polymarket_code": "aguilar",
        "provider_aliases": {"kalstrop_v1": ["Merida, Daniel"]},
        "pm_aliases": ["Daniel Merida"],
    },
    "mertens, elise": {
        "polymarket_code": "mertens",
        "provider_aliases": {"kalstrop_v1": ["Mertens, Elise"]},
        "pm_aliases": ["Elise Mertens"],
    },
    "michelsen, alex": {
        "polymarket_code": "michels",
        "provider_aliases": {"kalstrop_v1": ["Michelsen, Alex"]},
        "pm_aliases": ["Alex Michelsen"],
    },
    "midon, lautaro": {
        "polymarket_code": "midon",
        "provider_aliases": {"kalstrop_v1": ["Midon, Lautaro"]},
        "pm_aliases": ["Lautaro Midon"],
    },
    "miguel, luis felipe": {
        "polymarket_code": "migu",
        "provider_aliases": {"kalstrop_v1": ["Miguel, Luis Felipe"]},
        "pm_aliases": ["Luis Felipe Miguel"],
    },
    "mikrut, luka": {
        "polymarket_code": "mikrut",
        "provider_aliases": {"kalstrop_v1": ["Mikrut, Luka"]},
        "pm_aliases": ["Luka Mikrut"],
    },
    "mikulskyte, justina": {
        "polymarket_code": "mikulsk",
        "provider_aliases": {"kalstrop_v1": ["Mikulskyte, Justina"]},
        "pm_aliases": ["Justina Mikulskyte"],
    },
    "milev, yanaki": {
        "polymarket_code": "milev",
        "provider_aliases": {"kalstrop_v1": ["Milev, Yanaki"]},
        "pm_aliases": ["Yanaki Milev"],
    },
    "milic, ognjen": {
        "polymarket_code": "milic",
        "provider_aliases": {"kalstrop_v1": ["Milic, Ognjen"]},
        "pm_aliases": ["Ognjen Milic"],
    },
    "minnen, greet": {
        "polymarket_code": "minnen",
        "provider_aliases": {"kalstrop_v1": ["Minnen, Greet"]},
        "pm_aliases": ["Greet Minnen"],
    },
    "miyazaki, yuriko lily": {
        "polymarket_code": "miyazak",
        "provider_aliases": {"kalstrop_v1": ["Miyazaki, Yuriko Lily"]},
        "pm_aliases": ["Yuriko Lily Miyazaki"],
    },
    "mmoh, michael": {
        "polymarket_code": "mmoh",
        "provider_aliases": {"kalstrop_v1": ["Mmoh, Michael"]},
        "pm_aliases": ["Michael Mmoh"],
    },
    "mochizuki, shintaro": {
        "polymarket_code": "mochizu",
        "provider_aliases": {"kalstrop_v1": ["Mochizuki, Shintaro"]},
        "pm_aliases": ["Shintaro Mochizuki"],
    },
    "moeller, marvin": {
        "polymarket_code": "moelle",
        "provider_aliases": {"kalstrop_v1": ["Moeller, Marvin"]},
        "pm_aliases": ["Marvin Moeller"],
    },
    "molcan, alex": {
        "polymarket_code": "molcan",
        "provider_aliases": {"kalstrop_v1": ["Molcan, Alex"]},
        "pm_aliases": ["Alex Molcan"],
    },
    "monday, johannus": {
        "polymarket_code": "monday",
        "provider_aliases": {"kalstrop_v1": ["Monday, Johannus"]},
        "pm_aliases": ["Johannus Monday"],
    },
    "monfils, gael": {
        "polymarket_code": "monfils",
        "provider_aliases": {"kalstrop_v1": ["Monfils, Gael"]},
        "pm_aliases": ["Gael Monfils"],
    },
    "monnet, carole": {
        "polymarket_code": "monnet",
        "provider_aliases": {"kalstrop_v1": ["Monnet, Carole"]},
        "pm_aliases": ["Carole Monnet"],
    },
    "montes-de la torre, inaki": {
        "polymarket_code": "montes",
        "provider_aliases": {"kalstrop_v1": ["Montes-de la Torre, Inaki"]},
        "pm_aliases": ["Inaki Montes"],
    },
    "montgomery, robin": {
        "polymarket_code": "montgom",
        "provider_aliases": {"kalstrop_v1": ["Montgomery, Robin"]},
        "pm_aliases": ["Robin Montgomery"],
    },
    "montsi, khololwam": {
        "polymarket_code": "montsi",
        "provider_aliases": {"kalstrop_v1": ["Montsi, Khololwam"]},
        "pm_aliases": ["Khololwam Montsi"],
    },
    "moro canas, alejandro": {
        "polymarket_code": "canas",
        "provider_aliases": {"kalstrop_v1": ["Moro Canas, Alejandro"]},
        "pm_aliases": ["Alejandro Moro Canas"],
    },
    "moutet, corentin": {
        "polymarket_code": "moutet",
        "provider_aliases": {"kalstrop_v1": ["Moutet, Corentin"]},
        "pm_aliases": ["Corentin Moutet"],
    },
    "mpetshi perricard, giovanni": {
        "polymarket_code": "perrica",
        "provider_aliases": {"kalstrop_v1": ["Mpetshi Perricard, Giovanni"]},
        "pm_aliases": ["Giovanni Mpetshi Perricard"],
    },
    "mrva, maxim": {
        "polymarket_code": "mrva",
        "provider_aliases": {"kalstrop_v1": ["Mrva, Maxim"]},
        "pm_aliases": ["Maxim Mrva"],
    },
    "muchova, karolina": {
        "polymarket_code": "muchova",
        "provider_aliases": {"kalstrop_v1": ["Muchova, Karolina"]},
        "pm_aliases": ["Karolina Muchova"],
    },
    "muller, alexandre": {
        "polymarket_code": "muller",
        "provider_aliases": {"kalstrop_v1": ["Muller, Alexandre"]},
        "pm_aliases": ["Alexandre Muller"],
    },
    "munar, jaume": {
        "polymarket_code": "munar",
        "provider_aliases": {"kalstrop_v1": ["Munar, Jaume"]},
        "pm_aliases": ["Jaume Munar"],
    },
    "naef, celine": {
        "polymarket_code": "naef",
        "provider_aliases": {"kalstrop_v1": ["Naef, Celine"]},
        "pm_aliases": ["Celine Naef"],
    },
    "nakashima, brandon": {
        "polymarket_code": "nakashi",
        "provider_aliases": {"kalstrop_v1": ["Nakashima, Brandon"]},
        "pm_aliases": ["Brandon Nakashima"],
    },
    "nardi, luca": {
        "polymarket_code": "nardi",
        "provider_aliases": {"kalstrop_v1": ["Nardi, Luca"]},
        "pm_aliases": ["Luca Nardi"],
    },
    "nava, emilio": {
        "polymarket_code": "nava",
        "provider_aliases": {"kalstrop_v1": ["Nava, Emilio"]},
        "pm_aliases": ["Emilio Nava"],
    },
    "navarro, emma": {
        "polymarket_code": "navarro",
        "provider_aliases": {"kalstrop_v1": ["Navarro, Emma"]},
        "pm_aliases": ["Emma Navarro"],
    },
    "navone, mariano": {
        "polymarket_code": "navone",
        "provider_aliases": {"kalstrop_v1": ["Navone, Mariano"]},
        "pm_aliases": ["Mariano Navone"],
    },
    "nedic, andrej": {
        "polymarket_code": "nedic",
        "provider_aliases": {"kalstrop_v1": ["Nedic, Andrej"]},
        "pm_aliases": ["Andrej Nedic"],
    },
    "nesterov, pyotr": {
        "polymarket_code": "nestero",
        "provider_aliases": {"kalstrop_v1": ["Nesterov, Pyotr"]},
        "pm_aliases": ["Pyotr Nesterov"],
    },
    "noha akugue, noma": {
        "polymarket_code": "akugue",
        "provider_aliases": {"kalstrop_v1": ["Noha Akugue, Noma"]},
        "pm_aliases": ["Noma Noha Akugue"],
    },
    "norrie, cameron": {
        "polymarket_code": "norrie",
        "provider_aliases": {"kalstrop_v1": ["Norrie, Cameron"]},
        "pm_aliases": ["Cameron Norrie"],
    },
    "o'connell, christopher": {
        "polymarket_code": "oconnel",
        "provider_aliases": {"kalstrop_v1": ["O'Connell, Christopher"]},
        "pm_aliases": ["Christopher O'Connell"],
    },
    "ofner, sebastian": {
        "polymarket_code": "ofner",
        "provider_aliases": {"kalstrop_v1": ["Ofner, Sebastian"]},
        "pm_aliases": ["Sebastian Ofner"],
    },
    "okonkwo, oliver": {
        "polymarket_code": "okonkwo",
        "provider_aliases": {"kalstrop_v1": ["Okonkwo, Oliver"]},
        "pm_aliases": ["Oliver Okonkwo"],
    },
    "olivieri, genaro alberto": {
        "polymarket_code": "olivier",
        "provider_aliases": {"kalstrop_v1": ["Olivieri, Genaro Alberto"]},
        "pm_aliases": ["Genaro Alberto Olivieri"],
    },
    "oliynykova, oleksandra": {
        "polymarket_code": "oliynyk",
        "provider_aliases": {"kalstrop_v1": ["Oliynykova, Oleksandra"]},
        "pm_aliases": ["Oleksandra Oliynykova"],
    },
    "onclin, gauthier": {
        "polymarket_code": "onclin",
        "provider_aliases": {"kalstrop_v1": ["Onclin, Gauthier"]},
        "pm_aliases": ["Gauthier Onclin"],
    },
    "opelka, reilly": {
        "polymarket_code": "opelka",
        "provider_aliases": {"kalstrop_v1": ["Opelka, Reilly"]},
        "pm_aliases": ["Reilly Opelka"],
    },
    "ortenzi, jazmin": {
        "polymarket_code": "ortenzi",
        "provider_aliases": {"kalstrop_v1": ["Ortenzi, Jazmin"]},
        "pm_aliases": ["Jazmin Ortenzi"],
    },
    "osaka, naomi": {
        "polymarket_code": "osaka",
        "provider_aliases": {"kalstrop_v1": ["Osaka, Naomi"]},
        "pm_aliases": ["Naomi Osaka"],
    },
    "osorio, camila": {
        "polymarket_code": "osorio",
        "provider_aliases": {"kalstrop_v1": ["Osorio, Camila"]},
        "pm_aliases": ["Camila Osorio"],
    },
    "palicova, barbora": {
        "polymarket_code": "palicov",
        "provider_aliases": {"kalstrop_v1": ["Palicova, Barbora"]},
        "pm_aliases": ["Barbora Palicova"],
    },
    "papamichail, despina": {
        "polymarket_code": "papamic",
        "provider_aliases": {"kalstrop_v1": ["Papamichail, Despina"]},
        "pm_aliases": ["Despina Papamichail"],
    },
    "papoe, radu mihai": {
        "polymarket_code": "papoe",
        "provider_aliases": {"kalstrop_v1": ["Papoe, Radu Mihai"]},
        "pm_aliases": ["Radu Mihai Papoe"],
    },
    "paquet, chloe": {
        "polymarket_code": "paquet",
        "provider_aliases": {"kalstrop_v1": ["Paquet, Chloe"]},
        "pm_aliases": ["Chloe Paquet"],
    },
    "parks, alycia": {
        "polymarket_code": "parks",
        "provider_aliases": {"kalstrop_v1": ["Parks, Alycia"]},
        "pm_aliases": ["Alycia Parks"],
    },
    "parry, diane": {
        "polymarket_code": "parry",
        "provider_aliases": {"kalstrop_v1": ["Parry, Diane"]},
        "pm_aliases": ["Diane Parry"],
    },
    "paul, tommy": {
        "polymarket_code": "paul",
        "provider_aliases": {"kalstrop_v1": ["Paul, Tommy"]},
        "pm_aliases": ["Tommy Paul"],
    },
    "pavlovic, luka": {
        "polymarket_code": "pavlovi",
        "provider_aliases": {"kalstrop_v1": ["Pavlovic, Luka"]},
        "pm_aliases": ["Luka Pavlovic"],
    },
    "pedone, giorgia": {
        "polymarket_code": "pedone",
        "provider_aliases": {"kalstrop_v1": ["Pedone, Giorgia"]},
        "pm_aliases": ["Giorgia Pedone"],
    },
    "pellegrino, andrea": {
        "polymarket_code": "pellegr",
        "provider_aliases": {"kalstrop_v1": ["Pellegrino, Andrea"]},
        "pm_aliases": ["Andrea Pellegrino"],
    },
    "pereira, tiago": {
        "polymarket_code": "pereira",
        "provider_aliases": {"kalstrop_v1": ["Pereira, Tiago"]},
        "pm_aliases": ["Tiago Pereira"],
    },
    "pieri, jessica": {
        "polymarket_code": "pieri",
        "provider_aliases": {"kalstrop_v1": ["Pieri, Jessica"]},
        "pm_aliases": ["Jessica Pieri"],
    },
    "pieri, samuele": {
        "polymarket_code": "pieri",
        "provider_aliases": {"kalstrop_v1": ["Pieri, Samuele"]},
        "pm_aliases": ["Samuele Pieri"],
    },
    "pieri, tatiana": {
        "polymarket_code": "pieri",
        "provider_aliases": {"kalstrop_v1": ["Pieri, Tatiana"]},
        "pm_aliases": ["Tatiana Pieri"],
    },
    "pigato, lisa": {
        "polymarket_code": "pigato",
        "provider_aliases": {"kalstrop_v1": ["Pigato, Lisa"]},
        "pm_aliases": ["Lisa Pigato"],
    },
    "pigossi, laura": {
        "polymarket_code": "pigossi",
        "provider_aliases": {"kalstrop_v1": ["Pigossi, Laura"]},
        "pm_aliases": ["Laura Pigossi"],
    },
    "pinnington jones, jack": {
        "polymarket_code": "jones",
        "provider_aliases": {"kalstrop_v1": ["Pinnington Jones, Jack"]},
        "pm_aliases": ["Jack Pinnington Jones"],
    },
    "piraino, gabriele": {
        "polymarket_code": "piraino",
        "provider_aliases": {"kalstrop_v1": ["Piraino, Gabriele"]},
        "pm_aliases": ["Gabriele Piraino"],
    },
    "piros, zsombor": {
        "polymarket_code": "piros",
        "provider_aliases": {"kalstrop_v1": ["Piros, Zsombor"]},
        "pm_aliases": ["Zsombor Piros"],
    },
    "pliskova, karolina": {
        "polymarket_code": "pliskov",
        "provider_aliases": {"kalstrop_v1": ["Pliskova, Karolina"]},
        "pm_aliases": ["Karolina Pliskova"],
    },
    "podrez, veronika": {
        "polymarket_code": "podrez",
        "provider_aliases": {"kalstrop_v1": ["Podrez, Veronika"]},
        "pm_aliases": ["Veronika Podrez"],
    },
    "pohankova, mia": {
        "polymarket_code": "pohanko",
        "provider_aliases": {"kalstrop_v1": ["Pohankova, Mia"]},
        "pm_aliases": ["Mia Pohankova"],
    },
    "pokorny, lukas": {
        "polymarket_code": "pokorny",
        "provider_aliases": {"kalstrop_v1": ["Pokorny, Lukas"]},
        "pm_aliases": ["Lukas Pokorny"],
    },
    "poling, karl": {
        "polymarket_code": "poling",
        "provider_aliases": {"kalstrop_v1": ["Poling, Karl"]},
        "pm_aliases": ["Karl Poling"],
    },
    "poljicak, mili": {
        "polymarket_code": "poljica",
        "provider_aliases": {"kalstrop_v1": ["Poljicak, Mili"]},
        "pm_aliases": ["Mili Poljicak"],
    },
    "polmans, marc": {
        "polymarket_code": "polmans",
        "provider_aliases": {"kalstrop_v1": ["Polmans, Marc"]},
        "pm_aliases": ["Marc Polmans"],
    },
    "ponchet, jessika": {
        "polymarket_code": "ponchet",
        "provider_aliases": {"kalstrop_v1": ["Ponchet, Jessika"]},
        "pm_aliases": ["Jessika Ponchet"],
    },
    "popyrin, alexei": {
        "polymarket_code": "popyrin",
        "provider_aliases": {"kalstrop_v1": ["Popyrin, Alexei"]},
        "pm_aliases": ["Alexei Popyrin"],
    },
    "potapova, anastasia": {
        "polymarket_code": "potapov",
        "provider_aliases": {"kalstrop_v1": ["Potapova, Anastasia"]},
        "pm_aliases": ["Anastasia Potapova"],
    },
    "potenza, luca": {
        "polymarket_code": "potenza",
        "provider_aliases": {"kalstrop_v1": ["Potenza, Luca"]},
        "pm_aliases": ["Luca Potenza"],
    },
    "prado angelo, juan carlos": {
        "polymarket_code": "prado",
        "provider_aliases": {"kalstrop_v1": ["Prado Angelo, Juan Carlos"]},
        "pm_aliases": ["Juan Carlos Prado"],
    },
    "preston, taylah": {
        "polymarket_code": "preston",
        "provider_aliases": {"kalstrop_v1": ["Preston, Taylah"]},
        "pm_aliases": ["Taylah Preston"],
    },
    "pridankina, elena": {
        "polymarket_code": "pridank",
        "provider_aliases": {"kalstrop_v1": ["Pridankina, Elena"]},
        "pm_aliases": ["Elena Pridankina"],
    },
    "prizmic, dino": {
        "polymarket_code": "prizmic",
        "provider_aliases": {"kalstrop_v1": ["Prizmic, Dino"]},
        "pm_aliases": ["Dino Prizmic"],
    },
    "prozorova, tatiana": {
        "polymarket_code": "prozoro",
        "provider_aliases": {"kalstrop_v1": ["Prozorova, Tatiana"]},
        "pm_aliases": ["Tatiana Prozorova"],
    },
    "pucinelli de almeida, matheus": {
        "polymarket_code": "almeida",
        "provider_aliases": {"kalstrop_v1": ["Pucinelli de Almeida, Matheus"]},
        "pm_aliases": ["Matheus Pucinelli de Almeida"],
    },
    "putintseva, yulia": {
        "polymarket_code": "putints",
        "provider_aliases": {"kalstrop_v1": ["Putintseva, Yulia"]},
        "pm_aliases": ["Yulia Putintseva"],
    },
    "quevedo, kaitlin": {
        "polymarket_code": "quevedo",
        "provider_aliases": {"kalstrop_v1": ["Quevedo, Kaitlin"]},
        "pm_aliases": ["Kaitlin Quevedo"],
    },
    "quinn, ethan": {
        "polymarket_code": "quinn",
        "provider_aliases": {"kalstrop_v1": ["Quinn, Ethan"]},
        "pm_aliases": ["Ethan Quinn"],
    },
    "radivojevic, lola": {
        "polymarket_code": "radivoj",
        "provider_aliases": {"kalstrop_v1": ["Radivojevic, Lola"]},
        "pm_aliases": ["Lola Radivojevic"],
    },
    "raducanu, emma": {
        "polymarket_code": "raducan",
        "provider_aliases": {"kalstrop_v1": ["Raducanu, Emma"]},
        "pm_aliases": ["Emma Raducanu"],
    },
    "radulov, iliyan": {
        "polymarket_code": "radulov",
        "provider_aliases": {"kalstrop_v1": ["Radulov, Iliyan"]},
        "pm_aliases": ["Iliyan Radulov"],
    },
    "rajecki, amelia": {
        "polymarket_code": "rajecki",
        "provider_aliases": {"kalstrop_v1": ["Rajecki, Amelia"]},
        "pm_aliases": ["Amelia Rajecki"],
    },
    "rakhimova, kamilla": {
        "polymarket_code": "rakhimo",
        "provider_aliases": {"kalstrop_v1": ["Rakhimova, Kamilla"]},
        "pm_aliases": ["Kamilla Rakhimova"],
    },
    "rakotomanga rajaonah, tiantsoa sarah": {
        "polymarket_code": "rakotom",
        "provider_aliases": {"kalstrop_v1": ["Rakotomanga Rajaonah, Tiantsoa Sarah"]},
        "pm_aliases": ["Tiantsoa Sarah Rakotomanga Rajaonah"],
    },
    "rame, alice": {
        "polymarket_code": "rame",
        "provider_aliases": {"kalstrop_v1": ["Rame, Alice"]},
        "pm_aliases": ["Alice Rame"],
    },
    "rehberg, max hans": {
        "polymarket_code": "rehberg",
        "provider_aliases": {"kalstrop_v1": ["Rehberg, Max Hans"]},
        "pm_aliases": ["Max Hans Rehberg"],
    },
    "reis da silva, joao lucas": {
        "polymarket_code": "silv",
        "provider_aliases": {"kalstrop_v1": ["Reis Da Silva, Joao Lucas"]},
        "pm_aliases": ["Joao Lucas Da Silva"],
    },
    "ribeiro, eduardo": {
        "polymarket_code": "ribeiro",
        "provider_aliases": {"kalstrop_v1": ["Ribeiro, Eduardo"]},
        "pm_aliases": ["Eduardo Ribeiro"],
    },
    "riedi, leandro": {
        "polymarket_code": "riedi",
        "provider_aliases": {"kalstrop_v1": ["Riedi, Leandro"]},
        "pm_aliases": ["Leandro Riedi"],
    },
    "riera, julia": {
        "polymarket_code": "riera",
        "provider_aliases": {"kalstrop_v1": ["Riera, Julia"]},
        "pm_aliases": ["Julia Riera"],
    },
    "rincon, daniel": {
        "polymarket_code": "rincon",
        "provider_aliases": {"kalstrop_v1": ["Rincon, Daniel"]},
        "pm_aliases": ["Daniel Rincon"],
    },
    "rinderknech, arthur": {
        "polymarket_code": "rinderk",
        "provider_aliases": {"kalstrop_v1": ["Rinderknech, Arthur"]},
        "pm_aliases": ["Arthur Rinderknech"],
    },
    "roca batalla, oriol": {
        "polymarket_code": "batalla",
        "provider_aliases": {"kalstrop_v1": ["Roca Batalla, Oriol"]},
        "pm_aliases": ["Oriol Roca Batalla"],
    },
    "rocha, henrique": {
        "polymarket_code": "rocha",
        "provider_aliases": {"kalstrop_v1": ["Rocha, Henrique"]},
        "pm_aliases": ["Henrique Rocha"],
    },
    "rodesch, chris": {
        "polymarket_code": "rodesch",
        "provider_aliases": {"kalstrop_v1": ["Rodesch, Chris"]},
        "pm_aliases": ["Chris Rodesch"],
    },
    "rodionov, jurij": {
        "polymarket_code": "rodiono",
        "provider_aliases": {"kalstrop_v1": ["Rodionov, Jurij"]},
        "pm_aliases": ["Jurij Rodionov"],
    },
    "rodriguez rodriguez, johan alexander": {
        "polymarket_code": "rodr",
        "provider_aliases": {"kalstrop_v1": ["Rodriguez Rodriguez, Johan Alexander"]},
        "pm_aliases": ["Johan Alexander Rodriguez"],
    },
    "rodriguez taverna, santiago": {
        "polymarket_code": "taverna",
        "provider_aliases": {"kalstrop_v1": ["Rodriguez Taverna, Santiago"]},
        "pm_aliases": ["Santiago Rodriguez Taverna"],
    },
    "rodriguez, lorenzo joaquin": {
        "polymarket_code": "rodrig",
        "provider_aliases": {"kalstrop_v1": ["Rodriguez, Lorenzo Joaquin"]},
        "pm_aliases": ["Lorenzo Joaquin Rodriguez"],
    },
    "romano, filippo": {
        "polymarket_code": "romano",
        "provider_aliases": {"kalstrop_v1": ["Romano, Filippo"]},
        "pm_aliases": ["Filippo Romano"],
    },
    "romero gormaz, leyre": {
        "polymarket_code": "gormaz",
        "provider_aliases": {"kalstrop_v1": ["Romero Gormaz, Leyre"]},
        "pm_aliases": ["Leyre Romero Gormaz"],
    },
    "rottgering, mees": {
        "polymarket_code": "rottger",
        "provider_aliases": {"kalstrop_v1": ["Rottgering, Mees"]},
        "pm_aliases": ["Mees Rottgering"],
    },
    "royer, valentin": {
        "polymarket_code": "royer",
        "provider_aliases": {"kalstrop_v1": ["Royer, Valentin"]},
        "pm_aliases": ["Valentin Royer"],
    },
    "rubio fierros, alan fernando": {
        "polymarket_code": "rubio",
        "provider_aliases": {"kalstrop_v1": ["Rubio Fierros, Alan Fernando"]},
        "pm_aliases": ["Alan Rubio"],
    },
    "rublev, andrey": {
        "polymarket_code": "rublev",
        "provider_aliases": {"kalstrop_v1": ["Rublev, Andrey"]},
        "pm_aliases": ["Andrey Rublev"],
    },
    "ruggeri, jennifer": {
        "polymarket_code": "ruggeri",
        "provider_aliases": {"kalstrop_v1": ["Ruggeri, Jennifer"]},
        "pm_aliases": ["Jennifer Ruggeri"],
    },
    "rus, arantxa": {
        "polymarket_code": "rus",
        "provider_aliases": {"kalstrop_v1": ["Rus, Arantxa"]},
        "pm_aliases": ["Arantxa Rus"],
    },
    "ruse, elena-gabriela": {
        "polymarket_code": "ruse",
        "provider_aliases": {"kalstrop_v1": ["Ruse, Elena-Gabriela"]},
        "pm_aliases": ["Elena-Gabriela Ruse"],
    },
    "ruud, casper": {
        "polymarket_code": "ruud",
        "provider_aliases": {"kalstrop_v1": ["Ruud, Casper"]},
        "pm_aliases": ["Casper Ruud"],
    },
    "ruzic, antonia": {
        "polymarket_code": "ruzic",
        "provider_aliases": {"kalstrop_v1": ["Ruzic, Antonia"]},
        "pm_aliases": ["Antonia Ruzic"],
    },
    "rybakina, elena": {
        "polymarket_code": "rybakin",
        "provider_aliases": {"kalstrop_v1": ["Rybakina, Elena"]},
        "pm_aliases": ["Elena Rybakina"],
    },
    "sabalenka, aryna": {
        "polymarket_code": "sabalen",
        "provider_aliases": {"kalstrop_v1": ["Sabalenka, Aryna"]},
        "pm_aliases": ["Aryna Sabalenka"],
    },
    "sachko, vitaliy": {
        "polymarket_code": "sachko",
        "provider_aliases": {"kalstrop_v1": ["Sachko, Vitaliy"]},
        "pm_aliases": ["Vitaliy Sachko"],
    },
    "safiullin, roman": {
        "polymarket_code": "safiull",
        "provider_aliases": {"kalstrop_v1": ["Safiullin, Roman"]},
        "pm_aliases": ["Roman Safiullin"],
    },
    "sakamoto, rei": {
        "polymarket_code": "sakamot",
        "provider_aliases": {"kalstrop_v1": ["Sakamoto, Rei"]},
        "pm_aliases": ["Rei Sakamoto"],
    },
    "sakatsume, himeno": {
        "polymarket_code": "sakatsu",
        "provider_aliases": {"kalstrop_v1": ["Sakatsume, Himeno"]},
        "pm_aliases": ["Himeno Sakatsume"],
    },
    "sakkari, maria": {
        "polymarket_code": "sakkari",
        "provider_aliases": {"kalstrop_v1": ["Sakkari, Maria"]},
        "pm_aliases": ["Maria Sakkari"],
    },
    "salkova, dominika": {
        "polymarket_code": "salkova",
        "provider_aliases": {"kalstrop_v1": ["Salkova, Dominika"]},
        "pm_aliases": ["Dominika Salkova"],
    },
    "samson, laura": {
        "polymarket_code": "samson",
        "provider_aliases": {"kalstrop_v1": ["Samson, Laura"]},
        "pm_aliases": ["Laura Samson"],
    },
    "samsonova, liudmila": {
        "polymarket_code": "samsono",
        "provider_aliases": {"kalstrop_v1": ["Samsonova, Liudmila"]},
        "pm_aliases": ["Liudmila Samsonova"],
    },
    "samuel, toby": {
        "polymarket_code": "samuel",
        "provider_aliases": {"kalstrop_v1": ["Samuel, Toby"]},
        "pm_aliases": ["Toby Samuel"],
    },
    "sanchez izquierdo, nikolas": {
        "polymarket_code": "izquier",
        "provider_aliases": {"kalstrop_v1": ["Sanchez Izquierdo, Nikolas"]},
        "pm_aliases": ["Nikolas Sanchez Izquierdo"],
    },
    "sanchez jover, carlos": {
        "polymarket_code": "jover",
        "provider_aliases": {"kalstrop_v1": ["Sanchez Jover, Carlos"]},
        "pm_aliases": ["Carlos Sanchez Jover"],
    },
    "santillan, akira": {
        "polymarket_code": "santill",
        "provider_aliases": {"kalstrop_v1": ["Santillan, Akira"]},
        "pm_aliases": ["Akira Santillan"],
    },
    "sasnovich, aliaksandra": {
        "polymarket_code": "sasnovi",
        "provider_aliases": {"kalstrop_v1": ["Sasnovich, Aliaksandra"]},
        "pm_aliases": ["Aliaksandra Sasnovich"],
    },
    "sawangkaew, mananchaya": {
        "polymarket_code": "sawangk",
        "provider_aliases": {"kalstrop_v1": ["Sawangkaew, Mananchaya"]},
        "pm_aliases": ["Mananchaya Sawangkaew"],
    },
    "schoenhaus, max": {
        "polymarket_code": "schoenh",
        "provider_aliases": {"kalstrop_v1": ["Schoenhaus, Max"]},
        "pm_aliases": ["Max Schoenhaus"],
    },
    "schoolkate, tristan": {
        "polymarket_code": "schoolk",
        "provider_aliases": {"kalstrop_v1": ["Schoolkate, Tristan"]},
        "pm_aliases": ["Tristan Schoolkate"],
    },
    "schwaerzler, joel": {
        "polymarket_code": "schwaer",
        "provider_aliases": {"kalstrop_v1": ["Schwaerzler, Joel"]},
        "pm_aliases": ["Joel Schwaerzler"],
    },
    "searle, henry": {
        "polymarket_code": "searle",
        "provider_aliases": {"kalstrop_v1": ["Searle, Henry"]},
        "pm_aliases": ["Henry Searle"],
    },
    "seidel, ella": {
        "polymarket_code": "seidel",
        "provider_aliases": {"kalstrop_v1": ["Seidel, Ella"]},
        "pm_aliases": ["Ella Seidel"],
    },
    "selekhmeteva, oksana": {
        "polymarket_code": "selekhm",
        "provider_aliases": {"kalstrop_v1": ["Selekhmeteva, Oksana"]},
        "pm_aliases": ["Oksana Selekhmeteva"],
    },
    "sels, jelle": {
        "polymarket_code": "sels",
        "provider_aliases": {"kalstrop_v1": ["Sels, Jelle"]},
        "pm_aliases": ["Jelle Sels"],
    },
    "semenistaja, darja": {
        "polymarket_code": "semenis",
        "provider_aliases": {"kalstrop_v1": ["Semenistaja, Darja"]},
        "pm_aliases": ["Darja Semenistaja"],
    },
    "shapovalov, denis": {
        "polymarket_code": "shapova",
        "provider_aliases": {"kalstrop_v1": ["Shapovalov, Denis"]},
        "pm_aliases": ["Denis Shapovalov"],
    },
    "shelbayh, abdullah": {
        "polymarket_code": "shelbay",
        "provider_aliases": {"kalstrop_v1": ["Shelbayh, Abdullah"]},
        "pm_aliases": ["Abdullah Shelbayh"],
    },
    "shelton, ben": {
        "polymarket_code": "shelton",
        "provider_aliases": {"kalstrop_v1": ["Shelton, Ben"]},
        "pm_aliases": ["Ben Shelton"],
    },
    "sherif ahmed abdelaziz, maiar": {
        "polymarket_code": "sherif",
        "provider_aliases": {"kalstrop_v1": ["Sherif Ahmed Abdelaziz, Maiar"]},
        "pm_aliases": ["Maiar Sherif Ahmed Abdelaziz"],
    },
    "shevchenko, alexander": {
        "polymarket_code": "shevche",
        "provider_aliases": {"kalstrop_v1": ["Shevchenko, Alexander", "Shevchenko, Aleksandr"]},
        "pm_aliases": ["Alexander Shevchenko"],
    },
    "shick, braden": {
        "polymarket_code": "shick",
        "provider_aliases": {"kalstrop_v1": ["Shick, Braden"]},
        "pm_aliases": ["Braden Shick"],
    },
    "shimabukuro, sho": {
        "polymarket_code": "shimabu",
        "provider_aliases": {"kalstrop_v1": ["Shimabukuro, Sho"]},
        "pm_aliases": ["Sho Shimabukuro"],
    },
    "shimizu, yuta": {
        "polymarket_code": "shimizu",
        "provider_aliases": {"kalstrop_v1": ["Shimizu, Yuta"]},
        "pm_aliases": ["Yuta Shimizu"],
    },
    "shnaider, diana": {
        "polymarket_code": "shnaide",
        "provider_aliases": {"kalstrop_v1": ["Shnaider, Diana"]},
        "pm_aliases": ["Diana Shnaider"],
    },
    "shubladze, alexandra": {
        "polymarket_code": "shublad",
        "provider_aliases": {"kalstrop_v1": ["Shubladze, Alexandra"]},
        "pm_aliases": ["Alexandra Shubladze"],
    },
    "shymanovich, iryna": {
        "polymarket_code": "shymano",
        "provider_aliases": {"kalstrop_v1": ["Shymanovich, Iryna"]},
        "pm_aliases": ["Iryna Shymanovich"],
    },
    "siegemund, laura": {
        "polymarket_code": "siegemu",
        "provider_aliases": {"kalstrop_v1": ["Siegemund, Laura"]},
        "pm_aliases": ["Laura Siegemund"],
    },
    "sierra, solana": {
        "polymarket_code": "sierra",
        "provider_aliases": {"kalstrop_v1": ["Sierra, Solana"]},
        "pm_aliases": ["Solana Sierra"],
    },
    "simakin, ilia": {
        "polymarket_code": "simakin",
        "provider_aliases": {"kalstrop_v1": ["Simakin, Ilia"]},
        "pm_aliases": ["Ilia Simakin"],
    },
    "siniakova, katerina": {
        "polymarket_code": "siniako",
        "provider_aliases": {"kalstrop_v1": ["Siniakova, Katerina"]},
        "pm_aliases": ["Katerina Siniakova"],
    },
    "sinner, jannik": {
        "polymarket_code": "sinner",
        "provider_aliases": {"kalstrop_v1": ["Sinner, Jannik"]},
        "pm_aliases": ["Jannik Sinner"],
    },
    "siskova, anna": {
        "polymarket_code": "siskova",
        "provider_aliases": {"kalstrop_v1": ["Siskova, Anna"]},
        "pm_aliases": ["Anna Siskova"],
    },
    "skatov, timofey": {
        "polymarket_code": "skatov",
        "provider_aliases": {"kalstrop_v1": ["Skatov, Timofey"]},
        "pm_aliases": ["Timofey Skatov"],
    },
    "smith, keegan": {
        "polymarket_code": "smit",
        "provider_aliases": {"kalstrop_v1": ["Smith, Keegan"]},
        "pm_aliases": ["Keegan Smith"],
    },
    "snigur, daria": {
        "polymarket_code": "snigur",
        "provider_aliases": {"kalstrop_v1": ["Snigur, Daria"]},
        "pm_aliases": ["Daria Snigur"],
    },
    "sonego, lorenzo": {
        "polymarket_code": "sonego",
        "provider_aliases": {"kalstrop_v1": ["Sonego, Lorenzo"]},
        "pm_aliases": ["Lorenzo Sonego"],
    },
    "sonmez, zeynep": {
        "polymarket_code": "sonmez",
        "provider_aliases": {"kalstrop_v1": ["Sonmez, Zeynep"]},
        "pm_aliases": ["Zeynep Sonmez"],
    },
    "sorribes tormo, sara": {
        "polymarket_code": "tormo",
        "provider_aliases": {"kalstrop_v1": ["Sorribes Tormo, Sara"]},
        "pm_aliases": ["Sara Sorribes Tormo"],
    },
    "spizzirri, eliot": {
        "polymarket_code": "spizzir",
        "provider_aliases": {"kalstrop_v1": ["Spizzirri, Eliot"]},
        "pm_aliases": ["Eliot Spizzirri"],
    },
    "starodubtseva, yulia": {
        "polymarket_code": "starodu",
        "provider_aliases": {"kalstrop_v1": ["Starodubtseva, Yulia"]},
        "pm_aliases": ["Yulia Starodubtseva"],
    },
    "starodubtseva, yuliia": {
        "polymarket_code": "starodu",
        "provider_aliases": {"kalstrop_v1": ["Starodubtseva, Yuliia"]},
        "pm_aliases": ["Yulia Starodubtseva"],
    },
    "stearns, peyton": {
        "polymarket_code": "stearns",
        "provider_aliases": {"kalstrop_v1": ["Stearns, Peyton"]},
        "pm_aliases": ["Peyton Stearns"],
    },
    "stefanini, lucrezia": {
        "polymarket_code": "stefani",
        "provider_aliases": {"kalstrop_v1": ["Stefanini, Lucrezia"]},
        "pm_aliases": ["Lucrezia Stefanini"],
    },
    "stewart, hamish": {
        "polymarket_code": "stewart",
        "provider_aliases": {"kalstrop_v1": ["Stewart, Hamish"]},
        "pm_aliases": ["Hamish Stewart"],
    },
    "stoiana, mary": {
        "polymarket_code": "stoiana",
        "provider_aliases": {"kalstrop_v1": ["Stoiana, Mary"]},
        "pm_aliases": ["Mary Stoiana"],
    },
    "stojsavljevic, mika": {
        "polymarket_code": "stojsav",
        "provider_aliases": {"kalstrop_v1": ["Stojsavljevic, Mika"]},
        "pm_aliases": ["Mika Stojsavljevic"],
    },
    "struff, jan-lennard": {
        "polymarket_code": "struff",
        "provider_aliases": {"kalstrop_v1": ["Struff, Jan-Lennard"]},
        "pm_aliases": ["Jan-Lennard Struff"],
    },
    "sun, lulu": {
        "polymarket_code": "sun",
        "provider_aliases": {"kalstrop_v1": ["Sun, Lulu"]},
        "pm_aliases": ["Lulu Sun"],
    },
    "svajda, zachary": {
        "polymarket_code": "svajda",
        "provider_aliases": {"kalstrop_v1": ["Svajda, Zachary"]},
        "pm_aliases": ["Zachary Svajda"],
    },
    "svitolina, elina": {
        "polymarket_code": "svitoli",
        "provider_aliases": {"kalstrop_v1": ["Svitolina, Elina"]},
        "pm_aliases": ["Elina Svitolina"],
    },
    "svrcina, dalibor": {
        "polymarket_code": "svrcina",
        "provider_aliases": {"kalstrop_v1": ["Svrcina, Dalibor"]},
        "pm_aliases": ["Dalibor Svrcina"],
    },
    "swan, katie": {
        "polymarket_code": "swan",
        "provider_aliases": {"kalstrop_v1": ["Swan, Katie"]},
        "pm_aliases": ["Katie Swan"],
    },
    "sweeny, dane": {
        "polymarket_code": "sweeny",
        "provider_aliases": {"kalstrop_v1": ["Sweeny, Dane"]},
        "pm_aliases": ["Dane Sweeny"],
    },
    "swiatek, iga": {
        "polymarket_code": "swiatek",
        "provider_aliases": {"kalstrop_v1": ["Swiatek, Iga"]},
        "pm_aliases": ["Iga Swiatek"],
    },
    "tabilo, alejandro": {
        "polymarket_code": "tabilo",
        "provider_aliases": {"kalstrop_v1": ["Tabilo, Alejandro"]},
        "pm_aliases": ["Alejandro Tabilo"],
    },
    "tabur, clement": {
        "polymarket_code": "tabur",
        "provider_aliases": {"kalstrop_v1": ["Tabur, Clement"]},
        "pm_aliases": ["Clement Tabur"],
    },
    "tagger, lilli": {
        "polymarket_code": "tagger",
        "provider_aliases": {"kalstrop_v1": ["Tagger, Lilli"]},
        "pm_aliases": ["Lilli Tagger"],
    },
    "tan, harmony": {
        "polymarket_code": "tan",
        "provider_aliases": {"kalstrop_v1": ["Tan, Harmony"]},
        "pm_aliases": ["Harmony Tan"],
    },
    "tararudee, lanlana": {
        "polymarket_code": "tararud",
        "provider_aliases": {"kalstrop_v1": ["Tararudee, Lanlana"]},
        "pm_aliases": ["Lanlana Tararudee"],
    },
    "tarvet, oliver": {
        "polymarket_code": "tarvet",
        "provider_aliases": {"kalstrop_v1": ["Tarvet, Oliver"]},
        "pm_aliases": ["Oliver Tarvet"],
    },
    "tauson, clara": {
        "polymarket_code": "tauson",
        "provider_aliases": {"kalstrop_v1": ["Tauson, Clara"]},
        "pm_aliases": ["Clara Tauson"],
    },
    "teichmann, jil": {
        "polymarket_code": "teichma",
        "provider_aliases": {"kalstrop_v1": ["Teichmann, Jil"]},
        "pm_aliases": ["Jil Teichmann"],
    },
    "thamm, mariella": {
        "polymarket_code": "thamm",
        "provider_aliases": {"kalstrop_v1": ["Thamm, Mariella"]},
        "pm_aliases": ["Mariella Thamm"],
    },
    "tiafoe, frances": {
        "polymarket_code": "tiafoe",
        "provider_aliases": {"kalstrop_v1": ["Tiafoe, Frances"]},
        "pm_aliases": ["Frances Tiafoe"],
    },
    "tien, learner": {
        "polymarket_code": "tien",
        "provider_aliases": {"kalstrop_v1": ["Tien, Learner"]},
        "pm_aliases": ["Learner Tien"],
    },
    "timofeeva, maria": {
        "polymarket_code": "timofee",
        "provider_aliases": {"kalstrop_v1": ["Timofeeva, Maria"]},
        "pm_aliases": ["Maria Timofeeva"],
    },
    "tirante, thiago agustin": {
        "polymarket_code": "tirante",
        "provider_aliases": {"kalstrop_v1": ["Tirante, Thiago Agustin"]},
        "pm_aliases": ["Thiago Agustin Tirante"],
    },
    "tjen, janice": {
        "polymarket_code": "tjen",
        "provider_aliases": {"kalstrop_v1": ["Tjen, Janice"]},
        "pm_aliases": ["Janice Tjen"],
    },
    "tobon, miguel": {
        "polymarket_code": "tobon",
        "provider_aliases": {"kalstrop_v1": ["Tobon, Miguel"]},
        "pm_aliases": ["Miguel Tobon"],
    },
    "tomic, bernard": {
        "polymarket_code": "tomic",
        "provider_aliases": {"kalstrop_v1": ["Tomic, Bernard"]},
        "pm_aliases": ["Bernard Tomic"],
    },
    "tomljanovic, ajla": {
        "polymarket_code": "tomljan",
        "provider_aliases": {"kalstrop_v1": ["Tomljanovic, Ajla"]},
        "pm_aliases": ["Ajla Tomljanovic"],
    },
    "tomova, viktoriya": {
        "polymarket_code": "tomova",
        "provider_aliases": {"kalstrop_v1": ["Tomova, Viktoriya"]},
        "pm_aliases": ["Viktoriya Tomova"],
    },
    "topo, marko": {
        "polymarket_code": "topo",
        "provider_aliases": {"kalstrop_v1": ["ToPo, Marko"]},
        "pm_aliases": ["Marko ToPo"],
    },
    "torres, juan bautista": {
        "polymarket_code": "torres",
        "provider_aliases": {"kalstrop_v1": ["Torres, Juan Bautista"]},
        "pm_aliases": ["Juan Bautista Torres"],
    },
    "townsend, taylor": {
        "polymarket_code": "townsen",
        "provider_aliases": {"kalstrop_v1": ["Townsend, Taylor"]},
        "pm_aliases": ["Taylor Townsend"],
    },
    "trevisan, martina": {
        "polymarket_code": "trevisa",
        "provider_aliases": {"kalstrop_v1": ["Trevisan, Martina"]},
        "pm_aliases": ["Martina Trevisan"],
    },
    "trungelliti, marco": {
        "polymarket_code": "trungel",
        "provider_aliases": {"kalstrop_v1": ["Trungelliti, Marco"]},
        "pm_aliases": ["Marco Trungelliti"],
    },
    "tseng, chun hsin": {
        "polymarket_code": "tseng",
        "provider_aliases": {"kalstrop_v1": ["Tseng, Chun Hsin"]},
        "pm_aliases": ["Chun Hsin Tseng", "Chun-Hsin Tseng"],
    },
    "tsitsipas, stefanos": {
        "polymarket_code": "tsitsip",
        "provider_aliases": {"kalstrop_v1": ["Tsitsipas, Stefanos"]},
        "pm_aliases": ["Stefanos Tsitsipas"],
    },
    "tubello, alice": {
        "polymarket_code": "tubello",
        "provider_aliases": {"kalstrop_v1": ["Tubello, Alice"]},
        "pm_aliases": ["Alice Tubello"],
    },
    "uchida, kaichi": {
        "polymarket_code": "uchida",
        "provider_aliases": {"kalstrop_v1": ["Uchida, Kaichi"]},
        "pm_aliases": ["Kaichi Uchida"],
    },
    "uchijima, moyuka": {
        "polymarket_code": "uchijim",
        "provider_aliases": {"kalstrop_v1": ["Uchijima, Moyuka"]},
        "pm_aliases": ["Moyuka Uchijima"],
    },
    "udvardy, panna": {
        "polymarket_code": "udvardy",
        "provider_aliases": {"kalstrop_v1": ["Udvardy, Panna"]},
        "pm_aliases": ["Panna Udvardy"],
    },
    "ugo carabelli, camilo": {
        "polymarket_code": "carabel",
        "provider_aliases": {"kalstrop_v1": ["Ugo Carabelli, Camilo"]},
        "pm_aliases": ["Camilo Ugo Carabelli"],
    },
    "urgesi, federica": {
        "polymarket_code": "urgesi",
        "provider_aliases": {"kalstrop_v1": ["Urgesi, Federica"]},
        "pm_aliases": ["Federica Urgesi"],
    },
    "urhobo, akasha": {
        "polymarket_code": "urhobo",
        "provider_aliases": {"kalstrop_v1": ["Urhobo, Akasha"]},
        "pm_aliases": ["Akasha Urhobo"],
    },
    "vacherot, valentin": {
        "polymarket_code": "vachero",
        "provider_aliases": {"kalstrop_v1": ["Vacherot, Valentin"]},
        "pm_aliases": ["Valentin Vacherot"],
    },
    "valdmannova, vendula": {
        "polymarket_code": "valdman",
        "provider_aliases": {"kalstrop_v1": ["Valdmannova, Vendula"]},
        "pm_aliases": ["Vendula Valdmannova"],
    },
    "valentova, tereza": {
        "polymarket_code": "valento",
        "provider_aliases": {"kalstrop_v1": ["Valentova, Tereza"]},
        "pm_aliases": ["Tereza Valentova"],
    },
    "vallejo, adolfo daniel": {
        "polymarket_code": "vallejo",
        "provider_aliases": {"kalstrop_v1": ["Vallejo, Adolfo Daniel"]},
        "pm_aliases": ["Adolfo Vallejo"],
    },
    "van assche, luca": {
        "polymarket_code": "assche",
        "provider_aliases": {"kalstrop_v1": ["Van Assche, Luca"]},
        "pm_aliases": ["Luca Van Assche"],
    },
    "van de zandschulp, botic": {
        "polymarket_code": "zandsch",
        "provider_aliases": {"kalstrop_v1": ["Van de Zandschulp, Botic"]},
        "pm_aliases": ["Botic van de Zandschulp"],
    },
    "vandewinkel, hanne": {
        "polymarket_code": "vandewi",
        "provider_aliases": {"kalstrop_v1": ["Vandewinkel, Hanne"]},
        "pm_aliases": ["Hanne Vandewinkel"],
    },
    "vandromme, jeline": {
        "polymarket_code": "vandrom",
        "provider_aliases": {"kalstrop_v1": ["Vandromme, Jeline"]},
        "pm_aliases": ["Jeline Vandromme"],
    },
    "vasa, eero": {
        "polymarket_code": "vasa",
        "provider_aliases": {"kalstrop_v1": ["Vasa, Eero"]},
        "pm_aliases": ["Eero Vasa"],
    },
    "vasilev, alexander": {
        "polymarket_code": "vasilev",
        "provider_aliases": {"kalstrop_v1": ["Vasilev, Alexander"]},
        "pm_aliases": ["Alexander Vasilev"],
    },
    "villalon valdes, nicolas": {
        "polymarket_code": "villalo",
        "provider_aliases": {"kalstrop_v1": ["Villalon Valdes, Nicolas"]},
        "pm_aliases": ["Nicolas Villalon"],
    },
    "vekic, donna": {
        "polymarket_code": "vekic",
        "provider_aliases": {"kalstrop_v1": ["Vekic, Donna"]},
        "pm_aliases": ["Donna Vekic"],
    },
    "vidmanova, darja": {
        "polymarket_code": "vidmano",
        "provider_aliases": {"kalstrop_v1": ["Vidmanova, Darja"]},
        "pm_aliases": ["Darja Vidmanova"],
    },
    "virtanen, otto": {
        "polymarket_code": "virtane",
        "provider_aliases": {"kalstrop_v1": ["Virtanen, Otto"]},
        "pm_aliases": ["Otto Virtanen"],
    },
    "visker, niels": {
        "polymarket_code": "visker",
        "provider_aliases": {"kalstrop_v1": ["Visker, Niels"]},
        "pm_aliases": ["Niels Visker"],
    },
    "volynets, katie": {
        "polymarket_code": "volynet",
        "provider_aliases": {"kalstrop_v1": ["Volynets, Katie"]},
        "pm_aliases": ["Katie Volynets"],
    },
    "vukic, aleksandar": {
        "polymarket_code": "vukic",
        "provider_aliases": {"kalstrop_v1": ["Vukic, Aleksandar"]},
        "pm_aliases": ["Aleksandar Vukic"],
    },
    "walton, adam": {
        "polymarket_code": "walton",
        "provider_aliases": {"kalstrop_v1": ["Walton, Adam"]},
        "pm_aliases": ["Adam Walton"],
    },
    "wang, xinyu": {
        "polymarket_code": "wa",
        "provider_aliases": {"kalstrop_v1": ["Wang, Xinyu"]},
        "pm_aliases": ["Xinyu Wang"],
    },
    "wang, xiyu": {
        "polymarket_code": "wan",
        "provider_aliases": {"kalstrop_v1": ["Wang, Xiyu"]},
        "pm_aliases": ["Xiyu Wang"],
    },
    "watson, heather": {
        "polymarket_code": "watson",
        "provider_aliases": {"kalstrop_v1": ["Watson, Heather"]},
        "pm_aliases": ["Heather Watson"],
    },
    "watt, james": {
        "polymarket_code": "watt",
        "provider_aliases": {"kalstrop_v1": ["Watt, James"]},
        "pm_aliases": ["James Watt"],
    },
    "wawrinka, stan": {
        "polymarket_code": "wawrink",
        "provider_aliases": {"kalstrop_v1": ["Wawrinka, Stan"]},
        "pm_aliases": ["Stan Wawrinka"],
    },
    "wendelken, harry": {
        "polymarket_code": "wendelk",
        "provider_aliases": {"kalstrop_v1": ["Wendelken, Harry"]},
        "pm_aliases": ["Harry Wendelken"],
    },
    "werner, caroline": {
        "polymarket_code": "werner",
        "provider_aliases": {"kalstrop_v1": ["Werner, Caroline"]},
        "pm_aliases": ["Caroline Werner"],
    },
    "wong, coleman": {
        "polymarket_code": "wong",
        "provider_aliases": {"kalstrop_v1": ["Wong, Coleman"]},
        "pm_aliases": ["Coleman Wong"],
    },
    "wu, yibing": {
        "polymarket_code": "wu",
        "provider_aliases": {"kalstrop_v1": ["Wu, Yibing"]},
        "pm_aliases": ["Yibing Wu"],
    },
    "xu, mingge": {
        "polymarket_code": "xu",
        "provider_aliases": {"kalstrop_v1": ["Xu, Mingge", "Xu, Mimi"]},
        "pm_aliases": ["Mingge Xu"],
    },
    "yastremska, dayana": {
        "polymarket_code": "yastrem",
        "provider_aliases": {"kalstrop_v1": ["Yastremska, Dayana"]},
        "pm_aliases": ["Dayana Yastremska"],
    },
    "yevseyev, denis": {
        "polymarket_code": "yevseye",
        "provider_aliases": {"kalstrop_v1": ["Yevseyev, Denis"]},
        "pm_aliases": ["Denis Yevseyev"],
    },
    "ymer, elias": {
        "polymarket_code": "ymer",
        "provider_aliases": {"kalstrop_v1": ["Ymer, Elias"]},
        "pm_aliases": ["Elias Ymer"],
    },
    "you, xiaodi": {
        "polymarket_code": "you",
        "provider_aliases": {"kalstrop_v1": ["You, Xiaodi"]},
        "pm_aliases": ["Xiaodi You"],
    },
    "yuan, yue": {
        "polymarket_code": "yuan",
        "provider_aliases": {"kalstrop_v1": ["Yuan, Yue"]},
        "pm_aliases": ["Yue Yuan"],
    },
    "zakharova, anastasia": {
        "polymarket_code": "zakharo",
        "provider_aliases": {"kalstrop_v1": ["Zakharova, Anastasia"]},
        "pm_aliases": ["Anastasia Zakharova"],
    },
    "zarate, carlos maria": {
        "polymarket_code": "zarate",
        "provider_aliases": {"kalstrop_v1": ["Zarate, Carlos Maria"]},
        "pm_aliases": ["Carlos Maria Zarate"],
    },
    "zarazua, renata": {
        "polymarket_code": "zarazua",
        "provider_aliases": {"kalstrop_v1": ["Zarazua, Renata"]},
        "pm_aliases": ["Renata Zarazua"],
    },
    "zeitune, maximo": {
        "polymarket_code": "zeitune",
        "provider_aliases": {"kalstrop_v1": ["Zeitune, Maximo"]},
        "pm_aliases": ["Maximo Zeitune"],
    },
    "zhang, shuai": {
        "polymarket_code": "zhang",
        "provider_aliases": {"kalstrop_v1": ["Zhang, Shuai"]},
        "pm_aliases": ["Shuai Zhang"],
    },
    "zhang, zhizhen": {
        "polymarket_code": "zhang",
        "provider_aliases": {"kalstrop_v1": ["Zhang, Zhizhen"]},
        "pm_aliases": ["Zhizhen Zhang"],
    },
    "zhao, carol": {
        "polymarket_code": "zhao",
        "provider_aliases": {"kalstrop_v1": ["Zhao, Carol"]},
        "pm_aliases": ["Carol Zhao"],
    },
    "zheng, michael": {
        "polymarket_code": "zheng",
        "provider_aliases": {"kalstrop_v1": ["Zheng, Michael"]},
        "pm_aliases": ["Michael Zheng"],
    },
    "zheng, qinwen": {
        "polymarket_code": "zhen",
        "provider_aliases": {"kalstrop_v1": ["Zheng, Qinwen"]},
        "pm_aliases": ["Qinwen Zheng"],
    },
    "zhu, lin": {
        "polymarket_code": "zhu",
        "provider_aliases": {"kalstrop_v1": ["Zhu, Lin"]},
        "pm_aliases": ["Lin Zhu"],
    },
    "zidansek, tamara": {
        "polymarket_code": "zidanse",
        "provider_aliases": {"kalstrop_v1": ["Zidansek, Tamara"]},
        "pm_aliases": ["Tamara Zidansek"],
    },
    "zverev, alexander": {
        "polymarket_code": "zverev",
        "provider_aliases": {"kalstrop_v1": ["Zverev, Alexander"]},
        "pm_aliases": ["Alexander Zverev"],
    },
    "ajdukovic, duje": {
        "polymarket_code": "ajdukov",
        "provider_aliases": {"kalstrop_v1": ["Ajdukovic, Duje"]},
        "pm_aliases": ["Duje Ajdukovic"],
    },
    "aksu, ayla": {
        "polymarket_code": "aksu",
        "provider_aliases": {"kalstrop_v1": ["Aksu, Ayla"]},
        "pm_aliases": ["Ayla Aksu"],
    },
    "albot, radu": {
        "polymarket_code": "albot",
        "provider_aliases": {"kalstrop_v1": ["Albot, Radu"]},
        "pm_aliases": ["Radu Albot"],
    },
    "ansari, carolyn": {
        "polymarket_code": "ansari",
        "provider_aliases": {"kalstrop_v1": ["Ansari, Carolyn"]},
        "pm_aliases": ["Carolyn Ansari"],
    },
    "bandecchi, susan": {
        "polymarket_code": "bandecc",
        "provider_aliases": {"kalstrop_v1": ["Bandecchi, Susan"]},
        "pm_aliases": ["Susan Bandecchi"],
    },
    "basel, valentin": {
        "polymarket_code": "basel",
        "provider_aliases": {"kalstrop_v1": ["Basel, Valentin"]},
        "pm_aliases": ["Valentin Basel"],
    },
    "blus, aleksander": {
        "polymarket_code": "_unknown",
        "provider_aliases": {"kalstrop_v1": ["Blus, Aleksander"]},
        "pm_aliases": ["Aleksander Blus"],
    },
    "borba, juan": {
        "polymarket_code": "_unknown",
        "provider_aliases": {"kalstrop_v1": ["Borba, Juan"]},
        "pm_aliases": ["Juan Borba"],
    },
    "chiesa, deborah": {
        "polymarket_code": "_unknown",
        "provider_aliases": {"kalstrop_v1": ["Chiesa, Deborah"]},
        "pm_aliases": ["Deborah Chiesa"],
    },
    "compagnucci, tommaso": {
        "polymarket_code": "compagn",
        "provider_aliases": {"kalstrop_v1": ["Compagnucci, Tommaso"]},
        "pm_aliases": ["Tommaso Compagnucci"],
    },
    "couto loureiro, joao victor": {
        "polymarket_code": "_unknown",
        "provider_aliases": {"kalstrop_v1": ["Couto Loureiro, Joao Victor"]},
        "pm_aliases": ["Joao Victor Couto Loureiro"],
    },
    "crawford, oliver": {
        "polymarket_code": "crawfor",
        "provider_aliases": {"kalstrop_v1": ["Crawford, Oliver"]},
        "pm_aliases": ["Oliver Crawford"],
    },
    "crawley, fiona": {
        "polymarket_code": "crawley",
        "provider_aliases": {"kalstrop_v1": ["Crawley, Fiona"]},
        "pm_aliases": ["Fiona Crawley"],
    },
    "de almeida, gustavo ribeiro": {
        "polymarket_code": "_unknown",
        "provider_aliases": {"kalstrop_v1": ["de Almeida, Gustavo Ribeiro"]},
        "pm_aliases": ["Gustavo Ribeiro de Almeida"],
    },
    "del pino, mateo": {
        "polymarket_code": "_unknown",
        "provider_aliases": {"kalstrop_v1": ["Del Pino, Mateo"]},
        "pm_aliases": ["Mateo Del Pino"],
    },
    "delage, pierre": {
        "polymarket_code": "delage",
        "provider_aliases": {"kalstrop_v1": ["Delage, Pierre"]},
        "pm_aliases": ["Pierre Delage"],
    },
    "erhard, mathys": {
        "polymarket_code": "erhard",
        "provider_aliases": {"kalstrop_v1": ["Erhard, Mathys"]},
        "pm_aliases": ["Mathys Erhard"],
    },
    "feldbausch, kilian": {
        "polymarket_code": "feldbau",
        "provider_aliases": {"kalstrop_v1": ["Feldbausch, Kilian"]},
        "pm_aliases": ["Kilian Feldbausch"],
    },
    "frutos alonso, alvaro ariel": {
        "polymarket_code": "_unknown",
        "provider_aliases": {"kalstrop_v1": ["Frutos Alonso, Alvaro Ariel"]},
        "pm_aliases": ["Alvaro Ariel Frutos Alonso"],
    },
    "giovannini, luisina": {
        "polymarket_code": "giovann",
        "provider_aliases": {"kalstrop_v1": ["Giovannini, Luisina"]},
        "pm_aliases": ["Luisina Giovannini"],
    },
    "giustino, lorenzo": {
        "polymarket_code": "giustin",
        "provider_aliases": {"kalstrop_v1": ["Giustino, Lorenzo"]},
        "pm_aliases": ["Lorenzo Giustino"],
    },
    "gjorcheska, lina": {
        "polymarket_code": "gjorche",
        "provider_aliases": {"kalstrop_v1": ["Gjorcheska, Lina"]},
        "pm_aliases": ["Lina Gjorcheska"],
    },
    "gomez, juan sebastian": {
        "polymarket_code": "gome",
        "provider_aliases": {"kalstrop_v1": ["Gomez, Juan Sebastian"]},
        "pm_aliases": ["Juan Sebastian Gomez"],
    },
    "gulin, svyatoslav": {
        "polymarket_code": "gulin",
        "provider_aliases": {"kalstrop_v1": ["Gulin, Svyatoslav"]},
        "pm_aliases": ["Svyatoslav Gulin"],
    },
    "hayen, alejandro": {
        "polymarket_code": "_unknown",
        "provider_aliases": {"kalstrop_v1": ["Hayen, Alejandro"]},
        "pm_aliases": ["Alejandro Hayen"],
    },
    "hemery, calvin": {
        "polymarket_code": "hemery",
        "provider_aliases": {"kalstrop_v1": ["Hemery, Calvin"]},
        "pm_aliases": ["Calvin Hemery"],
    },
    "hruncakova, viktoria": {
        "polymarket_code": "hruncak",
        "provider_aliases": {"kalstrop_v1": ["Hruncakova, Viktoria"]},
        "pm_aliases": ["Viktoria Hruncakova"],
    },
    "kazionova, ekaterina": {
        "polymarket_code": "_unknown",
        "provider_aliases": {"kalstrop_v1": ["Kazionova, Ekaterina"]},
        "pm_aliases": ["Ekaterina Kazionova"],
    },
    "kinoshita, hayu": {
        "polymarket_code": "kinoshi",
        "provider_aliases": {"kalstrop_v1": ["Kinoshita, Hayu"]},
        "pm_aliases": ["Hayu Kinoshita"],
    },
    "kobori, momoko": {
        "polymarket_code": "_unknown",
        "provider_aliases": {"kalstrop_v1": ["Kobori, Momoko"]},
        "pm_aliases": ["Momoko Kobori"],
    },
    "kulikova, anastasia": {
        "polymarket_code": "_unknown",
        "provider_aliases": {"kalstrop_v1": ["Kulikova, Anastasia"]},
        "pm_aliases": ["Anastasia Kulikova"],
    },
    "kuzmanov, dimitar": {
        "polymarket_code": "kuzmano",
        "provider_aliases": {"kalstrop_v1": ["Kuzmanov, Dimitar"]},
        "pm_aliases": ["Dimitar Kuzmanov"],
    },
    "leonard, manon": {
        "polymarket_code": "leonard",
        "provider_aliases": {"kalstrop_v1": ["Leonard, Manon"]},
        "pm_aliases": ["Manon Leonard"],
    },
    "li, ann": {
        "polymarket_code": "_unknown",
        "provider_aliases": {"kalstrop_v1": ["Li, Ann"]},
        "pm_aliases": ["Ann Li"],
    },
    "linde palacios, samuel alejandro": {
        "polymarket_code": "_unknown",
        "provider_aliases": {"kalstrop_v1": ["Linde Palacios, Samuel Alejandro"]},
        "pm_aliases": ["Samuel Alejandro Linde Palacios"],
    },
    "lizarazo, yuliana": {
        "polymarket_code": "lizaraz",
        "provider_aliases": {"kalstrop_v1": ["Lizarazo, Yuliana"]},
        "pm_aliases": ["Yuliana Lizarazo"],
    },
    "lokoli, laurent": {
        "polymarket_code": "lokoli",
        "provider_aliases": {"kalstrop_v1": ["Lokoli, Laurent"]},
        "pm_aliases": ["Laurent Lokoli"],
    },
    "lopez morillo, imanol": {
        "polymarket_code": "morillo",
        "provider_aliases": {"kalstrop_v1": ["Lopez Morillo, Imanol"]},
        "pm_aliases": ["Imanol Lopez Morillo"],
    },
    "marti pujolras, alex": {
        "polymarket_code": "_unknown",
        "provider_aliases": {"kalstrop_v1": ["Marti Pujolras, Alex"]},
        "pm_aliases": ["Alex Marti Pujolras"],
    },
    "martin manzano, juan cruz": {
        "polymarket_code": "_unknown",
        "provider_aliases": {"kalstrop_v1": ["Martin Manzano, Juan Cruz"]},
        "pm_aliases": ["Juan Cruz Martin Manzano", "Juan Martin"],
    },
    "martin tiffon, pol": {
        "polymarket_code": "tiffon",
        "provider_aliases": {"kalstrop_v1": ["Martin Tiffon, Pol"]},
        "pm_aliases": ["Pol Martin Tiffon"],
    },
    "mazzola, alessandra": {
        "polymarket_code": "mazzola",
        "provider_aliases": {"kalstrop_v1": ["Mazzola, Alessandra"]},
        "pm_aliases": ["Alessandra Mazzola"],
    },
    "mejia, nicolas": {
        "polymarket_code": "mejia",
        "provider_aliases": {"kalstrop_v1": ["Mejia, Nicolas"]},
        "pm_aliases": ["Nicolas Mejia"],
    },
    "meligeni alves, felipe": {
        "polymarket_code": "alves",
        "provider_aliases": {"kalstrop_v1": ["Meligeni Alves, Felipe"]},
        "pm_aliases": ["Felipe Meligeni Alves"],
    },
    "molleker, rudolf": {
        "polymarket_code": "_unknown",
        "provider_aliases": {"kalstrop_v1": ["Molleker, Rudolf"]},
        "pm_aliases": ["Rudolf Molleker"],
    },
    "moller, elmer": {
        "polymarket_code": "moeller",
        "provider_aliases": {"kalstrop_v1": ["Moller, Elmer"]},
        "pm_aliases": ["Elmer Moeller"],
    },
    "monzon, ignacio": {
        "polymarket_code": "monzon",
        "provider_aliases": {"kalstrop_v1": ["Monzon, Ignacio"]},
        "pm_aliases": ["Ignacio Monzon"],
    },
    "nagal, sumit": {
        "polymarket_code": "nagal",
        "provider_aliases": {"kalstrop_v1": ["Nagal, Sumit"]},
        "pm_aliases": ["Sumit Nagal"],
    },
    "napolitano, stefano": {
        "polymarket_code": "napolit",
        "provider_aliases": {"kalstrop_v1": ["Napolitano, Stefano"]},
        "pm_aliases": ["Stefano Napolitano"],
    },
    "osuigwe, whitney": {
        "polymarket_code": "osuigwe",
        "provider_aliases": {"kalstrop_v1": ["Osuigwe, Whitney"]},
        "pm_aliases": ["Whitney Osuigwe"],
    },
    "parisca romera, ignacio": {
        "polymarket_code": "_unknown",
        "provider_aliases": {"kalstrop_v1": ["Parisca Romera, Ignacio"]},
        "pm_aliases": ["Ignacio Parisca Romera"],
    },
    "pieczonka, filip": {
        "polymarket_code": "_unknown",
        "provider_aliases": {"kalstrop_v1": ["Pieczonka, Filip"]},
        "pm_aliases": ["Filip Pieczonka"],
    },
    "ratti, lucio": {
        "polymarket_code": "ratti",
        "provider_aliases": {"kalstrop_v1": ["Ratti, Lucio"]},
        "pm_aliases": ["Lucio Ratti"],
    },
    "roncadelli, franco": {
        "polymarket_code": "roncade",
        "provider_aliases": {"kalstrop_v1": ["Roncadelli, Franco"]},
        "pm_aliases": ["Franco Roncadelli"],
    },
    "sadzik, jan": {
        "polymarket_code": "_unknown",
        "provider_aliases": {"kalstrop_v1": ["Sadzik, Jan"]},
        "pm_aliases": ["Jan Sadzik"],
    },
    "sanchez, ana sofia": {
        "polymarket_code": "sanchez",
        "provider_aliases": {"kalstrop_v1": ["Sanchez, Ana Sofia"]},
        "pm_aliases": ["Ana Sofia Sanchez"],
    },
    "serban, raluca georgiana": {
        "polymarket_code": "serban",
        "provider_aliases": {"kalstrop_v1": ["Serban, Raluca Georgiana"]},
        "pm_aliases": ["Raluca Serban"],
    },
    "sesko, ziga": {
        "polymarket_code": "sesko",
        "provider_aliases": {"kalstrop_v1": ["Sesko, Ziga"]},
        "pm_aliases": ["Ziga Sesko"],
    },
    "smith, colton": {
        "polymarket_code": "smith",
        "provider_aliases": {"kalstrop_v1": ["Smith, Colton"]},
        "pm_aliases": ["Colton Smith"],
    },
    "sobolieva, anastasiia": {
        "polymarket_code": "sobolie",
        "provider_aliases": {"kalstrop_v1": ["Sobolieva, Anastasiia"]},
        "pm_aliases": ["Anastasiia Sobolieva"],
    },
    "vickery, sachia": {
        "polymarket_code": "vickery",
        "provider_aliases": {"kalstrop_v1": ["Vickery, Sachia"]},
        "pm_aliases": ["Sachia Vickery"],
    },
    "voloshchuk, angelina": {
        "polymarket_code": "_unknown",
        "provider_aliases": {"kalstrop_v1": ["Voloshchuk, Angelina"]},
        "pm_aliases": ["Angelina Voloshchuk"],
    },
    "von deichmann, kathinka": {
        "polymarket_code": "deichma",
        "provider_aliases": {"kalstrop_v1": ["Von Deichmann, Kathinka"]},
        "pm_aliases": ["Kathinka von Deichmann"],
    },
    "wallin, olle": {
        "polymarket_code": "_unknown",
        "provider_aliases": {"kalstrop_v1": ["Wallin, Olle"]},
        "pm_aliases": ["Olle Wallin"],
    },
    "waltert, simona": {
        "polymarket_code": "waltert",
        "provider_aliases": {"kalstrop_v1": ["Waltert, Simona"]},
        "pm_aliases": ["Simona Waltert"],
    },
    "yaneva, elizara": {
        "polymarket_code": "yaneva",
        "provider_aliases": {"kalstrop_v1": ["Yaneva, Elizara"]},
        "pm_aliases": ["Elizara Yaneva"],
    },
    "zvonareva, vera": {
        "polymarket_code": "zvonare",
        "provider_aliases": {"kalstrop_v1": ["Zvonareva, Vera"]},
        "pm_aliases": ["Vera Zvonareva"],
    },
    "alcala gurri, max": {
        "polymarket_code": "gurri",
        "provider_aliases": {"kalstrop_v1": ["Alcala Gurri, Max"]},
        "pm_aliases": ["Max Alcala Gurri"],
    },
    "bailly, gilles arnaud": {
        "polymarket_code": "bailly",
        "provider_aliases": {"kalstrop_v1": ["Bailly, Gilles Arnaud"]},
        "pm_aliases": ["Gilles Arnaud Bailly"],
    },
    "bar biryukov, petr": {
        "polymarket_code": "biryuko",
        "provider_aliases": {"kalstrop_v1": ["Bar Biryukov, Petr"]},
        "pm_aliases": ["Petr Bar Biryukov"],
    },
    "berkieta, tomasz": {
        "polymarket_code": "berkiet",
        "provider_aliases": {"kalstrop_v1": ["Berkieta, Tomasz"]},
        "pm_aliases": ["Tomasz Berkieta"],
    },
    "bertea, elena ruxandra": {
        "polymarket_code": "bertea",
        "provider_aliases": {"kalstrop_v1": ["Bertea, Elena Ruxandra"]},
        "pm_aliases": ["Elena Ruxandra Bertea"],
    },
    "bertrand, robin": {
        "polymarket_code": "bertran",
        "provider_aliases": {"kalstrop_v1": ["Bertrand, Robin"]},
        "pm_aliases": ["Robin Bertrand"],
    },
    "blancaneaux, geoffrey": {
        "polymarket_code": "blancan",
        "provider_aliases": {"kalstrop_v1": ["Blancaneaux, Geoffrey"]},
        "pm_aliases": ["Geoffrey Blancaneaux"],
    },
    "broska, florian": {
        "polymarket_code": "broska",
        "provider_aliases": {"kalstrop_v1": ["Broska, Florian"]},
        "pm_aliases": ["Florian Broska"],
    },
    "brouwer, gijs": {
        "polymarket_code": "brouwer",
        "provider_aliases": {"kalstrop_v1": ["Brouwer, Gijs"]},
        "pm_aliases": ["Gijs Brouwer"],
    },
    "buldorini, peter": {
        "polymarket_code": "buldori",
        "provider_aliases": {"kalstrop_v1": ["Buldorini, Peter"]},
        "pm_aliases": ["Peter Buldorini"],
    },
    "bulgaru, miriam bianca": {
        "polymarket_code": "bulgaru",
        "provider_aliases": {"kalstrop_v1": ["Bulgaru, Miriam Bianca"]},
        "pm_aliases": ["Miriam Bulgaru"],
    },
    "campana lee, gerard": {
        "polymarket_code": "gerardc",
        "provider_aliases": {"kalstrop_v1": ["Campana Lee, Gerard"]},
        "pm_aliases": ["Gerard Campana Lee"],
    },
    "coulibaly, eliakim": {
        "polymarket_code": "couliba",
        "provider_aliases": {"kalstrop_v1": ["Coulibaly, Eliakim"]},
        "pm_aliases": ["Eliakim Coulibaly"],
    },
    "de la fuente, santiago": {
        "polymarket_code": "fuente",
        "provider_aliases": {"kalstrop_v1": ["De la Fuente, Santiago"]},
        "pm_aliases": ["Santiago De La Fuente"],
    },
    "demanet, emilien": {
        "polymarket_code": "demanet",
        "provider_aliases": {"kalstrop_v1": ["Demanet, Emilien"]},
        "pm_aliases": ["Emilien Demanet"],
    },
    "dhamne, manas": {
        "polymarket_code": "manas",
        "provider_aliases": {"kalstrop_v1": ["Dhamne, Manas"]},
        "pm_aliases": ["Manoj Dhamne Manas"],
    },
    "dimitrov, grigor": {
        "polymarket_code": "dimitro",
        "provider_aliases": {"kalstrop_v1": ["Dimitrov, Grigor"]},
        "pm_aliases": ["Grigor Dimitrov"],
    },
    "dinev, dinko": {
        "polymarket_code": "dinev",
        "provider_aliases": {"kalstrop_v1": ["Dinev, Dinko"]},
        "pm_aliases": ["Dinko Dinev"],
    },
    "djere, laslo": {
        "polymarket_code": "djere",
        "provider_aliases": {"kalstrop_v1": ["Djere, Laslo"]},
        "pm_aliases": ["Laslo Djere"],
    },
    "erel, yanki": {
        "polymarket_code": "erel",
        "provider_aliases": {"kalstrop_v1": ["Erel, Yanki"]},
        "pm_aliases": ["Yanki Erel"],
    },
    "escurra isnardi, hernando jose": {
        "polymarket_code": "_unknown",
        "provider_aliases": {"kalstrop_v1": ["Escurra Isnardi, Hernando Jose"]},
        "pm_aliases": ["Hernando Jose Escurra Isnardi", "Hernando Escurra"],
    },
    "falei, aliona": {
        "polymarket_code": "falei",
        "provider_aliases": {"kalstrop_v1": ["Falei, Aliona"]},
        "pm_aliases": ["Aliona Falei"],
    },
    "fancutt, thomas": {
        "polymarket_code": "fancutt",
        "provider_aliases": {"kalstrop_v1": ["Fancutt, Thomas"]},
        "pm_aliases": ["Thomas Fancutt"],
    },
    "ferreira silva, frederico": {
        "polymarket_code": "sil",
        "provider_aliases": {"kalstrop_v1": ["Ferreira Silva, Frederico"]},
        "pm_aliases": ["Frederico Ferreira Silva"],
    },
    "ferro, fiona": {
        "polymarket_code": "ferro",
        "provider_aliases": {"kalstrop_v1": ["Ferro, Fiona"]},
        "pm_aliases": ["Fiona Ferro"],
    },
    "gannon, conor": {
        "polymarket_code": "gannon",
        "provider_aliases": {"kalstrop_v1": ["Gannon, Conor"]},
        "pm_aliases": ["Conor Gannon"],
    },
    "glinka, daniil": {
        "polymarket_code": "glinka",
        "provider_aliases": {"kalstrop_v1": ["Glinka, Daniil"]},
        "pm_aliases": ["Daniil Glinka"],
    },
    "heide, gustavo": {
        "polymarket_code": "heide",
        "provider_aliases": {"kalstrop_v1": ["Heide, Gustavo"]},
        "pm_aliases": ["Gustavo Heide"],
    },
    "henning, philip": {
        "polymarket_code": "henning",
        "provider_aliases": {"kalstrop_v1": ["Henning, Philip"]},
        "pm_aliases": ["Philip Henning"],
    },
    "hernandez, alex": {
        "polymarket_code": "hernand",
        "provider_aliases": {"kalstrop_v1": ["Hernandez, Alex"]},
        "pm_aliases": ["Alex Hernandez"],
    },
    "hontama, mai": {
        "polymarket_code": "hontama",
        "provider_aliases": {"kalstrop_v1": ["Hontama, Mai"]},
        "pm_aliases": ["Mai Hontama"],
    },
    "ivanov, ivan": {
        "polymarket_code": "ivanov",
        "provider_aliases": {"kalstrop_v1": ["Ivanov, Ivan"]},
        "pm_aliases": ["Ivan Ivanov"],
    },
    "jade, daniel": {
        "polymarket_code": "jade",
        "provider_aliases": {"kalstrop_v1": ["Jade, Daniel"]},
        "pm_aliases": ["Daniel Jade"],
    },
    "janvier, maxime": {
        "polymarket_code": "janvier",
        "provider_aliases": {"kalstrop_v1": ["Janvier, Maxime"]},
        "pm_aliases": ["Maxime Janvier"],
    },
    "jeanjean, leolia": {
        "polymarket_code": "jeanjea",
        "provider_aliases": {"kalstrop_v1": ["Jeanjean, Leolia"]},
        "pm_aliases": ["Leolia Jeanjean"],
    },
    "jorge, francisca": {
        "polymarket_code": "jorge",
        "provider_aliases": {"kalstrop_v1": ["Jorge, Francisca"]},
        "pm_aliases": ["Francisca Jorge"],
    },
    "jorge, matilde": {
        "polymarket_code": "jorg",
        "provider_aliases": {"kalstrop_v1": ["Jorge, Matilde"]},
        "pm_aliases": ["Matilde Jorge"],
    },
    "karol, milos": {
        "polymarket_code": "karol",
        "provider_aliases": {"kalstrop_v1": ["Karol, Milos"]},
        "pm_aliases": ["Milos Karol"],
    },
    "kotov, pavel": {
        "polymarket_code": "kotov",
        "provider_aliases": {"kalstrop_v1": ["Kotov, Pavel"]},
        "pm_aliases": ["Pavel Kotov"],
    },
    "kovinic, danka": {
        "polymarket_code": "kovinic",
        "provider_aliases": {"kalstrop_v1": ["Kovinic, Danka"]},
        "pm_aliases": ["Danka Kovinic"],
    },
    "ku, yeon woo": {
        "polymarket_code": "ku",
        "provider_aliases": {"kalstrop_v1": ["Ku, Yeon Woo"]},
        "pm_aliases": ["Yeon-Woo Ku"],
    },
    "kubka, martyna": {
        "polymarket_code": "kubka",
        "provider_aliases": {"kalstrop_v1": ["Kubka, Martyna"]},
        "pm_aliases": ["Martyna Kubka"],
    },
    "kuzmova, katarina": {
        "polymarket_code": "kuzmova",
        "provider_aliases": {"kalstrop_v1": ["Kuzmova, Katarina"]},
        "pm_aliases": ["Katarina Kuzmova"],
    },
    "kym, jerome": {
        "polymarket_code": "kym",
        "provider_aliases": {"kalstrop_v1": ["Kym, Jerome"]},
        "pm_aliases": ["Jerome Kym"],
    },
    "lys, eva": {
        "polymarket_code": "lys",
        "provider_aliases": {"kalstrop_v1": ["Lys, Eva"]},
        "pm_aliases": ["Eva Lys"],
    },
    "ma, yexin": {
        "polymarket_code": "ma",
        "provider_aliases": {"kalstrop_v1": ["MA, Yexin"]},
        "pm_aliases": ["Yexin Ma"],
    },
    "michalski, daniel": {
        "polymarket_code": "michals",
        "provider_aliases": {"kalstrop_v1": ["Michalski, Daniel"]},
        "pm_aliases": ["Daniel Michalski"],
    },
    "micic, elena": {
        "polymarket_code": "micic",
        "provider_aliases": {"kalstrop_v1": ["Micic, Elena"]},
        "pm_aliases": ["Elena Micic"],
    },
    "mintegi del olmo, ane": {
        "polymarket_code": "olmo",
        "provider_aliases": {"kalstrop_v1": ["Mintegi Del Olmo, Ane"]},
        "pm_aliases": ["Ane Mintegi Del Olmo"],
    },
    "mladenovic, kristina": {
        "polymarket_code": "mladeno",
        "provider_aliases": {"kalstrop_v1": ["Mladenovic, Kristina"]},
        "pm_aliases": ["Kristina Mladenovic"],
    },
    "morvayova, viktoria": {
        "polymarket_code": "morvayo",
        "provider_aliases": {"kalstrop_v1": ["Morvayova, Viktoria"]},
        "pm_aliases": ["Viktoria Morvayova"],
    },
    "neumayer, lukas": {
        "polymarket_code": "neumaye",
        "provider_aliases": {"kalstrop_v1": ["Neumayer, Lukas"]},
        "pm_aliases": ["Lukas Neumayer"],
    },
    "ngounoue, clervie": {
        "polymarket_code": "ngounou",
        "provider_aliases": {"kalstrop_v1": ["Ngounoue, Clervie"]},
        "pm_aliases": ["Clervie Ngounoue"],
    },
    "noguchi, rio": {
        "polymarket_code": "noguchi",
        "provider_aliases": {"kalstrop_v1": ["Noguchi, Rio"]},
        "pm_aliases": ["Rio Noguchi"],
    },
    "noskova, linda": {
        "polymarket_code": "noskova",
        "provider_aliases": {"kalstrop_v1": ["Noskova, Linda"]},
        "pm_aliases": ["Linda Noskova"],
    },
    "nunez vera, alex santino": {
        "polymarket_code": "_unknown",
        "provider_aliases": {"kalstrop_v1": ["Nunez Vera, Alex Santino"]},
        "pm_aliases": ["Alex Santino Nunez Vera", "Alex Santino Nunez"],
    },
    "pace, francesca": {
        "polymarket_code": "pace",
        "provider_aliases": {"kalstrop_v1": ["Pace, Francesca"]},
        "pm_aliases": ["Francesca Pace"],
    },
    "passaro, francesco": {
        "polymarket_code": "passaro",
        "provider_aliases": {"kalstrop_v1": ["Passaro, Francesco"]},
        "pm_aliases": ["Francesco Passaro"],
    },
    "perot, raphael": {
        "polymarket_code": "perot",
        "provider_aliases": {"kalstrop_v1": ["Perot, Raphael"]},
        "pm_aliases": ["Raphael Perot"],
    },
    "poullain, lucas": {
        "polymarket_code": "poullai",
        "provider_aliases": {"kalstrop_v1": ["Poullain, Lucas"]},
        "pm_aliases": ["Lucas Poullain"],
    },
    "raquillet, leo": {
        "polymarket_code": "raquill",
        "provider_aliases": {"kalstrop_v1": ["Raquillet, Leo"]},
        "pm_aliases": ["Leo Raquillet"],
    },
    "ribecai, michele": {
        "polymarket_code": "ribecai",
        "provider_aliases": {"kalstrop_v1": ["Ribecai, Michele"]},
        "pm_aliases": ["Michele Ribecai"],
    },
    "ribero, franco": {
        "polymarket_code": "ribero",
        "provider_aliases": {"kalstrop_v1": ["Ribero, Franco"]},
        "pm_aliases": ["Franco Ribero"],
    },
    "rinaldo persson, kajsa": {
        "polymarket_code": "persson",
        "provider_aliases": {"kalstrop_v1": ["Rinaldo Persson, Kajsa"]},
        "pm_aliases": ["Kajsa Rinaldo Persson"],
    },
    "ristic, mia": {
        "polymarket_code": "ristic",
        "provider_aliases": {"kalstrop_v1": ["Ristic, Mia"]},
        "pm_aliases": ["Mia Ristic"],
    },
    "rolland de ravel, cosme": {
        "polymarket_code": "_unknown",
        "provider_aliases": {"kalstrop_v1": ["Rolland de Ravel, Cosme"]},
        "pm_aliases": ["Cosme Rolland de Ravel", "Cosme de Ravel"],
    },
    "ryser, valentina": {
        "polymarket_code": "ryser",
        "provider_aliases": {"kalstrop_v1": ["Ryser, Valentina"]},
        "pm_aliases": ["Valentina Ryser"],
    },
    "sakellaridis, stefanos": {
        "polymarket_code": "sakella",
        "provider_aliases": {"kalstrop_v1": ["Sakellaridis, Stefanos"]},
        "pm_aliases": ["Stefanos Sakellaridis"],
    },
    "santamarta roig, andres": {
        "polymarket_code": "_unknown",
        "provider_aliases": {"kalstrop_v1": ["Santamarta Roig, Andres"]},
        "pm_aliases": ["Andres Santamarta Roig", "Andres Santamarta"],
    },
    "saraiva dos santos, paulo andre": {
        "polymarket_code": "santos",
        "provider_aliases": {"kalstrop_v1": ["Saraiva Dos Santos, Paulo Andre"]},
        "pm_aliases": ["Paulo Andre Saraiva Dos Santos", "Paulo Dos Santos"],
    },
    "seggerman, ryan": {
        "polymarket_code": "seggerm",
        "provider_aliases": {"kalstrop_v1": ["Seggerman, Ryan"]},
        "pm_aliases": ["Ryan Seggerman"],
    },
    "seyboth wild, thiago": {
        "polymarket_code": "wild",
        "provider_aliases": {"kalstrop_v1": ["Seyboth Wild, Thiago"]},
        "pm_aliases": ["Thiago Seyboth Wild"],
    },
    "soto, matias": {
        "polymarket_code": "soto",
        "provider_aliases": {"kalstrop_v1": ["Soto, Matias"]},
        "pm_aliases": ["Matias Soto"],
    },
    "spink, indianna": {
        "polymarket_code": "spink",
        "provider_aliases": {"kalstrop_v1": ["Spink, Indianna"]},
        "pm_aliases": ["Indianna Spink"],
    },
    "squire, henri": {
        "polymarket_code": "squire",
        "provider_aliases": {"kalstrop_v1": ["Squire, Henri"]},
        "pm_aliases": ["Henri Squire"],
    },
    "sramkova, rebecca": {
        "polymarket_code": "sramkov",
        "provider_aliases": {"kalstrop_v1": ["Sramkova, Rebecca"]},
        "pm_aliases": ["Rebecca Sramkova"],
    },
    "tabata, ryo": {
        "polymarket_code": "tabata",
        "provider_aliases": {"kalstrop_v1": ["Tabata, Ryo"]},
        "pm_aliases": ["Ryo Tabata"],
    },
    "tian, fangran": {
        "polymarket_code": "tian",
        "provider_aliases": {"kalstrop_v1": ["Tian, Fangran"]},
        "pm_aliases": ["Fangran Tian"],
    },
    "travaglia, stefano": {
        "polymarket_code": "travagl",
        "provider_aliases": {"kalstrop_v1": ["Travaglia, Stefano"]},
        "pm_aliases": ["Stefano Travaglia"],
    },
    "vandecasteele, quinn": {
        "polymarket_code": "vandeca",
        "provider_aliases": {"kalstrop_v1": ["Vandecasteele, Quinn"]},
        "pm_aliases": ["Quinn Vandecasteele"],
    },
    "vasami, jacopo": {
        "polymarket_code": "vasami",
        "provider_aliases": {"kalstrop_v1": ["Vasami, Jacopo"]},
        "pm_aliases": ["Jacopo Vasami"],
    },
    "villanueva, gonzalo": {
        "polymarket_code": "villanu",
        "provider_aliases": {"kalstrop_v1": ["Villanueva, Gonzalo"]},
        "pm_aliases": ["Gonzalo Villanueva"],
    },
    "wazny, alan": {
        "polymarket_code": "wazny",
        "provider_aliases": {"kalstrop_v1": ["Wazny, Alan"]},
        "pm_aliases": ["Alan Wazny"],
    },
    "weis, alexander": {
        "polymarket_code": "weis",
        "provider_aliases": {"kalstrop_v1": ["Weis, Alexander"]},
        "pm_aliases": ["Alexander Weis"],
    },
    "wiedenmann, luca": {
        "polymarket_code": "wiedenm",
        "provider_aliases": {"kalstrop_v1": ["Wiedenmann, Luca"]},
        "pm_aliases": ["Luca Wiedenmann"],
    },
    "willwerth, benjamin": {
        "polymarket_code": "willwer",
        "provider_aliases": {"kalstrop_v1": ["Willwerth, Benjamin"]},
        "pm_aliases": ["Benjamin Willwerth"],
    },
    "zamarripa, allura": {
        "polymarket_code": "zamarri",
        "provider_aliases": {"kalstrop_v1": ["Zamarripa, Allura"]},
        "pm_aliases": ["Allura Zamarripa"],
    },
    "zhou, yi": {
        "polymarket_code": "zhou",
        "provider_aliases": {"kalstrop_v1": ["Zhou, Yi"]},
        "pm_aliases": ["Yi Zhou"],
    },
    "zhu, michael": {
        "polymarket_code": "zhu",
        "provider_aliases": {"kalstrop_v1": ["Zhu, Michael"]},
        "pm_aliases": ["Michael Zhu"],
    },
    "hsu, yu hsiou": {
        "polymarket_code": "hsu",
        "provider_aliases": {"kalstrop_v1": ["Hsu, Yu Hsiou"]},
        "pm_aliases": ["Yu-Hsiou Hsu"],
    },
    "pegula, jessica": {
        "polymarket_code": "pegula",
        "provider_aliases": {"kalstrop_v1": ["Pegula, Jessica"]},
        "pm_aliases": ["Jessica Pegula"],
    },
    "bullamah, filipe": {
        "polymarket_code": "bullama",
        "provider_aliases": {"kalstrop_v1": ["Bullamah, Filipe"]},
        "pm_aliases": ["Filipe Bullamah"],
    },
    "cretu, cezar": {
        "polymarket_code": "cretu",
        "provider_aliases": {"kalstrop_v1": ["Cretu, Cezar (2001)"]},
        "pm_aliases": ["Cezar Cretu"],
    },
    "gima, sebastian": {
        "polymarket_code": "gima",
        "provider_aliases": {"kalstrop_v1": ["Gima, Sebastian"]},
        "pm_aliases": ["Sebastian Gima"],
    },
    "gojo, borna": {
        "polymarket_code": "gojo",
        "provider_aliases": {"kalstrop_v1": ["Gojo, Borna"]},
        "pm_aliases": ["Borna Gojo"],
    },
    "leite, wilson": {
        "polymarket_code": "leite",
        "provider_aliases": {"kalstrop_v1": ["Leite, Wilson"]},
        "pm_aliases": ["Wilson Leite"],
    },
    "mayot, harold": {
        "polymarket_code": "mayot",
        "provider_aliases": {"kalstrop_v1": ["Mayot, Harold"]},
        "pm_aliases": ["Harold Mayot"],
    },
    "oliveira, nicolas": {
        "polymarket_code": "oliveir",
        "provider_aliases": {"kalstrop_v1": ["Oliveira, Nicolas"]},
        "pm_aliases": ["Nicolas Oliveira"],
    },
    "sebov, katherine": {
        "polymarket_code": "sebov",
        "provider_aliases": {"kalstrop_v1": ["Sebov, Katherine"]},
        "pm_aliases": ["Katherine Sebov"],
    },
    "zanellato, nicolas": {
        "polymarket_code": "zanella",
        "provider_aliases": {"kalstrop_v1": ["Zanellato, Nicolas"]},
        "pm_aliases": ["Nicolas Zanellato"],
    },
}


TENNIS_LEAGUES = {
    "rgm": {
        "polymarket_league_code": "atp",
        "sets_to_win": 3,
        "v1_aliases": ["french open men singles"],
        "boltodds_aliases": ["roland garros (m) - tennis"],
    },
    "rgw": {
        "polymarket_league_code": "wta",
        "v1_aliases": ["french open women singles"],
        "boltodds_aliases": ["roland garros (w) - tennis"],
    },
    "birmm": {
        "polymarket_league_code": "atp",
        "v1_aliases": ["atp challenger birmingham"],
        "boltodds_aliases": ["challenger birmingham - tennis"],
    },
    "tyler": {
        "polymarket_league_code": "atp",
        "v1_aliases": ["atp challenger tyler"],
        "boltodds_aliases": ["challenger tyler - tennis"],
    },
    "centurion": {
        "polymarket_league_code": "atp",
        "v1_aliases": ["atp challenger centurion 2"],
        "boltodds_aliases": ["challenger centurion - tennis"],
    },
    "perugia": {
        "polymarket_league_code": "atp",
        "v1_aliases": ["atp challenger perugia"],
        "boltodds_aliases": ["challenger perugia - tennis"],
    },
    "heilbronn": {
        "polymarket_league_code": "atp",
        "v1_aliases": ["atp challenger bad rappenau"],
        "boltodds_aliases": ["challenger heilbronn - tennis"],
    },
    "prostejov": {
        "polymarket_league_code": "atp",
        "v1_aliases": ["atp challenger prostejov"],
        "boltodds_aliases": ["challenger prostejov - tennis"],
    },
    "stuttgart": {
        "polymarket_league_code": "atp",
        "v1_aliases": ["atp stuttgart"],
        "boltodds_aliases": ["atp stuttgart - tennis"],
    },
    "libema_m": {
        "polymarket_league_code": "atp",
        "v1_aliases": ["atp s-hertogenbosch"],
        "boltodds_aliases": ["atp s-hertogenbosch - tennis", "atp hertogenbosch - tennis"],
    },
    "bratislava": {
        "polymarket_league_code": "atp",
        "v1_aliases": ["atp challenger bratislava"],
        "boltodds_aliases": ["challenger bratislava - tennis"],
    },
    "cattolica": {
        "polymarket_league_code": "atp",
        "v1_aliases": ["atp challenger cattolica"],
        "boltodds_aliases": ["challenger cattolica - tennis"],
    },
    "ilkley_m": {
        "polymarket_league_code": "atp",
        "v1_aliases": ["atp challenger ilkley"],
        "boltodds_aliases": ["challenger ilkley - tennis"],
    },
    "lyon": {
        "polymarket_league_code": "atp",
        "v1_aliases": ["atp challenger lyon"],
        "boltodds_aliases": ["challenger lyon - tennis"],
    },
    "tucuman": {
        "polymarket_league_code": "atp",
        "v1_aliases": ["atp challenger san miguel de tucuman"],
        "boltodds_aliases": ["challenger san miguel de tucuman - tennis"],
    },
    "halle_m": {
        "polymarket_league_code": "atp",
        "v1_aliases": ["atp halle"],
        "boltodds_aliases": ["atp halle - tennis"],
    },
    "queen_m": {
        "polymarket_league_code": "atp",
        "v1_aliases": ["atp london"],
        "boltodds_aliases": ["atp london - tennis"],
    },
    "nott_m": {
        "polymarket_league_code": "atp",
        "v1_aliases": ["atp challenger nottingham 2"],
        "boltodds_aliases": ["challenger nottingham - tennis"],
    },
    "asuncion": {
        "polymarket_league_code": "atp",
        "v1_aliases": ["atp challenger asuncion 2"],
        "boltodds_aliases": ["challenger asuncion - tennis"],
    },
    "royan": {
        "polymarket_league_code": "atp",
        "v1_aliases": ["atp challenger royan"],
        "boltodds_aliases": ["challenger royan - tennis"],
    },
    "parma": {
        "polymarket_league_code": "atp",
        "v1_aliases": ["atp challenger parma"],
        "boltodds_aliases": ["challenger parma - tennis"],
    },
    "poznan": {
        "polymarket_league_code": "atp",
        "v1_aliases": ["atp challenger poznan"],
        "boltodds_aliases": ["challenger poznan - tennis"],
    },
    "dublin": {
        "polymarket_league_code": "atp",
        "v1_aliases": ["atp challenger dublin"],
        "boltodds_aliases": ["challenger dublin - tennis"],
    },
    "mallorca_m": {
        "polymarket_league_code": "atp",
        "v1_aliases": ["atp mallorca"],
        "boltodds_aliases": ["atp mallorca - tennis"],
    },
    "eastbourne_m": {
        "polymarket_league_code": "atp",
        "v1_aliases": ["atp eastbourne"],
        "boltodds_aliases": ["atp eastbourne - tennis"],
    },
    "wimbledon_qual_m": {
        "polymarket_league_code": "atp",
        "v1_aliases": ["wimbledon men singles"],
        "boltodds_aliases": ["wimbledon (m) - tennis"],
    },
    "piracicaba": {
        "polymarket_league_code": "atp",
        "v1_aliases": ["atp challenger piracicaba"],
        "boltodds_aliases": ["challenger piracicaba - tennis"],
    },
    "plovdiv": {
        "polymarket_league_code": "atp",
        "v1_aliases": ["atp challenger plovdiv"],
        "boltodds_aliases": ["challenger plovdiv - tennis"],
    },
    "targu_mures": {
        "polymarket_league_code": "atp",
        "v1_aliases": ["atp challenger targu mures"],
        "boltodds_aliases": ["challenger targu mures - tennis"],
    },
    "wimbledon_qual_w": {
        "polymarket_league_code": "wta",
        "v1_aliases": ["wimbledon women singles"],
        "boltodds_aliases": ["wimbledon (w) - tennis"],
    },
}
