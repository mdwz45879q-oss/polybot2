"""Kalshi tennis player mappings.

Deterministic mapping from canonical player keys to Kalshi names and V1 aliases.
Used by the sniper for exact-match linking (no fuzzy matching).

Sources:
  - 519 players: V1 aliases from PLAYER_MAP_TENNIS (verified)
  - 40 players: V1 aliases from current V1 catalog (verified)
  - 70 players: V1 aliases derived from Kalshi name (unconfirmed)
"""

KALSHI_PLAYER_MAP = {
    "aboian, valerio": {
        "kalshi_name": "Valerio Aboian",
        "v1_aliases": ["Aboian, Valerio"],
    },
    "adeshina, esther": {
        "kalshi_name": "Esther Adeshina",
        "v1_aliases": ["Adeshina, Esther"],
    },
    "agamenone, franco": {
        "kalshi_name": "Franco Agamenone",
        "v1_aliases": ["Agamenone, Franco"],
    },
    "aguiard, enzo": {
        "kalshi_name": "Enzo Aguiard",
        "v1_aliases": ["Aguiard, Enzo"],
    },
    "aguilar cardozo, joaquin": {
        "kalshi_name": "Joaquin Aguilar Cardozo",
        "v1_aliases": ["Aguilar Cardozo, Joaquin"],
    },
    "aguirre, cesar ariel hidalgo": {  # v1_alias_unconfirmed
        "kalshi_name": "Cesar Ariel Hidalgo Aguirre",
        "v1_aliases": ["Aguirre, Cesar Ariel Hidalgo"],
    },
    "ajdukovic, duje": {
        "kalshi_name": "Duje Ajdukovic",
        "v1_aliases": ["Ajdukovic, Duje"],
    },
    "aksu, ayla": {
        "kalshi_name": "Ayla Aksu",
        "v1_aliases": ["Aksu, Ayla"],
    },
    "alboran, nicolas moreno de": {  # v1_alias_unconfirmed
        "kalshi_name": "Nicolas Moreno de Alboran",
        "v1_aliases": ["Alboran, Nicolas Moreno de"],
    },
    "albot, radu": {
        "kalshi_name": "Radu Albot",
        "v1_aliases": ["Albot, Radu"],
    },
    "alcala gurri, max": {
        "kalshi_name": "Max Alcala Gurri",
        "v1_aliases": ["Alcala Gurri, Max"],
    },
    "alexandrova, ekaterina": {
        "kalshi_name": "Ekaterina Alexandrova",
        "v1_aliases": ["Alexandrova, Ekaterina"],
    },
    "almazan valiente, izan": {
        "kalshi_name": "Izan Almazan Valiente",
        "v1_aliases": ["Almazan Valiente, Izan"],
    },
    "altmaier, daniel": {
        "kalshi_name": "Daniel Altmaier",
        "v1_aliases": ["Altmaier, Daniel"],
    },
    "alujas, rodrigo": {  # v1_alias_unconfirmed
        "kalshi_name": "Rodrigo Alujas",
        "v1_aliases": ["Alujas, Rodrigo"],
    },
    "alvarado, patricio": {  # v1_alias_unconfirmed
        "kalshi_name": "Patricio Alvarado",
        "v1_aliases": ["Alvarado, Patricio"],
    },
    "andrade da silva, lucas": {
        "kalshi_name": "Lucas Andrade Da Silva",
        "v1_aliases": ["Andrade Da Silva, Lucas"],
    },
    "andreescu, bianca": {
        "kalshi_name": "Bianca Andreescu",
        "v1_aliases": ["Andreescu, Bianca"],
    },
    "andreescu, stefan adrian": {  # v1_alias_unconfirmed
        "kalshi_name": "Stefan Adrian Andreescu",
        "v1_aliases": ["Andreescu, Stefan Adrian"],
    },
    "andreeva, erika": {
        "kalshi_name": "Erika Andreeva",
        "v1_aliases": ["Andreeva, Erika"],
    },
    "andreeva, mirra": {
        "kalshi_name": "Mirra Andreeva",
        "v1_aliases": ["Andreeva, Mirra"],
    },
    "angelini, lorenzo": {
        "kalshi_name": "Lorenzo Angelini",
        "v1_aliases": ["Angelini, Lorenzo"],
    },
    "anisimova, amanda": {
        "kalshi_name": "Amanda Anisimova",
        "v1_aliases": ["Anisimova, Amanda"],
    },
    "ansari, carolyn": {
        "kalshi_name": "Carolyn Ansari",
        "v1_aliases": ["Ansari, Carolyn"],
    },
    "arango, emiliana": {
        "kalshi_name": "Emiliana Arango",
        "v1_aliases": ["Arango, Emiliana"],
    },
    "arnaboldi, federico": {  # v1_alias_unconfirmed
        "kalshi_name": "Federico Arnaboldi",
        "v1_aliases": ["Arnaboldi, Federico"],
    },
    "arnaldi, matteo": {
        "kalshi_name": "Matteo Arnaldi",
        "v1_aliases": ["Arnaldi, Matteo"],
    },
    "astakhova, darya": {
        "kalshi_name": "Darya Astakhova",
        "v1_aliases": ["Astakhova, Darya"],
    },
    "atmane, terence": {
        "kalshi_name": "Terence Atmane",
        "v1_aliases": ["Atmane, Terence"],
    },
    "auger-aliassime, felix": {
        "kalshi_name": "Felix Auger-Aliassime",
        "v1_aliases": ["Auger-Aliassime, Felix"],
    },
    "badosa, paula": {
        "kalshi_name": "Paula Badosa",
        "v1_aliases": ["Badosa, Paula"],
    },
    "baez, sebastian": {
        "kalshi_name": "Sebastian Baez",
        "v1_aliases": ["Baez, Sebastian"],
    },
    "bai, zhuoxuan": {
        "kalshi_name": "Zhuoxuan Bai",
        "v1_aliases": ["Bai, Zhuoxuan"],
    },
    "balshaw, felix": {
        "kalshi_name": "Felix Balshaw",
        "v1_aliases": ["Balshaw, Felix"],
    },
    "bandecchi, susan": {
        "kalshi_name": "Susan Bandecchi",
        "v1_aliases": ["Bandecchi, Susan"],
    },
    "banerjee, samir": {
        "kalshi_name": "Samir Banerjee",
        "v1_aliases": ["Banerjee, Samir"],
    },
    "baris, ozan": {
        "kalshi_name": "Ozan Baris",
        "v1_aliases": ["Baris, Ozan"],
    },
    "barrios vera, marcelo tomas": {
        "kalshi_name": "Marcelo Tomas Barrios Vera",
        "v1_aliases": ["Barrios Vera, Marcelo Tomas"],
    },
    "bartunkova, nikola": {
        "kalshi_name": "Nikola Bartunkova",
        "v1_aliases": ["Bartunkova, Nikola"],
    },
    "basiletti, noemi": {
        "kalshi_name": "Noemi Basiletti",
        "v1_aliases": ["Basiletti, Noemi"],
    },
    "basing, max": {
        "kalshi_name": "Max Basing",
        "v1_aliases": ["Basing, Max"],
    },
    "bassols ribera, marina": {
        "kalshi_name": "Marina Bassols Ribera",
        "v1_aliases": ["Bassols Ribera, Marina"],
    },
    "bautista agut, roberto": {
        "kalshi_name": "Roberto Bautista Agut",
        "v1_aliases": ["Bautista Agut, Roberto"],
    },
    "becroft, isaac": {  # v1_alias_unconfirmed
        "kalshi_name": "Isaac Becroft",
        "v1_aliases": ["Becroft, Isaac"],
    },
    "begu, irina-camelia": {
        "kalshi_name": "Irina-Camelia Begu",
        "v1_aliases": ["Begu, Irina-Camelia"],
    },
    "bejlek, sara": {
        "kalshi_name": "Sara Bejlek",
        "v1_aliases": ["Bejlek, Sara"],
    },
    "bellucci, mattia": {
        "kalshi_name": "Mattia Bellucci",
        "v1_aliases": ["Bellucci, Mattia"],
    },
    "bencic, belinda": {
        "kalshi_name": "Belinda Bencic",
        "v1_aliases": ["Bencic, Belinda"],
    },
    "bergs, zizou": {
        "kalshi_name": "Zizou Bergs",
        "v1_aliases": ["Bergs, Zizou"],
    },
    "berkieta, tomasz": {
        "kalshi_name": "Tomasz Berkieta",
        "v1_aliases": ["Berkieta, Tomasz"],
    },
    "berrettini, matteo": {
        "kalshi_name": "Matteo Berrettini",
        "v1_aliases": ["Berrettini, Matteo"],
    },
    "bertea, elena ruxandra": {
        "kalshi_name": "Elena Ruxandra Bertea",
        "v1_aliases": ["Bertea, Elena Ruxandra"],
    },
    "bertola, remy": {
        "kalshi_name": "Remy Bertola",
        "v1_aliases": ["Bertola, Remy"],
    },
    "bilardo, jacopo": {  # v1_alias_unconfirmed
        "kalshi_name": "Jacopo Bilardo",
        "v1_aliases": ["Bilardo, Jacopo"],
    },
    "birrell, kimberly": {
        "kalshi_name": "Kimberly Birrell",
        "v1_aliases": ["Birrell, Kimberly"],
    },
    "blanch, dali": {
        "kalshi_name": "Dali Blanch",
        "v1_aliases": ["Blanch, Dali"],
    },
    "blanch, darwin": {
        "kalshi_name": "Darwin Blanch",
        "v1_aliases": ["Blanch, Darwin"],
    },
    "blinkova, anna": {
        "kalshi_name": "Anna Blinkova",
        "v1_aliases": ["Blinkova, Anna"],
    },
    "blockx, alexander": {
        "kalshi_name": "Alexander Blockx",
        "v1_aliases": ["Blockx, Alexander"],
    },
    "boisson, lois": {
        "kalshi_name": "Lois Boisson",
        "v1_aliases": ["Boisson, Lois"],
    },
    "bolkvadze, mariam": {
        "kalshi_name": "Mariam Bolkvadze",
        "v1_aliases": ["Bolkvadze, Mariam"],
    },
    "bondar, anna": {
        "kalshi_name": "Anna Bondar",
        "v1_aliases": ["Bondar, Anna"],
    },
    "bonzi, benjamin": {
        "kalshi_name": "Benjamin Bonzi",
        "v1_aliases": ["Bonzi, Benjamin"],
    },
    "borges, nuno": {
        "kalshi_name": "Nuno Borges",
        "v1_aliases": ["Borges, Nuno"],
    },
    "boulais, justin": {
        "kalshi_name": "Justin Boulais",
        "v1_aliases": ["Boulais, Justin"],
    },
    "boulter, katie": {
        "kalshi_name": "Katie Boulter",
        "v1_aliases": ["Boulter, Katie"],
    },
    "bouzas maneiro, jessica": {
        "kalshi_name": "Jessica Bouzas Maneiro",
        "v1_aliases": ["Bouzas Maneiro, Jessica"],
    },
    "bouzkova, marie": {
        "kalshi_name": "Marie Bouzkova",
        "v1_aliases": ["Bouzkova, Marie"],
    },
    "boyer, tristan": {
        "kalshi_name": "Tristan Boyer",
        "v1_aliases": ["Boyer, Tristan"],
    },
    "brancaccio, nuria": {
        "kalshi_name": "Nuria Brancaccio",
        "v1_aliases": ["Brancaccio, Nuria"],
    },
    "brancaccio, raul": {
        "kalshi_name": "Raul Brancaccio",
        "v1_aliases": ["Brancaccio, Raul"],
    },
    "britton, daniella": {
        "kalshi_name": "Daniella Britton",
        "v1_aliases": ["Britton, Daniella"],
    },
    "bronzetti, lucia": {
        "kalshi_name": "Lucia Bronzetti",
        "v1_aliases": ["Bronzetti, Lucia"],
    },
    "brooksby, jenson": {
        "kalshi_name": "Jenson Brooksby",
        "v1_aliases": ["Brooksby, Jenson"],
    },
    "bu, yunchaokete": {
        "kalshi_name": "Yunchaokete Bu",
        "v1_aliases": ["Bu, Yunchaokete"],
    },
    "bublik, alexander": {
        "kalshi_name": "Alexander Bublik",
        "v1_aliases": ["Bublik, Alexander"],
    },
    "budkov kjaer, nicolai": {
        "kalshi_name": "Nicolai Budkov Kjaer",
        "v1_aliases": ["Budkov Kjaer, Nicolai"],
    },
    "bulgaru, miriam bianca": {
        "kalshi_name": "Miriam Bianca Bulgaru",
        "v1_aliases": ["Bulgaru, Miriam Bianca"],
    },
    "burruchaga, roman andres": {
        "kalshi_name": "Roman Andres Burruchaga",
        "v1_aliases": ["Burruchaga, Roman Andres"],
    },
    "buse, ignacio": {
        "kalshi_name": "Ignacio Buse",
        "v1_aliases": ["Buse, Ignacio"],
    },
    "camus, charlie": {  # v1_alias_unconfirmed
        "kalshi_name": "Charlie Camus",
        "v1_aliases": ["Camus, Charlie"],
    },
    "carballes baena, roberto": {
        "kalshi_name": "Roberto Carballes Baena",
        "v1_aliases": ["Carballes Baena, Roberto"],
    },
    "carle, maria": {
        "kalshi_name": "Maria Carle",
        "v1_aliases": ["Carle, Maria"],
    },
    "carreno busta, pablo": {
        "kalshi_name": "Pablo Carreno Busta",
        "v1_aliases": ["Carreno Busta, Pablo"],
    },
    "casanova, hernan": {
        "kalshi_name": "Hernan Casanova",
        "v1_aliases": ["Casanova, Hernan"],
    },
    "cassone, murphy": {
        "kalshi_name": "Murphy Cassone",
        "v1_aliases": ["Cassone, Murphy"],
    },
    "castelnuovo, luca": {
        "kalshi_name": "Luca Castelnuovo",
        "v1_aliases": ["Castelnuovo, Luca"],
    },
    "cazacu, dragos nicolae": {  # v1_alias_unconfirmed
        "kalshi_name": "Dragos Nicolae Cazacu",
        "v1_aliases": ["Cazacu, Dragos Nicolae"],
    },
    "cerundolo, francisco": {
        "kalshi_name": "Francisco Cerundolo",
        "v1_aliases": ["Cerundolo, Francisco"],
    },
    "cerundolo, juan manuel": {
        "kalshi_name": "Juan Manuel Cerundolo",
        "v1_aliases": ["Cerundolo, Juan Manuel"],
    },
    "charaeva, alina": {
        "kalshi_name": "Alina Charaeva",
        "v1_aliases": ["Charaeva, Alina"],
    },
    "chazal, maxime": {
        "kalshi_name": "Maxime Chazal",
        "v1_aliases": ["Chazal, Maxime"],
    },
    "cherubini, diletta": {  # v1_alias_unconfirmed
        "kalshi_name": "Diletta Cherubini",
        "v1_aliases": ["Cherubini, Diletta"],
    },
    "chidekh, clement": {
        "kalshi_name": "Clement Chidekh",
        "v1_aliases": ["Chidekh, Clement"],
    },
    "chiesa, deborah": {
        "kalshi_name": "Deborah Chiesa",
        "v1_aliases": ["Chiesa, Deborah"],
    },
    "choinski, jan": {
        "kalshi_name": "Jan Choinski",
        "v1_aliases": ["Choinski, Jan"],
    },
    "chwalinska, maja": {
        "kalshi_name": "Maja Chwalinska",
        "v1_aliases": ["Chwalinska, Maja"],
    },
    "ciavarella, niccolo": {  # v1_alias_unconfirmed
        "kalshi_name": "Niccolo Ciavarella",
        "v1_aliases": ["Ciavarella, Niccolo"],
    },
    "cigarran, thiago": {
        "kalshi_name": "Thiago Cigarran",
        "v1_aliases": ["Cigarran, Thiago"],
    },
    "cilic, marin": {
        "kalshi_name": "Marin Cilic",
        "v1_aliases": ["Cilic, Marin"],
    },
    "cina, federico": {
        "kalshi_name": "Federico Cina",
        "v1_aliases": ["Cina, Federico"],
    },
    "ciric-bagaric, lucija": {  # v1_alias_unconfirmed
        "kalshi_name": "Lucija Ciric-Bagaric",
        "v1_aliases": ["Ciric-Bagaric, Lucija"],
    },
    "cirstea, sorana": {
        "kalshi_name": "Sorana Cirstea",
        "v1_aliases": ["Cirstea, Sorana"],
    },
    "cobolli, flavio": {
        "kalshi_name": "Flavio Cobolli",
        "v1_aliases": ["Cobolli, Flavio"],
    },
    "cocciaretto, elisabetta": {
        "kalshi_name": "Elisabetta Cocciaretto",
        "v1_aliases": ["Cocciaretto, Elisabetta"],
    },
    "colasanto, francesco giuseppe": {  # v1_alias_unconfirmed
        "kalshi_name": "Francesco Giuseppe Colasanto",
        "v1_aliases": ["Colasanto, Francesco Giuseppe"],
    },
    "collignon, raphael": {
        "kalshi_name": "Raphael Collignon",
        "v1_aliases": ["Collignon, Raphael"],
    },
    "comesana, francisco": {
        "kalshi_name": "Francisco Comesana",
        "v1_aliases": ["Comesana, Francisco"],
    },
    "compagnucci, tommaso": {
        "kalshi_name": "Tommaso Compagnucci",
        "v1_aliases": ["Compagnucci, Tommaso"],
    },
    "coppejans, kimmer": {
        "kalshi_name": "Kimmer Coppejans",
        "v1_aliases": ["Coppejans, Kimmer"],
    },
    "coria, federico": {
        "kalshi_name": "Federico Coria",
        "v1_aliases": ["Coria, Federico"],
    },
    "crawley, fiona": {
        "kalshi_name": "Fiona Crawley",
        "v1_aliases": ["Crawley, Fiona"],
    },
    "cretu, cezar": {
        "kalshi_name": "Cezar Cretu (b. 2001)",
        "v1_aliases": ["Cretu, Cezar (2001)"],
    },
    "cristian, jaqueline": {
        "kalshi_name": "Jaqueline Cristian",
        "v1_aliases": ["Cristian, Jaqueline"],
    },
    "cui, jie": {  # v1_alias_unconfirmed
        "kalshi_name": "Jie Cui",
        "v1_aliases": ["Cui, Jie"],
    },
    "dalla valle, enrico": {
        "kalshi_name": "Enrico Dalla Valle",
        "v1_aliases": ["Dalla Valle, Enrico"],
    },
    "damm jr, martin": {
        "kalshi_name": "Martin Damm Jr",
        "v1_aliases": ["Damm Jr, Martin"],
    },
    "damonte, augustin": {  # v1_alias_unconfirmed
        "kalshi_name": "Augustin Damonte",
        "v1_aliases": ["Damonte, Augustin"],
    },
    "darderi, luciano": {
        "kalshi_name": "Luciano Darderi",
        "v1_aliases": ["Darderi, Luciano"],
    },
    "dart, harriet": {
        "kalshi_name": "Harriet Dart",
        "v1_aliases": ["Dart, Harriet"],
    },
    "davidovich fokina, alejandro": {
        "kalshi_name": "Alejandro Davidovich Fokina",
        "v1_aliases": ["Davidovich Fokina, Alejandro"],
    },
    "day, kayla": {
        "kalshi_name": "Kayla Day",
        "v1_aliases": ["Day, Kayla"],
    },
    "de jong, jesper": {
        "kalshi_name": "Jesper De Jong",
        "v1_aliases": ["De Jong, Jesper"],
    },
    "de la fuente, santiago": {
        "kalshi_name": "Santiago De la Fuente",
        "v1_aliases": ["De la Fuente, Santiago"],
    },
    "de minaur, alex": {
        "kalshi_name": "Alex de Minaur",
        "v1_aliases": ["de Minaur, Alex"],
    },
    "delaney, jesse": {  # v1_alias_unconfirmed
        "kalshi_name": "Jesse Delaney",
        "v1_aliases": ["Delaney, Jesse"],
    },
    "dev, s d prajwal": {  # v1_alias_unconfirmed
        "kalshi_name": "S D Prajwal Dev",
        "v1_aliases": ["Dev, S D Prajwal"],
    },
    "diallo, gabriel": {
        "kalshi_name": "Gabriel Diallo",
        "v1_aliases": ["Diallo, Gabriel"],
    },
    "dimitrov, grigor": {
        "kalshi_name": "Grigor Dimitrov",
        "v1_aliases": ["Dimitrov, Grigor"],
    },
    "dios, felipe de": {  # v1_alias_unconfirmed
        "kalshi_name": "Felipe de Dios",
        "v1_aliases": ["Dios, Felipe de"],
    },
    "djere, laslo": {
        "kalshi_name": "Laslo Djere",
        "v1_aliases": ["Djere, Laslo"],
    },
    "djokovic, novak": {
        "kalshi_name": "Novak Djokovic",
        "v1_aliases": ["Djokovic, Novak"],
    },
    "dodig, matej": {
        "kalshi_name": "Matej Dodig",
        "v1_aliases": ["Dodig, Matej"],
    },
    "dodin, oceane": {
        "kalshi_name": "Oceane Dodin",
        "v1_aliases": ["Dodin, Oceane"],
    },
    "dolehide, caroline": {
        "kalshi_name": "Caroline Dolehide",
        "v1_aliases": ["Dolehide, Caroline"],
    },
    "donald, matthew william": {
        "kalshi_name": "Matthew William Donald",
        "v1_aliases": ["Donald, Matthew William"],
    },
    "dougaz, aziz": {
        "kalshi_name": "Aziz Dougaz",
        "v1_aliases": ["Dougaz, Aziz"],
    },
    "draper, jack": {
        "kalshi_name": "Jack Draper",
        "v1_aliases": ["Draper, Jack"],
    },
    "droguet, titouan": {
        "kalshi_name": "Titouan Droguet",
        "v1_aliases": ["Droguet, Titouan"],
    },
    "duckworth, james": {
        "kalshi_name": "James Duckworth",
        "v1_aliases": ["Duckworth, James"],
    },
    "dudeney, alicia": {
        "kalshi_name": "Alicia Dudeney",
        "v1_aliases": ["Dudeney, Alicia"],
    },
    "dzumhur, damir": {
        "kalshi_name": "Damir Dzumhur",
        "v1_aliases": ["Dzumhur, Damir"],
    },
    "eala, alexandra": {
        "kalshi_name": "Alexandra Eala",
        "v1_aliases": ["Eala, Alexandra"],
    },
    "echargui, moez": {
        "kalshi_name": "Moez Echargui",
        "v1_aliases": ["Echargui, Moez"],
    },
    "echazu, mauricio": {  # v1_alias_unconfirmed
        "kalshi_name": "Mauricio Echazu",
        "v1_aliases": ["Echazu, Mauricio"],
    },
    "elizalde, darwin andres macias": {  # v1_alias_unconfirmed
        "kalshi_name": "Darwin Andres Macias Elizalde",
        "v1_aliases": ["Elizalde, Darwin Andres Macias"],
    },
    "erhard, mathys": {
        "kalshi_name": "Mathys Erhard",
        "v1_aliases": ["Erhard, Mathys"],
    },
    "erjavec, veronika": {
        "kalshi_name": "Veronika Erjavec",
        "v1_aliases": ["Erjavec, Veronika"],
    },
    "erler, lancelot": {  # v1_alias_unconfirmed
        "kalshi_name": "Lancelot Erler",
        "v1_aliases": ["Erler, Lancelot"],
    },
    "etcheverry, tomas martin": {
        "kalshi_name": "Tomas Martin Etcheverry",
        "v1_aliases": ["Etcheverry, Tomas Martin"],
    },
    "evans, daniel": {
        "kalshi_name": "Daniel Evans",
        "v1_aliases": ["Evans, Daniel"],
    },
    "falei, aliona": {
        "kalshi_name": "Aliona Falei",
        "v1_aliases": ["Falei, Aliona"],
    },
    "faria, jaime": {
        "kalshi_name": "Jaime Faria",
        "v1_aliases": ["Faria, Jaime"],
    },
    "fearnley, jacob": {
        "kalshi_name": "Jacob Fearnley",
        "v1_aliases": ["Fearnley, Jacob"],
    },
    "feng, shuo": {  # v1_alias_unconfirmed
        "kalshi_name": "Shuo Feng",
        "v1_aliases": ["Feng, Shuo"],
    },
    "fenty, andrew": {  # v1_alias_unconfirmed
        "kalshi_name": "Andrew Fenty",
        "v1_aliases": ["Fenty, Andrew"],
    },
    "fernandez, bruno": {
        "kalshi_name": "Bruno Fernandez",
        "v1_aliases": ["Fernandez, Bruno"],
    },
    "fernandez, leylah": {
        "kalshi_name": "Leylah Fernandez",
        "v1_aliases": ["Fernandez, Leylah"],
    },
    "ferro, fiona": {
        "kalshi_name": "Fiona Ferro",
        "v1_aliases": ["Ferro, Fiona"],
    },
    "fery, arthur": {
        "kalshi_name": "Arthur Fery",
        "v1_aliases": ["Fery, Arthur"],
    },
    "fils, arthur": {
        "kalshi_name": "Arthur Fils",
        "v1_aliases": ["Fils, Arthur"],
    },
    "fomin, sergey": {
        "kalshi_name": "Sergey Fomin",
        "v1_aliases": ["Fomin, Sergey"],
    },
    "fonseca, joao": {
        "kalshi_name": "Joao Fonseca",
        "v1_aliases": ["Fonseca, Joao"],
    },
    "forti, francesco": {
        "kalshi_name": "Francesco Forti",
        "v1_aliases": ["Forti, Francesco"],
    },
    "frech, magdalena": {
        "kalshi_name": "Magdalena Frech",
        "v1_aliases": ["Frech, Magdalena"],
    },
    "friend, jay": {  # v1_alias_unconfirmed
        "kalshi_name": "Jay Friend",
        "v1_aliases": ["Friend, Jay"],
    },
    "fritz, taylor": {
        "kalshi_name": "Taylor Fritz",
        "v1_aliases": ["Fritz, Taylor"],
    },
    "fruhvirtova, linda": {
        "kalshi_name": "Linda Fruhvirtova",
        "v1_aliases": ["Fruhvirtova, Linda"],
    },
    "fucsovics, marton": {
        "kalshi_name": "Marton Fucsovics",
        "v1_aliases": ["Fucsovics, Marton"],
    },
    "galarneau, alexis": {
        "kalshi_name": "Alexis Galarneau",
        "v1_aliases": ["Galarneau, Alexis"],
    },
    "gao, xinyu": {
        "kalshi_name": "Xinyu Gao",
        "v1_aliases": ["Gao, Xinyu"],
    },
    "garakani, sana": {  # v1_alias_unconfirmed
        "kalshi_name": "Sana Garakani",
        "v1_aliases": ["Garakani, Sana"],
    },
    "garin, cristian": {
        "kalshi_name": "Cristian Garin",
        "v1_aliases": ["Garin, Cristian"],
    },
    "garland, joanna": {
        "kalshi_name": "Joanna Garland",
        "v1_aliases": ["Garland, Joanna"],
    },
    "gasanova, anastasia": {
        "kalshi_name": "Anastasia Gasanova",
        "v1_aliases": ["Gasanova, Anastasia"],
    },
    "gaston, hugo": {
        "kalshi_name": "Hugo Gaston",
        "v1_aliases": ["Gaston, Hugo"],
    },
    "gaubas, vilius": {
        "kalshi_name": "Vilius Gaubas",
        "v1_aliases": ["Gaubas, Vilius"],
    },
    "gauff, coco": {
        "kalshi_name": "Coco Gauff",
        "v1_aliases": ["Gauff, Coco"],
    },
    "gea, arthur": {
        "kalshi_name": "Arthur Gea",
        "v1_aliases": ["Gea, Arthur"],
    },
    "geldof, benoit": {  # v1_alias_unconfirmed
        "kalshi_name": "Benoit Geldof",
        "v1_aliases": ["Geldof, Benoit"],
    },
    "gentzsch, tom": {
        "kalshi_name": "Tom Gentzsch",
        "v1_aliases": ["Gentzsch, Tom"],
    },
    "ghazouani durand, yanis": {
        "kalshi_name": "Yanis Ghazouani Durand",
        "v1_aliases": ["Ghazouani Durand, Yanis"],
    },
    "ghetu, gabriel": {
        "kalshi_name": "Gabriel Ghetu",
        "v1_aliases": ["Ghetu, Gabriel"],
    },
    "ghibaudo, antoine": {
        "kalshi_name": "Antoine Ghibaudo",
        "v1_aliases": ["Ghibaudo, Antoine"],
    },
    "gibson, talia": {
        "kalshi_name": "Talia Gibson",
        "v1_aliases": ["Gibson, Talia"],
    },
    "gill, felix": {
        "kalshi_name": "Felix Gill",
        "v1_aliases": ["Gill, Felix"],
    },
    "gima, sebastian": {
        "kalshi_name": "Sebastian Gima",
        "v1_aliases": ["Gima, Sebastian"],
    },
    "giovannini, luisina": {
        "kalshi_name": "Luisina Giovannini",
        "v1_aliases": ["Giovannini, Luisina"],
    },
    "giron, marcos": {
        "kalshi_name": "Marcos Giron",
        "v1_aliases": ["Giron, Marcos"],
    },
    "gjorcheska, lina": {
        "kalshi_name": "Lina Gjorcheska",
        "v1_aliases": ["Gjorcheska, Lina"],
    },
    "goity zapico, segundo": {
        "kalshi_name": "Segundo Goity Zapico",
        "v1_aliases": ["Goity Zapico, Segundo"],
    },
    "gojo, borna": {
        "kalshi_name": "Borna Gojo",
        "v1_aliases": ["Gojo, Borna"],
    },
    "golubic, viktorija": {
        "kalshi_name": "Viktorija Golubic",
        "v1_aliases": ["Golubic, Viktorija"],
    },
    "gombos, norbert": {
        "kalshi_name": "Norbert Gombos",
        "v1_aliases": ["Gombos, Norbert"],
    },
    "gorgodze, ekaterine": {
        "kalshi_name": "Ekaterine Gorgodze",
        "v1_aliases": ["Gorgodze, Ekaterine"],
    },
    "grabher, julia": {
        "kalshi_name": "Julia Grabher",
        "v1_aliases": ["Grabher, Julia"],
    },
    "grant, tyra caterina": {
        "kalshi_name": "Tyra Caterina Grant",
        "v1_aliases": ["Grant, Tyra Caterina"],
    },
    "grenier, hugo": {
        "kalshi_name": "Hugo Grenier",
        "v1_aliases": ["Grenier, Hugo"],
    },
    "griekspoor, tallon": {
        "kalshi_name": "Tallon Griekspoor",
        "v1_aliases": ["Griekspoor, Tallon"],
    },
    "guerrieri, andrea": {
        "kalshi_name": "Andrea Guerrieri",
        "v1_aliases": ["Guerrieri, Andrea"],
    },
    "gueymard wayenburg, sascha": {
        "kalshi_name": "Sascha Gueymard Wayenburg",
        "v1_aliases": ["Gueymard Wayenburg, Sascha"],
    },
    "guillen meza, alvaro": {
        "kalshi_name": "Alvaro Guillen Meza",
        "v1_aliases": ["Guillen Meza, Alvaro"],
    },
    "gulin, svyatoslav": {
        "kalshi_name": "Svyatoslav Gulin",
        "v1_aliases": ["Gulin, Svyatoslav"],
    },
    "guo, hanyu": {
        "kalshi_name": "Hanyu Guo",
        "v1_aliases": ["Guo, Hanyu"],
    },
    "haddad maia, beatriz": {
        "kalshi_name": "Beatriz Haddad Maia",
        "v1_aliases": ["Haddad Maia, Beatriz"],
    },
    "halys, quentin": {
        "kalshi_name": "Quentin Halys",
        "v1_aliases": ["Halys, Quentin"],
    },
    "hanfmann, yannick": {
        "kalshi_name": "Yannick Hanfmann",
        "v1_aliases": ["Hanfmann, Yannick"],
    },
    "hardt, nick": {
        "kalshi_name": "Nick Hardt",
        "v1_aliases": ["Hardt, Nick"],
    },
    "harris, billy": {
        "kalshi_name": "Billy Harris",
        "v1_aliases": ["Harris, Billy"],
    },
    "hassan, benjamin": {
        "kalshi_name": "Benjamin Hassan",
        "v1_aliases": ["Hassan, Benjamin"],
    },
    "hayen, alejandro": {
        "kalshi_name": "Alejandro Hayen",
        "v1_aliases": ["Hayen, Alejandro"],
    },
    "heide, gustavo": {
        "kalshi_name": "Gustavo Heide",
        "v1_aliases": ["Heide, Gustavo"],
    },
    "hernandez-aguila, abel": {  # v1_alias_unconfirmed
        "kalshi_name": "Abel Hernandez-Aguila",
        "v1_aliases": ["Hernandez-Aguila, Abel"],
    },
    "hibino, nao": {
        "kalshi_name": "Nao Hibino",
        "v1_aliases": ["Hibino, Nao"],
    },
    "hijikata, rinky": {
        "kalshi_name": "Rinky Hijikata",
        "v1_aliases": ["Hijikata, Rinky"],
    },
    "holmgren, august": {
        "kalshi_name": "August Holmgren",
        "v1_aliases": ["Holmgren, August"],
    },
    "hon, priscilla": {
        "kalshi_name": "Priscilla Hon",
        "v1_aliases": ["Hon, Priscilla"],
    },
    "hontama, mai": {
        "kalshi_name": "Mai Hontama",
        "v1_aliases": ["Hontama, Mai"],
    },
    "hruncakova, viktoria": {
        "kalshi_name": "Viktoria Hruncakova",
        "v1_aliases": ["Hruncakova, Viktoria"],
    },
    "huang, tsung-hao": {  # v1_alias_unconfirmed
        "kalshi_name": "Tsung-Hao Huang",
        "v1_aliases": ["Huang, Tsung-Hao"],
    },
    "huertas del pino, conner": {
        "kalshi_name": "Connor Huertas Del Pino",
        "v1_aliases": ["Huertas Del Pino, Conner", "Huertas Del Pino, Connor"],
    },
    "humbert, ugo": {
        "kalshi_name": "Ugo Humbert",
        "v1_aliases": ["Humbert, Ugo"],
    },
    "hunter, storm": {
        "kalshi_name": "Storm Hunter",
        "v1_aliases": ["Hunter, Storm"],
    },
    "hurkacz, hubert": {
        "kalshi_name": "Hubert Hurkacz",
        "v1_aliases": ["Hurkacz, Hubert"],
    },
    "hussey, giles": {
        "kalshi_name": "Giles Hussey",
        "v1_aliases": ["Hussey, Giles"],
    },
    "iatcenko, polina": {
        "kalshi_name": "Polina Iatcenko",
        "v1_aliases": ["Iatcenko, Polina"],
    },
    "imamura, masamichi": {  # v1_alias_unconfirmed
        "kalshi_name": "Masamichi Imamura",
        "v1_aliases": ["Imamura, Masamichi"],
    },
    "isakova, polina": {  # v1_alias_unconfirmed
        "kalshi_name": "Polina Isakova",
        "v1_aliases": ["Isakova, Polina"],
    },
    "ishii, sayaka": {
        "kalshi_name": "Sayaka Ishii",
        "v1_aliases": ["Ishii, Sayaka"],
    },
    "ito, aoi": {
        "kalshi_name": "Aoi Ito",
        "v1_aliases": ["Ito, Aoi"],
    },
    "jacquemot, elsa": {
        "kalshi_name": "Elsa Jacquemot",
        "v1_aliases": ["Jacquemot, Elsa"],
    },
    "jacquet, kyrian": {
        "kalshi_name": "Kyrian Jacquet",
        "v1_aliases": ["Jacquet, Kyrian"],
    },
    "janvier, maxime": {
        "kalshi_name": "Maxime Janvier",
        "v1_aliases": ["Janvier, Maxime"],
    },
    "jeanjean, leolia": {
        "kalshi_name": "Leolia Jeanjean",
        "v1_aliases": ["Jeanjean, Leolia"],
    },
    "jecan, alexandru": {  # v1_alias_unconfirmed
        "kalshi_name": "Alexandru Jecan",
        "v1_aliases": ["Jecan, Alexandru"],
    },
    "jianu, filip cristian": {
        "kalshi_name": "Filip Cristian Jianu",
        "v1_aliases": ["Jianu, Filip Cristian"],
    },
    "jimenez kasintseva, victoria": {
        "kalshi_name": "Victoria Jimenez Kasintseva",
        "v1_aliases": ["Jimenez Kasintseva, Victoria"],
    },
    "jodar, rafael": {
        "kalshi_name": "Rafael Jodar",
        "v1_aliases": ["Jodar, Rafael"],
    },
    "joint, maya": {
        "kalshi_name": "Maya Joint",
        "v1_aliases": ["Joint, Maya"],
    },
    "jones, emerson": {
        "kalshi_name": "Emerson Jones",
        "v1_aliases": ["Jones, Emerson"],
    },
    "jones, francesca": {
        "kalshi_name": "Francesca Jones",
        "v1_aliases": ["Jones, Francesca"],
    },
    "jorge, francisca": {
        "kalshi_name": "Francisca Jorge",
        "v1_aliases": ["Jorge, Francisca"],
    },
    "jorge, matilde": {
        "kalshi_name": "Matilde Jorge",
        "v1_aliases": ["Jorge, Matilde"],
    },
    "jovic, iva": {
        "kalshi_name": "Iva Jovic",
        "v1_aliases": ["Jovic, Iva"],
    },
    "jubb, paul": {
        "kalshi_name": "Paul Jubb",
        "v1_aliases": ["Jubb, Paul"],
    },
    "kalieva, elvina": {
        "kalshi_name": "Elvina Kalieva",
        "v1_aliases": ["Kalieva, Elvina"],
    },
    "kalinina, anhelina": {
        "kalshi_name": "Anhelina Kalinina",
        "v1_aliases": ["Kalinina, Anhelina"],
    },
    "kalinskaya, anna": {
        "kalshi_name": "Anna Kalinskaya",
        "v1_aliases": ["Kalinskaya, Anna"],
    },
    "karki, ronit": {
        "kalshi_name": "Ronit Karki",
        "v1_aliases": ["Karki, Ronit"],
    },
    "kasatkina, daria": {
        "kalshi_name": "Daria Kasatkina",
        "v1_aliases": ["Kasatkina, Daria"],
    },
    "kawa, katarzyna": {
        "kalshi_name": "Katarzyna Kawa",
        "v1_aliases": ["Kawa, Katarzyna"],
    },
    "kazionova, ekaterina": {
        "kalshi_name": "Ekaterina Kazionova",
        "v1_aliases": ["Kazionova, Ekaterina"],
    },
    "kecmanovic, miomir": {
        "kalshi_name": "Miomir Kecmanovic",
        "v1_aliases": ["Kecmanovic, Miomir"],
    },
    "kenin, sofia": {
        "kalshi_name": "Sofia Kenin",
        "v1_aliases": ["Kenin, Sofia"],
    },
    "kessler, mccartney": {
        "kalshi_name": "McCartney Kessler",
        "v1_aliases": ["Kessler, McCartney"],
    },
    "keys, madison": {
        "kalshi_name": "Madison Keys",
        "v1_aliases": ["Keys, Madison"],
    },
    "khachanov, karen": {
        "kalshi_name": "Karen Khachanov",
        "v1_aliases": ["Khachanov, Karen"],
    },
    "kicker, nicolas": {
        "kalshi_name": "Nicolas Kicker",
        "v1_aliases": ["Kicker, Nicolas"],
    },
    "kinoshita, hayu": {
        "kalshi_name": "Hayu Kinoshita",
        "v1_aliases": ["Kinoshita, Hayu"],
    },
    "klugman, hannah": {
        "kalshi_name": "Hannah Klugman",
        "v1_aliases": ["Klugman, Hannah"],
    },
    "knutson, gabriela": {
        "kalshi_name": "Gabriela Knutson",
        "v1_aliases": ["Knutson, Gabriela"],
    },
    "kobori, momoko": {
        "kalshi_name": "Momoko Kobori",
        "v1_aliases": ["Kobori, Momoko"],
    },
    "kohlmann de freitas, enzo": {
        "kalshi_name": "Enzo Kohlmann de Freitas",
        "v1_aliases": ["Kohlmann de Freitas, Enzo"],
    },
    "kokkinakis, thanasi": {
        "kalshi_name": "Thanasi Kokkinakis",
        "v1_aliases": ["Kokkinakis, Thanasi"],
    },
    "kopp, sandro": {
        "kalshi_name": "Sandro Kopp",
        "v1_aliases": ["Kopp, Sandro"],
    },
    "kopriva, vit": {
        "kalshi_name": "Vit Kopriva",
        "v1_aliases": ["Kopriva, Vit"],
    },
    "korneeva, alina": {
        "kalshi_name": "Alina Korneeva",
        "v1_aliases": ["Korneeva, Alina"],
    },
    "korpatsch, tamara": {
        "kalshi_name": "Tamara Korpatsch",
        "v1_aliases": ["Korpatsch, Tamara"],
    },
    "kostovic, teodora": {
        "kalshi_name": "Teodora Kostovic",
        "v1_aliases": ["Kostovic, Teodora"],
    },
    "kostyuk, marta": {
        "kalshi_name": "Marta Kostyuk",
        "v1_aliases": ["Kostyuk, Marta"],
    },
    "kotliar, yelyzaveta": {  # v1_alias_unconfirmed
        "kalshi_name": "Yelyzaveta Kotliar",
        "v1_aliases": ["Kotliar, Yelyzaveta"],
    },
    "kovacevic, aleksandar": {
        "kalshi_name": "Aleksandar Kovacevic",
        "v1_aliases": ["Kovacevic, Aleksandar"],
    },
    "kovinic, danka": {
        "kalshi_name": "Danka Kovinic",
        "v1_aliases": ["Kovinic, Danka"],
    },
    "kraus, sinja": {
        "kalshi_name": "Sinja Kraus",
        "v1_aliases": ["Kraus, Sinja"],
    },
    "kravchenko, georgii": {
        "kalshi_name": "Georgii Kravchenko",
        "v1_aliases": ["Kravchenko, Georgii"],
    },
    "krejcikova, barbora": {
        "kalshi_name": "Barbora Krejcikova",
        "v1_aliases": ["Krejcikova, Barbora"],
    },
    "krueger, ashlyn": {
        "kalshi_name": "Ashlyn Krueger",
        "v1_aliases": ["Krueger, Ashlyn"],
    },
    "krueger, mitchell": {
        "kalshi_name": "Mitchell Krueger",
        "v1_aliases": ["Krueger, Mitchell"],
    },
    "krumich, martin": {
        "kalshi_name": "Martin Krumich",
        "v1_aliases": ["Krumich, Martin"],
    },
    "ku, yeon woo": {
        "kalshi_name": "Yeon Woo Ku",
        "v1_aliases": ["Ku, Yeon Woo"],
    },
    "kubka, martyna": {
        "kalshi_name": "Martyna Kubka",
        "v1_aliases": ["Kubka, Martyna"],
    },
    "kudermetova, polina": {
        "kalshi_name": "Polina Kudermetova",
        "v1_aliases": ["Kudermetova, Polina"],
    },
    "kulikova, anastasia": {
        "kalshi_name": "Anastasia Kulikova",
        "v1_aliases": ["Kulikova, Anastasia"],
    },
    "kumstat, jan": {  # v1_alias_unconfirmed
        "kalshi_name": "Jan Kumstat",
        "v1_aliases": ["Kumstat, Jan"],
    },
    "kuzmanov, dimitar": {
        "kalshi_name": "Dimitar Kuzmanov",
        "v1_aliases": ["Kuzmanov, Dimitar"],
    },
    "kuzmova, katarina": {
        "kalshi_name": "Katarina Kuzmova",
        "v1_aliases": ["Kuzmova, Katarina"],
    },
    "kwon, soonwoo": {
        "kalshi_name": "Soonwoo Kwon",
        "v1_aliases": ["Kwon, Soonwoo"],
    },
    "kym, jerome": {
        "kalshi_name": "Jerome Kym",
        "v1_aliases": ["Kym, Jerome"],
    },
    "kypson, patrick": {
        "kalshi_name": "Patrick Kypson",
        "v1_aliases": ["Kypson, Patrick"],
    },
    "kyrgios, nick": {
        "kalshi_name": "Nick Kyrgios",
        "v1_aliases": ["Kyrgios, Nick"],
    },
    "la serna, juan manuel": {
        "kalshi_name": "Juan Manuel La Serna",
        "v1_aliases": ["La Serna, Juan Manuel"],
    },
    "lagutin, pavel": {
        "kalshi_name": "Pavel Lagutin",
        "v1_aliases": ["Lagutin, Pavel"],
    },
    "lajal, mark": {
        "kalshi_name": "Mark Lajal",
        "v1_aliases": ["Lajal, Mark"],
    },
    "lajovic, dusan": {
        "kalshi_name": "Dusan Lajovic",
        "v1_aliases": ["Lajovic, Dusan"],
    },
    "lamens, suzan": {
        "kalshi_name": "Suzan Lamens",
        "v1_aliases": ["Lamens, Suzan"],
    },
    "landaluce, martin": {
        "kalshi_name": "Martin Landaluce",
        "v1_aliases": ["Landaluce, Martin"],
    },
    "langmo, christian": {
        "kalshi_name": "Christian Langmo",
        "v1_aliases": ["Langmo, Christian"],
    },
    "latinovic, stefan": {  # v1_alias_unconfirmed
        "kalshi_name": "Stefan Latinovic",
        "v1_aliases": ["Latinovic, Stefan"],
    },
    "lazaro garcia, andrea": {
        "kalshi_name": "Andrea Lazaro Garcia",
        "v1_aliases": ["Lazaro Garcia, Andrea"],
    },
    "lechno-wasiutynski, fryderyk": {
        "kalshi_name": "Fryderyk Lechno-Wasiutynski",
        "v1_aliases": ["Lechno-Wasiutynski, Fryderyk"],
    },
    "lehecka, jiri": {
        "kalshi_name": "Jiri Lehecka",
        "v1_aliases": ["Lehecka, Jiri"],
    },
    "leite, wilson": {
        "kalshi_name": "Wilson Leite",
        "v1_aliases": ["Leite, Wilson"],
    },
    "leonard, manon": {
        "kalshi_name": "Manon Leonard",
        "v1_aliases": ["Leonard, Manon"],
    },
    "lepchenko, varvara": {
        "kalshi_name": "Varvara Lepchenko",
        "v1_aliases": ["Lepchenko, Varvara"],
    },
    "li, ann": {
        "kalshi_name": "Ann Li",
        "v1_aliases": ["Li, Ann"],
    },
    "linette, magda": {
        "kalshi_name": "Magda Linette",
        "v1_aliases": ["Linette, Magda"],
    },
    "liu, claire": {
        "kalshi_name": "Claire Liu",
        "v1_aliases": ["Liu, Claire"],
    },
    "liu, min": {  # v1_alias_unconfirmed
        "kalshi_name": "Min Liu",
        "v1_aliases": ["Liu, Min"],
    },
    "lizarazo, yuliana": {
        "kalshi_name": "Yuliana Lizarazo",
        "v1_aliases": ["Lizarazo, Yuliana"],
    },
    "llamas ruiz, pablo": {
        "kalshi_name": "Pablo Llamas Ruiz",
        "v1_aliases": ["Llamas Ruiz, Pablo"],
    },
    "losciale, valentina": {  # v1_alias_unconfirmed
        "kalshi_name": "Valentina Losciale",
        "v1_aliases": ["Losciale, Valentina"],
    },
    "lys, eva": {
        "kalshi_name": "Eva Lys",
        "v1_aliases": ["Lys, Eva"],
    },
    "majchrzak, kamil": {
        "kalshi_name": "Kamil Majchrzak",
        "v1_aliases": ["Majchrzak, Kamil"],
    },
    "makk, peter": {
        "kalshi_name": "Peter Makk",
        "v1_aliases": ["Makk, Peter"],
    },
    "maldonado, martin": {
        "kalshi_name": "Martin Maldonado",
        "v1_aliases": ["Maldonado, Martin"],
    },
    "mandlik, elizabeth": {
        "kalshi_name": "Elizabeth Mandlik",
        "v1_aliases": ["Mandlik, Elizabeth"],
    },
    "mannarino, adrian": {
        "kalshi_name": "Adrian Mannarino",
        "v1_aliases": ["Mannarino, Adrian"],
    },
    "marcinko, petra": {
        "kalshi_name": "Petra Marcinko",
        "v1_aliases": ["Marcinko, Petra"],
    },
    "maria, tatjana": {
        "kalshi_name": "Tatjana Maria",
        "v1_aliases": ["Maria, Tatjana"],
    },
    "marozsan, fabian": {
        "kalshi_name": "Fabian Marozsan",
        "v1_aliases": ["Marozsan, Fabian"],
    },
    "martin tiffon, pol": {
        "kalshi_name": "Pol Martin Tiffon",
        "v1_aliases": ["Martin Tiffon, Pol"],
    },
    "martin, dan": {  # v1_alias_unconfirmed
        "kalshi_name": "Dan Martin",
        "v1_aliases": ["Martin, Dan"],
    },
    "martineau, matteo": {
        "kalshi_name": "Matteo Martineau",
        "v1_aliases": ["Martineau, Matteo"],
    },
    "martinez, carmen lopez": {  # v1_alias_unconfirmed
        "kalshi_name": "Carmen Lopez Martinez",
        "v1_aliases": ["Martinez, Carmen Lopez"],
    },
    "martinez, tomas": {
        "kalshi_name": "Tomas Martinez",
        "v1_aliases": ["Martinez, Tomas"],
    },
    "masur, daniel": {
        "kalshi_name": "Daniel Masur",
        "v1_aliases": ["Masur, Daniel"],
    },
    "mayew, ian": {
        "kalshi_name": "Ian Mayew",
        "v1_aliases": ["Mayew, Ian"],
    },
    "mayot, harold": {
        "kalshi_name": "Harold Mayot",
        "v1_aliases": ["Mayot, Harold"],
    },
    "mazza, manuel": {  # v1_alias_unconfirmed
        "kalshi_name": "Manuel Mazza",
        "v1_aliases": ["Mazza, Manuel"],
    },
    "mazzola, alessandra": {
        "kalshi_name": "Alessandra Mazzola",
        "v1_aliases": ["Mazzola, Alessandra"],
    },
    "mcdonald, ella": {
        "kalshi_name": "Ella McDonald",
        "v1_aliases": ["McDonald, Ella"],
    },
    "mcdonald, mackenzie": {
        "kalshi_name": "Mackenzie McDonald",
        "v1_aliases": ["McDonald, Mackenzie"],
    },
    "mcdonald, niels": {
        "kalshi_name": "Niels McDonald",
        "v1_aliases": ["McDonald, Niels"],
    },
    "mcnally, caty": {
        "kalshi_name": "Caty McNally",
        "v1_aliases": ["McNally, Caty"],
    },
    "medjedovic, hamad": {
        "kalshi_name": "Hamad Medjedovic",
        "v1_aliases": ["Medjedovic, Hamad"],
    },
    "medvedev, daniil": {
        "kalshi_name": "Daniil Medvedev",
        "v1_aliases": ["Medvedev, Daniil"],
    },
    "mejia, nicolas": {
        "kalshi_name": "Nicolas Mejia",
        "v1_aliases": ["Mejia, Nicolas"],
    },
    "meliss, verena": {  # v1_alias_unconfirmed
        "kalshi_name": "Verena Meliss",
        "v1_aliases": ["Meliss, Verena"],
    },
    "mensik, jakub": {
        "kalshi_name": "Jakub Mensik",
        "v1_aliases": ["Mensik, Jakub"],
    },
    "merida, daniel": {
        "kalshi_name": "Daniel Merida",
        "v1_aliases": ["Merida, Daniel"],
    },
    "mertens, elise": {
        "kalshi_name": "Elise Mertens",
        "v1_aliases": ["Mertens, Elise"],
    },
    "michalski, daniel": {
        "kalshi_name": "Daniel Michalski",
        "v1_aliases": ["Michalski, Daniel"],
    },
    "michelsen, alex": {
        "kalshi_name": "Alex Michelsen",
        "v1_aliases": ["Michelsen, Alex"],
    },
    "micic, elena": {
        "kalshi_name": "Elena Micic",
        "v1_aliases": ["Micic, Elena"],
    },
    "miguel, luis felipe": {
        "kalshi_name": "Luis Felipe Miguel",
        "v1_aliases": ["Miguel, Luis Felipe"],
    },
    "mikulskyte, justina": {
        "kalshi_name": "Justina Mikulskyte",
        "v1_aliases": ["Mikulskyte, Justina"],
    },
    "milavsky, daniel": {
        "kalshi_name": "Daniel Milavsky",
        "v1_aliases": ["Milavsky, Daniel"],
    },
    "milic, ognjen": {
        "kalshi_name": "Ognjen Milic",
        "v1_aliases": ["Milic, Ognjen"],
    },
    "minnen, greet": {
        "kalshi_name": "Greet Minnen",
        "v1_aliases": ["Minnen, Greet"],
    },
    "mintegi del olmo, ane": {
        "kalshi_name": "Ane Mintegi Del Olmo",
        "v1_aliases": ["Mintegi Del Olmo, Ane"],
    },
    "miyazaki, yuriko lily": {
        "kalshi_name": "Yuriko Lily Miyazaki",
        "v1_aliases": ["Miyazaki, Yuriko Lily"],
    },
    "mladenovic, kristina": {
        "kalshi_name": "Kristina Mladenovic",
        "v1_aliases": ["Mladenovic, Kristina"],
    },
    "mmoh, michael": {
        "kalshi_name": "Michael Mmoh",
        "v1_aliases": ["Mmoh, Michael"],
    },
    "mochizuki, shintaro": {
        "kalshi_name": "Shintaro Mochizuki",
        "v1_aliases": ["Mochizuki, Shintaro"],
    },
    "molcan, alex": {
        "kalshi_name": "Alex Molcan",
        "v1_aliases": ["Molcan, Alex"],
    },
    "moller, elmer": {
        "kalshi_name": "Elmer Moller",
        "v1_aliases": ["Moller, Elmer"],
    },
    "monnet, carole": {
        "kalshi_name": "Carole Monnet",
        "v1_aliases": ["Monnet, Carole"],
    },
    "montes-de la torre, inaki": {
        "kalshi_name": "Inaki Montes-de la Torre",
        "v1_aliases": ["Montes-de la Torre, Inaki"],
    },
    "montgomery, robin": {
        "kalshi_name": "Robin Montgomery",
        "v1_aliases": ["Montgomery, Robin"],
    },
    "moriya, hiroki": {  # v1_alias_unconfirmed
        "kalshi_name": "Hiroki Moriya",
        "v1_aliases": ["Moriya, Hiroki"],
    },
    "moro canas, alejandro": {
        "kalshi_name": "Alejandro Moro Canas",
        "v1_aliases": ["Moro Canas, Alejandro"],
    },
    "morvayova, viktoria": {
        "kalshi_name": "Viktoria Morvayova",
        "v1_aliases": ["Morvayova, Viktoria"],
    },
    "moutet, corentin": {
        "kalshi_name": "Corentin Moutet",
        "v1_aliases": ["Moutet, Corentin"],
    },
    "mpetshi perricard, giovanni": {
        "kalshi_name": "Giovanni Mpetshi Perricard",
        "v1_aliases": ["Mpetshi Perricard, Giovanni"],
    },
    "muchova, karolina": {
        "kalshi_name": "Karolina Muchova",
        "v1_aliases": ["Muchova, Karolina"],
    },
    "muller, alexandre": {
        "kalshi_name": "Alexandre Muller",
        "v1_aliases": ["Muller, Alexandre"],
    },
    "munar, jaume": {
        "kalshi_name": "Jaume Munar",
        "v1_aliases": ["Munar, Jaume"],
    },
    "naef, celine": {
        "kalshi_name": "Celine Naef",
        "v1_aliases": ["Naef, Celine"],
    },
    "nagal, sumit": {
        "kalshi_name": "Sumit Nagal",
        "v1_aliases": ["Nagal, Sumit"],
    },
    "nakashima, brandon": {
        "kalshi_name": "Brandon Nakashima",
        "v1_aliases": ["Nakashima, Brandon"],
    },
    "nardi, luca": {
        "kalshi_name": "Luca Nardi",
        "v1_aliases": ["Nardi, Luca"],
    },
    "nava, emilio": {
        "kalshi_name": "Emilio Nava",
        "v1_aliases": ["Nava, Emilio"],
    },
    "navarro, emma": {
        "kalshi_name": "Emma Navarro",
        "v1_aliases": ["Navarro, Emma"],
    },
    "navone, mariano": {
        "kalshi_name": "Mariano Navone",
        "v1_aliases": ["Navone, Mariano"],
    },
    "nedic, andrej": {
        "kalshi_name": "Andrej Nedic",
        "v1_aliases": ["Nedic, Andrej"],
    },
    "nesterov, pyotr": {
        "kalshi_name": "Pyotr Nesterov",
        "v1_aliases": ["Nesterov, Pyotr"],
    },
    "ngounoue, clervie": {
        "kalshi_name": "Clervie Ngounoue",
        "v1_aliases": ["Ngounoue, Clervie"],
    },
    "noha akugue, noma": {
        "kalshi_name": "Noma Noha Akugue",
        "v1_aliases": ["Noha Akugue, Noma"],
    },
    "norrie, cameron": {
        "kalshi_name": "Cameron Norrie",
        "v1_aliases": ["Norrie, Cameron"],
    },
    "noskova, linda": {
        "kalshi_name": "Linda Noskova",
        "v1_aliases": ["Noskova, Linda"],
    },
    "nourescu, alejandro mateo berge": {  # v1_alias_unconfirmed
        "kalshi_name": "Alejandro Mateo Berge Nourescu",
        "v1_aliases": ["Nourescu, Alejandro Mateo Berge"],
    },
    "o'connell, christopher": {
        "kalshi_name": "Christopher O'Connell",
        "v1_aliases": ["O'Connell, Christopher"],
    },
    "ofner, sebastian": {
        "kalshi_name": "Sebastian Ofner",
        "v1_aliases": ["Ofner, Sebastian"],
    },
    "oliynykova, oleksandra": {
        "kalshi_name": "Oleksandra Oliynykova",
        "v1_aliases": ["Oliynykova, Oleksandra"],
    },
    "onclin, gauthier": {
        "kalshi_name": "Gauthier Onclin",
        "v1_aliases": ["Onclin, Gauthier"],
    },
    "ortenzi, jazmin": {
        "kalshi_name": "Jazmin Ortenzi",
        "v1_aliases": ["Ortenzi, Jazmin"],
    },
    "osaka, naomi": {
        "kalshi_name": "Naomi Osaka",
        "v1_aliases": ["Osaka, Naomi"],
    },
    "osorio, camila": {
        "kalshi_name": "Camila Osorio",
        "v1_aliases": ["Osorio, Camila"],
    },
    "ostapenko, jelena": {
        "kalshi_name": "Jelena Ostapenko",
        "v1_aliases": ["Ostapenko, Jelena"],
    },
    "osuigwe, whitney": {
        "kalshi_name": "Whitney Osuigwe",
        "v1_aliases": ["Osuigwe, Whitney"],
    },
    "pace, francesca": {
        "kalshi_name": "Francesca Pace",
        "v1_aliases": ["Pace, Francesca"],
    },
    "palan, dominik": {  # v1_alias_unconfirmed
        "kalshi_name": "Dominik Palan",
        "v1_aliases": ["Palan, Dominik"],
    },
    "paldanius, oskari": {  # v1_alias_unconfirmed
        "kalshi_name": "Oskari Paldanius",
        "v1_aliases": ["Paldanius, Oskari"],
    },
    "palicova, barbora": {
        "kalshi_name": "Barbora Palicova",
        "v1_aliases": ["Palicova, Barbora"],
    },
    "paolini, jasmine": {
        "kalshi_name": "Jasmine Paolini",
        "v1_aliases": ["Paolini, Jasmine"],
    },
    "papamalamis, theo": {
        "kalshi_name": "Theo Papamalamis",
        "v1_aliases": ["Papamalamis, Theo"],
    },
    "paquet, chloe": {
        "kalshi_name": "Chloe Paquet",
        "v1_aliases": ["Paquet, Chloe"],
    },
    "parks, alycia": {
        "kalshi_name": "Alycia Parks",
        "v1_aliases": ["Parks, Alycia"],
    },
    "parry, diane": {
        "kalshi_name": "Diane Parry",
        "v1_aliases": ["Parry, Diane"],
    },
    "passaro, francesco": {
        "kalshi_name": "Francesco Passaro",
        "v1_aliases": ["Passaro, Francesco"],
    },
    "paul, tommy": {
        "kalshi_name": "Tommy Paul",
        "v1_aliases": ["Paul, Tommy"],
    },
    "pavlovic, luka": {
        "kalshi_name": "Luka Pavlovic",
        "v1_aliases": ["Pavlovic, Luka"],
    },
    "pedone, giorgia": {
        "kalshi_name": "Giorgia Pedone",
        "v1_aliases": ["Pedone, Giorgia"],
    },
    "pegula, jessica": {
        "kalshi_name": "Jessica Pegula",
        "v1_aliases": ["Pegula, Jessica"],
    },
    "pellegrino, andrea": {
        "kalshi_name": "Andrea Pellegrino",
        "v1_aliases": ["Pellegrino, Andrea"],
    },
    "pieri, tatiana": {
        "kalshi_name": "Tatiana Pieri",
        "v1_aliases": ["Pieri, Tatiana"],
    },
    "pinnington jones, jack": {
        "kalshi_name": "Jack Pinnington Jones",
        "v1_aliases": ["Pinnington Jones, Jack"],
    },
    "piros, zsombor": {
        "kalshi_name": "Zsombor Piros",
        "v1_aliases": ["Piros, Zsombor"],
    },
    "pliskova, karolina": {
        "kalshi_name": "Karolina Pliskova",
        "v1_aliases": ["Pliskova, Karolina"],
    },
    "podoroska, nadia": {
        "kalshi_name": "Nadia Podoroska",
        "v1_aliases": ["Podoroska, Nadia"],
    },
    "podrez, veronika": {
        "kalshi_name": "Veronika Podrez",
        "v1_aliases": ["Podrez, Veronika"],
    },
    "poling, karl": {
        "kalshi_name": "Karl Poling",
        "v1_aliases": ["Poling, Karl"],
    },
    "poljak, david": {
        "kalshi_name": "David Poljak",
        "v1_aliases": ["Poljak, David"],
    },
    "poljicak, mili": {
        "kalshi_name": "Mili Poljicak",
        "v1_aliases": ["Poljicak, Mili"],
    },
    "polmans, marc": {
        "kalshi_name": "Marc Polmans",
        "v1_aliases": ["Polmans, Marc"],
    },
    "popyrin, alexei": {
        "kalshi_name": "Alexei Popyrin",
        "v1_aliases": ["Popyrin, Alexei"],
    },
    "potapova, anastasia": {
        "kalshi_name": "Anastasia Potapova",
        "v1_aliases": ["Potapova, Anastasia"],
    },
    "potenza, luca": {
        "kalshi_name": "Luca Potenza",
        "v1_aliases": ["Potenza, Luca"],
    },
    "prashanth, vijay sundar": {  # v1_alias_unconfirmed
        "kalshi_name": "Vijay Sundar Prashanth",
        "v1_aliases": ["Prashanth, Vijay Sundar"],
    },
    "price, salvador": {
        "kalshi_name": "Salvador Price",
        "v1_aliases": ["Price, Salvador"],
    },
    "pridankina, elena": {
        "kalshi_name": "Elena Pridankina",
        "v1_aliases": ["Pridankina, Elena"],
    },
    "prizmic, dino": {
        "kalshi_name": "Dino Prizmic",
        "v1_aliases": ["Prizmic, Dino"],
    },
    "prozorova, tatiana": {
        "kalshi_name": "Tatiana Prozorova",
        "v1_aliases": ["Prozorova, Tatiana"],
    },
    "pucinelli de almeida, matheus": {
        "kalshi_name": "Matheus Pucinelli de Almeida",
        "v1_aliases": ["Pucinelli de Almeida, Matheus"],
    },
    "putintseva, yulia": {
        "kalshi_name": "Yulia Putintseva",
        "v1_aliases": ["Putintseva, Yulia"],
    },
    "quevedo, kaitlin": {
        "kalshi_name": "Kaitlin Quevedo",
        "v1_aliases": ["Quevedo, Kaitlin"],
    },
    "quinn, ethan": {
        "kalshi_name": "Ethan Quinn",
        "v1_aliases": ["Quinn, Ethan"],
    },
    "radivojevic, lola": {
        "kalshi_name": "Lola Radivojevic",
        "v1_aliases": ["Radivojevic, Lola"],
    },
    "raducanu, emma": {
        "kalshi_name": "Emma Raducanu",
        "v1_aliases": ["Raducanu, Emma"],
    },
    "rakhimova, kamilla": {
        "kalshi_name": "Kamilla Rakhimova",
        "v1_aliases": ["Rakhimova, Kamilla"],
    },
    "rakotomanga rajaonah, tiantsoa sarah": {
        "kalshi_name": "Tiantsoa Sarah Rakotomanga Rajaonah",
        "v1_aliases": ["Rakotomanga Rajaonah, Tiantsoa Sarah"],
    },
    "ramanathan, ramkumar": {  # v1_alias_unconfirmed
        "kalshi_name": "Ramkumar Ramanathan",
        "v1_aliases": ["Ramanathan, Ramkumar"],
    },
    "rapagnetta, daniele": {
        "kalshi_name": "Daniele Rapagnetta",
        "v1_aliases": ["Rapagnetta, Daniele"],
    },
    "ribero, franco": {
        "kalshi_name": "Franco Ribero",
        "v1_aliases": ["Ribero, Franco"],
    },
    "riera, julia": {
        "kalshi_name": "Julia Riera",
        "v1_aliases": ["Riera, Julia"],
    },
    "rinaldo persson, kajsa": {
        "kalshi_name": "Kajsa Rinaldo Persson",
        "v1_aliases": ["Rinaldo Persson, Kajsa"],
    },
    "rinderknech, arthur": {
        "kalshi_name": "Arthur Rinderknech",
        "v1_aliases": ["Rinderknech, Arthur"],
    },
    "ristic, mia": {
        "kalshi_name": "Mia Ristic",
        "v1_aliases": ["Ristic, Mia"],
    },
    "roca batalla, oriol": {
        "kalshi_name": "Oriol Roca Batalla",
        "v1_aliases": ["Roca Batalla, Oriol"],
    },
    "rodesch, chris": {
        "kalshi_name": "Chris Rodesch",
        "v1_aliases": ["Rodesch, Chris"],
    },
    "rodionov, jurij": {
        "kalshi_name": "Jurij Rodionov",
        "v1_aliases": ["Rodionov, Jurij"],
    },
    "rodriguez taverna, santiago": {
        "kalshi_name": "Santiago Rodriguez Taverna",
        "v1_aliases": ["Rodriguez Taverna, Santiago"],
    },
    "romero gormaz, leyre": {
        "kalshi_name": "Leyre Romero Gormaz",
        "v1_aliases": ["Romero Gormaz, Leyre"],
    },
    "royer, valentin": {
        "kalshi_name": "Valentin Royer",
        "v1_aliases": ["Royer, Valentin"],
    },
    "rubio fierros, alan fernando": {
        "kalshi_name": "Alan Fernando Rubio Fierros",
        "v1_aliases": ["Rubio Fierros, Alan Fernando"],
    },
    "rublev, andrey": {
        "kalshi_name": "Andrey Rublev",
        "v1_aliases": ["Rublev, Andrey"],
    },
    "ruggeri, jennifer": {
        "kalshi_name": "Jennifer Ruggeri",
        "v1_aliases": ["Ruggeri, Jennifer"],
    },
    "ruiz, jorge": {
        "kalshi_name": "Jorge Ruiz",
        "v1_aliases": ["Ruiz, Jorge"],
    },
    "rus, arantxa": {
        "kalshi_name": "Arantxa Rus",
        "v1_aliases": ["Rus, Arantxa"],
    },
    "ruse, elena-gabriela": {
        "kalshi_name": "Elena-Gabriela Ruse",
        "v1_aliases": ["Ruse, Elena-Gabriela"],
    },
    "ruud, casper": {
        "kalshi_name": "Casper Ruud",
        "v1_aliases": ["Ruud, Casper"],
    },
    "ruzic, antonia": {
        "kalshi_name": "Antonia Ruzic",
        "v1_aliases": ["Ruzic, Antonia"],
    },
    "rybakina, elena": {
        "kalshi_name": "Elena Rybakina",
        "v1_aliases": ["Rybakina, Elena"],
    },
    "rybakov, alex": {  # v1_alias_unconfirmed
        "kalshi_name": "Alex Rybakov",
        "v1_aliases": ["Rybakov, Alex"],
    },
    "ryser, valentina": {
        "kalshi_name": "Valentina Ryser",
        "v1_aliases": ["Ryser, Valentina"],
    },
    "sabalenka, aryna": {
        "kalshi_name": "Aryna Sabalenka",
        "v1_aliases": ["Sabalenka, Aryna"],
    },
    "sachko, vitaliy": {
        "kalshi_name": "Vitaliy Sachko",
        "v1_aliases": ["Sachko, Vitaliy"],
    },
    "safiullin, roman": {
        "kalshi_name": "Roman Safiullin",
        "v1_aliases": ["Safiullin, Roman"],
    },
    "sakamoto, rei": {
        "kalshi_name": "Rei Sakamoto",
        "v1_aliases": ["Sakamoto, Rei"],
    },
    "sakatsume, himeno": {
        "kalshi_name": "Himeno Sakatsume",
        "v1_aliases": ["Sakatsume, Himeno"],
    },
    "sakellaridis, stefanos": {
        "kalshi_name": "Stefanos Sakellaridis",
        "v1_aliases": ["Sakellaridis, Stefanos"],
    },
    "sakkari, maria": {
        "kalshi_name": "Maria Sakkari",
        "v1_aliases": ["Sakkari, Maria"],
    },
    "salazar, amador": {  # v1_alias_unconfirmed
        "kalshi_name": "Amador Salazar",
        "v1_aliases": ["Salazar, Amador"],
    },
    "salden, lara": {  # v1_alias_unconfirmed
        "kalshi_name": "Lara Salden",
        "v1_aliases": ["Salden, Lara"],
    },
    "salkova, dominika": {
        "kalshi_name": "Dominika Salkova",
        "v1_aliases": ["Salkova, Dominika"],
    },
    "samson, laura": {
        "kalshi_name": "Laura Samson",
        "v1_aliases": ["Samson, Laura"],
    },
    "samsonova, liudmila": {
        "kalshi_name": "Liudmila Samsonova",
        "v1_aliases": ["Samsonova, Liudmila"],
    },
    "samuel, toby": {
        "kalshi_name": "Toby Samuel",
        "v1_aliases": ["Samuel, Toby"],
    },
    "sanchez izquierdo, nikolas": {
        "kalshi_name": "Nikolas Sanchez Izquierdo",
        "v1_aliases": ["Sanchez Izquierdo, Nikolas"],
    },
    "sanchez quilez, alejo": {
        "kalshi_name": "Alejo Sanchez Quilez",
        "v1_aliases": ["Sanchez Quilez, Alejo"],
    },
    "sanchez, ana sofia": {
        "kalshi_name": "Ana Sofia Sanchez",
        "v1_aliases": ["Sanchez, Ana Sofia"],
    },
    "santamarta roig, andres": {
        "kalshi_name": "Andres Santamarta Roig",
        "v1_aliases": ["Santamarta Roig, Andres"],
    },
    "santillan, akira": {
        "kalshi_name": "Akira Santillan",
        "v1_aliases": ["Santillan, Akira"],
    },
    "saraiva dos santos, paulo andre": {
        "kalshi_name": "Paulo Andre Saraiva Dos Santos",
        "v1_aliases": ["Saraiva Dos Santos, Paulo Andre"],
    },
    "sasnovich, aliaksandra": {
        "kalshi_name": "Aliaksandra Sasnovich",
        "v1_aliases": ["Sasnovich, Aliaksandra"],
    },
    "sawangkaew, mananchaya": {
        "kalshi_name": "Mananchaya Sawangkaew",
        "v1_aliases": ["Sawangkaew, Mananchaya"],
    },
    "schepper, kenny de": {  # v1_alias_unconfirmed
        "kalshi_name": "Kenny De Schepper",
        "v1_aliases": ["Schepper, Kenny De"],
    },
    "schoolkate, tristan": {
        "kalshi_name": "Tristan Schoolkate",
        "v1_aliases": ["Schoolkate, Tristan"],
    },
    "searle, henry": {
        "kalshi_name": "Henry Searle",
        "v1_aliases": ["Searle, Henry"],
    },
    "sebov, katherine": {
        "kalshi_name": "Katherine Sebov",
        "v1_aliases": ["Sebov, Katherine"],
    },
    "seghetti, samuele": {  # v1_alias_unconfirmed
        "kalshi_name": "Samuele Seghetti",
        "v1_aliases": ["Seghetti, Samuele"],
    },
    "seidel, ella": {
        "kalshi_name": "Ella Seidel",
        "v1_aliases": ["Seidel, Ella"],
    },
    "selekhmeteva, oksana": {
        "kalshi_name": "Oksana Selekhmeteva",
        "v1_aliases": ["Selekhmeteva, Oksana"],
    },
    "sels, jelle": {
        "kalshi_name": "Jelle Sels",
        "v1_aliases": ["Sels, Jelle"],
    },
    "semenistaja, darja": {
        "kalshi_name": "Darja Semenistaja",
        "v1_aliases": ["Semenistaja, Darja"],
    },
    "serban, raluca georgiana": {
        "kalshi_name": "Raluca Georgiana Serban",
        "v1_aliases": ["Serban, Raluca Georgiana"],
    },
    "seyboth wild, thiago": {
        "kalshi_name": "Thiago Seyboth Wild",
        "v1_aliases": ["Seyboth Wild, Thiago"],
    },
    "shapovalov, denis": {
        "kalshi_name": "Denis Shapovalov",
        "v1_aliases": ["Shapovalov, Denis"],
    },
    "shelbayh, abdullah": {
        "kalshi_name": "Abdullah Shelbayh",
        "v1_aliases": ["Shelbayh, Abdullah"],
    },
    "shelton, ben": {
        "kalshi_name": "Ben Shelton",
        "v1_aliases": ["Shelton, Ben"],
    },
    "sherif ahmed abdelaziz, maiar": {
        "kalshi_name": "Maiar Sherif Ahmed Abdelaziz",
        "v1_aliases": ["Sherif Ahmed Abdelaziz, Maiar"],
    },
    "shevchenko, alexander": {
        "kalshi_name": "Aleksandr Shevchenko",
        "v1_aliases": ["Shevchenko, Alexander", "Shevchenko, Aleksandr"],
    },
    "shimabukuro, sho": {
        "kalshi_name": "Sho Shimabukuro",
        "v1_aliases": ["Shimabukuro, Sho"],
    },
    "shimizu, yuta": {
        "kalshi_name": "Yuta Shimizu",
        "v1_aliases": ["Shimizu, Yuta"],
    },
    "shnaider, diana": {
        "kalshi_name": "Diana Shnaider",
        "v1_aliases": ["Shnaider, Diana"],
    },
    "shubladze, alexandra": {
        "kalshi_name": "Alexandra Shubladze",
        "v1_aliases": ["Shubladze, Alexandra"],
    },
    "shymanovich, iryna": {
        "kalshi_name": "Iryna Shymanovich",
        "v1_aliases": ["Shymanovich, Iryna"],
    },
    "siegemund, laura": {
        "kalshi_name": "Laura Siegemund",
        "v1_aliases": ["Siegemund, Laura"],
    },
    "sierra, solana": {
        "kalshi_name": "Solana Sierra",
        "v1_aliases": ["Sierra, Solana"],
    },
    "simakin, ilia": {
        "kalshi_name": "Ilia Simakin",
        "v1_aliases": ["Simakin, Ilia"],
    },
    "sinescu, jan patrick": {  # v1_alias_unconfirmed
        "kalshi_name": "Jan Patrick Sinescu",
        "v1_aliases": ["Sinescu, Jan Patrick"],
    },
    "sinha, nitin kumar": {  # v1_alias_unconfirmed
        "kalshi_name": "Nitin Kumar Sinha",
        "v1_aliases": ["Sinha, Nitin Kumar"],
    },
    "siniakova, katerina": {
        "kalshi_name": "Katerina Siniakova",
        "v1_aliases": ["Siniakova, Katerina"],
    },
    "sinner, jannik": {
        "kalshi_name": "Jannik Sinner",
        "v1_aliases": ["Sinner, Jannik"],
    },
    "siskova, anna": {
        "kalshi_name": "Anna Siskova",
        "v1_aliases": ["Siskova, Anna"],
    },
    "skatov, timofey": {
        "kalshi_name": "Timofey Skatov",
        "v1_aliases": ["Skatov, Timofey"],
    },
    "smith, alana": {  # v1_alias_unconfirmed
        "kalshi_name": "Alana Smith",
        "v1_aliases": ["Smith, Alana"],
    },
    "smith, colton": {
        "kalshi_name": "Colton Smith",
        "v1_aliases": ["Smith, Colton"],
    },
    "smith, keegan": {
        "kalshi_name": "Keegan Smith",
        "v1_aliases": ["Smith, Keegan"],
    },
    "snigur, daria": {
        "kalshi_name": "Daria Snigur",
        "v1_aliases": ["Snigur, Daria"],
    },
    "sobolieva, anastasiia": {
        "kalshi_name": "Anastasiia Sobolieva",
        "v1_aliases": ["Sobolieva, Anastasiia"],
    },
    "sonego, lorenzo": {
        "kalshi_name": "Lorenzo Sonego",
        "v1_aliases": ["Sonego, Lorenzo"],
    },
    "sonmez, zeynep": {
        "kalshi_name": "Zeynep Sonmez",
        "v1_aliases": ["Sonmez, Zeynep"],
    },
    "sorribes tormo, sara": {
        "kalshi_name": "Sara Sorribes Tormo",
        "v1_aliases": ["Sorribes Tormo, Sara"],
    },
    "soto, matias": {
        "kalshi_name": "Matias Soto",
        "v1_aliases": ["Soto, Matias"],
    },
    "sperle, john": {  # v1_alias_unconfirmed
        "kalshi_name": "John Sperle",
        "v1_aliases": ["Sperle, John"],
    },
    "sramkova, rebecca": {
        "kalshi_name": "Rebecca Sramkova",
        "v1_aliases": ["Sramkova, Rebecca"],
    },
    "staeheli, luca": {
        "kalshi_name": "Luca Staeheli",
        "v1_aliases": ["Staeheli, Luca"],
    },
    "starodubtseva, yuliia": {
        "kalshi_name": "Yuliia Starodubtseva",
        "v1_aliases": ["Starodubtseva, Yuliia"],
    },
    "stearns, peyton": {
        "kalshi_name": "Peyton Stearns",
        "v1_aliases": ["Stearns, Peyton"],
    },
    "stefanini, lucrezia": {
        "kalshi_name": "Lucrezia Stefanini",
        "v1_aliases": ["Stefanini, Lucrezia"],
    },
    "stewart, hamish": {
        "kalshi_name": "Hamish Stewart",
        "v1_aliases": ["Stewart, Hamish"],
    },
    "stoiana, mary": {
        "kalshi_name": "Mary Stoiana",
        "v1_aliases": ["Stoiana, Mary"],
    },
    "stojsavljevic, mika": {
        "kalshi_name": "Mika Stojsavljevic",
        "v1_aliases": ["Stojsavljevic, Mika"],
    },
    "struff, jan-lennard": {
        "kalshi_name": "Jan-Lennard Struff",
        "v1_aliases": ["Struff, Jan-Lennard"],
    },
    "struplova, julie": {  # v1_alias_unconfirmed
        "kalshi_name": "Julie Struplova",
        "v1_aliases": ["Struplova, Julie"],
    },
    "sun, lulu": {
        "kalshi_name": "Lulu Sun",
        "v1_aliases": ["Sun, Lulu"],
    },
    "suresh, dhakshineswar": {
        "kalshi_name": "Dhakshineswar Suresh",
        "v1_aliases": ["Suresh, Dhakshineswar"],
    },
    "svajda, trevor": {  # v1_alias_unconfirmed
        "kalshi_name": "Trevor Svajda",
        "v1_aliases": ["Svajda, Trevor"],
    },
    "svajda, zachary": {
        "kalshi_name": "Zachary Svajda",
        "v1_aliases": ["Svajda, Zachary"],
    },
    "svitolina, elina": {
        "kalshi_name": "Elina Svitolina",
        "v1_aliases": ["Svitolina, Elina"],
    },
    "svrcina, dalibor": {
        "kalshi_name": "Dalibor Svrcina",
        "v1_aliases": ["Svrcina, Dalibor"],
    },
    "swan, katie": {
        "kalshi_name": "Katie Swan",
        "v1_aliases": ["Swan, Katie"],
    },
    "sweeny, dane": {
        "kalshi_name": "Dane Sweeny",
        "v1_aliases": ["Sweeny, Dane"],
    },
    "swiatek, iga": {
        "kalshi_name": "Iga Swiatek",
        "v1_aliases": ["Swiatek, Iga"],
    },
    "tabacco, fausto": {
        "kalshi_name": "Fausto Tabacco",
        "v1_aliases": ["Tabacco, Fausto"],
    },
    "tabilo, alejandro": {
        "kalshi_name": "Alejandro Tabilo",
        "v1_aliases": ["Tabilo, Alejandro"],
    },
    "tabur, clement": {
        "kalshi_name": "Clement Tabur",
        "v1_aliases": ["Tabur, Clement"],
    },
    "tagger, lilli": {
        "kalshi_name": "Lilli Tagger",
        "v1_aliases": ["Tagger, Lilli"],
    },
    "tan, harmony": {
        "kalshi_name": "Harmony Tan",
        "v1_aliases": ["Tan, Harmony"],
    },
    "tararudee, lanlana": {
        "kalshi_name": "Lanlana Tararudee",
        "v1_aliases": ["Tararudee, Lanlana"],
    },
    "tarvet, oliver": {
        "kalshi_name": "Oliver Tarvet",
        "v1_aliases": ["Tarvet, Oliver"],
    },
    "tauson, clara": {
        "kalshi_name": "Clara Tauson",
        "v1_aliases": ["Tauson, Clara"],
    },
    "teichmann, jil": {
        "kalshi_name": "Jil Teichmann",
        "v1_aliases": ["Teichmann, Jil"],
    },
    "tenti, fermin": {  # v1_alias_unconfirmed
        "kalshi_name": "Fermin Tenti",
        "v1_aliases": ["Tenti, Fermin"],
    },
    "tiafoe, frances": {
        "kalshi_name": "Frances Tiafoe",
        "v1_aliases": ["Tiafoe, Frances"],
    },
    "tian, fangran": {
        "kalshi_name": "Fangran Tian",
        "v1_aliases": ["Tian, Fangran"],
    },
    "tien, learner": {
        "kalshi_name": "Learner Tien",
        "v1_aliases": ["Tien, Learner"],
    },
    "timofeeva, maria": {
        "kalshi_name": "Maria Timofeeva",
        "v1_aliases": ["Timofeeva, Maria"],
    },
    "tirante, thiago agustin": {
        "kalshi_name": "Thiago Agustin Tirante",
        "v1_aliases": ["Tirante, Thiago Agustin"],
    },
    "tjen, janice": {
        "kalshi_name": "Janice Tjen",
        "v1_aliases": ["Tjen, Janice"],
    },
    "tomai, carla": {  # v1_alias_unconfirmed
        "kalshi_name": "Carla Tomai",
        "v1_aliases": ["Tomai, Carla"],
    },
    "tomic, bernard": {
        "kalshi_name": "Bernard Tomic",
        "v1_aliases": ["Tomic, Bernard"],
    },
    "tomljanovic, ajla": {
        "kalshi_name": "Ajla Tomljanovic",
        "v1_aliases": ["Tomljanovic, Ajla"],
    },
    "topo, marko": {
        "kalshi_name": "Marko ToPo",
        "v1_aliases": ["ToPo, Marko"],
    },
    "torres, juan bautista": {
        "kalshi_name": "Juan Bautista Torres",
        "v1_aliases": ["Torres, Juan Bautista"],
    },
    "toth, amarissa kiara": {  # v1_alias_unconfirmed
        "kalshi_name": "Amarissa Kiara Toth",
        "v1_aliases": ["Toth, Amarissa Kiara"],
    },
    "townsend, taylor": {
        "kalshi_name": "Taylor Townsend",
        "v1_aliases": ["Townsend, Taylor"],
    },
    "travaglia, stefano": {
        "kalshi_name": "Stefano Travaglia",
        "v1_aliases": ["Travaglia, Stefano"],
    },
    "trevisan, martina": {
        "kalshi_name": "Martina Trevisan",
        "v1_aliases": ["Trevisan, Martina"],
    },
    "trotter, james kent": {
        "kalshi_name": "James Kent Trotter",
        "v1_aliases": ["Trotter, James Kent"],
    },
    "trungelliti, marco": {
        "kalshi_name": "Marco Trungelliti",
        "v1_aliases": ["Trungelliti, Marco"],
    },
    "tsitsipas, stefanos": {
        "kalshi_name": "Stefanos Tsitsipas",
        "v1_aliases": ["Tsitsipas, Stefanos"],
    },
    "turcanu, radu david": {  # v1_alias_unconfirmed
        "kalshi_name": "Radu David Turcanu",
        "v1_aliases": ["Turcanu, Radu David"],
    },
    "udvardy, panna": {
        "kalshi_name": "Panna Udvardy",
        "v1_aliases": ["Udvardy, Panna"],
    },
    "ugo carabelli, camilo": {
        "kalshi_name": "Camilo Ugo Carabelli",
        "v1_aliases": ["Ugo Carabelli, Camilo"],
    },
    "urhobo, akasha": {
        "kalshi_name": "Akasha Urhobo",
        "v1_aliases": ["Urhobo, Akasha"],
    },
    "urrea, andres": {
        "kalshi_name": "Andres Urrea",
        "v1_aliases": ["Urrea, Andres"],
    },
    "ursu, vadym": {  # v1_alias_unconfirmed
        "kalshi_name": "Vadym Ursu",
        "v1_aliases": ["Ursu, Vadym"],
    },
    "valdmannova, vendula": {
        "kalshi_name": "Vendula Valdmannova",
        "v1_aliases": ["Valdmannova, Vendula"],
    },
    "valentova, tereza": {
        "kalshi_name": "Tereza Valentova",
        "v1_aliases": ["Valentova, Tereza"],
    },
    "vales, amit": {
        "kalshi_name": "Amit Vales",
        "v1_aliases": ["Vales, Amit"],
    },
    "vallejo, adolfo daniel": {
        "kalshi_name": "Adolfo Daniel Vallejo",
        "v1_aliases": ["Vallejo, Adolfo Daniel"],
    },
    "van assche, luca": {
        "kalshi_name": "Luca Van Assche",
        "v1_aliases": ["Van Assche, Luca"],
    },
    "van de zandschulp, botic": {
        "kalshi_name": "Botic Van de Zandschulp",
        "v1_aliases": ["Van de Zandschulp, Botic"],
    },
    "vandecasteele, quinn": {
        "kalshi_name": "Quinn Vandecasteele",
        "v1_aliases": ["Vandecasteele, Quinn"],
    },
    "vandewinkel, hanne": {
        "kalshi_name": "Hanne Vandewinkel",
        "v1_aliases": ["Vandewinkel, Hanne"],
    },
    "vandromme, jeline": {
        "kalshi_name": "Jeline Vandromme",
        "v1_aliases": ["Vandromme, Jeline"],
    },
    "vaquero, maria martinez": {  # v1_alias_unconfirmed
        "kalshi_name": "Maria Martinez Vaquero",
        "v1_aliases": ["Vaquero, Maria Martinez"],
    },
    "vasa, eero": {
        "kalshi_name": "Eero Vasa",
        "v1_aliases": ["Vasa, Eero"],
    },
    "vasilev, alexander": {
        "kalshi_name": "Alexander Vasilev",
        "v1_aliases": ["Vasilev, Alexander"],
    },
    "vekic, donna": {
        "kalshi_name": "Donna Vekic",
        "v1_aliases": ["Vekic, Donna"],
    },
    "vela, giuseppe la": {  # v1_alias_unconfirmed
        "kalshi_name": "Giuseppe La Vela",
        "v1_aliases": ["Vela, Giuseppe La"],
    },
    "vickery, sachia": {
        "kalshi_name": "Sachia Vickery",
        "v1_aliases": ["Vickery, Sachia"],
    },
    "vidmanova, darja": {
        "kalshi_name": "Darja Vidmanova",
        "v1_aliases": ["Vidmanova, Darja"],
    },
    "villanueva, gonzalo": {
        "kalshi_name": "Gonzalo Villanueva",
        "v1_aliases": ["Villanueva, Gonzalo"],
    },
    "virtanen, otto": {
        "kalshi_name": "Otto Virtanen",
        "v1_aliases": ["Virtanen, Otto"],
    },
    "voloshchuk, angelina": {
        "kalshi_name": "Angelina Voloshchuk",
        "v1_aliases": ["Voloshchuk, Angelina"],
    },
    "volynets, katie": {
        "kalshi_name": "Katie Volynets",
        "v1_aliases": ["Volynets, Katie"],
    },
    "von deichmann, kathinka": {
        "kalshi_name": "Kathinka Von Deichmann",
        "v1_aliases": ["Von Deichmann, Kathinka"],
    },
    "vrbensky, michael": {  # v1_alias_unconfirmed
        "kalshi_name": "Michael Vrbensky",
        "v1_aliases": ["Vrbensky, Michael"],
    },
    "vukic, aleksandar": {
        "kalshi_name": "Aleksandar Vukic",
        "v1_aliases": ["Vukic, Aleksandar"],
    },
    "wallin, olle": {
        "kalshi_name": "Olle Wallin",
        "v1_aliases": ["Wallin, Olle"],
    },
    "waltert, simona": {
        "kalshi_name": "Simona Waltert",
        "v1_aliases": ["Waltert, Simona"],
    },
    "walton, adam": {
        "kalshi_name": "Adam Walton",
        "v1_aliases": ["Walton, Adam"],
    },
    "wang, xinyu": {
        "kalshi_name": "Xinyu Wang",
        "v1_aliases": ["Wang, Xinyu"],
    },
    "wang, xiyu": {
        "kalshi_name": "Xiyu Wang",
        "v1_aliases": ["Wang, Xiyu"],
    },
    "watson, heather": {
        "kalshi_name": "Heather Watson",
        "v1_aliases": ["Watson, Heather"],
    },
    "wawrinka, stan": {
        "kalshi_name": "Stan Wawrinka",
        "v1_aliases": ["Wawrinka, Stan"],
    },
    "wehnelt, kai": {
        "kalshi_name": "Kai Wehnelt",
        "v1_aliases": ["Wehnelt, Kai"],
    },
    "wendelken, harry": {
        "kalshi_name": "Harry Wendelken",
        "v1_aliases": ["Wendelken, Harry"],
    },
    "werner, caroline": {
        "kalshi_name": "Caroline Werner",
        "v1_aliases": ["Werner, Caroline"],
    },
    "williams, serena": {
        "kalshi_name": "Serena Williams",
        "v1_aliases": ["Williams, Serena"],
    },
    "winter, edward": {
        "kalshi_name": "Edward Winter",
        "v1_aliases": ["Winter, Edward"],
    },
    "wong, tsz fu": {  # v1_alias_unconfirmed
        "kalshi_name": "Tsz Fu Wong",
        "v1_aliases": ["Wong, Tsz Fu"],
    },
    "wu, yibing": {
        "kalshi_name": "Yibing Wu",
        "v1_aliases": ["Wu, Yibing"],
    },
    "xu, mingge": {
        "kalshi_name": "Mingge Xu",
        "v1_aliases": ["Xu, Mingge", "Xu, Mimi"],
    },
    "yaneva, elizara": {
        "kalshi_name": "Elizara Yaneva",
        "v1_aliases": ["Yaneva, Elizara"],
    },
    "yastremska, dayana": {
        "kalshi_name": "Dayana Yastremska",
        "v1_aliases": ["Yastremska, Dayana"],
    },
    "yevseyev, denis": {
        "kalshi_name": "Denis Yevseyev",
        "v1_aliases": ["Yevseyev, Denis"],
    },
    "ymer, elias": {
        "kalshi_name": "Elias Ymer",
        "v1_aliases": ["Ymer, Elias"],
    },
    "you, xiaodi": {
        "kalshi_name": "Xiaodi You",
        "v1_aliases": ["You, Xiaodi"],
    },
    "yunez, lucas": {  # v1_alias_unconfirmed
        "kalshi_name": "Lucas Yunez",
        "v1_aliases": ["Yunez, Lucas"],
    },
    "zakharova, anastasia": {
        "kalshi_name": "Anastasia Zakharova",
        "v1_aliases": ["Zakharova, Anastasia"],
    },
    "zamarripa, allura": {
        "kalshi_name": "Allura Zamarripa",
        "v1_aliases": ["Zamarripa, Allura"],
    },
    "zanellato, nicolas": {
        "kalshi_name": "Nicolas Zanellato",
        "v1_aliases": ["Zanellato, Nicolas"],
    },
    "zarazua, renata": {
        "kalshi_name": "Renata Zarazua",
        "v1_aliases": ["Zarazua, Renata"],
    },
    "zeballos, federico": {
        "kalshi_name": "Federico Zeballos",
        "v1_aliases": ["Zeballos, Federico"],
    },
    "zelnickova, radka": {  # v1_alias_unconfirmed
        "kalshi_name": "Radka Zelnickova",
        "v1_aliases": ["Zelnickova, Radka"],
    },
    "zhang, shuai": {
        "kalshi_name": "Shuai Zhang",
        "v1_aliases": ["Zhang, Shuai"],
    },
    "zheng, michael": {
        "kalshi_name": "Michael Zheng",
        "v1_aliases": ["Zheng, Michael"],
    },
    "zheng, qinwen": {
        "kalshi_name": "Qinwen Zheng",
        "v1_aliases": ["Zheng, Qinwen"],
    },
    "zhou, yi": {
        "kalshi_name": "Yi Zhou",
        "v1_aliases": ["Zhou, Yi"],
    },
    "zhu, amy": {  # v1_alias_unconfirmed
        "kalshi_name": "Amy Zhu",
        "v1_aliases": ["Zhu, Amy"],
    },
    "zhu, lin": {
        "kalshi_name": "Lin Zhu",
        "v1_aliases": ["Zhu, Lin"],
    },
    "zhu, michael": {
        "kalshi_name": "Michael Zhu",
        "v1_aliases": ["Zhu, Michael"],
    },
    "zidansek, tamara": {
        "kalshi_name": "Tamara Zidansek",
        "v1_aliases": ["Zidansek, Tamara"],
    },
    "zverev, alexander": {
        "kalshi_name": "Alexander Zverev",
        "v1_aliases": ["Zverev, Alexander"],
    },
    "zvonareva, vera": {
        "kalshi_name": "Vera Zvonareva",
        "v1_aliases": ["Zvonareva, Vera"],
    },
}
