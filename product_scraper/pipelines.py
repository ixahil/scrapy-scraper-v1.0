import pandas as pd
import os
import json
import csv
import re
from tqdm import tqdm
import logging
from urllib.parse import urlparse
import requests


# logging.getLogger("scrapy").setLevel(logging.ERROR)
# logging.getLogger("scrapy-playwright").setLevel(logging.ERROR)
# logging.getLogger("playwright").setLevel(logging.ERROR)

# ---------------------------------------------------------------------------
# Color name → hex lookup (from https://gist.github.com/CheeseCake87/25891a581f5d363fde1b8324b146b562)
# Keys are stored lowercase for case-insensitive matching.
# ---------------------------------------------------------------------------
_RAW_COLOR_MAP = {'Black': '#000000', 'Night': '#0C090A', 'Charcoal': '#34282C', 'Oil': '#3B3131', 'Dark Gray': '#3A3B3C', 'Light Black': '#454545',
          'Black Cat': '#413839', 'Iridium': '#3D3C3A', 'Black Eel': '#463E3F', 'Black Cow': '#4C4646', 'Gray Wolf': '#504A4B', 'Vampire Gray': '#565051',
          'Iron Gray': '#52595D', 'Gray Dolphin': '#5C5858', 'Carbon Gray': '#625D5D', 'Ash Gray': '#666362', 'DimGray DimGrey': '#696969',
          'Nardo Gray': '#686A6C', 'Cloudy Gray': '#6D6968', 'Smokey Gray': '#726E6D', 'Alien Gray': '#736F6E', 'Sonic Silver': '#757575',
          'Platinum Gray': '#797979', 'Granite': '#837E7C', 'Gray Grey': '#808080', 'Battleship Gray': '#848482', 'Gunmetal Gray': '#8D918D',
          'DarkGray DarkGrey': '#A9A9A9', 'Gray Cloud': '#B6B6B4', 'Silver': '#C0C0C0', 'Pale Silver': '#C9C0BB', 'Gray Goose': '#D1D0CE',
          'Platinum Silver': '#CECECE', 'LightGray LightGrey': '#D3D3D3', 'Gainsboro': '#DCDCDC', 'Platinum': '#E5E4E2', 'Metallic Silver': '#BCC6CC',
          'Blue Gray': '#98AFC7', 'Roman Silver': '#838996', 'LightSlateGray LightSlateGrey': '#778899', 'SlateGray SlateGrey': '#708090',
          'Rat Gray': '#6D7B8D', 'Slate Granite Gray': '#657383', 'Jet Gray': '#616D7E', 'Mist Blue': '#646D7E', 'Marble Blue': '#566D7E',
          'Slate Blue Grey': '#737CA1', 'Light Purple Blue': '#728FCE', 'Azure Blue': '#4863A0', 'Blue Jay': '#2B547E', 'Charcoal Blue': '#36454F',
          'Dark Blue Grey': '#29465B', 'Dark Slate': '#2B3856', 'Deep-Sea Blue': '#123456', 'Night Blue': '#151B54', 'MidnightBlue': '#191970',
          'Navy': '#000080', 'Denim Dark Blue': '#151B8D', 'DarkBlue': '#00008B', 'Lapis Blue': '#15317E', 'New Midnight Blue': '#0000A0',
          'Earth Blue': '#0000A5', 'Cobalt Blue': '#0020C2', 'MediumBlue': '#0000CD', 'Blueberry Blue': '#0041C2', 'Canary Blue': '#2916F5', 'Blue': '#0000FF',
          'Bright Blue': '#0909FF', 'Blue Orchid': '#1F45FC', 'Sapphire Blue': '#2554C7', 'Blue Eyes': '#1569C7', 'Bright Navy Blue': '#1974D2',
          'Balloon Blue': '#2B60DE', 'RoyalBlue': '#4169E1', 'Ocean Blue': '#2B65EC', 'Blue Ribbon': '#306EFF', 'Blue Dress': '#157DEC', 'Neon Blue': '#1589FF',
          'DodgerBlue': '#1E90FF', 'Glacial Blue Ice': '#368BC1', 'SteelBlue': '#4682B4', 'Silk Blue': '#488AC7', 'Windows Blue': '#357EC7',
          'Blue Ivy': '#3090C7', 'Blue Koi': '#659EC7', 'Columbia Blue': '#87AFC7', 'Baby Blue': '#95B9C7', 'CornflowerBlue': '#6495ED',
          'Sky Blue Dress': '#6698FF', 'Iceberg': '#56A5EC', 'Butterfly Blue': '#38ACEC', 'DeepSkyBlue': '#00BFFF', 'Midday Blue': '#3BB9FF',
          'Crystal Blue': '#5CB3FF', 'Denim Blue': '#79BAEC', 'Day Sky Blue': '#82CAFF', 'LightSkyBlue': '#87CEFA', 'SkyBlue': '#87CEEB',
          'Jeans Blue': '#A0CFEC', 'Blue Angel': '#B7CEEC', 'Pastel Blue': '#B4CFEC', 'Light Day Blue': '#ADDFFF', 'Sea Blue': '#C2DFFF',
          'Heavenly Blue': '#C6DEFF', 'Robin Egg Blue': '#BDEDFF', 'PowderBlue': '#B0E0E6', 'Coral Blue': '#AFDCEC', 'LightBlue': '#ADD8E6',
          'LightSteelBlue': '#B0CFDE', 'Gulf Blue': '#C9DFEC', 'Pastel Light Blue': '#D5D6EA', 'Lavender Blue': '#E3E4FA', 'Lavender': '#E6E6FA',
          'Water': '#EBF4FA', 'AliceBlue': '#F0F8FF', 'GhostWhite': '#F8F8FF', 'Azure': '#F0FFFF', 'LightCyan': '#E0FFFF', 'Light Slate': '#CCFFFF',
          'Electric Blue': '#9AFEFF', 'Tron Blue': '#7DFDFE', 'Blue Zircon': '#57FEFF', 'Aqua Cyan': '#00FFFF', 'Bright Cyan': '#0AFFFF', 'Celeste': '#50EBEC',
          'Blue Diamond': '#4EE2EC', 'Bright Turquoise': '#16E2F5', 'Blue Lagoon': '#8EEBEC', 'PaleTurquoise': '#AFEEEE', 'Pale Blue Lily': '#CFECEC',
          'Tiffany Blue': '#81D8D0', 'Blue Hosta': '#77BFC7', 'Cyan Opaque': '#92C7C7', 'Northern Lights Blue': '#78C7C7', 'Blue Green': '#7BCCB5',
          'MediumAquaMarine': '#66CDAA', 'Magic Mint': '#AAF0D1', 'Aquamarine': '#7FFFD4', 'Light Aquamarine': '#93FFE8', 'Turquoise': '#40E0D0',
          'MediumTurquoise': '#48D1CC', 'Deep Turquoise': '#48CCCD', 'Jellyfish': '#46C7C7', 'Blue Turquoise': '#43C6DB', 'DarkTurquoise': '#00CED1',
          'Macaw Blue Green': '#43BFC7', 'LightSeaGreen': '#20B2AA', 'Seafoam Green': '#3EA99F', 'CadetBlue': '#5F9EA0', 'Deep-Sea': '#3B9C9C',
          'DarkCyan': '#008B8B', 'Teal': '#008080', 'Medium Teal': '#045F5F', 'Deep Teal': '#033E3E', 'DarkSlateGray DarkSlateGrey': '#25383C',
          'Gunmetal': '#2C3539', 'Blue Moss Green': '#3C565B', 'Beetle Green': '#4C787E', 'Grayish Turquoise': '#5E7D7E', 'Greenish Blue': '#307D7E',
          'Aquamarine Stone': '#348781', 'Sea Turtle Green': '#438D80', 'Dull-Sea Green': '#4E8975', 'Deep-Sea Green': '#306754', 'SeaGreen': '#2E8B57',
          'Dark Mint': '#31906E', 'Jade': '#00A36C', 'Earth Green': '#34A56F', 'Emerald': '#50C878', 'Mint': '#3EB489', 'MediumSeaGreen': '#3CB371',
          'Camouflage Green': '#78866B', 'Sage Green': '#848B79', 'Hazel Green': '#617C58', 'Venom Green': '#728C00', 'OliveDrab': '#6B8E23',
          'Olive': '#808000', 'DarkOliveGreen': '#556B2F', 'Army Green': '#4B5320', 'Fern Green': '#667C26', 'Fall Forest Green': '#4E9258',
          'Pine Green': '#387C44', 'Medium Forest Green': '#347235', 'Jungle Green': '#347C2C', 'ForestGreen': '#228B22', 'Green': '#008000',
          'DarkGreen': '#006400', 'Deep Emerald Green': '#046307', 'Dark Forest Green': '#254117', 'Seaweed Green': '#437C17', 'Shamrock Green': '#347C17',
          'Green Onion': '#6AA121', 'Green Pepper': '#4AA02C', 'Dark Lime Green': '#41A317', 'Parrot Green': '#12AD2B', 'Clover Green': '#3EA055',
          'Dinosaur Green': '#73A16C', 'Green Snake': '#6CBB3C', 'Alien Green': '#6CC417', 'Green Apple': '#4CC417', 'LimeGreen': '#32CD32',
          'Pea Green': '#52D017', 'Kelly Green': '#4CC552', 'Zombie Green': '#54C571', 'Frog Green': '#99C68E', 'DarkSeaGreen': '#8FBC8F',
          'Green Peas': '#89C35C', 'Dollar Bill Green': '#85BB65', 'Iguana Green': '#9CB071', 'Acid Green': '#B0BF1A', 'Avocado Green': '#B2C248',
          'Pistachio Green': '#9DC209', 'Salad Green': '#A1C935', 'YellowGreen': '#9ACD32', 'Pastel Green': '#77DD77', 'Hummingbird Green': '#7FE817',
          'Nebula Green': '#59E817', 'Stoplight Go Green': '#57E964', 'Neon Green': '#16F529', 'Jade Green': '#5EFB6E', 'Lime Mint Green': '#36F57F',
          'SpringGreen': '#00FF7F', 'MediumSpringGreen': '#00FA9A', 'Emerald Green': '#5FFB17', 'Lime': '#00FF00', 'LawnGreen': '#7CFC00',
          'Bright Green': '#66FF00', 'Chartreuse': '#7FFF00', 'Yellow Lawn Green': '#87F717', 'Aloe Vera Green': '#98F516', 'Dull Green Yellow': '#B1FB17',
          'GreenYellow': '#ADFF2F', 'Chameleon Green': '#BDF516', 'Neon Yellow Green': '#DAEE01', 'Yellow Green Grosbeak': '#E2F516', 'Tea Green': '#CCFB5D',
          'Slime Green': '#BCE954', 'Algae Green': '#64E986', 'LightGreen': '#90EE90', 'Dragon Green': '#6AFB92', 'PaleGreen': '#98FB98',
          'Mint Green': '#98FF98', 'Green Thumb': '#B5EAAA', 'Organic Brown': '#E3F9A6', 'Light Jade': '#C3FDB8', 'Light Rose Green': '#DBF9DB',
          'HoneyDew': '#F0FFF0', 'MintCream': '#F5FFFA', 'LemonChiffon': '#FFFACD', 'Parchment': '#FFFFC2', 'Cream': '#FFFFCC', 'Cream White': '#FFFDD0',
          'LightGoldenRodYellow': '#FAFAD2', 'LightYellow': '#FFFFE0', 'Beige': '#F5F5DC', 'Cornsilk': '#FFF8DC', 'Blonde': '#FBF6D9', 'Champagne': '#F7E7CE',
          'AntiqueWhite': '#FAEBD7', 'PapayaWhip': '#FFEFD5', 'BlanchedAlmond': '#FFEBCD', 'Bisque': '#FFE4C4', 'Wheat': '#F5DEB3', 'Moccasin': '#FFE4B5',
          'Peach': '#FFE5B4', 'Light Orange': '#FED8B1', 'PeachPuff': '#FFDAB9', 'NavajoWhite': '#FFDEAD', 'Golden Blonde': '#FBE7A1', 'Golden Silk': '#F3E3C3',
          'Dark Blonde': '#F0E2B6', 'Light Gold': '#F1E5AC', 'Vanilla': '#F3E5AB', 'Tan Brown': '#ECE5B6', 'Dirty White': '#E8E4C9', 'PaleGoldenRod': '#EEE8AA',
          'Khaki': '#F0E68C', 'Cardboard Brown': '#EDDA74', 'Harvest Gold': '#EDE275', 'Sun Yellow': '#FFE87C', 'Corn Yellow': '#FFF380',
          'Pastel Yellow': '#FAF884', 'Neon Yellow': '#FFFF33', 'Yellow': '#FFFF00', 'Canary Yellow': '#FFEF00', 'Banana Yellow': '#F5E216',
          'Mustard Yellow': '#FFDB58', 'Golden Yellow': '#FFDF00', 'Bold Yellow': '#F9DB24', 'Rubber Ducky Yellow': '#FFD801', 'Gold': '#FFD700',
          'Bright Gold': '#FDD017', 'Chrome Gold': '#FFCE44', 'Golden Brown': '#EAC117', 'Deep Yellow': '#F6BE00', 'Macaroni and Cheese': '#F2BB66',
          'Saffron': '#FBB917', 'Beer': '#FBB117', 'Yellow Orange Orange Yellow': '#FFAE42', 'Cantaloupe': '#FFA62F', 'Orange': '#FFA500',
          'Brown Sand': '#EE9A4D', 'SandyBrown': '#F4A460', 'Brown Sugar': '#E2A76F', 'Camel Brown': '#C19A6B', 'Deer Brown': '#E6BF83', 'BurlyWood': '#DEB887',
          'Tan': '#D2B48C', 'Light French Beige': '#C8AD7F', 'Sand': '#C2B280', 'Sage': '#BCB88A', 'Fall Leaf Brown': '#C8B560', 'Ginger Brown': '#C9BE62',
          'DarkKhaki': '#BDB76B', 'Olive Green': '#BAB86C', 'Brass': '#B5A642', 'Cookie Brown': '#C7A317', 'Metallic Gold': '#D4AF37', 'Bee Yellow': '#E9AB17',
          'School Bus Yellow': '#E8A317', 'GoldenRod': '#DAA520', 'Orange Gold': '#D4A017', 'Caramel': '#C68E17', 'DarkGoldenRod': '#B8860B',
          'Cinnamon': '#C58917', 'Peru': '#CD853F', 'Bronze': '#CD7F32', 'Tiger Orange': '#C88141', 'Copper': '#B87333', 'Dark Gold': '#AA6C39',
          'Dark Almond': '#AB784E', 'Wood': '#966F33', 'Oak Brown': '#806517', 'Antique Bronze': '#665D1E', 'Hazel': '#8E7618', 'Dark Yellow': '#8B8000',
          'Dark Moccasin': '#827839', 'Khaki Green': '#8A865D', 'Bullet Shell': '#AF9B60', 'Army Brown': '#827B60', 'Sandstone': '#786D5F', 'Taupe': '#483C32',
          'Mocha': '#493D26', 'Milk Chocolate': '#513B1C', 'Gray Brown': '#3D3635', 'Dark Coffee': '#3B2F2F', 'Old Burgundy': '#43302E',
          'Western Charcoal': '#49413F', 'Bakers Brown': '#5C3317', 'Dark Brown': '#654321', 'Sepia Brown': '#704214', 'Dark Bronze': '#804A00',
          'Coffee': '#6F4E37', 'Brown Bear': '#835C3B', 'Red Dirt': '#7F5217', 'Sepia': '#7F462C', 'Sienna': '#A0522D', 'SaddleBrown': '#8B4513',
          'Dark Sienna': '#8A4117', 'Sangria': '#7E3817', 'Blood Red': '#7E3517', 'Chestnut': '#954535', 'Chestnut Red': '#C34A2C', 'Mahogany': '#C04000',
          'Red Fox': '#C35817', 'Dark Bisque': '#B86500', 'Light Brown': '#B5651D', 'Petra Gold': '#B76734', 'Rust': '#C36241', 'Copper Red': '#CB6D51',
          'Orange Salmon': '#C47451', 'Chocolate': '#D2691E', 'Sedona': '#CC6600', 'Papaya Orange': '#E56717', 'Halloween Orange': '#E66C2C',
          'Neon Orange': '#FF6700', 'Bright Orange': '#FF5F1F', 'Pumpkin Orange': '#F87217', 'Carrot Orange': '#F88017', 'DarkOrange': '#FF8C00',
          'Construction Cone Orange': '#F87431', 'Indian Saffron': '#FF7722', 'Sunrise Orange': '#E67451', 'Mango Orange': '#FF8040', 'Coral': '#FF7F50',
          'Basket Ball Orange': '#F88158', 'Light Salmon Rose': '#F9966B', 'LightSalmon': '#FFA07A', 'DarkSalmon': '#E9967A', 'Tangerine': '#E78A61',
          'Light Copper': '#DA8A67', 'Salmon': '#FA8072', 'LightCoral': '#F08080', 'Pastel Red': '#F67280', 'Pink Coral': '#E77471', 'Bean Red': '#F75D59',
          'Valentine Red': '#E55451', 'IndianRed': '#CD5C5C', 'Tomato': '#FF6347', 'Shocking Orange': '#E55B3C', 'OrangeRed': '#FF4500', 'Red': '#FF0000',
          'Neon Red': '#FD1C03', 'Scarlet': '#FF2400', 'Ruby Red': '#F62217', 'Ferrari Red': '#F70D1A', 'Fire Engine Red': '#F62817', 'Lava Red': '#E42217',
          'Love Red': '#E41B17', 'Grapefruit': '#DC381F', 'Cherry Red': '#C24641', 'Chilli Pepper': '#C11B17', 'FireBrick': '#B22222',
          'Tomato Sauce Red': '#B21807', 'Brown': '#A52A2A', 'Carbon Red': '#A70D2A', 'Cranberry': '#9F000F', 'Saffron Red': '#931314',
          'Crimson Red': '#990000', 'Red Wine Wine Red': '#990012', 'DarkRed': '#8B0000', 'Maroon': '#800000', 'Burgundy': '#8C001A', 'Vermilion': '#7E191B',
          'Deep Red': '#800517', 'Red Blood': '#660000', 'Blood Night': '#551606', 'Dark Scarlet': '#560319', 'Black Bean': '#3D0C02',
          'Chocolate Brown': '#3F000F', 'Midnight': '#2B1B17', 'Purple Lily': '#550A35', 'Purple Maroon': '#810541', 'Plum Pie': '#7D0541',
          'Plum Velvet': '#7D0552', 'Dark Raspberry': '#872657', 'Velvet Maroon': '#7E354D', 'Rosy-Finch': '#7F4E52', 'Dull Purple': '#7F525D',
          'Puce': '#7F5A58', 'Rose Dust': '#997070', 'Rosy Pink': '#B38481', 'RosyBrown': '#BC8F8F', 'Khaki Rose': '#C5908E', 'Lipstick Pink': '#C48793',
          'Pink Brown': '#C48189', 'Pink Daisy': '#E799A3', 'Dusty Pink': '#D58A94', 'Rose': '#E8ADAA', 'Silver Pink': '#C4AEAD', 'Rose Gold': '#ECC5C0',
          'Deep Peach': '#FFCBA4', 'Pastel Orange': '#F8B88B', 'Desert Sand': '#EDC9AF', 'Unbleached Silk': '#FFDDCA', 'Pig Pink': '#FDD7E4',
          'Blush': '#FFE6E8', 'MistyRose': '#FFE4E1', 'Pink Bubble Gum': '#FFDFDD', 'Light Red': '#FFCCCB', 'Light Rose': '#FBCFCD', 'Deep Rose': '#FBBBB9',
          'Pink': '#FFC0CB', 'LightPink': '#FFB6C1', 'Donut Pink': '#FAAFBE', 'Baby Pink': '#FAAFBA', 'Flamingo Pink': '#F9A7B0', 'Pastel Pink': '#FEA3AA',
          'Rose Pink Pink Rose': '#E7A1B0', 'Cadillac Pink': '#E38AAE', 'Carnation Pink': '#F778A1', 'Blush Red': '#E56E94', 'PaleVioletRed': '#DB7093',
          'Purple Pink': '#D16587', 'Tulip Pink': '#C25A7C', 'Bashful Pink': '#C25283', 'Dark Pink': '#E75480', 'Dark Hot Pink': '#F660AB',
          'HotPink': '#FF69B4', 'Watermelon Pink': '#FC6C85', 'Violet Red': '#F6358A', 'Hot Deep Pink': '#F52887', 'DeepPink': '#FF1493',
          'Neon Pink': '#F535AA', 'Neon Hot Pink': '#FD349C', 'Pink Cupcake': '#E45E9D', 'Dimorphotheca Magenta': '#E3319D', 'Pink Lemonade': '#E4287C',
          'Raspberry': '#E30B5D', 'Crimson': '#DC143C', 'Bright Maroon': '#C32148', 'Rose Red': '#C21E56', 'Rogue Pink': '#C12869', 'Burnt Pink': '#C12267',
          'Pink Violet': '#CA226B', 'MediumVioletRed': '#C71585', 'Dark Carnation Pink': '#C12283', 'Raspberry Purple': '#B3446C', 'Pink Plum': '#B93B8F',
          'Orchid': '#DA70D6', 'Deep Mauve': '#DF73D4', 'Violet': '#EE82EE', 'Bright Neon Pink': '#F433FF', 'Fuchsia Magenta': '#FF00FF',
          'Crimson Purple': '#E238EC', 'Heliotrope Purple': '#D462FF', 'Tyrian Purple': '#C45AEC', 'MediumOrchid': '#BA55D3', 'Purple Flower': '#A74AC7',
          'Orchid Purple': '#B048B5', 'Pastel Violet': '#D291BC', 'Mauve Taupe': '#915F6D', 'Viola Purple': '#7E587E', 'Eggplant': '#614051',
          'Plum Purple': '#583759', 'Grape': '#5E5A80', 'Purple Navy': '#4E5180', 'SlateBlue': '#6A5ACD', 'Blue Lotus': '#6960EC',
          'Light Slate Blue': '#736AFF', 'MediumSlateBlue': '#7B68EE', 'Periwinkle Purple': '#7575CF', 'Very Peri': '#6667AB', 'Bright Grape': '#6F2DA8',
          'Purple Amethyst': '#6C2DC7', 'Bright Purple': '#6A0DAD', 'Deep Periwinkle': '#5453A6', 'DarkSlateBlue': '#483D8B', 'Purple Haze': '#4E387E',
          'Purple Iris': '#571B7E', 'Dark Purple': '#4B0150', 'Deep Purple': '#36013F', 'Purple Monster': '#461B7E', 'Indigo': '#4B0082',
          'Blue Whale': '#342D7E', 'RebeccaPurple': '#663399', 'Purple Jam': '#6A287E', 'DarkMagenta': '#8B008B', 'Purple': '#800080',
          'French Lilac': '#86608E', 'DarkOrchid': '#9932CC', 'DarkViolet': '#9400D3', 'Purple Violet': '#8D38C9', 'Jasmine Purple': '#A23BEC',
          'Purple Daffodil': '#B041FF', 'Clematis Violet': '#842DCE', 'BlueViolet': '#8A2BE2', 'Purple Sage Bush': '#7A5DC7', 'Lovely Purple': '#7F38EC',
          'Neon Purple': '#9D00FF', 'Purple Plum': '#8E35EF', 'Aztech Purple': '#893BFF', 'Lavender Purple': '#967BB6', 'MediumPurple': '#9370DB',
          'Light Purple': '#8467D7', 'Crocus Purple': '#9172EC', 'Purple Mimosa': '#9E7BFF', 'Periwinkle': '#CCCCFF', 'Pale Lilac': '#DCD0FF',
          'Mauve': '#E0B0FF', 'Bright Lilac': '#D891EF', 'Rich Lilac': '#B666D2', 'Purple Dragon': '#C38EC7', 'Lilac': '#C8A2C8', 'Plum': '#DDA0DD',
          'Blush Pink': '#E6A9EC', 'Pastel Purple': '#F2A2E8', 'Blossom Pink': '#F9B7FF', 'Wisteria Purple': '#C6AEC7', 'Purple Thistle': '#D2B9D3',
          'Thistle': '#D8BFD8', 'Periwinkle Pink': '#E9CFEC', 'Cotton Candy': '#FCDFFF', 'Lavender Pinocchio': '#EBDDE2', 'Dark White': '#E1D9D1',
          'Ash White': '#E9E4D4', 'White Chocolate': '#EDE6D6', 'Soft Ivory': '#FAF0DD', 'Off White': '#F8F0E3', 'Pearl White': '#F8F6F0',
          'LavenderBlush': '#FFF0F5', 'Pearl': '#FDEEF4', 'Egg Shell': '#FFF9E3', 'OldLace': '#FDF5E6', 'Linen': '#FAF0E6', 'SeaShell': '#FFF5EE',
          'Rice': '#FAF5EF', 'FloralWhite': '#FFFAF0', 'Ivory': '#FFFFF0', 'Light White': '#FFFFF7', 'WhiteSmoke': '#F5F5F5', 'Cotton': '#FBFBF9',
          'Snow': '#FFFAFA', 'Milk White': '#FEFCFF', 'White': '#FFFFFF'}

# Build a secondary lookup: normalized multi-word keys
_COLOR_LOOKUP = {k.lower(): v for k, v in _RAW_COLOR_MAP.items()}


def _hex_for_color(color_value: str) -> str | None:
    """
    Given a product color value (e.g. "Dark Navy Blue", "RED", "Slate Gray"),
    return the best-matching hex code from the color map, or None if not found.
    """
    normalized = color_value.lower().strip()

    # 1. Exact match
    if normalized in _COLOR_LOOKUP:
        return _COLOR_LOOKUP[normalized]

    # 2. Try removing common adjective prefixes/suffixes word by word
    #    e.g. "flat black" → try "black"; "light steel blue" → "steel blue", "blue"
    words = normalized.split()
    for start in range(1, len(words)):
        candidate = " ".join(words[start:])
        if candidate in _COLOR_LOOKUP:
            return _COLOR_LOOKUP[candidate]
    for end in range(len(words) - 1, 0, -1):
        candidate = " ".join(words[:end])
        if candidate in _COLOR_LOOKUP:
            return _COLOR_LOOKUP[candidate]

    # 3. Partial key match — find any key that is a substring of the value
    for key, hex_val in _COLOR_LOOKUP.items():
        if key in normalized:
            return hex_val

    return None


# Color-related option name keywords (case-insensitive)
_COLOR_OPTION_NAMES = {"color", "colour", "colors", "colours", "finish color", "color/finish"}


def _is_color_option(option_name: str) -> bool:
    return option_name.lower().strip() in _COLOR_OPTION_NAMES or \
           any(kw in option_name.lower() for kw in ("color", "colour"))


def format_bc_options(options: list) -> str:
    """
    Convert a list of variant option dicts [{name, value}, ...] into a
    BigCommerce v3 import-ready Options string.

    Color/colour options use Swatch format with hex lookup.
    All others use Radio format.

    Multiple options are joined with ' || '.

    Example output:
        "Type=Radio|Name=Size|Value=M || Type=Swatch|Name=Color|Value=Red[#FF0000]"
    """
    if not options:
        return ""

    parts = []
    for opt in options:
        name = (opt.get("name") or "").strip()
        value = (opt.get("value") or "").strip()
        if not name or not value:
            continue

        if _is_color_option(name):
            hex_code = _hex_for_color(value)
            if hex_code:
                formatted = f"Type=Swatch|Name={name}|Value={value}[{hex_code}]"
            else:
                # No hex found — still use Swatch but without hex
                formatted = f"Type=Swatch|Name={name}|Value={value}"
        else:
            formatted = f"Type=Radio|Name={name}|Value={value}"

        parts.append(formatted)

    return " || ".join(parts)

COLUMNS = [
    "Status", "URL", "#Zoro","Supplier","Lead Time", "Item", "Name", "Type", "SKU", "Options",
    "Inventory Tracking", "Current Stock", "Low Stock", "Price",
    "Cost Price", "Retail Price", "Sale Price", "Brand ID", "Channels",
    "Categories Names","Categories", "Description", "Custom Fields", "Availability",
    "Page Title", "Product URL", "Meta Description", "Search Keywords",
    "Meta Keywords", "Bin Picking Number", "UPC/EAN", "Global Trade Number",
    "Manufacturer Part Number", "Free Shipping", "Fixed Shipping Cost",
    "Weight", "Width", "Height", "Depth", "Is Visible", "Is Featured",
    "Warranty", "Tax Class", "Product Condition", "Show Product Condition",
    "Sort Order", "Variant Image URL", "Internal Image URL (Export)",
    "Image URL (Import)", "Image Description", "Image is Thumbnail",
    "Image Sort Order", "YouTube ID", "Video Title", "Video Description",
    "Video Sort Order",
]

MAX_FILE_BYTES = 19 * 1024 * 1024  # 20 MB

# MAX_FILE_BYTES = 500 * 1024  # 500kb



class BigcommercePipeline:

    def _should_rotate(self):
        return os.path.getsize(self.output_path) >= MAX_FILE_BYTES

    def _make_output_path(self, index: int) -> str:
        suffix = f"-part{index}" if index > 1 else ""
        return os.path.join(self._output_dir, f"{self._base_name}{suffix}-import.csv")

    def _open_csv(self, index: int):
        self.output_path = self._make_output_path(index)
        self.csv_file = open(self.output_path, "w", newline="", encoding="utf-8-sig")
        self.writer = csv.DictWriter(self.csv_file, fieldnames=COLUMNS, extrasaction="ignore")
        self.writer.writeheader()
        self.csv_file.flush()

    def open_spider(self, spider):
        self.success = 0
        self.failed = 0
        self.total = 0

        if hasattr(spider, "input_file") and spider.input_file:
            try:
                df = pd.read_excel(spider.input_file)
                self.total = len(df)
            except:
                pass

        self.pbar = tqdm(total=self.total, desc="Scraping", unit="product",
                         bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]")

        input_file = getattr(spider, "input_file", None)
        self._base_name = os.path.splitext(os.path.basename(input_file))[0] if input_file else "output"
        self._output_dir = "transformed-done-new"
        os.makedirs(self._output_dir, exist_ok=True)

        self._file_index = 1
        self._open_csv(self._file_index)

    def write_row(self, row):
        full_row = {col: row.get(col, "") for col in COLUMNS}
        self.writer.writerow(full_row)
        self.csv_file.flush()
        # # Rotate if 20 MB reached
        # if os.path.getsize(self.output_path) >= MAX_FILE_BYTES:
        #     self.csv_file.close()
        #     self._file_index += 1
        #     self._open_csv(self._file_index)

    def get_resources_block(self, item):
        MAX_LEN = 32000
        BUFFER = 2000
        MAX_ALLOWED = MAX_LEN - BUFFER  # 30000

        desc_len = len(item.get("description", ""))
        wrapper_overhead = len("<div></div>")  # 11 chars

        budget = MAX_ALLOWED - desc_len - wrapper_overhead
        if budget <= 0:
            return ""

        resources = item.get("resources", [])
        rows = []
        used = 0

        for idx, res in enumerate(resources, start=1):
            href = res.get("href", "")
            title = res.get("title", "Technical Guide")
            type_ = res.get("type", "application/pdf")

            row = f'''<li class="description-product-spec-link-pdf__row" index="{idx}">
            <div class="description-product-spec-link-pdf__first-column">
                <div class="description-product-spec-link-pdf__icon-contain">
                <a class="description-product-spec-link-pdf__icon-link" href="{href}" rel="noopener noreferrer" target="_blank">
                    <span class="description-product-spec-link-pdf__icon" data-type="{type_}"></span>
                    <span class="sr-only">opens in a new tab</span>
                </a>
                </div>
                <div class="description-product-spec-link-pdf__title-contain">
                <h2 class="description-product-spec-link-pdf__title">
                    <a href="{href}" rel="noopener noreferrer" target="_blank">{title}<span class="sr-only">opens in a new tab</span></a>
                </h2>
                </div>
            </div>
            <div class="description-product-spec-link-pdf__second-column">
                <div class="description-product-spec-link-pdf__contain">
                <a class="description-product-spec-link-pdf" href="{href}" target="_blank" title="Click here to download {title}">
                    View<span class="sr-only">opens in a new tab</span>
                </a>
                </div>
            </div>
            </li>'''.strip()

            if used + len(row) > budget:
                break

            rows.append(row)
            used += len(row)

        if not rows:
            return ""

        return (
            '<div class="cmp-container description-resource" id="description-resource">'
            "<h6>Resources</h6>"
            '<ul class="description-product-spec-link-pdf__row-contain">'
            + "".join(rows)
            + "</ul></div>"
        )

    def process_item(self, item, spider):
         # 🔥 Rotate BEFORE writing a new product
        if self._should_rotate():
            self.csv_file.close()
            self._file_index += 1
            self._open_csv(self._file_index)
        images = item.get("images", [])
        variants = item.get("variants", [])
        custom_fields = item.get("custom_fields", [])

        for idx, res in enumerate(item.get("resources", []), start=1):
            href = res.get("href", "")
            title = res.get("title", "Technical Guide")
            value = f"<a href='{href}' target='_blank'>Download Here</a>"
            if len(value) <= 255:
                custom_fields.append({"name": f"{title}", "value": value})

        has_variants = len(variants) > 1
        custom_fields_str = json.dumps(custom_fields) if custom_fields else ""

        is_failed = not item.get("title")
        status = "FAIL" if is_failed else "SUCCESS"
        if is_failed:
            self.failed += 1
        else:
            self.success += 1

        self.pbar.update(1)
        self.pbar.set_postfix_str(f"✓{self.success} ✗{self.failed}")

        self.write_row({
            "Status": status,
            "URL": item.get("url", ""),
            "Item": "Product",
            "Name": item.get("title"),
            "Type": "physical",
            "SKU": "" if has_variants else item.get("sku"),
            "Inventory Tracking": "none",
            "Current Stock": 0,
            "Low Stock": 0,
            "Price": "" if has_variants else item.get("price"),
            "Cost Price": 0,
            "Retail Price": 0,
            "Sale Price": 0,
            "Brand ID": 50,  # Hoffman brand ID in BigCommerce
            "Channels": 1,
            "Categories Names": item.get("category"),
            "Categories": "",
            "Description": f"</div>{item.get('description', "")} {self.get_resources_block(item)}</div>",
            "Custom Fields": custom_fields_str,
            "Availability": item.get("availability"),
            "Page Title": f'{item.get("page_title", "")} | Genesee Supply Co',
            "Product URL": item.get("product_url", ""),
            "Meta Description": item.get("meta_description", ""),
            "Search Keywords": item.get("search_keywords", ""),
            "Meta Keywords": item.get("meta_keywords", ""),
            "UPC/EAN": f'="{item.get("upc", "")}"',
            "Manufacturer Part Number": item.get("sku"),
            "Free Shipping": False,
            "Fixed Shipping Cost": 0,
            "Weight": 0, "Width": 0, "Height": 0, "Depth": 0,
            "Is Visible": True,
            "Is Featured": False,
            "Warranty": "",
            "Tax Class": 0,
            "Product Condition": "New",
            "Show Product Condition": False,
            "Sort Order": 0,
            "Supplier": item.get("supplier"),
            "Lead Time": item.get("lead_time"),
            "#Zoro": item.get("zoro"),
        })

        for idx, img in enumerate(images, start=1):
            self.write_row({
                "Status": "",
                "Item": "Image",
                "Image URL (Import)": img,
                "Image Description": f"{item.get('page_title')}-{idx}",
                "Image is Thumbnail": idx == 1,
                "Image Sort Order": idx,
            })

        for variant in variants:
            self.write_row({
                "Status": "",
                "Item": "SKU",
                "SKU": variant.get("sku"),
                "Price": variant.get("price"),
                # BigCommerce v3 option string — e.g. "Type=Radio|Name=Size|Value=M"
                # Color/colour options automatically use Swatch format with hex lookup
                "Options": format_bc_options(variant.get("options", [])),
                # Per-variant image URL so BigCommerce links the image to this SKU
                "Variant Image URL": variant.get("image_url", ""),
            })

        return item

    def close_spider(self, spider):
        self.pbar.close()
        self.csv_file.close()
        print(f"\n✓ {self.success} success | ✗ {self.failed} failed | saved to {self._output_dir}/")




class ResourceDownloadPipeline:

    def open_spider(self, spider):
        input_file = getattr(spider, "input_file", None)
        base_name = os.path.splitext(os.path.basename(input_file))[0] if input_file else "output"

        self.output_dir = "transformed-done-new"
        os.makedirs(self.output_dir, exist_ok=True)

        # file for JDownloader
        self.links_file_path = os.path.join(self.output_dir, f"{base_name}-download-links.txt")
        self.links_file = open(self.links_file_path, "w", encoding="utf-8")

        # avoid duplicates
        self.seen_links = set()

    def close_spider(self, spider):
        self.links_file.close()

    def process_item(self, item, spider):
        resources = item.get("resources", [])
        if not resources:
            return item

        updated_resources = []

        for res in resources:
            href = res.get("href", "")
            if not href:
                updated_resources.append(res)  # ✅ keep as-is, don't drop it
                continue

            filename = os.path.basename(urlparse(href).path)

            if not filename:
                updated_resources.append(res)  # ✅ keep as-is if URL has no filename
                continue

            if href not in self.seen_links:
                self.links_file.write(href + "\n")
                self.seen_links.add(href)

            cdn_url = f"/content/tripp-lite/{filename}"
            updated_resources.append({**res, "href": cdn_url, "filename": filename})

        item["resources"] = updated_resources  # now never loses resources
        return item

# class ResourceDownloadPipeline:

#     def open_spider(self, spider):
#         input_file = getattr(spider, "input_file", None)
#         base_name = os.path.splitext(os.path.basename(input_file))[0] if input_file else "output"
#         self.download_dir = os.path.join("transformed-done-new", f"{base_name}-downloads")
#         os.makedirs(self.download_dir, exist_ok=True)

#     def process_item(self, item, spider):
#         resources = item.get("resources", [])
#         if not resources:
#             return item

#         updated_resources = []

#         # reuse session (important)
#         session = requests.Session()
#         session.headers.update({
#             "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
#             "Referer": "https://www.zoro.com/",
#             "Accept": "application/pdf,*/*",
#             "Connection": "keep-alive",
#         })

#         for res in resources:
#             href = res.get("href", "")
#             if not href:
#                 continue

#             filename = os.path.basename(urlparse(href).path)
#             local_path = os.path.join(self.download_dir, filename)

#             try:
#                 r = session.get(href, timeout=30)

#                 if r.status_code == 200:
#                     with open(local_path, "wb") as f:
#                         f.write(r.content)

#                     cdn_url = f"/content/tripp-lite/{filename}"
#                 else:
#                     raise Exception(f"Status {r.status_code}")

#             except Exception as e:
#                 spider.logger.error(f"Failed to download {href}: {e}")
#                 cdn_url = href

#             updated_resources.append({
#                 **res,
#                 "href": cdn_url,
#                 "filename": filename
#             })

#         item["resources"] = updated_resources
#         return item

# # import pandas as pd
# # import os
# # import json
# # from tqdm import tqdm

# # class BigcommercePipeline:

# #     def open_spider(self, spider):
# #         self.rows = []
# #         self.total = 0
# #         self.success = 0
# #         self.failed = 0

# #         # Count total URLs for progress bar
# #         if hasattr(spider, "input_file") and spider.input_file:
# #             try:
# #                 df = pd.read_excel(spider.input_file)
# #                 self.total = len(df)
# #             except:
# #                 self.total = 0

# #         self.pbar = tqdm(total=self.total, desc="Scraping products", unit="product",
# #                          bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] ✓{postfix}")

# #     def process_item(self, item, spider):
# #         images = item.get("images", [])
# #         variants = item.get("variants", [])
# #         custom_fields = item.get("custom_fields", [])

# #         has_variants = len(variants) > 1
# #         custom_fields_str = json.dumps(custom_fields) if custom_fields else ""

# #         # Determine status
# #         is_failed = not item.get("title")
# #         status = "FAIL" if is_failed else "SUCCESS"
# #         if is_failed:
# #             self.failed += 1
# #         else:
# #             self.success += 1

# #         self.pbar.update(1)
# #         self.pbar.set_postfix_str(f"✓{self.success} ✗{self.failed}")

# #         # -----------------------
# #         # 1. MAIN PRODUCT ROW
# #         # -----------------------
# #         self.rows.append({
# #             "Status": status,
# #             "URL": item.get("url"),
# #             "Item": "Product",
# #             "Name": item.get("title"),
# #             "Type": "physical",
# #             "SKU": "" if has_variants else item.get("sku"),
# #             "Inventory Tracking": "none",
# #             "Current Stock": 0,
# #             "Low Stock": 0,
# #             "Price": "" if has_variants else item.get("price"),
# #             "Cost Price": 0,
# #             "Retail Price": 0,
# #             "Sale Price": 0,
# #             "Brand": item.get("brand"),
# #             "Channels": 0,
# #             "Categories": item.get("category"),
# #             "Description": item.get("description"),
# #             "Custom Fields": custom_fields_str,
# #             "Availability": item.get("availability"),
# #             "Page Title": "",
# #             "Product URL": "",
# #             "Meta Description": "",
# #             "Search Keywords": "",
# #             "Meta Keywords": "",
# #             "Bin Picking Number": "",
# #             "UPC/EAN": item.get("upc"),
# #             "Global Trade Number": "",
# #             "Manufacturer Part Number": item.get("sku"),
# #             "Free Shipping": False,
# #             "Fixed Shipping Cost": 0,
# #             "Weight": 0,
# #             "Width": 0,
# #             "Height": 0,
# #             "Depth": 0,
# #             "Is Visible": True,
# #             "Is Featured": False,
# #             "Warranty": "",
# #             "Tax Class": 0,
# #             "Product Condition": "New",
# #             "Show Product Condition": False,
# #             "Sort Order": 0,
# #         })

# #         # -----------------------
# #         # 2. IMAGE ROWS
# #         # -----------------------
# #         for idx, img in enumerate(images, start=1):
# #             self.rows.append({
# #                 "Status": "",
# #                 "Item": "Image",
# #                 "Name": "",
# #                 "Image URL (Import)": img,
# #                 "Image Description": "",
# #                 "Image is Thumbnail": idx == 1,
# #                 "Image Sort Order": idx,
# #             })

# #         # -----------------------
# #         # 3. VARIANT ROWS
# #         # -----------------------
# #         for variant in variants:
# #             self.rows.append({
# #                 "Status": "",
# #                 "Item": "SKU",
# #                 "Name": "",
# #                 "SKU": variant.get("sku"),
# #                 "Price": variant.get("price"),
# #                 "Option1 Value": variant.get("option1"),
# #             })

# #         return item

# #     def handle_error(self, failure, spider):
# #         """Call this from spider errback to track failed URLs"""
# #         self.failed += 1
# #         self.pbar.update(1)
# #         self.pbar.set_postfix_str(f"✓{self.success} ✗{self.failed}")
# #         self.rows.append({
# #             "Status": "FAIL",
# #             "URL": failure.request.url,
# #             "Item": "",
# #             "Name": "",
# #         })

# #     def close_spider(self, spider):
# #         self.pbar.close()
# #         print(f"\n✓ Success: {self.success} | ✗ Failed: {self.failed} | Total: {self.total}")

# #         df = pd.DataFrame(self.rows)

# #         columns = [
# #             "Status",
# #             "URL",
# #             "Item",
# #             "Name",
# #             "Type",
# #             "SKU",
# #             "Options",
# #             "Inventory Tracking",
# #             "Current Stock",
# #             "Low Stock",
# #             "Price",
# #             "Cost Price",
# #             "Retail Price",
# #             "Sale Price",
# #             "Brand",
# #             "Channels",
# #             "Categories",
# #             "Description",
# #             "Custom Fields",
# #             "Availability",
# #             "Page Title",
# #             "Product URL",
# #             "Meta Description",
# #             "Search Keywords",
# #             "Meta Keywords",
# #             "Bin Picking Number",
# #             "UPC/EAN",
# #             "Global Trade Number",
# #             "Manufacturer Part Number",
# #             "Free Shipping",
# #             "Fixed Shipping Cost",
# #             "Weight",
# #             "Width",
# #             "Height",
# #             "Depth",
# #             "Is Visible",
# #             "Is Featured",
# #             "Warranty",
# #             "Tax Class",
# #             "Product Condition",
# #             "Show Product Condition",
# #             "Sort Order",
# #             "Variant Image URL",
# #             "Internal Image URL (Export)",
# #             "Image URL (Import)",
# #             "Image Description",
# #             "Image is Thumbnail",
# #             "Image Sort Order",
# #             "YouTube ID",
# #             "Video Title",
# #             "Video Description",
# #             "Video Sort Order",
# #         ]

# #         for col in columns:
# #             if col not in df.columns:
# #                 df[col] = ""

# #         df = df[columns]

# #         input_file = getattr(spider, "input_file", None)
# #         base_name = os.path.splitext(os.path.basename(input_file))[0] if input_file else "output"

# #         output_dir = "transformed_done"
# #         os.makedirs(output_dir, exist_ok=True)
# #         output_path = os.path.join(output_dir, f"{base_name}-import.csv")

# #         df.to_csv(output_path, index=False, encoding="utf-8-sig")
# #         print(f"Saved to: {output_path}")


# import pandas as pd
# import os
# import json
# import csv
# from tqdm import tqdm
# import logging
# from urllib.parse import urlparse

# logging.getLogger("scrapy").setLevel(logging.ERROR)
# logging.getLogger("scrapy-playwright").setLevel(logging.ERROR)
# logging.getLogger("playwright").setLevel(logging.ERROR)

# COLUMNS = [
#     "Status", "URL", "Item", "Name", "Type", "SKU", "Options",
#     "Inventory Tracking", "Current Stock", "Low Stock", "Price",
#     "Cost Price", "Retail Price", "Sale Price", "Brand", "Channels",
#     "Categories", "Description", "Custom Fields", "Availability",
#     "Page Title", "Product URL", "Meta Description", "Search Keywords",
#     "Meta Keywords", "Bin Picking Number", "UPC/EAN", "Global Trade Number",
#     "Manufacturer Part Number", "Free Shipping", "Fixed Shipping Cost",
#     "Weight", "Width", "Height", "Depth", "Is Visible", "Is Featured",
#     "Warranty", "Tax Class", "Product Condition", "Show Product Condition",
#     "Sort Order", "Variant Image URL", "Internal Image URL (Export)",
#     "Image URL (Import)", "Image Description", "Image is Thumbnail",
#     "Image Sort Order", "YouTube ID", "Video Title", "Video Description",
#     "Video Sort Order",
# ]

# MAX_FILE_BYTES = 20 * 1024 * 1024   # 20 MB


# class BigcommercePipeline:

#     # ADD _make_output_path and _rotate helpers inside BigcommercePipeline class
#     # (place right after open_spider):

#     def _make_output_path(self, index: int) -> str:
#         suffix = f"-part{index}" if index > 1 else ""
#         return os.path.join(self._output_dir, f"{self._base_name}{suffix}-import.csv")

#     def _open_csv(self, index: int):
#         self.output_path = self._make_output_path(index)
#         # self.csv_file = open(self.output_path, "w", newline="", encoding="utf-8-sig")
#         # self.writer = csv.DictWriter(self.csv_file, fieldnames=COLUMNS, extrasaction="ignore")
#         # self.writer.writeheader()
#         # self.csv_file.flush()
#         self._file_index = 1
#         self._output_dir = output_dir
#         self._base_name = base_name
#         self._open_csv(self._file_index)

#     def open_spider(self, spider):
#         self.success = 0
#         self.failed = 0

#         # Count total for progress bar
#         self.total = 0
#         if hasattr(spider, "input_file") and spider.input_file:
#             try:
#                 df = pd.read_excel(spider.input_file)
#                 self.total = len(df)
#             except:
#                 pass

#         self.pbar = tqdm(total=self.total, desc="Scraping", unit="product",
#                          bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]")

#         # Open CSV immediately and write header
#         input_file = getattr(spider, "input_file", None)
#         base_name = os.path.splitext(os.path.basename(input_file))[0] if input_file else "output"
#         output_dir = "transformed-done"
#         os.makedirs(output_dir, exist_ok=True)
#         self.output_path = os.path.join(output_dir, f"{base_name}-import.csv")

#         self.csv_file = open(self.output_path, "w", newline="", encoding="utf-8-sig")
#         self.writer = csv.DictWriter(self.csv_file, fieldnames=COLUMNS, extrasaction="ignore")
#         self.writer.writeheader()
#         self.csv_file.flush()

#     def write_row(self, row):
#         """Write a single row and flush immediately to disk."""
#         # Fill missing columns with empty string
#         full_row = {col: row.get(col, "") for col in COLUMNS}
#         self.writer.writerow(full_row)
#         self.csv_file.flush()  # ← key: writes to disk immediately, not buffer

#     def get_resources_block(self, item):
#         resources = item.get("resources", [])
#         rows = []
#         for idx, res in enumerate(resources, start=1):
#             href = res.get("href", "")
#             title = res.get("title", "Resource")
#             row = f'''<li class="description-product-spec-link-pdf__row" index="{idx}">
#                     <div class="description-product-spec-link-pdf__first-column">
#                         <div class="description-product-spec-link-pdf__icon-contain">
#                         <a class="description-product-spec-link-pdf__icon-link" href="{href}" rel="noopener noreferrer" target="_blank">
#                             <span class="description-product-spec-link-pdf__icon" data-type="application/pdf"></span>
#                             <span class="sr-only">opens in a new tab</span>
#                         </a>
#                         </div>
#                         <div class="description-product-spec-link-pdf__title-contain">
#                         <h2 class="description-product-spec-link-pdf__title">
#                             <a href="{href}" rel="noopener noreferrer" target="_blank">{title}<span class="sr-only">opens in a new tab</span></a>
#                         </h2>
#                         </div>
#                     </div>
#                     <div class="description-product-spec-link-pdf__second-column">
#                         <div class="description-product-spec-link-pdf__contain">
#                         <a class="description-product-spec-link-pdf" href="{href}" target="_blank" title="Click here to download {title}">
#                             View<span class="sr-only">opens in a new tab</span>
#                         </a>
#                         </div>
#                     </div>
#                     </li>'''.strip()
#             rows.append(row)

#         resource_html = ""
#         if rows:
#             resource_html = (
#                 '<div class="cmp-container description-resource" id="description-resource">'
#                 "<h6>Resources</h6>"
#                 '<ul class="description-product-spec-link-pdf__row-contain">'
#                 + "".join(rows)
#                 + "</ul></div>"
#             )

#         return resource_html

#     def process_item(self, item, spider):
#         images = item.get("images", [])
#         variants = item.get("variants", [])
#         custom_fields = item.get("custom_fields", [])
#         for idx, res in enumerate(item.get("resources", []), start=1):
#             href = res.get("href", "")
#             title = res.get("title", "Resource")
#             value = f'<a href="{href}" target="_blank">{title}</a>'
#             if len(value) <= 255:
#                 custom_fields.append({"name": f"Resource {idx}", "value": value})

#         has_variants = len(variants) > 1
#         custom_fields_str = json.dumps(custom_fields) if custom_fields else ""

#         is_failed = not item.get("title")
#         status = "FAIL" if is_failed else "SUCCESS"
#         if is_failed:
#             self.failed += 1
#         else:
#             self.success += 1

#         self.pbar.update(1)
#         self.pbar.set_postfix_str(f"✓{self.success} ✗{self.failed}")

#         # Product row
#         self.write_row({
#             "Status": status,
#             "URL": item.get("url", ""),
#             "Item": "Product",
#             "Name": item.get("title"),
#             "Type": "physical",
#             "SKU": "" if has_variants else item.get("sku"),
#             "Inventory Tracking": "none",
#             "Current Stock": 0,
#             "Low Stock": 0,
#             "Price": "" if has_variants else item.get("price"),
#             "Cost Price": 0,
#             "Retail Price": 0,
#             "Sale Price": 0,
#             "Brand": item.get("brand"),
#             "Channels": 1,
#             "Categories": item.get("category"),
#             "Description": f'{item.get("description")}{self.get_resources_block(item)}',
#             "Custom Fields": custom_fields_str,
#             "Availability": item.get("availability"),
#             "UPC/EAN": item.get("upc"),
#             "Manufacturer Part Number": item.get("sku"),
#             "Free Shipping": False,
#             "Fixed Shipping Cost": 0,
#             "Weight": 0, "Width": 0, "Height": 0, "Depth": 0,
#             "Is Visible": True,
#             "Is Featured": False,
#             "Warranty": "",
#             "Tax Class": 0,
#             "Product Condition": "New",
#             "Show Product Condition": False,
#             "Sort Order": 0,
#         })

#         # Image rows
#         for idx, img in enumerate(images, start=1):
#             self.write_row({
#                 "Status": "",
#                 "Item": "Image",
#                 "Image URL (Import)": img,
#                 "Image is Thumbnail": idx == 1,
#                 "Image Sort Order": idx,
#             })

#         # Variant rows
#         for variant in variants:
#             self.write_row({
#                 "Status": "",
#                 "Item": "SKU",
#                 "SKU": variant.get("sku"),
#                 "Price": variant.get("price"),
#             })

#         return item

#     def close_spider(self, spider):
#         self.pbar.close()
#         self.csv_file.close()
#         print(f"\n✓ {self.success} success | ✗ {self.failed} failed | saved to {self.output_path}")



# class ResourceDownloadPipeline:

#     def open_spider(self, spider):
#         input_file = getattr(spider, "input_file", None)
#         base_name = os.path.splitext(os.path.basename(input_file))[0] if input_file else "output"
#         self.download_dir = os.path.join("transformed-done", f"{base_name}-downloads")
#         os.makedirs(self.download_dir, exist_ok=True)

#     def process_item(self, item, spider):
#         resources = item.get("resources", [])  # list of {"href": ..., "title": ...}
#         if not resources:
#             return item

#         updated_resources = []
#         for res in resources:
#             href = res.get("href", "")
#             if not href:
#                 continue

#             filename = os.path.basename(urlparse(href).path)
#             local_path = os.path.join(self.download_dir, filename)

#             # Download synchronously (Scrapy pipeline process_item is sync)
#             try:
#                 import urllib.request
#                 urllib.request.urlretrieve(href, local_path)
#                 cdn_url = f"/content/hoffman/{filename}"
#             except Exception as e:
#                 spider.logger.error(f"Failed to download {href}: {e}")
#                 cdn_url = href  # fallback to original

#             updated_resources.append({**res, "href": cdn_url, "filename": filename})

#         item["resources"] = updated_resources
#         return item