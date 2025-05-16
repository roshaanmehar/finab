"""
city_abbreviations.py - City abbreviation resolution
-------------------------------------------------
Functions for resolving city abbreviations to full names and vice versa.
"""

# Dictionary mapping city abbreviations to full names
CITY_ABBREVIATIONS = {
    'AB': 'Aberdeen', 'AL': 'St Albans', 'B': 'Birmingham', 'BA': 'Bath', 'BB': 'Blackburn', 
    'BD': 'Bradford', 'BF': 'British Forces', 'BH': 'Bournemouth', 'BL': 'Bolton', 
    'BN': 'Brighton', 'BR': 'Bromley', 'BS': 'Bristol', 'BT': 'Northern Ireland', 
    'CA': 'Carlisle', 'CB': 'Cambridge', 'CF': 'Cardiff', 'CH': 'Chester', 
    'CM': 'Chelmsford', 'CO': 'Colchester', 'CR': 'Croydon', 'CT': 'Canterbury', 
    'CV': 'Coventry', 'CW': 'Crewe', 'DA': 'Dartford', 'DD': 'Dundee', 
    'DE': 'Derby', 'DG': 'Dumfries and Galloway', 'DH': 'Durham', 'DL': 'Darlington', 
    'DN': 'Doncaster', 'DT': 'Dorchester', 'DY': 'Dudley', 'E': 'East London', 
    'EC': 'Central London', 'EH': 'Edinburgh', 'EN': 'Enfield', 'EX': 'Exeter', 
    'FK': 'Falkirk and Stirling', 'FY': 'Blackpool', 'G': 'Glasgow', 'GL': 'Gloucester', 
    'GU': 'Guildford', 'HA': 'Harrow', 'HD': 'Huddersfield', 'HG': 'Harrogate', 
    'HP': 'Hemel Hempstead', 'HR': 'Hereford', 'HS': 'Outer Hebrides', 'HU': 'Hull', 
    'HX': 'Halifax', 'IG': 'Ilford', 'IP': 'Ipswich', 'IV': 'Inverness', 
    'KA': 'Kilmarnock', 'KT': 'Kingston upon Thames', 'KW': 'Kirkwall', 'KY': 'Kirkcaldy', 
    'L': 'Liverpool', 'LA': 'Lancaster', 'LD': 'Llandrindod Wells', 'LE': 'Leicester', 
    'LL': 'Llandudno', 'LN': 'Lincoln', 'LS': 'Leeds', 'LU': 'Luton', 
    'M': 'Manchester', 'ME': 'Rochester', 'MK': 'Milton Keynes', 'ML': 'Motherwell', 
    'N': 'North London', 'NE': 'Newcastle upon Tyne', 'NG': 'Nottingham', 'NN': 'Northampton', 
    'NP': 'Newport', 'NR': 'Norwich', 'NW': 'North West London', 'OL': 'Oldham', 
    'OX': 'Oxford', 'PA': 'Paisley', 'PE': 'Peterborough', 'PH': 'Perth', 
    'PL': 'Plymouth', 'PO': 'Portsmouth', 'PR': 'Preston', 'RG': 'Reading', 
    'RH': 'Redhill', 'RM': 'Romford', 'S': 'Sheffield', 'SA': 'Swansea', 
    'SE': 'South East London', 'SG': 'Stevenage', 'SK': 'Stockport', 'SL': 'Slough', 
    'SM': 'Sutton', 'SN': 'Swindon', 'SO': 'Southampton', 'SP': 'Salisbury', 
    'SR': 'Sunderland', 'SS': 'Southend-on-Sea', 'ST': 'Stoke-on-Trent', 
    'SW': 'South West London', 'SY': 'Shrewsbury', 'TA': 'Taunton', 'TD': 'Galashiels', 
    'TF': 'Telford', 'TN': 'Tonbridge', 'TQ': 'Torquay', 'TR': 'Truro', 
    'TS': 'Cleveland', 'TW': 'Twickenham', 'UB': 'Southall', 'W': 'West London', 
    'WA': 'Warrington', 'WC': 'Central London', 'WD': 'Watford', 'WF': 'Wakefield', 
    'WN': 'Wigan', 'WR': 'Worcester', 'WS': 'Walsall', 'WV': 'Wolverhampton', 
    'YO': 'York', 'ZE': 'Lerwick'
}

# Create a reverse mapping (city name to abbreviation)
CITY_TO_ABBREVIATION = {v.lower(): k for k, v in CITY_ABBREVIATIONS.items()}


def get_city_name(city_abbreviation: str) -> str:
    """
    Get the full city name from an abbreviation.
    
    Args:
        city_abbreviation: City abbreviation
        
    Returns:
        Full city name or the original input if not found
    """
    return CITY_ABBREVIATIONS.get(city_abbreviation.upper(), city_abbreviation)


def get_city_abbreviation(city_name: str) -> str:
    """
    Get the abbreviation for a city name.
    
    Args:
        city_name: Full city name
        
    Returns:
        City abbreviation or None if not found
    """
    return CITY_TO_ABBREVIATION.get(city_name.lower())
