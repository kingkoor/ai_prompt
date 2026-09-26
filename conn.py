# changed at Sep 25 - full state names -> codes (GoCanvas sends "Texas", Nexus sends "TX")
STATE_NAME_TO_CODE = {
    # United States
    "ALABAMA": "AL", "ALASKA": "AK", "ARIZONA": "AZ", "ARKANSAS": "AR", "CALIFORNIA": "CA",
    "COLORADO": "CO", "CONNECTICUT": "CT", "DELAWARE": "DE", "DISTRICT OF COLUMBIA": "DC",
    "FLORIDA": "FL", "GEORGIA": "GA", "HAWAII": "HI", "IDAHO": "ID", "ILLINOIS": "IL",
    "INDIANA": "IN", "IOWA": "IA", "KANSAS": "KS", "KENTUCKY": "KY", "LOUISIANA": "LA",
    "MAINE": "ME", "MARYLAND": "MD", "MASSACHUSETTS": "MA", "MICHIGAN": "MI", "MINNESOTA": "MN",
    "MISSISSIPPI": "MS", "MISSOURI": "MO", "MONTANA": "MT", "NEBRASKA": "NE", "NEVADA": "NV",
    "NEW HAMPSHIRE": "NH", "NEW JERSEY": "NJ", "NEW MEXICO": "NM", "NEW YORK": "NY",
    "NORTH CAROLINA": "NC", "NORTH DAKOTA": "ND", "OHIO": "OH", "OKLAHOMA": "OK", "OREGON": "OR",
    "PENNSYLVANIA": "PA", "RHODE ISLAND": "RI", "SOUTH CAROLINA": "SC", "SOUTH DAKOTA": "SD",
    "TENNESSEE": "TN", "TEXAS": "TX", "UTAH": "UT", "VERMONT": "VT", "VIRGINIA": "VA",
    "WASHINGTON": "WA", "WEST VIRGINIA": "WV", "WISCONSIN": "WI", "WYOMING": "WY",
    # Canada
    "ALBERTA": "AB", "BRITISH COLUMBIA": "BC", "MANITOBA": "MB", "NEW BRUNSWICK": "NB",
    "NEWFOUNDLAND AND LABRADOR": "NL", "NOVA SCOTIA": "NS", "ONTARIO": "ON",
    "PRINCE EDWARD ISLAND": "PE", "QUEBEC": "QC", "SASKATCHEWAN": "SK",
    "NORTHWEST TERRITORIES": "NT", "NUNAVUT": "NU", "YUKON": "YT",
    # Australia
    "NEW SOUTH WALES": "NSW", "VICTORIA": "VIC", "QUEENSLAND": "QLD", "WESTERN AUSTRALIA": "WA",
    "SOUTH AUSTRALIA": "SA", "TASMANIA": "TAS", "AUSTRALIAN CAPITAL TERRITORY": "ACT",
    "NORTHERN TERRITORY": "NT",
    # common variants seen in GoCanvas data
    "WASHINGTON DC": "DC", "PUERTO RICO": "PR", "QUÉBEC": "QC", "NEWFOUNDLAND": "NL",
}
def normalize_state_province(col, country_col):
    """Normalises a state/province column expression based on country.

    The country is first converted to an ISO code (normalize_country), so
    "United States" works the same as "US". For countries that use meaningful
    state/province codes (US, CA, AU, BR, MX) the value is uppercased and full
    US/CA/AU names are mapped to their standard code ("TEXAS" -> "TX"); values
    that are already codes pass through unchanged. For all other countries an
    empty string is returned, as sub-national divisions are not consistently coded.

    Args:
        col         (Column): Spark column expression containing the raw state/province.
        country_col (Column): Spark column expression containing the country (code or name).

    Returns:
        Column: State/province code for supported countries, '' otherwise.
    """

    iso_country = normalize_country(country_col)
    s = f.regexp_replace(f.upper(col), r"[.,]", "")
    s = f.trim(f.regexp_replace(s, r"\s+", " "))
    mapping = f.create_map([f.lit(x) for pair in STATE_NAME_TO_CODE.items() for x in pair])
    s = f.coalesce(mapping[s], s)

    return f.when(
        iso_country.isin("US", "CA", "AU", "BR", "MX"),
        s,
    ).otherwise(f.lit(""))


# added at Sep 8
def normalize_city(col):
    """Normalises a city column expression.

    Steps applied (in order):
       1. Lowercases the value.
       2. Folds common accented characters to plain ASCII.
       3. Removes street-level noise words that leak into city fields.
       4. Strips any remaining non-letter characters.
       5. Collapses repeated whitespace and trims.
       6. Returns '' for results shorter than three characters, which are
          almost always junk rather than a real place name.

    Args:
        col (Column): Spark column expression containing the raw city.

    Returns:
        Column: Normalised city column expression (UPPERCASE), '' if junk.
    """

    c = f.lower(col)

    # Multi-character folds first - translate() cannot expand one char to two.
    c = f.regexp_replace(c, "æ", "ae")
    c = f.regexp_replace(c, "ø", "o")
    c = f.regexp_replace(c, "ß", "ss")
    c = f.translate(
        c,
        "àáâãäåçèéêëìíîïñòóôõöùúûüý",
        "aaaaaaceeeeiiiinooooouuuuy",
    )
