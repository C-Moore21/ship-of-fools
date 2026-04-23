# city_coords.py — geocoding lookup table for Archive.org coverage strings.
# Used by app.py to resolve show locations for the Crow's Nest map.
# Keys are lowercase city names and "city, state" variants.
# Add entries here when new coverage strings appear in map build warnings.

CITY_COORDS = {
    # California
    "san francisco":      (37.77, -122.42), "san francisco, ca":   (37.77, -122.42),
    "berkeley":           (37.87, -122.27), "berkeley, ca":        (37.87, -122.27),
    "los angeles":        (34.05, -118.24), "los angeles, ca":     (34.05, -118.24),
    "santa barbara":      (34.42, -119.70), "santa barbara, ca":   (34.42, -119.70),
    "san jose":           (37.34, -121.89), "san jose, ca":        (37.34, -121.89),
    "san diego":          (32.72, -117.15), "san diego, ca":       (32.72, -117.15),
    "sacramento":         (38.58, -121.49), "sacramento, ca":      (38.58, -121.49),
    "ventura":            (34.28, -119.29), "palo alto":           (37.44, -122.14),
    "santa clara":        (37.35, -121.95), "oakland":             (37.80, -122.27),
    "oakland, ca":        (37.80, -122.27), "stockton":            (37.96, -121.29),
    "fresno":             (36.74, -119.77), "anaheim":             (33.84, -117.91),
    "irvine":             (33.68, -117.79), "long beach":          (33.77, -118.19),
    "pasadena":           (34.15, -118.14), "san rafael":          (37.97, -122.53),
    "sebastopol":         (38.40, -122.82), "santa rosa":          (38.44, -122.71),
    "santa cruz":         (36.97, -122.03), "pacific grove":       (36.62, -121.92),
    "monterey":           (36.60, -121.89), "concord":             (37.97, -122.03),
    "concord, ca":        (37.97, -122.03), "vallejo":             (38.10, -122.26),
    "chico":              (39.73, -121.84), "davis":               (38.54, -121.74),
    "san bernardino":     (34.11, -117.29), "riverside":           (33.98, -117.37),
    "modesto":            (37.64, -120.99), "lodi":                (38.13, -121.27),
    "healdsburg":         (38.61, -122.87), "novato":              (38.11, -122.57),
    "cotati":             (38.33, -122.71), "van nuys":            (34.19, -118.45),
    "van nuys, ca":       (34.19, -118.45), "inglewood":           (33.96, -118.35),
    "inglewood, ca":      (33.96, -118.35), "daly city":           (37.69, -122.47),
    "daly city, ca":      (37.69, -122.47), "mill valley":         (37.91, -122.54),
    "mill valley, ca":    (37.91, -122.54), "bakersfield":         (35.37, -119.02),
    "bakersfield, ca":    (35.37, -119.02), "loma mar":            (37.27, -122.28),
    "san mateo":          (37.56, -122.32), "san leandro":         (37.72, -122.16),
    "walnut creek":       (37.91, -122.06), "fairfax":             (37.99, -122.59),
    "san anselmo":        (37.98, -122.56), "pleasanton":          (37.66, -121.87),
    "antioch":            (38.00, -121.81), "redding":             (40.59, -122.39),
    "eureka":             (40.80, -124.16),
    # Pacific Northwest
    "portland":           (45.52, -122.68), "portland, or":        (45.52, -122.68),
    "seattle":            (47.61, -122.33), "seattle, wa":         (47.61, -122.33),
    "eugene":             (44.05, -123.09), "eugene, or":          (44.05, -123.09),
    "vancouver, bc":      (49.28, -123.12), "tacoma":              (47.25, -122.44),
    "tacoma, wa":         (47.25, -122.44), "olympia":             (47.04, -122.90),
    "corvallis":          (44.56, -123.26), "medford":             (42.33, -122.87),
    # Utah
    "salt lake city":     (40.76, -111.89), "salt lake city, ut":  (40.76, -111.89),
    "ogden":              (41.22, -111.97), "ogden, ut":           (41.22, -111.97),
    "provo":              (40.23, -111.66), "provo, ut":           (40.23, -111.66),
    # Nevada / Arizona
    "las vegas":          (36.17, -115.14), "las vegas, nv":       (36.17, -115.14),
    "phoenix":            (33.45, -112.07), "phoenix, az":         (33.45, -112.07),
    "tucson":             (32.22, -110.97), "tucson, az":          (32.22, -110.97),
    "reno":               (39.53, -119.81), "reno, nv":            (39.53, -119.81),
    "tempe":              (33.42, -111.94), "flagstaff":           (35.20, -111.65),
    # Colorado / Rocky Mountain
    "denver":             (39.74, -104.98), "denver, co":          (39.74, -104.98),
    "boulder":            (40.01, -105.27), "boulder, co":         (40.01, -105.27),
    "red rocks":          (39.66, -105.20), "morrison, co":        (39.65, -105.19),
    "fort collins":       (40.59, -105.07), "colorado springs":    (38.83, -104.82),
    "pueblo":             (38.25, -104.61), "greeley":             (40.42, -104.70),
    # Idaho / Montana / Wyoming
    "boise":              (43.62, -116.20), "boise, id":           (43.62, -116.20),
    "missoula":           (46.87, -114.01), "missoula, mt":        (46.87, -114.01),
    "billings":           (45.78, -108.50), "great falls":         (47.50, -111.30),
    "jackson":            (43.48, -110.76), "jackson hole":        (43.58, -110.82),
    # Texas / Southwest
    "dallas":             (32.78,  -96.80), "dallas, tx":          (32.78,  -96.80),
    "houston":            (29.76,  -95.37), "houston, tx":         (29.76,  -95.37),
    "austin":             (30.27,  -97.74), "austin, tx":          (30.27,  -97.74),
    "san antonio":        (29.42,  -98.49), "san antonio, tx":     (29.42,  -98.49),
    "albuquerque":        (35.08, -106.65), "albuquerque, nm":     (35.08, -106.65),
    "santa fe":           (35.69, -105.94), "santa fe, nm":        (35.69, -105.94),
    "el paso":            (31.76, -106.49), "lubbock":             (33.58, -101.86),
    "new orleans":        (29.95,  -90.07), "new orleans, la":     (29.95,  -90.07),
    "fort worth":         (32.75,  -97.33), "abilene":             (32.45,  -99.73),
    "oklahoma city":      (35.47,  -97.52), "oklahoma city, ok":   (35.47,  -97.52),
    "tulsa":              (36.15,  -95.99), "tulsa, ok":           (36.15,  -95.99),
    "norman":             (35.22,  -97.44),
    # Hawaii
    "honolulu":           (21.31, -157.86), "honolulu, hi":        (21.31, -157.86),
    # Midwest
    "chicago":            (41.88,  -87.63), "chicago, il":         (41.88,  -87.63),
    "milwaukee":          (43.04,  -87.91), "milwaukee, wi":       (43.04,  -87.91),
    "detroit":            (42.33,  -83.05), "detroit, mi":         (42.33,  -83.05),
    "minneapolis":        (44.98,  -93.27), "minneapolis, mn":     (44.98,  -93.27),
    "saint paul":         (44.94,  -93.09), "st. paul":            (44.94,  -93.09),
    "st. paul, mn":       (44.94,  -93.09), "columbus":            (39.96,  -82.99),
    "columbus, oh":       (39.96,  -82.99), "cleveland":           (41.50,  -81.69),
    "cleveland, oh":      (41.50,  -81.69), "cincinnati":          (39.10,  -84.51),
    "cincinnati, oh":     (39.10,  -84.51), "indianapolis":        (39.77,  -86.16),
    "indianapolis, in":   (39.77,  -86.16), "st. louis":           (38.63,  -90.20),
    "st. louis, mo":      (38.63,  -90.20), "kansas city":         (39.10,  -94.58),
    "kansas city, mo":    (39.10,  -94.58), "omaha":               (41.26,  -95.94),
    "omaha, ne":          (41.26,  -95.94), "iowa city":           (41.66,  -91.53),
    "iowa city, ia":      (41.66,  -91.53), "madison":             (43.07,  -89.40),
    "madison, wi":        (43.07,  -89.40), "ann arbor":           (42.28,  -83.74),
    "ann arbor, mi":      (42.28,  -83.74), "pontiac":             (42.64,  -83.29),
    "pontiac, mi":        (42.64,  -83.29), "dayton":              (39.76,  -84.19),
    "dayton, oh":         (39.76,  -84.19), "akron":               (41.08,  -81.52),
    "des moines":         (41.59,  -93.62), "des moines, ia":      (41.59,  -93.62),
    "lincoln":            (40.81,  -96.70), "lincoln, ne":         (40.81,  -96.70),
    "wichita":            (37.69,  -97.34), "wichita, ks":         (37.69,  -97.34),
    "columbia, mo":       (38.95,  -92.33), "springfield, mo":     (37.21,  -93.29),
    "grand rapids":       (42.96,  -85.67), "lansing":             (42.73,  -84.55),
    "kalamazoo":          (42.29,  -85.59), "champaign":           (40.12,  -88.24),
    "normal":             (40.51,  -88.99), "bloomington":         (40.48,  -88.99),
    "east lansing":       (42.73,  -84.48), "dekalb":              (41.93,  -88.75),
    "dekalb, il":         (41.93,  -88.75), "de kalb":             (41.93,  -88.75),
    "de kalb, il":        (41.93,  -88.75), "carbondale":          (37.73,  -89.22),
    "carbondale, il":     (37.73,  -89.22), "athens, oh":          (39.33,  -82.10),
    "oxford, oh":         (39.51,  -84.74), "bowling green":       (41.38,  -83.65),
    "bowling green, oh":  (41.38,  -83.65), "bowling green, ky":   (36.99,  -86.44),
    "cedar falls":        (42.53,  -92.45), "cedar falls, ia":     (42.53,  -92.45),
    "ames":               (42.03,  -93.62), "ames, ia":            (42.03,  -93.62),
    "muncie":             (40.19,  -85.39), "terre haute":         (39.47,  -87.41),
    # South / Southeast
    "atlanta":            (33.75,  -84.39), "atlanta, ga":         (33.75,  -84.39),
    "charlotte":          (35.23,  -80.84), "charlotte, nc":       (35.23,  -80.84),
    "nashville":          (36.17,  -86.78), "nashville, tn":       (36.17,  -86.78),
    "memphis":            (35.15,  -90.05), "memphis, tn":         (35.15,  -90.05),
    "birmingham":         (33.52,  -86.81), "birmingham, al":      (33.52,  -86.81),
    "jacksonville":       (30.33,  -81.66), "jacksonville, fl":    (30.33,  -81.66),
    "miami":              (25.77,  -80.19), "miami, fl":           (25.77,  -80.19),
    "orlando":            (28.54,  -81.38), "orlando, fl":         (28.54,  -81.38),
    "tampa":              (27.95,  -82.46), "tampa, fl":           (27.95,  -82.46),
    "tallahassee":        (30.44,  -84.28), "tallahassee, fl":     (30.44,  -84.28),
    "columbia":           (34.00,  -81.03), "columbia, sc":        (34.00,  -81.03),
    "raleigh":            (35.78,  -78.64), "raleigh, nc":         (35.78,  -78.64),
    "durham":             (35.99,  -78.90), "chapel hill":         (35.91,  -79.06),
    "greensboro":         (36.07,  -79.79), "greensboro, nc":      (36.07,  -79.79),
    "knoxville":          (35.96,  -83.92), "knoxville, tn":       (35.96,  -83.92),
    "chattanooga":        (35.05,  -85.31), "murfreesboro":        (35.85,  -86.39),
    "gainesville":        (29.65,  -82.32), "gainesville, fl":     (29.65,  -82.32),
    "gainesville, ga":    (34.30,  -83.82), "fort lauderdale":     (26.12,  -80.14),
    "daytona beach":      (29.21,  -81.02), "west palm beach":     (26.71,  -80.05),
    "clearwater":         (27.97,  -82.80), "sarasota":            (27.34,  -82.53),
    "pensacola":          (30.42,  -87.22), "mobile":              (30.69,  -88.04),
    "montgomery":         (32.37,  -86.30), "huntsville":          (34.73,  -86.59),
    "baton rouge":        (30.45,  -91.15), "shreveport":          (32.53,  -93.75),
    "jackson":            (32.30,  -90.18), "jackson, ms":         (32.30,  -90.18),
    "little rock":        (34.75,  -92.29), "little rock, ar":     (34.75,  -92.29),
    "fayetteville":       (36.07,  -94.16), "fayetteville, ar":    (36.07,  -94.16),
    "lexington":          (38.04,  -84.50), "lexington, ky":       (38.04,  -84.50),
    "louisville":         (38.25,  -85.76), "louisville, ky":      (38.25,  -85.76),
    "tuscaloosa":         (33.21,  -87.57), "tuscaloosa, al":      (33.21,  -87.57),
    "lakeland":           (28.04,  -81.95), "lakeland, fl":        (28.04,  -81.95),
    "pembroke pines":     (26.00,  -86.20), "pembroke pines, fl":  (26.00,  -86.20),
    "fort myers":         (26.64,  -81.87),
    # Virginia / Mid-South
    "richmond":           (37.54,  -77.43), "richmond, va":        (37.54,  -77.43),
    "charlottesville":    (38.03,  -78.48), "charlottesville, va": (38.03,  -78.48),
    "virginia beach":     (36.85,  -75.98), "virginia beach, va":  (36.85,  -75.98),
    "hampton":            (37.03,  -76.35), "hampton, va":         (37.03,  -76.35),
    "roanoke":            (37.27,  -79.94), "norfolk":             (36.85,  -76.29),
    "norfolk, va":        (36.85,  -76.29), "williamsburg":        (37.27,  -76.71),
    "williamsburg, va":   (37.27,  -76.71), "blacksburg":          (37.23,  -80.41),
    "blacksburg, va":     (37.23,  -80.41), "fredericksburg":      (38.30,  -77.46),
    # West Virginia
    "huntington":         (38.42,  -82.44), "huntington, wv":      (38.42,  -82.44),
    "morgantown":         (39.63,  -79.96), "charleston, wv":      (38.35,  -81.64),
    # Mid-Atlantic
    "new york":           (40.71,  -74.01), "new york, ny":        (40.71,  -74.01),
    "new york city":      (40.71,  -74.01), "nyc":                 (40.71,  -74.01),
    "brooklyn":           (40.65,  -73.95), "philadelphia":        (39.95,  -75.17),
    "philadelphia, pa":   (39.95,  -75.17), "baltimore":           (39.29,  -76.61),
    "baltimore, md":      (39.29,  -76.61), "washington":          (38.91,  -77.04),
    "washington, dc":     (38.91,  -77.04), "d.c.":                (38.91,  -77.04),
    "pittsburgh":         (40.44,  -79.99), "pittsburgh, pa":      (40.44,  -79.99),
    "buffalo":            (42.89,  -78.87), "buffalo, ny":         (42.89,  -78.87),
    "albany":             (42.65,  -73.75), "albany, ny":          (42.65,  -73.75),
    "rochester":          (43.16,  -77.61), "rochester, ny":       (43.16,  -77.61),
    "hartford":           (41.76,  -72.68), "hartford, ct":        (41.76,  -72.68),
    "new haven":          (41.31,  -72.92), "new haven, ct":       (41.31,  -72.92),
    "bridgeport":         (41.18,  -73.19), "waterbury":           (41.56,  -73.04),
    "providence":         (41.82,  -71.41), "providence, ri":      (41.82,  -71.41),
    "boston":             (42.36,  -71.06), "boston, ma":          (42.36,  -71.06),
    "cambridge":          (42.37,  -71.11), "cambridge, ma":       (42.37,  -71.11),
    "worcester":          (42.26,  -71.80), "springfield":         (42.10,  -72.59),
    "lowell":             (42.64,  -71.31), "portland, me":        (43.66,  -70.25),
    "burlington":         (44.48,  -73.21), "burlington, vt":      (44.48,  -73.21),
    "manchester, nh":     (42.99,  -71.46), "concord, nh":         (43.21,  -71.54),
    "saratoga springs":   (43.08,  -73.78), "saratoga springs, ny":(43.08,  -73.78),
    "long island":        (40.79,  -73.13), "stony brook":         (40.93,  -73.13),
    "stony brook, ny":    (40.93,  -73.13), "nassau":              (40.73,  -73.59),
    "uniondale":          (40.70,  -73.59), "east rutherford":     (40.82,  -74.07),
    "meadowlands":        (40.82,  -74.07), "hershey":             (40.29,  -76.65),
    "hershey, pa":        (40.29,  -76.65), "state college":       (40.79,  -77.86),
    "landover":           (38.93,  -76.89), "landover, md":        (38.93,  -76.89),
    "columbia, md":       (39.20,  -76.86), "glen echo":           (38.97,  -77.14),
    "sterling":           (39.00,  -77.43), "port chester":        (41.00,  -73.67),
    # New Jersey
    "passaic":            (40.86,  -74.13), "passaic, nj":         (40.86,  -74.13),
    "englishtown":        (40.30,  -74.36), "englishtown, nj":     (40.30,  -74.36),
    "jersey city":        (40.73,  -74.08), "jersey city, nj":     (40.73,  -74.08),
    "asbury park":        (40.22,  -74.01), "asbury park, nj":     (40.22,  -74.01),
    "princeton":          (40.36,  -74.66), "princeton, nj":       (40.36,  -74.66),
    "atlantic city":      (39.36,  -74.42), "atlantic city, nj":   (39.36,  -74.42),
    # Pennsylvania
    "upper darby":        (39.96,  -75.27), "upper darby, pa":     (39.96,  -75.27),
    "allentown":          (40.60,  -75.49), "allentown, pa":       (40.60,  -75.49),
    "wilkes-barre":       (41.25,  -75.88), "harrisburg":          (40.27,  -76.88),
    # New York state
    "ithaca":             (42.44,  -76.50), "ithaca, ny":          (42.44,  -76.50),
    "binghamton":         (42.10,  -75.91), "binghamton, ny":      (42.10,  -75.91),
    "syracuse":           (43.05,  -76.15), "syracuse, ny":        (43.05,  -76.15),
    "utica":              (43.10,  -75.23), "poughkeepsie":        (41.70,  -73.93),
    # New England
    "hanover":            (43.70,  -72.29), "hanover, nh":         (43.70,  -72.29),
    "orono":              (44.90,  -68.67), "orono, me":           (44.90,  -68.67),
    "amherst":            (42.37,  -72.52), "amherst, ma":         (42.37,  -72.52),
    "storrs":             (41.81,  -72.25), "storrs, ct":          (41.81,  -72.25),
    "northampton":        (42.32,  -72.63), "northampton, ma":     (42.32,  -72.63),
    # Europe
    "london":             (51.51,   -0.13), "london, england":     (51.51,   -0.13),
    "amsterdam":          (52.37,    4.90), "paris":               (48.85,    2.35),
    "berlin":             (52.52,   13.40), "hamburg":             (53.55,    9.99),
    "frankfurt":          (50.11,    8.68), "munich":              (48.14,   11.58),
    "rotterdam":          (51.92,    4.48), "zurich":              (47.38,    8.54),
    "copenhagen":         (55.68,   12.57), "stockholm":           (59.33,   18.07),
    "oslo":               (59.91,   10.75), "dublin":              (53.33,   -6.25),
    "vienna":             (48.21,   16.37), "brussels":            (50.85,    4.35),
    "birmingham, england":(52.49,   -1.90), "leeds":               (53.80,   -1.55),
    "newcastle":          (54.97,   -1.61), "edinburgh":           (55.95,   -3.19),
    "glasgow":            (55.86,   -4.25), "bristol":             (51.45,   -2.59),
    "manchester":         (53.48,   -2.24), "liverpool":           (53.41,   -2.99),
    "sheffield":          (53.38,   -1.47), "nottingham":          (52.95,   -1.15),
    "oxford":             (51.75,   -1.26), "cambridge, england":  (52.21,    0.12),
    "barcelona":          (41.39,    2.15), "madrid":              (40.42,   -3.70),
    "rome":               (41.90,   12.49), "milan":               (45.46,    9.19),
    "lyon":               (45.75,    4.83), "gothenburg":          (57.71,   11.97),
    "dusseldorf":         (51.23,    6.79), "cologne":             (50.94,    6.96),
    "düsseldorf":         (51.23,    6.79), "köln":                (50.94,    6.96),
    "wembley":            (51.56,   -0.98), "bickershaw":          (53.52,   -2.55),
    # Egypt (GD played the Pyramids, Sept 1978)
    "cairo":              (30.04,   31.24), "cairo, egypt":        (30.04,   31.24),
    "giza":               (29.98,   31.13), "giza, egypt":         (29.98,   31.13),
    "egypt":              (26.82,   30.80),
    # Canada
    "toronto":            (43.65,  -79.38), "toronto, on":         (43.65,  -79.38),
    "montreal":           (45.50,  -73.57), "montreal, qc":        (45.50,  -73.57),
    "ottawa":             (45.42,  -75.70), "winnipeg":            (49.90,  -97.14),
    "calgary":            (51.05, -114.07), "edmonton":            (53.55, -113.49),
    "vancouver":          (49.28, -123.12), "victoria, bc":        (48.43, -123.37),
    "hamilton":           (43.26,  -79.87), "london, on":          (42.98,  -81.23),
}

# Coverage strings from Archive.org that are meaningless for geocoding
JUNK_COVERAGES = {
    'unknown', 'various', 'multiple', 'n/a', 'tba', 'tbd', 'none',
    'unknown, unknown', 'various locations', 'various - see info file',
    'see info file',
}


def is_junk_coverage(cov_key):
    """Return True for coverage strings that cannot be geocoded."""
    if not cov_key:
        return True
    c = cov_key.lower().strip()
    if c in JUNK_COVERAGES:
        return True
    if c.startswith(('various', 'unknown', 'multiple', '???')):
        return True
    if all(ch in '?*.,- \t0123456789' for ch in c):
        return True
    return False


def coords_for_coverage(coverage):
    """Best-effort (lat, lng) from an Archive.org coverage string.

    Handles bare city names, 'City, ST' forms, and venue-prefixed strings
    like 'Fillmore West, San Francisco, CA'.
    Returns None if no match found.
    """
    if not coverage:
        return None
    c = coverage.lower().strip().rstrip('.')
    if c in CITY_COORDS:
        return CITY_COORDS[c]
    parts = [p.strip() for p in c.split(',')]
    for part in parts:
        if part in CITY_COORDS:
            return CITY_COORDS[part]
    for i in range(len(parts) - 1):
        combo = parts[i] + ', ' + parts[i + 1]
        if combo in CITY_COORDS:
            return CITY_COORDS[combo]
    return None
