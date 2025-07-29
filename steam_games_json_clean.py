import json
import sys
from pathlib import Path


RAW_FILE   = Path('data1/steam_games.json')
CLEAN_FILE = Path('data/steam_games_clean.json')

# The exact order we want every object to have
KEY_ORDER = [
    'id',
    'app_name',
    'title',
    'url',
    'release_date',
    'developer',
    'publisher',
    'genres',
    'tags',
    'price',
    'discount_price',
    'early_access',
    'metascore',
    'sentiment',
    'specs',
    'reviews_url'
]
# ----------------------------

def fix_type(val):
    """Convert the most common 'lazy' values to canonical JSON types."""
    if val is None:
        return None
    if isinstance(val, (int, float, bool)):
        return val
    if isinstance(val, str):
        val = val.strip()
        # boolean strings
        if val.lower() in ('true', 'false'):
            return val.lower() == 'true'
        # numeric strings
        try:
            if '.' in val:
                return float(val)
            return int(val)
        except ValueError:
            pass
        # "Free to Play", "Free", etc.
        if val.lower() in ('free to play', 'free'):
            return val  # keep as string, but normalized
        return val
    if isinstance(val, list):
        return [fix_type(v) for v in val]
    return val

def reorder(obj: dict) -> dict:
    """Return a new dict with keys in the desired order."""
    ordered = {}
    for k in KEY_ORDER:
        v = obj.get(k)
        ordered[k] = fix_type(v)
    # append any extra keys that the template missed
    for k, v in obj.items():
        if k not in KEY_ORDER:
            ordered[k] = fix_type(v)
    return ordered

def main() -> None:
    if not RAW_FILE.exists():
        sys.exit(f'{RAW_FILE} not found.')

    games = []
    with RAW_FILE.open(encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = eval(line)        #
                obj = reorder(obj)
                games.append(obj)
            except Exception as e:
                print(f'Skipped bad line: {e}')

    with CLEAN_FILE.open('w', encoding='utf-8') as f:
        json.dump(games, f, indent=2, ensure_ascii=False, sort_keys=False)

    print(f'Wrote {len(games)} objects to {CLEAN_FILE}')

if __name__ == '__main__':
    main()