import json
import sys
from pathlib import Path

RAW_FILE   = Path('data1/australian_users_items.json')
CLEAN_FILE = Path('data/australian_users_items_clean.json')

USER_KEY_ORDER = [
    'user_id',
    'steam_id',
    'items_count',
    'user_url',
    'items'
]

ITEM_KEY_ORDER = [
    'item_id',
    'item_name',
    'playtime_forever',
    'playtime_2weeks'
]

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

def reorder_item(item: dict) -> dict:
    """Return a new item dict with keys in ITEM_KEY_ORDER."""
    ordered = {}
    for k in ITEM_KEY_ORDER:
        v = item.get(k)
        ordered[k] = fix_type(v)
    # append any extra keys that the template missed
    for k, v in item.items():
        if k not in ITEM_KEY_ORDER:
            ordered[k] = fix_type(v)
    return ordered

def reorder_user(obj: dict) -> dict:
    """Return a new user dict with keys in USER_KEY_ORDER."""
    ordered = {}
    for k in USER_KEY_ORDER:
        v = obj.get(k)
        if k == 'items' and isinstance(v, list):
            v = [reorder_item(item) for item in v]
        ordered[k] = fix_type(v)
    # append any extra keys that the template missed
    for k, v in obj.items():
        if k not in USER_KEY_ORDER:
            ordered[k] = fix_type(v)
    return ordered

def main() -> None:
    if not RAW_FILE.exists():
        sys.exit(f'{RAW_FILE} not found.')

    users = []
    with RAW_FILE.open(encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = eval(line)         
                obj = reorder_user(obj)
                users.append(obj)
            except Exception as e:
                print(f'Skipped bad line: {e}')

    with CLEAN_FILE.open('w', encoding='utf-8') as f:
        json.dump(users, f, indent=2, ensure_ascii=False, sort_keys=False)

    print(f'Wrote {len(users)} user objects to {CLEAN_FILE}')

if __name__ == '__main__':
    main()