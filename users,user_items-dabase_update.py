import psycopg2
import json, csv



# Load JSON safely
with open('data/australian_users_items_clean.json', encoding='utf-8') as f:
    users = json.load(f)

# Write users.csv
with open('users.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['user_id', 'steam_id', 'items_count', 'user_url'])
    for u in users:
        writer.writerow([u.get('user_id'), u.get('steam_id'), u.get('items_count'), u.get('user_url')])

# Write user_items.csv safely
with open('user_items.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['user_id', 'item_id', 'item_name', 'playtime_forever', 'playtime_2weeks'])
    
    for u in users:
        items = u.get('items')
        if isinstance(items, list):
            print(f"[Warning] User {u.get('user_id')} has invalid items: {items}")
            for item in items:
                # Ensure it's a dictionary
                if isinstance(item, dict):
                    writer.writerow([
                        u.get('user_id'),
                        item.get('item_id', ''),
                        item.get('item_name', ''),
                        item.get('playtime_forever', 0),
                        item.get('playtime_2weeks', 0)
                    ])
