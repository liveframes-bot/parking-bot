import json

with open('credentials.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

result = json.dumps(data, ensure_ascii=False)

with open('creds_env.txt', 'w', encoding='utf-8') as f:
    f.write(result)

print(f"OK Gotovo! Length: {len(result)} symbols")
print(f"Saved in: creds_env.txt")
print()
print("FIRST 300:")
print(result[:300])
print("...")
print()
print("LAST 200:")
print(result[-200:])