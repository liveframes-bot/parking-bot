import json

with open('creds_env.txt', 'r', encoding='utf-8') as f:
    s = f.read()

try:
    data = json.loads(s)
    print("OK JSON VALID!")
    print(f"Email: {data.get('client_email')}")
    print(f"Project: {data.get('project_id')}")
    pk = data.get('private_key', '')
    print(f"Private key: {pk[:30]}... ({len(pk)} symbols)")
except json.JSONDecodeError as e:
    print(f"ERROR: {e}")
    print("String corrupted when copying")