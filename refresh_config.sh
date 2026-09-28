#!/bin/bash
# Refresh token / pack / reserve registries from the public grail.xyz API.
cd "$(dirname "$0")/config"
for p in 1 2 3; do curl -s "https://grail.xyz/api/tokens?page=$p" > tokens_p$p.json; done
curl -s "https://grail.xyz/api/packs" > packs.json
python3 - <<'PY'
import json, urllib.request
rows=[]; page=1
while True:
    d=json.load(urllib.request.urlopen(urllib.request.Request(f"https://grail.xyz/api/reserves?page={page}", headers={'User-Agent': 'grail-analytics/0.1'})))
    rows+=d['results']
    if not d.get('next'): break
    page+=1
json.dump(rows, open('reserves.json','w'))
print('reserves', len(rows))
PY
