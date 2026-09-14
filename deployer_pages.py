import json, requests, time
H={'user-agent':'grail-analytics/0.1'}
base='https://base.blockscout.com/api/v2/addresses/0xCb5A9f6C4709C3bb8e37F729BE10c6c2aa66aEFE/transactions'
params={'filter':'from'}; out=open('config/deployer_txs.jsonl','w'); n=0
while True:
    for a in range(6):
        r=requests.get(base,params=params,headers=H,timeout=60)
        try: d=r.json()
        except Exception: d={}
        if d.get('items') is not None: break
        time.sleep(5+5*a)
    items=d.get('items') or []
    for t in items:
        out.write(json.dumps({'ts':t['timestamp'],'hash':t['hash'],'to':(t.get('to') or {}).get('hash'),'to_name':(t.get('to') or {}).get('name'),'created':(t.get('created_contract') or {}).get('hash'),'method':t.get('method'),'block':t.get('block_number')})+'\n'); n+=1
    out.flush(); print('page items',len(items),'total',n,flush=True)
    nxt=d.get('next_page_params')
    if not nxt or not items: break
    params={'filter':'from', **nxt}; time.sleep(4)
print('DONE',n)
