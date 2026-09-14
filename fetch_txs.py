"""Fetch tx.from / tx.to for a list of tx hashes via publicnode batched eth_getTransactionByHash (100 per batch). Resumable."""
import json, sys, os, time, requests
import pandas as pd
H = {'content-type':'application/json','user-agent':'grail-analytics/0.1'}
RPC = 'https://base-rpc.publicnode.com'
hashes = set()
for f in ['data/parquet/swaps.parquet','data/parquet/transfers.parquet','data/parquet/usdc_vault.parquet','data/parquet/inventory.parquet','data/parquet/pack_buys.parquet']:
    if os.path.exists(f):
        df = pd.read_parquet(f)
        if 'tx' in df.columns: hashes |= set(df['tx'])
out = 'data/raw/txs.jsonl'
done = set()
if os.path.exists(out):
    for line in open(out): done.add(json.loads(line)['hash'])
todo = sorted(hashes - done); print('total', len(hashes), 'todo', len(todo), flush=True)
f = open(out, 'a'); S = requests.Session(); t0 = time.time()
for i in range(0, len(todo), 100):
    chunk = todo[i:i+100]
    batch = [{"jsonrpc":"2.0","id":k,"method":"eth_getTransactionByHash","params":[h]} for k, h in enumerate(chunk)]
    for attempt in range(10):
        try:
            j = S.post(RPC, data=json.dumps(batch), headers=H, timeout=120).json()
            if isinstance(j, dict) or any(not x.get('result') for x in j): raise RuntimeError(str(j)[:200])
            break
        except Exception as e:
            print('retry', attempt, str(e)[:150], flush=True); time.sleep(3 + 3*attempt)
            if attempt == 9: raise
    for x in j:
        r = x['result']
        f.write(json.dumps({'hash': r['hash'], 'from': r['from'], 'to': r['to'], 'sel': r['input'][:10], 'nonce': int(r['nonce'],16), 'gas_price': int(r.get('gasPrice','0x0'),16), 'type': r.get('type'), 'value': int(r['value'],16)}) + '\n')
    f.flush()
    if (i // 100) % 50 == 0: print(time.strftime('%H:%M:%S'), i, '/', len(todo), 'elapsed', round(time.time()-t0), flush=True)
print('DONE', flush=True)
