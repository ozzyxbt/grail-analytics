"""Sequential batched eth_getLogs over mainnet.base.org with adaptive range splitting.
Usage: fetch_rpc.py <job> <head>. job=main: filters A (pools+tokens+vault) and B (USDC->vault). job=exec: executor+router logs."""
import json, os, sys, time, requests
RPC = "https://mainnet.base.org"
H = {'content-type': 'application/json', 'user-agent': 'grail-analytics/0.1'}
USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
VAULT = "0x36b162de23e4e809d78fb0eae4a2272bc313d738"
T_TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
JOB = sys.argv[1]; HEAD = int(sys.argv[2])
toks = []
for p in ['config/tokens_p1.json','config/tokens_p2.json','config/tokens_p3.json']: toks += [t for t in json.load(open(p))['results'] if t.get('chain_id', 8453) == 8453]
START = min(t['block_number'] for t in toks) - 100
if JOB == 'main':
    ADDRS = sorted(set([t['pool_address'].lower() for t in toks] + [t['token_address'].lower() for t in toks] + [VAULT]))
    FILTERS = [dict(address=ADDRS), dict(address=USDC, topics=[T_TRANSFER, None, '0x'+VAULT[2:].rjust(64,'0')])]
    OUTS = ['data/raw/logs_main.jsonl', 'data/raw/logs_usdc_vault.jsonl']; cur = 'data/raw/rpc.cursor'
elif JOB == 'reserves':
    RES = [r['reserve_address'].lower() for r in json.load(open('config/reserves.json')) if r.get('chain_id', 8453) == 8453]
    FILTERS = [dict(address=RES)]
    OUTS = ['data/raw/logs_reserves.jsonl']; cur = 'data/raw/reserves.cursor'
else:
    FILTERS = [dict(address=["0x4491ac59d1e6a5d2e15a8048c2de34199e8de8da", "0x2cb51d6e53ba3e983a6a50d2247931c96f9d7358", "0x94df02cc6338e6b38f60a655ea893ea0c1c2961f"])]
    OUTS = ['data/raw/logs_executor.jsonl']; cur = 'data/raw/exec.cursor'
RNG = 2000; PER = max(1, 10 // len(FILTERS))
S = requests.Session()
def rpc(calls):
    for attempt in range(12):
        try:
            r = S.post(RPC, data=json.dumps(calls), headers=H, timeout=180); j = r.json()
            if isinstance(j, dict): raise RuntimeError(str(j.get('error')))
            errs = [x.get('error') for x in j if x.get('error')]
            if errs: raise RuntimeError(str(errs[0]))
            return sorted(j, key=lambda x: x['id'])
        except RuntimeError as e:
            if 'too large' in str(e): raise
            time.sleep(2 + 3*attempt)
        except Exception as e:
            time.sleep(2 + 3*attempt)
    raise RuntimeError('gave up')
def fetch(a, b):
    """returns list per filter of logs for [a,b], splitting on too-large."""
    calls = [{"jsonrpc":"2.0","id":k,"method":"eth_getLogs","params":[dict(f, fromBlock=hex(a), toBlock=hex(b))]} for k, f in enumerate(FILTERS)]
    try:
        return [x['result'] for x in rpc(calls)]
    except RuntimeError as e:
        if 'too large' in str(e) and b > a:
            m = (a + b) // 2; print('split', a, b, flush=True)
            l1 = fetch(a, m); l2 = fetch(m+1, b)
            return [x + y for x, y in zip(l1, l2)]
        raise
fb = int(open(cur).read()) if os.path.exists(cur) else START
outs = [open(o, 'a') for o in OUTS]; t0 = time.time(); n = [0]*len(FILTERS); nb = 0
while fb <= HEAD:
    ranges = [(fb + i*RNG, min(fb + (i+1)*RNG - 1, HEAD)) for i in range(PER) if fb + i*RNG <= HEAD]
    calls = []
    for ri, (a, b) in enumerate(ranges):
        for k, f in enumerate(FILTERS):
            calls.append({"jsonrpc":"2.0","id":ri*len(FILTERS)+k,"method":"eth_getLogs","params":[dict(f, fromBlock=hex(a), toBlock=hex(b))]})
    try:
        res = rpc(calls); per_filter = [[] for _ in FILTERS]
        for x in res: per_filter[x['id'] % len(FILTERS)] += x['result']
    except RuntimeError as e:
        if 'too large' not in str(e): raise
        per_filter = [[] for _ in FILTERS]
        for (a, b) in ranges:
            r = fetch(a, b)
            for k in range(len(FILTERS)): per_filter[k] += r[k]
    for k, logs in enumerate(per_filter):
        for l in logs:
            outs[k].write(json.dumps({'a':l['address'],'b':int(l['blockNumber'],16),'tx':l['transactionHash'],'li':int(l['logIndex'],16),'t':l['topics'],'d':l['data']}) + '\n'); n[k] += 1
        outs[k].flush()
    fb = ranges[-1][1] + 1; nb += 1; open(cur, 'w').write(str(fb))
    if nb % 20 == 0: print(time.strftime('%H:%M:%S'), 'block', fb, 'pct', round(100*(fb-START)/(HEAD-START),1), 'n', n, 'elapsed', round(time.time()-t0), flush=True)
print('DONE', fb, n, flush=True)
