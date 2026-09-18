"""Robinhood Chain (chain id 4663) fetcher for Grail tokens that live there (Uniswap V4 pools, e.g. gJENSEN vs tokenized NVDA).
Pulls PoolManager Swap + ModifyLiquidity logs for each pool id, ERC-20 Transfer logs for each token, tx metadata for
every swap tx, and a sparse block->timestamp table (blocks are ~0.1s and irregular, so timestamps are interpolated).
Writes parquet with the same schema as decode.py so analysis.py can simply concatenate. Resumable via data/raw/rh.cursor."""
import json, os, sys, time, bisect, requests
import pandas as pd

RPC = 'https://rpc.mainnet.chain.robinhood.com'
H = {'content-type': 'application/json', 'user-agent': 'grail-analytics/0.1'}
T_SWAP = '0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f'      # V4 Swap(id, sender, amount0, amount1, sqrtPriceX96, liquidity, tick, fee)
T_MODLIQ = '0xf208f4912782fd25c7f114ca3723a2d5dd6f3bcc3ac8db5af63baa85f711d5ec'    # V4 ModifyLiquidity(id, sender, tickLower, tickUpper, liquidityDelta, salt)
T_TRANSFER = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
RNG = 500_000
S = requests.Session()

def rpc(method, params, tries=12):
    for a in range(tries):
        try:
            r = S.post(RPC, data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}), headers=H, timeout=90).json()
            if 'result' in r: return r['result']
            msg = str(r.get('error'))
            if 'Too Many' in msg or '429' in msg: time.sleep(1.5 + a); continue
            raise RuntimeError(msg)
        except RuntimeError: raise
        except Exception: time.sleep(1.5 + a)
    raise RuntimeError('rpc gave up: ' + method)

def get_logs(flt, a, b):
    """eth_getLogs with adaptive splitting when the node's 10k-result cap is hit."""
    try:
        r = rpc('eth_getLogs', [dict(flt, fromBlock=hex(a), toBlock=hex(b))]); time.sleep(0.25); return r
    except RuntimeError as e:
        if 'exceeds limit' in str(e) and b > a:
            m = (a + b) // 2; return get_logs(flt, a, m) + get_logs(flt, m + 1, b)
        raise

def s256(h):
    v = int(h, 16); return v - (1 << 256) if v >= (1 << 255) else v
def addr(t): return '0x' + t[26:]

toks = []
for p in ['config/tokens_p1.json', 'config/tokens_p2.json', 'config/tokens_p3.json']:
    if os.path.exists(p): toks += json.load(open(p))['results']
RH = [t for t in toks if t.get('chain_id') == 4663 and t.get('v4_pool')]
if not RH:
    print('no Robinhood Chain tokens in registry'); sys.exit(0)
head = int(rpc('eth_blockNumber', []), 16)
os.makedirs('data/raw', exist_ok=True); os.makedirs('data/parquet', exist_ok=True)
cur_path = 'data/raw/rh.cursor'
cursor = json.load(open(cur_path)) if os.path.exists(cur_path) else {}
raw_path = 'data/raw/logs_rh.jsonl'
out = open(raw_path, 'a')
n_new = 0
for t in RH:
    sym = t['name'] if t['name'].startswith('g') else t['symbol']
    pm = t['v4_pool']['pool_manager_address'].lower(); pid = t['v4_pool']['pool_id'].lower(); tok = t['token_address'].lower()
    fb = cursor.get(sym, t['block_number'] - 10)
    while fb <= head:
        tb = min(fb + RNG - 1, head)
        for kind, flt in (('pm', {"address": pm, "topics": [[T_SWAP, T_MODLIQ], pid]}), ('tok', {"address": tok, "topics": [T_TRANSFER]})):
            logs = get_logs(flt, fb, tb)
            for l in logs:
                out.write(json.dumps({'sym': sym, 'k': kind, 'a': l['address'], 'b': int(l['blockNumber'], 16), 'tx': l['transactionHash'], 'li': int(l['logIndex'], 16), 't': l['topics'], 'd': l['data']}) + '\n'); n_new += 1
        fb = tb + 1; cursor[sym] = fb; out.flush(); json.dump(cursor, open(cur_path, 'w'))
out.close()
print('robinhood: new logs', n_new, 'head', head)

# ---- block -> timestamp table (sparse samples + every swap-dense region is interpolated)
ts_path = 'data/raw/rh_blocktimes.json'
bt = {int(k): v for k, v in json.load(open(ts_path)).items()} if os.path.exists(ts_path) else {}
start = min(t['block_number'] for t in RH) - 10
want = list(range(start, head, 20_000)) + [head]
for b in want:
    if b not in bt:
        bt[b] = int(rpc('eth_getBlockByNumber', [hex(b), False])['timestamp'], 16); time.sleep(0.15)
json.dump(bt, open(ts_path, 'w'))
bs = sorted(bt); tsv = [bt[b] for b in bs]
def ts_of(b):
    i = bisect.bisect_right(bs, b) - 1
    if i < 0: return tsv[0]
    if i >= len(bs) - 1: return tsv[-1]
    b0, b1 = bs[i], bs[i+1]; return int(tsv[i] + (tsv[i+1] - tsv[i]) * (b - b0) / (b1 - b0))

# ---- decode to the shared schema
META = {(t['name'] if t['name'].startswith('g') else t['symbol']): t for t in RH}
swaps, xfers, lp, seen = [], [], [], set()
for line in open(raw_path):
    try: l = json.loads(line)
    except Exception: continue
    key = (l['tx'], l['li'])
    if key in seen: continue
    seen.add(key)
    sym = l['sym']; t = META.get(sym)
    if not t: continue
    base = dict(block=l['b'], ts=ts_of(l['b']), tx=l['tx'], li=l['li'])
    tok0 = t['v4_pool']['pool_key']['currency0'].lower() == t['token_address'].lower()
    if l['k'] == 'pm' and l['t'][0] == T_SWAP:
        d = l['d'][2:]; a0, a1 = s256(d[0:64]), s256(d[64:128]); liq = int(d[192:256], 16)
        tok_amt, q_amt = (a0, a1) if tok0 else (a1, a0)          # V4 deltas are from the swapper's side: + received, - paid
        if tok_amt == 0: continue
        swaps.append(dict(**base, symbol=sym, pool=t['v4_pool']['pool_id'].lower(), sender=addr(l['t'][2]), recipient=addr(l['t'][2]),
                          token_delta=tok_amt/1e18, usdc_delta=q_amt/1e18, side='buy' if tok_amt > 0 else 'sell',
                          price=abs(q_amt)/abs(tok_amt), liquidity=float(liq)))
    elif l['k'] == 'pm' and l['t'][0] == T_MODLIQ:
        d = l['d'][2:]; delta = s256(d[128:192])
        lp.append(dict(**base, symbol=sym, kind='mint' if delta > 0 else 'burn', owner=addr(l['t'][2]), token_amt=0.0, usdc_amt=0.0))
    elif l['k'] == 'tok' and len(l['t']) == 3:
        xfers.append(dict(**base, symbol=sym, token=l['a'].lower(), frm=addr(l['t'][1]), to=addr(l['t'][2]), amount=int(l['d'], 16)/1e18))
sw = pd.DataFrame(swaps)
# quote asset (tokenized NVDA) -> USD: Grail's API USD price of the token divided by the last pool price in NVDA
for sym, t in META.items():
    m = sw.symbol == sym if len(sw) else []
    if len(sw) and m.any():
        last = sw[m].sort_values(['block', 'li']).price.iloc[-1]
        q_usd = float(t['market_price']) / last if last else 0.0
        sw.loc[m, 'usdc_delta'] = sw.loc[m, 'usdc_delta'] * q_usd; sw.loc[m, 'price'] = sw.loc[m, 'price'] * q_usd
        print(sym, 'swaps', int(m.sum()), 'quote', t.get('peg_ticker'), 'USD per quote', round(q_usd, 2), 'volume USD', round(sw[m].usdc_delta.abs().sum()))
sw.to_parquet('data/parquet/swaps_rh.parquet', index=False)
pd.DataFrame(xfers).to_parquet('data/parquet/transfers_rh.parquet', index=False)
pd.DataFrame(lp).to_parquet('data/parquet/lp_rh.parquet', index=False)

# ---- tx metadata (from / to / selector) for swap txs
tx_path = 'data/raw/txs_rh.jsonl'
done = {json.loads(x)['hash'] for x in open(tx_path)} if os.path.exists(tx_path) else set()
todo = sorted(set(sw.tx) - done) if len(sw) else []
f = open(tx_path, 'a')
for i in range(0, len(todo), 30):      # batched: 30 lookups per request is the sweet spot under this RPC's rate limit
    chunk = todo[i:i+30]
    batch = [{"jsonrpc": "2.0", "id": k, "method": "eth_getTransactionByHash", "params": [h]} for k, h in enumerate(chunk)]
    for a in range(12):
        try:
            j = S.post(RPC, data=json.dumps(batch), headers=H, timeout=90).json()
            if isinstance(j, list) and all(x.get('result') for x in j): break
        except Exception: pass
        time.sleep(1.5 + a)
    else: raise RuntimeError('tx batch gave up')
    for x in j:
        r = x['result']; f.write(json.dumps({'hash': r['hash'], 'from': r['from'], 'to': r['to'], 'sel': r['input'][:10]}) + '\n')
    f.flush(); time.sleep(0.4)
f.close()
print('robinhood: tx metadata fetched', len(todo), 'transfers', len(xfers), 'lp events', len(lp))
