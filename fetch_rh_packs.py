"""Robinhood Chain packs: executor purchase events, vault InventoryTransferred (card -> tokens), reveal receipts (which
NFT collection / tier each draw minted) and the NFT collections' own Transfer logs. Writes parquet in the Base schema
(+ chain column) so analysis.py can concatenate. Resumable via data/raw/rh_packs.cursor."""
import json, os, time, requests
import pandas as pd
from eth_hash.auto import keccak
RPC = 'https://rpc.mainnet.chain.robinhood.com'; H = {'content-type': 'application/json', 'user-agent': 'grail-analytics/0.1'}
EXEC = '0x4491ac59d1e6a5d2e15a8048c2de34199e8de8da'; VAULT = '0x36b162de23e4e809d78fb0eae4a2272bc313d738'; ZERO = '0x' + '0'*40
T_BUY2 = '0x1993895c5254b172c48b279d517793076822e31474d52db025d44031dea78c82'; T_BUY1 = '0x4adcdeed5800ab60da6be727ddec42e7fdc9872d9b428860e4093b05e42b216c'
T_REVEAL = '0xafdcc587c7c87c73d752e24c9e9c75a64f0e135de1d27a1ccffdfec49e34'; T_INV = '0xf50a1042e41409060ea88cabc649b3142b62ff5ad4d6384c92ea870d2485485d'
T_XFER = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
S = requests.Session()
def rpc(m, p):
    for a in range(10):
        try:
            r = S.post(RPC, data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": m, "params": p}), headers=H, timeout=90).json()
            if 'result' in r: return r['result']
            if 'exceeds limit' in str(r.get('error')): raise RuntimeError('split')
        except RuntimeError: raise
        except Exception: pass
        time.sleep(1.5 + a)
    raise RuntimeError('rpc gave up ' + m)
def get_logs(flt, a, b):
    try: r = rpc('eth_getLogs', [dict(flt, fromBlock=hex(a), toBlock=hex(b))]); time.sleep(0.25); return r
    except RuntimeError:
        if b > a: m = (a + b)//2; return get_logs(flt, a, m) + get_logs(flt, m+1, b)
        raise
toks = []
for p in ['config/tokens_p1.json', 'config/tokens_p2.json', 'config/tokens_p3.json']: toks += json.load(open(p))['results']
RH = [t for t in toks if t.get('chain_id') == 4663]
if not RH: raise SystemExit('no RH tokens')
TOKEN = {t['token_address'].lower(): (t['name'] if t['name'].startswith('g') else t['symbol']) for t in RH}
packs = json.load(open('config/packs.json'))['packs']; PACKHASH = {'0x' + keccak(p['pack_id'].encode()).hex(): p for p in packs}
head = int(rpc('eth_blockNumber', []), 16); start = min(t['block_number'] for t in RH) - 10
os.makedirs('data/raw', exist_ok=True); os.makedirs('data/parquet', exist_ok=True)
cur_path = 'data/raw/rh_packs.cursor'; cur = json.load(open(cur_path)) if os.path.exists(cur_path) else {}
raw = 'data/raw/logs_rh_packs.jsonl'; out = open(raw, 'a'); n = 0
fb = cur.get('exec', start)
while fb <= head:
    tb = min(fb + 99_999, head)
    for l in get_logs({"address": [EXEC, VAULT]}, fb, tb):
        out.write(json.dumps({'k': 'ev', 'a': l['address'], 'b': int(l['blockNumber'], 16), 'tx': l['transactionHash'], 'li': int(l['logIndex'], 16), 't': l['topics'], 'd': l['data']}) + '\n'); n += 1
    fb = tb + 1; cur['exec'] = fb; out.flush(); json.dump(cur, open(cur_path, 'w'))
out.close(); print('rh packs: executor/vault logs new', n)
# block times (reuse the token fetcher's table)
import bisect
bt = {int(k): v for k, v in json.load(open('data/raw/rh_blocktimes.json')).items()} if os.path.exists('data/raw/rh_blocktimes.json') else {}
for b in list(range(start, head, 20_000)) + [head]:
    if b not in bt: bt[b] = int(rpc('eth_getBlockByNumber', [hex(b), False])['timestamp'], 16); time.sleep(0.15)
json.dump(bt, open('data/raw/rh_blocktimes.json', 'w')); bs = sorted(bt); tsv = [bt[b] for b in bs]
def ts_of(b):
    i = bisect.bisect_right(bs, b) - 1
    if i < 0: return tsv[0]
    if i >= len(bs) - 1: return tsv[-1]
    return int(tsv[i] + (tsv[i+1] - tsv[i]) * (b - bs[i]) / (bs[i+1] - bs[i]))
def addr(t): return '0x' + t[26:]
logs = []; seen = set()
for line in open(raw):
    try: l = json.loads(line)
    except Exception: continue
    if (l['tx'], l['li']) in seen: continue
    seen.add((l['tx'], l['li'])); logs.append(l)
buys, inv, reveals = [], [], []
for l in logs:
    a = l['a'].lower(); t0 = l['t'][0]; base = dict(block=l['b'], ts=ts_of(l['b']), tx=l['tx'], li=l['li'])
    if a == EXEC and t0 in (T_BUY1, T_BUY2):
        d = l['d'][2:]; ph = l['t'][2]; p = PACKHASH.get(ph, {})
        if t0 == T_BUY2: qty, draws, usdc, tse = int(d[64:128], 16), int(d[128:192], 16), int(d[192:256], 16)/1e6, int(d[256:320], 16)
        else: qty, draws, usdc, tse = int(d[0:64], 16), int(d[64:128], 16), int(d[128:192], 16)/1e6, base['ts']
        buys.append(dict(**base, kind='pack_buy_v2' if t0 == T_BUY2 else 'pack_buy_v1', order=l['t'][1][2:34], pack_hash=ph, pack_id=p.get('pack_id'), pack_series=p.get('pack_series'), pack_kind=p.get('pack_kind'), buyer=addr(l['t'][3]), qty=qty, draws=draws, usdc=usdc, ts_event=tse, chain='Robinhood'))
    elif a == VAULT and t0 == T_INV:
        inv.append(dict(**base, token=addr(l['t'][1]), symbol=TOKEN.get(addr(l['t'][1])), to=addr(l['t'][2]), amount=int(l['d'], 16)/1e18, chain='Robinhood'))
    elif a == EXEC and t0.startswith(T_REVEAL[:20]):
        reveals.append(l)
pd.DataFrame(buys).to_parquet('data/parquet/pack_buys_rh.parquet', index=False); pd.DataFrame(inv).to_parquet('data/parquet/inventory_rh.parquet', index=False)
print('rh packs: buys', len(buys), 'packs', sum(b['qty'] for b in buys), 'usdc', sum(b['usdc'] for b in buys), 'redeems', len(inv), 'reveals', len(reveals))
# reveal receipts -> NFT mints (collection, tokenId, owner); cached
rc_path = 'data/raw/rh_reveal_mints.jsonl'
done = {json.loads(x)['tx'] for x in open(rc_path)} if os.path.exists(rc_path) else set()
f = open(rc_path, 'a')
for l in reveals:
    if l['tx'] in done: continue
    rc = rpc('eth_getTransactionReceipt', [l['tx']]); time.sleep(0.15)
    for x in rc['logs']:
        if x['topics'][0] == T_XFER and len(x['topics']) == 4 and x['topics'][1] == '0x' + '0'*64:
            f.write(json.dumps({'tx': l['tx'], 'block': int(x['blockNumber'], 16), 'collection': x['address'].lower(), 'to': addr(x['topics'][2]), 'token_id': int(x['topics'][3], 16)}) + '\n')
    f.write(json.dumps({'tx': l['tx'], 'block': l['b'], 'collection': None, 'to': None, 'token_id': None}) + '\n')   # marker: receipt done
f.close()
mints = pd.DataFrame([json.loads(x) for x in open(rc_path)]).dropna(subset=['collection'])
cols = sorted(set(mints.collection))
# collection names + all their transfers (burn on redeem, secondary transfers)
meta = {}
def call(a, sel):
    r = rpc('eth_call', [{'to': a, 'data': sel}, 'latest'])
    try:
        raw = bytes.fromhex(r[2:]); off = int.from_bytes(raw[0:32], 'big'); ln = int.from_bytes(raw[off:off+32], 'big')
        return raw[off+32:off+32+ln].decode(errors='ignore').strip()
    except Exception: return ''
for c in cols: meta[c] = dict(name=call(c, '0x06fdde03'), symbol=call(c, '0x95d89b41')); time.sleep(0.2)
xf = []
for c in cols:
    fbk = mints[mints.collection == c].block.min() - 5
    for a in range(fbk, head + 1, 100_000):
        for l in get_logs({"address": c, "topics": [T_XFER]}, a, min(a + 99_999, head)):
            if len(l['topics']) == 4: xf.append(dict(collection=c, frm=addr(l['topics'][1]), to=addr(l['topics'][2]), token_id=int(l['topics'][3], 16), block=int(l['blockNumber'], 16), tx=l['transactionHash']))
X = pd.DataFrame(xf)
rows = []
for c in cols:
    xc = X[X.collection == c]; minted = set(xc[xc.frm == ZERO].token_id); burned = set(xc[xc.to == ZERO].token_id)
    live = minted - burned; owner = xc.sort_values('block').groupby('token_id').to.last()
    holders = owner[owner.index.isin(live)]
    # tokens released per redeem of this tier: match burn txs to InventoryTransferred amounts
    inv_df = pd.DataFrame(inv); burn_txs = set(xc[xc.to == ZERO].tx); rel = inv_df[inv_df.tx.isin(burn_txs)].amount if len(inv_df) else pd.Series(dtype=float)
    rows.append(dict(collection=c, name=meta[c]['name'], symbol=meta[c]['symbol'], minted=len(minted), redeemed=len(burned), unredeemed=len(live), holders=int(holders.nunique()), secondary_transfers=int(((xc.frm != ZERO) & (xc.to != ZERO)).sum()),
        tokens_per_card=float(rel.median()) if len(rel) else None, tokens_released=float(rel.sum()) if len(rel) else 0.0, first_block=int(xc.block.min()) if len(xc) else None))
tiers = pd.DataFrame(rows).sort_values('tokens_per_card', ascending=False)
tiers.to_parquet('data/parquet/nft_tiers_rh.parquet', index=False); X.to_parquet('data/parquet/nft_transfers_rh.parquet', index=False); mints.to_parquet('data/parquet/nft_mints_rh.parquet', index=False)
pd.set_option('display.width', 220); print(tiers.to_string(index=False))
