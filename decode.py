"""Decode raw JSONL logs into parquet tables for DuckDB."""
import json, sys, os
import pandas as pd
from eth_hash.auto import keccak
GENESIS_TS = 1686789347
USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
VAULT = "0x36b162de23e4e809d78fb0eae4a2272bc313d738"
EXEC = "0x4491ac59d1e6a5d2e15a8048c2de34199e8de8da"
T_SWAP = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
T_TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
T_MINT = "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde"
T_BURN = "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c"
T_INVENTORY = "0xf50a1042e41409060ea88cabc649b3142b62ff5ad4d6384c92ea870d2485485d"
T_PACKBUY = "0x1993895c5254b172c48b279d517793076822e31474d52db025d44031dea78c82"
T_PACKBUY_V1 = "0x4adcdeed5800ab60da6be727ddec42e7fdc9872d9b428860e4093b05e42b216c"
toks = []
for p in ['config/tokens_p1.json','config/tokens_p2.json','config/tokens_p3.json']: toks += [t for t in json.load(open(p))['results'] if t.get('chain_id', 8453) == 8453]
POOL = {t['pool_address'].lower(): t for t in toks}
TOKEN = {t['token_address'].lower(): t for t in toks}
for t in toks:  # token0 is the numerically lower address
    t['token_is_0'] = int(t['token_address'], 16) < int(USDC, 16)
packs = json.load(open('config/packs.json'))['packs']
PACKHASH = {'0'+'x'+keccak(p['pack_id'].encode()).hex(): p for p in packs}
def s256(h):
    v = int(h, 16); return v - (1 << 256) if v >= (1 << 255) else v
def addr(t): return '0x' + t[26:]
def ts(b): return GENESIS_TS + 2*b

swaps, xfers, lp, inv, buys, execv, other = [], [], [], [], [], [], []
files = sys.argv[1:] or ['data/raw/logs_main.jsonl','data/raw/logs_usdc_vault.jsonl','data/raw/logs_executor.jsonl']
seen = set()
for fn in files:
    if not os.path.exists(fn): continue
    for line in open(fn):
        try: l = json.loads(line)
        except Exception: continue
        a = l["a"].lower(); t0 = l["t"][0]; key = (l["tx"], l["li"])
        if key in seen: continue
        seen.add(key)
        base = dict(block=l['b'], ts=ts(l['b']), tx=l['tx'], li=l['li'])
        if a in POOL and t0 == T_SWAP:
            tk = POOL[a]; d = l['d'][2:]
            a0, a1 = s256(d[0:64]), s256(d[64:128]); sqrtp = int(d[128:192], 16); liq = int(d[192:256],16)
            tok_amt, usdc_amt = (a0, a1) if tk['token_is_0'] else (a1, a0)
            # tok_amt>0 means pool received token (user sold); usdc_amt>0 means pool received USDC (user bought)
            swaps.append(dict(**base, symbol=tk['symbol'], pool=a, sender=addr(l['t'][1]), recipient=addr(l['t'][2]),
                token_delta=-tok_amt/1e18, usdc_delta=-usdc_amt/1e6, side='buy' if usdc_amt > 0 else 'sell',
                price=(abs(usdc_amt)/1e6)/(abs(tok_amt)/1e18) if tok_amt else None, liquidity=float(liq)))
        elif a in POOL and t0 in (T_MINT, T_BURN):
            tk = POOL[a]; d = l['d'][2:]
            if t0 == T_MINT: owner = addr(l['t'][1]); amt=int(d[64:128],16); x0=int(d[128:192],16); x1=int(d[192:256],16)
            else: owner = addr(l['t'][1]); amt=int(d[0:64],16); x0=int(d[64:128],16); x1=int(d[128:192],16)
            ta, ua = (x0, x1) if tk['token_is_0'] else (x1, x0)
            lp.append(dict(**base, symbol=tk['symbol'], kind='mint' if t0==T_MINT else 'burn', owner=owner, token_amt=ta/1e18, usdc_amt=ua/1e6))
        elif a in TOKEN and t0 == T_TRANSFER and len(l['t']) == 3:
            tk = TOKEN[a]
            xfers.append(dict(**base, symbol=tk['symbol'], token=a, frm=addr(l['t'][1]), to=addr(l['t'][2]), amount=int(l['d'],16)/1e18))
        elif a == USDC and t0 == T_TRANSFER:
            buys.append(dict(**base, kind='usdc_to_vault', frm=addr(l['t'][1]), to=addr(l['t'][2]), usdc=int(l['d'],16)/1e6))
        elif a == VAULT and t0 == T_INVENTORY:
            inv.append(dict(**base, token=addr(l['t'][1]), symbol=TOKEN.get(addr(l['t'][1]),{}).get('symbol'), to=addr(l['t'][2]), amount=int(l['d'],16)/1e18))
        elif a == EXEC and t0 == T_PACKBUY:
            d = l['d'][2:]; ph = l['t'][2]; p = PACKHASH.get(ph, {})
            execv.append(dict(**base, kind='pack_buy', order=l['t'][1][2:34], pack_hash=ph, pack_id=p.get('pack_id'), pack_series=p.get('pack_series'), pack_kind=p.get('pack_kind'),
                buyer=addr(l['t'][3]), qty=int(d[64:128],16), draws=int(d[128:192],16), usdc=int(d[192:256],16)/1e6, ts_event=int(d[256:320],16)))
        elif a == EXEC and t0 == T_PACKBUY_V1:
            d = l['d'][2:]; ph = l['t'][2]; p = PACKHASH.get(ph, {})
            execv.append(dict(**base, kind='pack_buy_v1', order=l['t'][1][2:34], pack_hash=ph, pack_id=p.get('pack_id'), pack_series=p.get('pack_series'), pack_kind=p.get('pack_kind'),
                buyer=addr(l['t'][3]), qty=int(d[0:64],16), draws=int(d[64:128],16), usdc=int(d[128:192],16)/1e6, ts_event=ts(l['b'])))
        else:
            other.append(dict(**base, address=a, topic0=t0, topics=json.dumps(l['t']), data=l['d']))
os.makedirs('data/parquet', exist_ok=True)
for name, rows in [('swaps',swaps),('transfers',xfers),('lp',lp),('inventory',inv),('usdc_vault',buys),('pack_buys',execv),('other',other)]:
    df = pd.DataFrame(rows); df.to_parquet(f'data/parquet/{name}.parquet', index=False); print(name, len(df))
