"""Grail user analytics: builds out/analysis.json + CSV exports from data/parquet + data/raw/txs.jsonl using DuckDB + pandas."""
import json, os, sys, math, time
from collections import defaultdict, Counter
import duckdb, pandas as pd, numpy as np

con = duckdb.connect()
GENESIS_TS = 1686789347
toks = []
for p in ['config/tokens_p1.json','config/tokens_p2.json','config/tokens_p3.json']: toks += json.load(open(p))['results']
for t in toks:  # registry quirk: some entries carry the g-prefixed ticker in `name` instead of `symbol`
    if not t['symbol'].startswith('g') and t['name'].startswith('g'): t['symbol'], t['name'] = t['name'], t['symbol']
TOK = {t['symbol']: t for t in toks}
CHAIN = {t['symbol']: ('Robinhood' if t.get('chain_id') == 4663 else 'Base') for t in toks}
PRICE = {t['symbol']: float(t['market_price'] or 0) for t in toks}
NAME = {t['symbol']: t['name'] for t in toks}
POOLS = {t['pool_address'].lower() for t in toks} | {t['v4_pool']['pool_manager_address'].lower() for t in toks if t.get('v4_pool')}
packs = json.load(open('config/packs.json'))['packs']
PACK = {p['pack_id']: p for p in packs}
VAULT = "0x36b162de23e4e809d78fb0eae4a2272bc313d738"
EXEC = "0x4491ac59d1e6a5d2e15a8048c2de34199e8de8da"
GRAIL_ROUTER = "0x2cb51d6e53ba3e983a6a50d2247931c96f9d7358"
GRAIL_OLD_ROUTER = "0x94df02cc6338e6b38f60a655ea893ea0c1c2961f"
GRAIL_DEPLOYER = "0xcb5a9f6c4709c3bb8e37f729be10c6c2aa66aefe"
GRAIL_RELAYERS = {"0x8c5a2bfb1b6bbc380abd6df6ee21679a3b6c0c93"}
# Grail staff wallets: the admin list hard-coded in the grail.xyz front-end, the team inventory wallet,
# and the wallet grailytics labels OURS. Excluded from traders, holders, pack cohorts and the GLIST.
GRAIL_TEAM = {'0xd748d069c675be1bcdd7868b42fdfe9c3eca478a','0xa00b7b0a79b88322f41a2c355587100159583fb8','0x2cd1e614ec851265463c77e8d5115852101cf343',
              '0x4d1382863382b2d93aa7d1a968586bf21e526c5b','0xa2875fe1a7579806e0ee42d408444124c3ba7d30','0x283fc513f1399ba53ac374a3976a71d238b5b1f3',
              '0x61f1e873402b18ea34ff27117cfbe578bfa9443c','0x390dfc1567d53a3f5277b8c2f8f70d119b0e910b','0x8a0c45b9276aedaabbae18afe42c1fcd8c379982'}
ZERO = "0x" + "0"*40
# pack -> token mapping (LAUNCH packs are single-token); genesis packs span many tokens
PACK_TOKEN = {'JENSENLAUNCH':'gJENSEN','VLADLAUNCH':'gVLAD','VITALIKLAUNCH':'gVITALIK','KAILAUNCHPACKS':'gKAI','ALLENLAUNCH':'gALLEN','COOPLAUNCH':'gCOOP','KIRKLAUNCHPACKS1':'gKIRK','KIRKLAUNCHPACK':'gKIRK','SWIFTLAUNCH':'gSWIFT','ELONLAUNCH':'gELON'}

# ---------- venue map: (prefix, suffix) -> label. Prefixes/suffixes from grailytics + known canonical addresses.
VENUES = [
 ('0x4491','8de8da','Grail'),('0x2cb5','7358','Grail'),('0x94df','961f','Grail'),('0x68a1','da7b','Grail'),('0x78a4','e95b','Grail'),
 ('0x5ff1','2789','Fomo'),('0x0000','a032','Fomo'),('0x4337','f108','Fomo'),('0xb92f','4f','Fomo'),
 ('0xd8ba','a4e2','GMGN'),('0xcfba','9d17','GMGN'),
 ('0xc8f6','9265','OKX Wallet'),('0x5e2f','9801','OKX Wallet'),('0x67d0','81df','OKX Wallet'),
 ('0x2626','e481','Uniswap App'),('0x3328','4e49','Uniswap App'),('0x3fc9','7fad','Uniswap App'),('0x6ff5','9b43','Uniswap App'),('0x492e','4104','Uniswap App'),('0x6fF5','9b43','Uniswap App'),
 ('0xdef1','5eff','0x API (Matcha/Rabby)'),('0x0000','2734','0x API (Matcha/Rabby)'),
 ('0x6131','37b5','KyberSwap'),('0x6131','b77b','KyberSwap'),('0x1111','0582','1inch'),('0x1111','2a65','1inch'),('0x1231','4eae','LI.FI'),
 ('0x6a00','1068','Paraswap'),('0x6352','4e64','OpenOcean'),('0xd63b','1026','Odos'),('0x19ce','95a1','Odos'),('0xca42','9680','CoW Swap'),('0x9008','ab41','CoW Swap'),
 ('0x327d','5d86','Socket'),('0x80e3','5c69','Bebop'),('0xbbbb','ad5f','Bebop'),('0xb300','028d','Banana Gun'),('0x20f6','860c','Maestro'),
 ('0x8cc6','4c35','Sigma'),('0x5e83','cc81','Sigma'),('0xd0a4','e4bf','BasedBot'),('0xd7f1','696f','Zerion'),('0x0000','10e2','Rainbow'),
 ('0xca11','ca11','Bots / Direct'),('0xafa8','f5fd','Unknown App C'),('0x013b','9060','Unknown App D'),('0xbce8','ea93','Unknown App E'),('0xef16','e318','Unknown App E'),('0x302a','17ac','Unknown App E'),('0x8876','0904','Uniswap App'),('0x55c2','6599','Sigma'),('0x6505','40dc','Unknown App G'),('0xe492','ce2b','Unknown App G'),('0x6e2a','6919','Unknown App H'),('0x463a','fd18','Unknown App F'),('0xccc8','15be','Fomo'),('0xac4c','8b75','SushiSwap'),
]
GRAIL_CONTRACTS = {EXEC, GRAIL_ROUTER, GRAIL_OLD_ROUTER}
def venue_of(to, frm):
    if not isinstance(to, str): return None   # no tx metadata (or contract creation)
    to = to.lower(); frm = frm if isinstance(frm, str) else ''
    if to in GRAIL_CONTRACTS or frm in GRAIL_RELAYERS: return 'Grail'
    for pre, suf, lab in VENUES:
        if to.startswith(pre.lower()) and to.endswith(suf.lower()): return lab
    return None
AGG_SET = {'SushiSwap','GMGN','OKX Wallet','KyberSwap','1inch','LI.FI','Paraswap','OpenOcean','Odos','CoW Swap','Socket','Bebop','0x API (Matcha/Rabby)','Zerion','Rainbow','Uniswap App'}
BOT_SET = {'Banana Gun','Maestro','Sigma','BasedBot'}
def venue_group(v):
    if v == 'Grail': return 'Grail app'
    if v == 'Fomo': return 'Fomo app'
    if v in AGG_SET: return 'Router / aggregator'
    if v in BOT_SET: return 'Telegram bot'
    if v == 'Bots / Direct': return 'Direct / MEV'
    return 'Other'

# ---------- load
swaps = pd.read_parquet('data/parquet/swaps.parquet'); xf = pd.read_parquet('data/parquet/transfers.parquet')
lp = pd.read_parquet('data/parquet/lp.parquet'); inv = pd.read_parquet('data/parquet/inventory.parquet')
uv = pd.read_parquet('data/parquet/usdc_vault.parquet')
pb = pd.read_parquet('data/parquet/pack_buys.parquet') if os.path.getsize('data/parquet/pack_buys.parquet') > 0 else pd.DataFrame()
txs = pd.read_json('data/raw/txs.jsonl', lines=True) if os.path.exists('data/raw/txs.jsonl') else pd.DataFrame(columns=['hash','from','to','sel'])
# other chains (Robinhood Chain V4 pools) are decoded by fetch_robinhood.py into the same schema
for nm, ref in (('swaps_rh', 'swaps'), ('transfers_rh', 'xf'), ('lp_rh', 'lp')):
    pth = f'data/parquet/{nm}.parquet'
    if os.path.exists(pth):
        extra = pd.read_parquet(pth)
        if len(extra):
            if ref == 'swaps': swaps = pd.concat([swaps, extra], ignore_index=True)
            elif ref == 'xf': xf = pd.concat([xf, extra], ignore_index=True)
            else: lp = pd.concat([lp, extra], ignore_index=True)
for nm in ('pack_buys_rh', 'inventory_rh'):
    pth = f'data/parquet/{nm}.parquet'
    if os.path.exists(pth) and os.path.getsize(pth) > 0:
        extra = pd.read_parquet(pth)
        if len(extra):
            if nm == 'pack_buys_rh': pb = pd.concat([pb, extra.drop(columns=['chain'], errors='ignore')], ignore_index=True)
            else: inv = pd.concat([inv, extra.drop(columns=['chain'], errors='ignore')], ignore_index=True)
if os.path.exists('data/raw/txs_rh.jsonl'):
    txs = pd.concat([txs, pd.read_json('data/raw/txs_rh.jsonl', lines=True)], ignore_index=True)
txs = txs.drop_duplicates('hash').rename(columns={'hash':'tx','from':'tx_from','to':'tx_to'})
txs['tx_from'] = txs['tx_from'].str.lower(); txs['tx_to'] = txs['tx_to'].str.lower()
for df in (swaps, xf, inv, uv): 
    for c in ('sender','recipient','frm','to'):
        if c in df.columns: df[c] = df[c].str.lower()
print('loaded swaps', len(swaps), 'transfers', len(xf), 'txs', len(txs), 'inventory', len(inv), 'pack_buys', len(pb))

# ---------- token launch info
launch = lp[lp.kind=='mint'].groupby('symbol').block.min().rename('launch_block')
first_swap = swaps.groupby('symbol').block.min().rename('first_swap_block')
tokinfo = pd.DataFrame({'symbol': list(TOK)}).set_index('symbol').join(launch).join(first_swap)
tokinfo = tokinfo.join(lp[lp.kind=='mint'].groupby('symbol').ts.min().rename('launch_ts')).join(swaps.groupby('symbol').ts.min().rename('first_swap_ts'))
tokinfo['deploy_block'] = [TOK[s]['block_number'] for s in tokinfo.index]
tokinfo['price'] = [PRICE[s] for s in tokinfo.index]

# ---------- actor resolution: net token flow per (tx, symbol, address) excluding pool
net = xf.copy()
out_ = net[['tx','symbol','frm','amount']].rename(columns={'frm':'addr'}); out_['amount'] = -out_['amount']
in_ = net[['tx','symbol','to','amount']].rename(columns={'to':'addr'})
flow = pd.concat([out_, in_]).groupby(['tx','symbol','addr'], as_index=False).amount.sum()
flow = flow[~flow.addr.isin(POOLS | {VAULT, ZERO})]
flow_pos = flow[flow.amount > 1e-9].sort_values('amount', ascending=False).drop_duplicates(['tx','symbol']).set_index(['tx','symbol']).addr
flow_neg = flow[flow.amount < -1e-9].sort_values('amount').drop_duplicates(['tx','symbol']).set_index(['tx','symbol']).addr
swaps = swaps.merge(txs[['tx','tx_from','tx_to','sel']], on='tx', how='left')
key = list(zip(swaps.tx, swaps.symbol))
swaps['actor'] = [ (flow_pos.get(k) if s=='buy' else flow_neg.get(k)) for k, s in zip(key, swaps.side) ]
swaps['actor'] = swaps['actor'].fillna(pd.Series(np.where(swaps.side=='buy', swaps.recipient, swaps.tx_from.fillna(swaps.sender)), index=swaps.index))
swaps['actor'] = swaps['actor'].str.lower()
swaps['usdc'] = swaps.usdc_delta.abs(); swaps['tokens'] = swaps.token_delta.abs()
swaps['venue'] = [venue_of(t, f) for t, f in zip(swaps.tx_to, swaps.tx_from)]
# bots/direct: caller contract is the pool-level sender and unlabeled, or actor==tx_from==sender pattern
direct = swaps.venue.isna() & ((swaps.tx_to == swaps.sender) | (swaps.tx_to == swaps.actor))
swaps.loc[direct, 'venue'] = 'Bots / Direct'
unknown_to = swaps[swaps.venue.isna()].groupby('tx_to').agg(n=('tx','count'), vol=('usdc','sum')).sort_values('vol', ascending=False)
swaps['venue'] = swaps.venue.fillna('Other / Unknown')
swaps['group'] = swaps.venue.map(venue_group)
swaps['gasless'] = swaps.tx_from.isin(GRAIL_RELAYERS) | (swaps.tx_from != swaps.actor)
swaps['day'] = pd.to_datetime(swaps.ts, unit='s').dt.strftime('%Y-%m-%d')

# ---------- pack buys & redeems
if len(pb):
    pb['buyer'] = pb.buyer.str.lower(); pb = pb[~pb.buyer.isin(GRAIL_TEAM)].copy(); pb['gated'] = pb.pack_id.map(lambda p: PACK.get(p,{}).get('gated'))
    pb['pack_kind'] = pb.pack_id.map(lambda p: PACK.get(p,{}).get('pack_kind'))
    pb['pack_series'] = pb.pack_id.map(lambda p: PACK.get(p,{}).get('pack_series'))
    pb['price'] = pb.pack_id.map(lambda p: float(PACK.get(p,{}).get('usdc_price') or 0))
inv['to'] = inv.to.str.lower()

# ---------- positions & P&L per (wallet, token): inflows = buys (cost usdc), redeems (cost = pack price allocation), transfers-in (cost 0, flagged)
# build event stream
ev = []
for r in swaps.itertuples():
    ev.append((r.ts, r.block, r.li, r.actor, r.symbol, 'buy' if r.side=='buy' else 'sell', r.tokens, r.usdc, r.venue))
# redeem cost: pack price per redeem event for buyer of that pack series; approximate: for LAUNCH packs price known; genesis: price/draws
redeem_cost = {}
if len(pb):
    for r in pb.itertuples():
        redeem_cost.setdefault(r.buyer, []).append((r.ts, r.pack_id, r.usdc / max(r.draws,1)))
for r in inv.itertuples():
    cost = 0.0
    lst = redeem_cost.get(r.to)
    if lst:
        # take earliest unconsumed pack draw
        for i, (ts_, pid, c) in enumerate(lst):
            if ts_ <= r.ts: cost = c; lst.pop(i); break
    ev.append((r.ts, r.block, r.li, r.to, r.symbol, 'redeem', r.amount, cost, 'Grail'))
# other transfers in/out between wallets (not pool/vault/zero) -> cost 0 inflow / outflow at avg cost
wallet_xf = xf[~xf.frm.isin(POOLS|{VAULT,ZERO}) & ~xf.to.isin(POOLS|{VAULT,ZERO})]
# exclude transfers that are part of a swap tx (router hops)
swap_txs = set(swaps.tx)
wallet_xf = wallet_xf[~wallet_xf.tx.isin(swap_txs)]
for r in wallet_xf.itertuples():
    ev.append((r.ts, r.block, r.li, r.frm, r.symbol, 'xfer_out', r.amount, 0.0, None))
    ev.append((r.ts, r.block, r.li, r.to, r.symbol, 'xfer_in', r.amount, 0.0, None))
mints = xf[xf.frm == ZERO]
for r in mints.itertuples():
    ev.append((r.ts, r.block, r.li, r.to, r.symbol, 'mint', r.amount, 0.0, None))
# market price per token at each block (last swap price before) for pricing transfer-in / mint inflows
px = {s_: d.sort_values('block')[['block','price']].dropna().values for s_, d in swaps.groupby('symbol')}
import bisect
def price_at(sym, blk):
    arr = px.get(sym)
    if arr is None or len(arr)==0: return PRICE.get(sym, 0.0)
    i = bisect.bisect_right(arr[:,0], blk) - 1
    return float(arr[i,1]) if i >= 0 else float(arr[0,1])
ev = [(e[0],e[1],e[2],e[3],e[4],e[5],e[6], (e[6]*price_at(e[4], e[1]) if (e[5] in ('xfer_in','mint') or (e[5]=='redeem' and e[7]==0)) else e[7]), e[8]) for e in ev]
ev.sort(key=lambda e: (e[1], e[2]))
pos = {}  # (wallet,symbol) -> dict
for ts_, blk, li, w, sym, kind, qty, usdc, venue in ev:
    p = pos.setdefault((w, sym), dict(qty=0.0, cost=0.0, realized=0.0, buy_usdc=0.0, sell_usdc=0.0, n_buy=0, n_sell=0, unpriced_in=0.0, redeemed=0.0, first_ts=ts_, last_ts=ts_, first_buy_ts=None, first_sell_ts=None, sell_venues=Counter(), buy_venues=Counter(), redeem_cost=0.0))
    p['last_ts'] = ts_
    if kind == 'buy':
        p['qty'] += qty; p['cost'] += usdc; p['buy_usdc'] += usdc; p['n_buy'] += 1; p['buy_venues'][venue] += 1
        if p['first_buy_ts'] is None: p['first_buy_ts'] = ts_
    elif kind in ('redeem', 'xfer_in', 'mint'):
        p['qty'] += qty; p['cost'] += usdc
        if kind == 'redeem': p['redeemed'] += qty; p['redeem_cost'] += usdc
        else: p['unpriced_in'] += qty
    elif kind in ('sell', 'xfer_out'):
        avg = p['cost'] / p['qty'] if p['qty'] > 1e-12 else 0.0
        q = min(qty, p['qty']) if p['qty'] > 1e-12 else 0.0
        basis = avg * q
        if kind == 'sell':
            p['realized'] += usdc - basis; p['sell_usdc'] += usdc; p['n_sell'] += 1; p['sell_venues'][venue] += 1
            if p['first_sell_ts'] is None: p['first_sell_ts'] = ts_
        p['qty'] -= q; p['cost'] -= basis
        if p['qty'] < 1e-9: p['qty'] = 0.0; p['cost'] = 0.0
rows = []
for (w, sym), p in pos.items():
    price = PRICE.get(sym, 0.0)
    unreal = p['qty'] * price - p['cost']
    rows.append(dict(wallet=w, symbol=sym, qty=p['qty'], value=p['qty']*price, realized=p['realized'], unrealized=unreal, total=p['realized']+unreal,
        buy_usdc=p['buy_usdc'], sell_usdc=p['sell_usdc'], n_buy=p['n_buy'], n_sell=p['n_sell'], unpriced_in=p['unpriced_in'], redeemed=p['redeemed'], redeem_cost=p['redeem_cost'],
        first_ts=p['first_ts'], first_buy_ts=p['first_buy_ts'], first_sell_ts=p['first_sell_ts'],
        top_sell_venue=(p['sell_venues'].most_common(1)[0][0] if p['sell_venues'] else None)))
pos_df = pd.DataFrame(rows)
deploy_mints = mints.merge(tokinfo[['deploy_block']], left_on='symbol', right_index=True)
TREASURY = set(deploy_mints[deploy_mints.block <= deploy_mints.deploy_block + 5].to) | set(mints.sort_values(['block','li']).drop_duplicates('symbol').to) | {'0x390dfc1567d53a3f5277b8c2f8f70d119b0e910b', '0x94df02cc6338e6b38f60a655ea893ea0c1c2961f'}  # Grail inventory + team wallet (labelled OURS on grailytics)
INFRA = POOLS | {VAULT, ZERO, EXEC, GRAIL_ROUTER, GRAIL_OLD_ROUTER, GRAIL_DEPLOYER} | TREASURY | GRAIL_TEAM
print('treasury wallets', TREASURY)
pos_df = pos_df[~pos_df.wallet.isin(INFRA)]

# ---------- wallet-level table
sw = swaps[~swaps.actor.isin(INFRA)]
w_stats = sw.groupby('actor').agg(n_swaps=('tx','count'), volume=('usdc','sum'), first_ts=('ts','min'), last_ts=('ts','max'), n_tokens=('symbol','nunique'),
                                  buy_vol=('usdc', lambda s: s[sw.loc[s.index,'side']=='buy'].sum()), sell_vol=('usdc', lambda s: s[sw.loc[s.index,'side']=='sell'].sum()))
prim = sw.groupby(['actor','venue']).size().reset_index(name='n').sort_values('n', ascending=False).drop_duplicates('actor').set_index('actor').venue
venues_used = sw.groupby('actor').venue.agg(lambda s: sorted(set(s)))
grail_any = sw.groupby('actor').venue.agg(lambda s: (s=='Grail').mean())
w_stats['primary_venue'] = prim; w_stats['venues'] = venues_used; w_stats['grail_share'] = grail_any
w_stats['primary_group'] = w_stats.primary_venue.map(venue_group)
pnl_w = pos_df.groupby('wallet').agg(realized=('realized','sum'), unrealized=('unrealized','sum'), total=('total','sum'), holdings_value=('value','sum'), unpriced=('unpriced_in','sum'), redeemed=('redeemed','sum')).reindex(w_stats.index).fillna(0)
w_stats = w_stats.join(pnl_w)
w_stats['bot'] = w_stats.primary_venue.eq('Bots / Direct')
w_stats['is_grail_app'] = (w_stats.grail_share > 0)
pack_buyers = set(pb.buyer) if len(pb) else set()
w_stats['pack_buyer'] = w_stats.index.isin(pack_buyers)
w_stats['redeemer'] = w_stats.index.isin(set(inv.to))
w_stats['profitable'] = w_stats.total > 0
w_stats['usage'] = np.select([(w_stats.grail_share==1), (w_stats.grail_share>0), (w_stats.grail_share==0)], ['Grail app only','Grail app + external','External only'], default='External only')
traders = w_stats[~w_stats.bot]
print('traders', len(traders), 'bots', int(w_stats.bot.sum()), 'profitable %', round(100*traders.profitable.mean(),1))

# ---------- snipers: first buy vs launch
fb_ = sw[sw.side=='buy'].sort_values('block').drop_duplicates(['actor','symbol'])[['actor','symbol','block','ts','usdc','venue','tx_to','tx_from']]
fb_ = fb_.join(tokinfo[['launch_block','first_swap_block','launch_ts','first_swap_ts']], on='symbol')
SNIPE_BLOCKS = 3            # first 3 blocks in which the token traded (Base, 2s blocks)
SNIPE_SECONDS = 6           # the same wall-clock window on chains with sub-second blocks
fb_['open_block'] = fb_.groupby('symbol').block.transform('min'); fb_['open_ts'] = fb_.groupby('symbol').ts.transform('min')
fb_['delta_blocks'] = fb_.block - fb_.open_block
fb_['delta_sec'] = fb_.ts - fb_.open_ts
fb_['delta_min'] = fb_.delta_sec / 60
_base = fb_.symbol.map(CHAIN) == 'Base'
snipes = fb_[(_base & (fb_.delta_blocks < SNIPE_BLOCKS)) | (~_base & (fb_.delta_sec <= SNIPE_SECONDS))]
snipes = snipes.merge(pos_df[['wallet','symbol','realized','unrealized','total','qty','first_sell_ts','n_sell','sell_usdc']], left_on=['actor','symbol'], right_on=['wallet','symbol'], how='left')
snipes['sold_within_24h'] = (snipes.first_sell_ts.notna()) & ((snipes.first_sell_ts - snipes.ts) <= 86400)
snipes['same_block'] = snipes.delta_blocks <= 0
sniper_w = snipes.groupby('actor').agg(tokens_sniped=('symbol','nunique'), symbols=('symbol', lambda s: ','.join(sorted(set(s)))), snipe_spend=('usdc','sum'), pnl=('total','sum'), realized=('realized','sum'),
                                       sold_24h=('sold_within_24h','sum'), same_block=('same_block','sum'), min_delta_min=('delta_min','min'), venues=('venue', lambda s: ','.join(sorted(set(s))))).sort_values('tokens_sniped', ascending=False)
sniper_w['tag'] = np.where(sniper_w.tokens_sniped >= 3, 'Serial sniper', 'Sniper')

# ---------- vaulting: every ERC-20 mint = physical card(s) entering the vault (amount / reserve multiplier). Recipient tells who vaulted.
reserves = json.load(open('config/reserves.json'))
def _mult(m):
    m = float(m); return m/1e18 if m > 1e15 else m
MULT = {r['token_symbol']: _mult(r['multiplier']) for r in reserves}
EXTRA_RESERVES = []   # tokens on other chains carry their reserves inline in the token registry
for sy, t in TOK.items():
    if sy not in MULT and t.get('reserves') and sy not in {r['token_symbol'] for r in reserves}:
        MULT[sy] = _mult(t['reserves'][0]['multiplier'])
        EXTRA_RESERVES += [dict(token_symbol=sy, name=r['name'], backed_supply=r['backed_supply'], multiplier=r['multiplier'], psa_pop=r.get('psa_pop'), reserve_price=r.get('reserve_price'), reserve_address=r['reserve_address']) for r in t['reserves']]
RES_ADDR = {r['reserve_address'].lower(): r['token_symbol'] for r in reserves}
T_BACKED = '0x3789b3d374'  # BackedSupplyChanged: emitted when a physical card is vaulted
T_XFER = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
_rl = [json.loads(l) for l in open('data/raw/logs_reserves.jsonl')] if os.path.exists('data/raw/logs_reserves.jsonl') else []
vault_event_txs = {l['tx'] for l in _rl if l['t'][0].startswith(T_BACKED)}
nft = pd.DataFrame([dict(sym=RES_ADDR[l['a'].lower()], frm='0x'+l['t'][1][26:], to='0x'+l['t'][2][26:], token_id=int(l['t'][3],16), block=l['b'], ts=GENESIS_TS+2*l['b'], tx=l['tx']) for l in _rl if l['t'][0]==T_XFER and len(l['t'])==4])

GRAIL_WALLETS = TREASURY | GRAIL_TEAM | {GRAIL_DEPLOYER, '0x9fcab3d5fa3c4cdf0b3e554f30bd290a5ad02118'}
mint_df = mints[mints.symbol.map(CHAIN) == 'Base'].merge(tokinfo[['deploy_block']], left_on='symbol', right_index=True)   # physical-card vaulting is a Base-token phenomenon; Robinhood items are single collectibles
mint_df['cards'] = [a / MULT.get(sy, 10000.0) for a, sy in zip(mint_df.amount, mint_df.symbol)]
mint_df['remint'] = (~mint_df.tx.isin(vault_event_txs) & (mint_df.symbol.map(CHAIN) == 'Base')) if vault_event_txs else False
mint_df['vaulted_by'] = np.where(mint_df.remint, 'Cancelled redemption', np.where(mint_df.to.isin(GRAIL_WALLETS | POOLS | {VAULT}), 'Grail', 'User'))
user_mints = mint_df[mint_df.vaulted_by == 'User']
vault_rows = []
for r in user_mints.itertuples():
    p = pos_df[(pos_df.wallet==r.to)&(pos_df.symbol==r.symbol)]
    sold = float(p.sell_usdc.sum()); n_sell = int(p.n_sell.sum()); qty = float(p.qty.sum()) if len(p) else 0.0
    vault_rows.append(dict(wallet=r.to, symbol=r.symbol, minted=r.amount, cards=r.cards, ts=r.ts, block=r.block, sold_usdc=sold, n_sell=n_sell, still_holding=qty, sold_after=(n_sell>0 and p.first_sell_ts.min()>=r.ts)))
vault_df = pd.DataFrame(vault_rows)
burns = xf[xf.to == ZERO].copy(); burns['cards'] = [a / MULT.get(sy, 10000.0) for a, sy in zip(burns.amount, burns.symbol)]
# attribute the burn to the wallet that sent the tokens into the burning contract in the same tx
_in = xf[xf.tx.isin(burns.tx) & ~xf.frm.isin({ZERO}) & xf.to.isin(set(burns.frm))].sort_values('li').drop_duplicates(['tx','symbol']).set_index(['tx','symbol']).frm
burns['redeemer'] = [(_in.get((t, sy)) or f) for t, sy, f in zip(burns.tx, burns.symbol, burns.frm)]
# --- provenance of each physical redemption (burn) and each user vault mint
RESERVE_PRICE = {}
for r in list(reserves) + EXTRA_RESERVES:
    RESERVE_PRICE.setdefault(r['token_symbol'], float(r.get('reserve_price') or 0))
def _hist(w, sym):
    ev = []
    for r in sw[(sw.actor==w)&(sw.symbol==sym)].itertuples():
        ev.append((r.block, ('buy_app' if r.venue=='Grail' else 'buy_dex') if r.side=='buy' else ('sell_app' if r.venue=='Grail' else 'sell_dex'), r.tokens, r.usdc))
    for r in inv[(inv.to==w)&(inv.symbol==sym)].itertuples(): ev.append((r.block, 'pack_pull', r.amount))
    for r in mints[(mints.to==w)&(mints.symbol==sym)].itertuples(): ev.append((r.block, 'vault_mint', r.amount))
    for r in burns[(burns.redeemer==w)&(burns.symbol==sym)].itertuples(): ev.append((r.block, 'burn', r.amount))
    for r in wallet_xf[(wallet_xf.to==w)&(wallet_xf.symbol==sym)].itertuples(): ev.append((r.block, 'xfer_in', r.amount))
    for r in wallet_xf[(wallet_xf.frm==w)&(wallet_xf.symbol==sym)].itertuples(): ev.append((r.block, 'xfer_out', r.amount))
    return sorted(ev)
def _sum(ev, kinds, lo=-1, hi=10**12): return float(sum(e[2] for e in ev if e[1] in kinds and lo < e[0] < hi))
def _usd(ev, kinds, lo=-1, hi=10**12): return float(sum(e[3] for e in ev if e[1] in kinds and len(e) > 3 and lo < e[0] < hi))
def _hrs(b1, b2): return (b2-b1)*2/3600
def fmt_tok(x): return f'{x:,.0f} tokens'
burn_rows = []
for r in burns.itertuples():
    ev = _hist(r.redeemer, r.symbol); b = r.block
    src = dict(app=_sum(ev,{'buy_app'},hi=b), dex=_sum(ev,{'buy_dex'},hi=b), pack=_sum(ev,{'pack_pull'},hi=b), mint=_sum(ev,{'vault_mint'},hi=b), xfer=_sum(ev,{'xfer_in'},hi=b))
    main = max(src, key=src.get)
    origin = {'app':'Bought via Grail app','dex':'Bought on DEX','pack':'Pack pull','mint':'Own vaulted card','xfer':'Received by transfer'}[main]
    claim = nft[(nft.tx==r.tx) & (nft.frm==ZERO)]
    claim_status, claim_id, closed_ts = 'No claim NFT found', None, None
    if len(claim):
        cid = int(claim.token_id.iloc[0]); csym = claim.sym.iloc[0]; claim_id = cid
        life = nft[(nft.sym==csym) & (nft.token_id==cid) & (nft.block >= b)].sort_values('block')
        burn_ev = life[(life.to==ZERO) & (life.block > b)]
        submitted = life[(life.to.isin(GRAIL_WALLETS | {GRAIL_OLD_ROUTER})) & (life.block > b)]
        if len(burn_ev):
            bb = int(burn_ev.block.iloc[0]); closed_ts = GENESIS_TS + 2*bb
            remint = mints[(mints.tx==burn_ev.tx.iloc[0]) & (mints.to==r.redeemer)]
            h = _hrs(b, bb)
            claim_status = (f'Cancelled after {round(h*60)} min, tokens returned' if h < 24 else f'Cancelled after {round(h/24)} d, tokens returned') if len(remint) else f'Shipped: claim closed {round(h/24)} d later'
        elif len(submitted): claim_status = 'Claim submitted, shipment pending'
        else: claim_status = 'Claim NFT still held by redeemer'
    later_sold = _sum(ev,{'sell_app','sell_dex'},lo=b)
    outcome = claim_status + (f'; later sold {fmt_tok(later_sold)}' if later_sold > 0 else '')
    burn_rows.append(dict(symbol=r.symbol, redeemer=r.redeemer, cards=r.cards, ts=r.ts, block=b, origin=origin, claim_id=claim_id, closed_ts=closed_ts, cancelled=claim_status.startswith('Cancelled'), shipped=claim_status.startswith('Shipped'), pending=('pending' in claim_status or 'held' in claim_status), src_app=src['app'], src_dex=src['dex'], src_pack=src['pack'], src_mint=src['mint'], src_xfer=src['xfer'], outcome=outcome))
burn_prov = pd.DataFrame(burn_rows)
vault_prov = []
for r in user_mints.itertuples():
    ev = _hist(r.to, r.symbol); b = r.block
    prior_burn = [e[0] for e in ev if e[1]=='burn' and e[0] < b]
    sold_after = _sum(ev,{'sell_app','sell_dex'},lo=b); burned_after = _sum(ev,{'burn'},lo=b); out_after = _sum(ev,{'xfer_out'},lo=b)
    origin = 'Vaulted after a physical redeem' if prior_burn else 'Vaulted card'
    frac = min(1.0, sold_after / r.amount)
    if burned_after >= r.amount * 0.99: what = 'Shipped back out (burned)'
    elif frac >= 0.9: what = f'Sold the card via tokens ({round(frac*100)}%)'
    elif frac > 0: what = f'Sold {round(frac*100)}% of tokens'
    elif out_after >= r.amount*0.5: what = 'Moved to another wallet'
    else: what = 'Holding'
    mint_price = price_at(r.symbol, b); mint_value = r.amount * mint_price
    sold_usd = _usd(ev, {'sell_app','sell_dex'}, lo=b); sold_tok = min(sold_after, r.amount)
    proceeds = sold_usd * (sold_tok / sold_after) if sold_after > 0 else 0.0          # pro-rate if they sold more than this mint
    held_tok = max(0.0, r.amount - sold_tok - burned_after - out_after)
    held_value = held_tok * PRICE.get(r.symbol, 0.0)
    card_value_now = r.cards * RESERVE_PRICE.get(r.symbol, 0.0)
    vault_prov.append(dict(wallet=r.to, symbol=r.symbol, block=b, origin=origin, what=what, sold_tokens=sold_after, burned_tokens=burned_after,
        mint_price=mint_price, mint_value=mint_value, sold_tok=sold_tok, proceeds=proceeds, avg_sell_price=(proceeds/sold_tok if sold_tok else None),
        held_tok=held_tok, held_value=held_value, total_value=proceeds + held_value, gain_vs_mint=proceeds + held_value - mint_value,
        card_value_now=card_value_now, gain_vs_card=proceeds + held_value - card_value_now))
vault_prov = pd.DataFrame(vault_prov)
vault_summary = dict(registry_cards=int(sum(r['backed_supply'] for r in reserves if CHAIN.get(r['token_symbol']) == 'Base')), minted_cards=float(mint_df[~mint_df.remint].cards.sum()), remint_cards=float(mint_df[mint_df.remint].cards.sum()), grail_cards=float(mint_df[mint_df.vaulted_by=='Grail'].cards.sum()), user_cards=float(user_mints.cards.sum()),
    grail_mints=int((mint_df.vaulted_by=='Grail').sum()), user_mints=int(len(user_mints)), burned_cards=float(burns.cards.sum()), burn_events=int(len(burns)), redemptions_cancelled=int(sum(1 for x in burn_rows if x['cancelled'])), redemptions_shipped=int(sum(1 for x in burn_rows if x['shipped'])), redemptions_pending=int(sum(1 for x in burn_rows if x['pending'])))
vault_monthly = mint_df[~mint_df.remint].assign(month=pd.to_datetime(mint_df.ts, unit='s').dt.strftime('%Y-%m')).groupby(['month','vaulted_by']).cards.sum().unstack(fill_value=0)
reserve_rows = [dict(token=r['token_symbol'], name=r['name'], cards=r['backed_supply'], tokens_per_card=_mult(r['multiplier']), psa_pop=r['psa_pop'], reserve_price=r['reserve_price'], reserve_address=r['reserve_address']) for r in list(reserves) + EXTRA_RESERVES]

# ---------- exclusive mint cohorts (gated packs) : buyers -> redeemed -> sold
cohort_rows, cohort_summary = [], []
_bal = pd.concat([xf[['symbol','to','amount']].rename(columns={'to':'w'}), xf[['symbol','frm','amount']].rename(columns={'frm':'w'}).assign(amount=lambda d: -d.amount)]).groupby(['w','symbol']).amount.sum()
# tokens parked in the pool as liquidity ("earn fees") and tokens burned to take the physical collectible are still "held", not sold
_ns = xf[~xf.tx.isin(set(sw.tx))]
_lp = (_ns[_ns.to.isin(POOLS)].rename(columns={'frm':'w'}).groupby(['w','symbol']).amount.sum()).sub(_ns[_ns.frm.isin(POOLS)].rename(columns={'to':'w'}).groupby(['w','symbol']).amount.sum(), fill_value=0).clip(lower=0)
_lp = _lp.groupby(level=[0,1]).sum(); _lpd = _lp.to_dict(); _bald = _bal.to_dict()
_phys = burn_prov[~burn_prov.outcome.str.startswith('Cancelled')].groupby(['redeemer','symbol']).cards.sum().to_dict() if len(burn_prov) else {}
def eff_balance(w, sy):
    burned = float(_phys.get((w, sy), 0.0)) * MULT.get(sy, 10000.0)
    return max(float(_bald.get((w, sy), 0.0)), 0.0) + float(_lpd.get((w, sy), 0.0)) + burned
def glist_tag(redeemed, kept):
    if not redeemed: return 'Unredeemed'
    return 'Holder' if kept >= 0.5 else ('Partial seller' if kept >= 0.05 else 'Flipper')
if len(pb):
    gated = pb[pb.pack_kind.isin(['LAUNCH','FOUNDER']) | pb.gated.fillna(False)]
    for series, g in gated.groupby('pack_series'):
        tok = None
        for pid in g.pack_id.unique():
            if pid in PACK_TOKEN: tok = PACK_TOKEN[pid]
        buyers = g.groupby('buyer').agg(packs=('qty','sum'), spent=('usdc','sum'), first_buy=('ts','min'), pack_kind=('pack_kind','first'))
        for b, br in buyers.iterrows():
            rd = inv[(inv.to==b) & (inv.ts >= br.first_buy) & ((inv.symbol==tok) if tok else True)]
            redeemed_amt = float(rd.amount.sum()); n_red = len(rd); red_syms = sorted(set(rd.symbol.dropna()))
            pp = pos_df[(pos_df.wallet==b) & (pos_df.symbol.isin(red_syms if red_syms else ([tok] if tok else [])))]
            sells = sw[(sw.actor==b) & (sw.side=='sell') & (sw.symbol.isin(red_syms if red_syms else ([tok] if tok else []))) & (sw.ts >= (rd.ts.min() if n_red else br.first_buy))]
            sold_tokens = float(sells.tokens.sum()); sold_usdc = float(sells.usdc.sum())
            hold_val = float(pp.value.sum())
            red_by_sym = rd.groupby('symbol').amount.sum()
            kept_tok = {sy: min(eff_balance(b, sy), float(amt)) for sy, amt in red_by_sym.items()}
            red_value = float(sum(amt * PRICE.get(sy, 0.0) for sy, amt in red_by_sym.items())); kept_value = float(sum(kept_tok[sy] * PRICE.get(sy, 0.0) for sy in kept_tok))
            kept_pct = (kept_value / red_value) if red_value > 0 else ((sum(kept_tok.values()) / redeemed_amt) if redeemed_amt else 0.0)
            cohort_rows.append(dict(pack_series=series, pack_kind=br.pack_kind, token=tok or ','.join(red_syms), wallet=b, packs=int(br.packs), spent=float(br.spent), first_buy_ts=int(br.first_buy),
                redeemed=n_red>0, n_redeemed=n_red, redeemed_tokens=redeemed_amt, first_redeem_ts=(int(rd.ts.min()) if n_red else None),
                sold=(n_red>0 and kept_pct < 0.95), sold_tokens=sold_tokens, sold_usdc=sold_usdc, kept_pct=kept_pct, pct_sold=((1 - kept_pct) if n_red else 0.0), red_value=red_value, kept_value=kept_value, row_tag=glist_tag(n_red>0, kept_pct),
                first_sell_ts=(int(sells.ts.min()) if len(sells) else None), hours_to_sell=((sells.ts.min()-rd.ts.min())/3600 if (len(sells) and n_red) else None),
                sell_venue=(sells.venue.mode().iloc[0] if len(sells) else None), holding_value=hold_val, net_usdc=sold_usdc - float(br.spent), roi=(sold_usdc + hold_val)/float(br.spent) - 1 if br.spent else None,
                sniper=b in set(sniper_w.index), grail_app_user=bool(w_stats.is_grail_app.get(b, False)) or True))
    cdf = pd.DataFrame(cohort_rows)
    if len(cdf):
        for series, g in cdf.groupby('pack_series'):
            cohort_summary.append(dict(pack_series=series, pack_kind=g.pack_kind.iloc[0], token=g.token.iloc[0], buyers=len(g), packs=int(g.packs.sum()), revenue=float(g.spent.sum()),
                redeemed_pct=float(g.redeemed.mean()), sold_pct=float(g.sold.mean()), sold_of_redeemed_pct=float(g[g.redeemed].sold.mean()) if g.redeemed.any() else 0.0,
                dumped_pct=float((g.row_tag=='Flipper').mean()), holder_pct=float((g.row_tag=='Holder').mean()), partial_pct=float((g.row_tag=='Partial seller').mean()), median_hours_to_sell=float(g.hours_to_sell.dropna().median()) if g.hours_to_sell.notna().any() else None,
                sold_usdc=float(g.sold_usdc.sum()), holding_value=float(g.holding_value.sum()), profitable_pct=float((g.roi.dropna()>0).mean()) if g.roi.notna().any() else None,
                sniper_pct=float(g.sniper.mean())))
else:
    cdf = pd.DataFrame()

# ---------- holders
bal = pd.concat([xf[['symbol','to','amount']].rename(columns={'to':'w'}), xf[['symbol','frm','amount']].rename(columns={'frm':'w'}).assign(amount=lambda d: -d.amount)]).groupby(['symbol','w']).amount.sum().reset_index()
holders = bal[(bal.amount > 1e-6) & ~bal.w.isin(INFRA)]
holders['value'] = holders.amount * holders.symbol.map(PRICE)
holders = holders.join(w_stats[['primary_venue','primary_group','is_grail_app']], on='w')
holders['primary_venue'] = holders.primary_venue.fillna('No swaps (pack/transfer)')
holders['primary_group'] = holders.primary_group.fillna('No swaps (pack/transfer)')

# ---------- aggregates for dashboard
def pct(x): return None if x is None or (isinstance(x,float) and math.isnan(x)) else round(float(x)*100, 1)
def q(df, cols): return json.loads(df[cols].to_json(orient='records')) if len(df) else []
out = {}
out['meta'] = dict(head_block=int(swaps[swaps.symbol.map(CHAIN)=='Base'].block.max()) if len(swaps) else None, head_ts=int(swaps.ts.max()) if len(swaps) else None, chains=sorted(set(CHAIN.values())), generated=int(time.time()), n_tokens=len(TOK), unknown_venues=q(unknown_to.reset_index().head(25), ['tx_to','n','vol']))
out['kpi'] = dict(volume=float(sw.usdc.sum()), swaps=int(len(sw)), traders=int(len(traders)), bots=int(w_stats.bot.sum()), holders=int(holders.w.nunique()),
    profitable_pct=pct(traders.profitable.mean()), realized_profitable_pct=pct((traders.realized>0).mean()), avg_pnl=float(traders.total.mean()) if len(traders) else 0, median_pnl=float(traders.total.median()) if len(traders) else 0,
    total_trader_pnl=float(traders.total.sum()), grail_app_users=int(traders.is_grail_app.sum()), pack_buyers=int(len(pack_buyers)), redeemers=int(inv.to.nunique()), vaulters=int(vault_df.wallet.nunique()) if len(vault_df) else 0,
    snipers=int((sniper_w.tag!='Early buyer').sum()), mcap=float(sum(PRICE[s]*float(TOK[s]['circulating_supply'] or 0) for s in TOK)))
# profitability distributions
bins = [-1e12,-1000,-100,-10,0,10,100,1000,1e12]; labels=['< -$1k','-$1k..-$100','-$100..-$10','-$10..0','0..$10','$10..$100','$100..$1k','> $1k']
out['pnl_hist'] = [dict(bucket=l, n=int(((traders.total>lo)&(traders.total<=hi)).sum())) for l,lo,hi in zip(labels,bins[:-1],bins[1:])]
out['profit_by_group'] = [dict(group=g, traders=int(len(d)), profitable_pct=pct(d.profitable.mean()), avg_pnl=float(d.total.mean()), median_pnl=float(d.total.median()), volume=float(d.volume.sum()), avg_swaps=float(d.n_swaps.mean())) for g,d in traders.groupby('primary_group')]
out['profit_by_venue'] = sorted([dict(venue=g, traders=int(len(d)), profitable_pct=pct(d.profitable.mean()), avg_pnl=float(d.total.mean()), median_pnl=float(d.total.median()), volume=float(d.volume.sum()), total_pnl=float(d.total.sum())) for g,d in traders.groupby('primary_venue')], key=lambda r: -r['traders'])
out['profit_by_usage'] = [dict(usage=g, traders=int(len(d)), profitable_pct=pct(d.profitable.mean()), avg_pnl=float(d.total.mean()), median_pnl=float(d.total.median()), volume=float(d.volume.sum())) for g,d in traders.groupby('usage')]
tp = pos_df[pos_df.wallet.isin(traders.index) & (pos_df.n_buy>0)]
out['profit_by_token'] = sorted([dict(symbol=s, name=NAME[s], traders=int(len(d)), profitable_pct=pct((d.total>0).mean()), avg_pnl=float(d.total.mean()), total_pnl=float(d.total.sum()), volume=float(d.buy_usdc.sum()+d.sell_usdc.sum()), price=PRICE[s]) for s,d in tp.groupby('symbol')], key=lambda r: -r['volume'])
vb = sw.groupby('venue').agg(swaps=('tx','count'), volume=('usdc','sum'), traders=('actor','nunique')).reset_index().sort_values('volume', ascending=False)
vb['group'] = vb.venue.map(venue_group); out['venues'] = q(vb, ['venue','group','swaps','volume','traders'])
gb = sw.groupby('group').agg(swaps=('tx','count'), volume=('usdc','sum'), traders=('actor','nunique')).reset_index().sort_values('volume', ascending=False); out['groups'] = q(gb, ['group','swaps','volume','traders'])
out['usage_split'] = [dict(usage=g, traders=int(len(d)), volume=float(d.volume.sum())) for g,d in traders.groupby('usage')]
sw['week'] = pd.to_datetime(sw.ts, unit='s').dt.to_period('W').dt.start_time.dt.strftime('%Y-%m-%d')
wk = sw.groupby(['week','group']).usdc.sum().unstack(fill_value=0); out['weekly_group_volume'] = dict(weeks=list(wk.index), series={c: [float(x) for x in wk[c]] for c in wk.columns})
daily = sw.groupby('day').agg(volume=('usdc','sum'), swaps=('tx','count'), traders=('actor','nunique')).reset_index()
first_day = sw.groupby('actor').day.min(); newd = first_day.value_counts().rename('new')
daily = daily.join(newd, on='day').fillna({'new':0}); daily['returning'] = daily.traders - daily.new; out['daily'] = q(daily, ['day','volume','swaps','traders','new','returning'])
hv = holders.groupby('primary_group').agg(holders=('w','nunique'), value=('value','sum')).reset_index().sort_values('holders', ascending=False); out['holders_by_group'] = q(hv, ['primary_group','holders','value'])
hv2 = holders.groupby('primary_venue').agg(holders=('w','nunique'), value=('value','sum')).reset_index().sort_values('holders', ascending=False); out['holders_by_venue'] = q(hv2, ['primary_venue','holders','value'])
out['holders_by_token'] = sorted([dict(symbol=s, holders=int(d.w.nunique()), value=float(d.value.sum())) for s,d in holders.groupby('symbol')], key=lambda r:-r['holders'])
# packs
if len(pb):
    ps = pb.groupby(['pack_series','pack_kind']).agg(packs=('qty','sum'), draws=('draws','sum'), buyers=('buyer','nunique'), revenue=('usdc','sum'), first=('ts','min'), last=('ts','max'), gated=('gated','first')).reset_index().sort_values('revenue', ascending=False)
    out['packs'] = q(ps, ['pack_series','pack_kind','gated','packs','draws','buyers','revenue','first','last'])
    out['pack_kpi'] = dict(revenue=float(pb.usdc.sum()), packs=int(pb.qty.sum()), draws=int(pb.draws.sum()), buyers=int(pb.buyer.nunique()), redeems=int(len(inv)), redeemers=int(inv.to.nunique()), redeemed_tokens=float(inv.amount.sum()))
    # redeem behaviour overall: pack buyers who redeemed / sold anything
    pbuy = pb.groupby('buyer').agg(spent=('usdc','sum'), packs=('qty','sum'), first=('ts','min'))
    pbuy['redeemed'] = pbuy.index.isin(set(inv.to)); pbuy['sold'] = pbuy.index.isin(set(sw[sw.side=='sell'].actor)); pbuy['traded'] = pbuy.index.isin(set(sw.actor))
    out['pack_buyer_funnel'] = dict(buyers=int(len(pbuy)), redeemed=int(pbuy.redeemed.sum()), sold=int(pbuy.sold.sum()), traded=int(pbuy.traded.sum()), spent=float(pbuy.spent.sum()))
out['cohorts'] = sorted(cohort_summary, key=lambda r: -r['revenue'])
if os.path.exists('data/parquet/nft_tiers_rh.parquet'):
    _t = pd.read_parquet('data/parquet/nft_tiers_rh.parquet'); out['nft_tiers'] = q(_t, list(_t.columns))
    _m = pd.read_parquet('data/parquet/nft_mints_rh.parquet') if os.path.exists('data/parquet/nft_mints_rh.parquet') else pd.DataFrame()
    if len(_m):
        _m['to'] = _m.to.str.lower(); _m = _m.merge(_t[['collection','name','tokens_per_card']], on='collection', how='left')
        _w = _m.groupby('to').agg(draws=('token_id','count'), tiers=('name', lambda s_: ' · '.join(f"{k}×{v}" for k, v in s_.value_counts().items())), tokens=('tokens_per_card','sum')).reset_index().rename(columns={'to':'wallet'}).sort_values('tokens', ascending=False)
        out['nft_pulls'] = q(_w, ['wallet','draws','tiers','tokens'])
out['cohort_rows'] = q(cdf.sort_values(['pack_series','spent'], ascending=[True,False]), ['pack_series','pack_kind','token','wallet','packs','spent','redeemed','redeemed_tokens','sold','pct_sold','kept_pct','row_tag','sold_usdc','hours_to_sell','sell_venue','holding_value','roi','sniper']) if len(cdf) else []
# redeem timing distribution by hours to sell for launch cohorts
if len(cdf) and cdf.hours_to_sell.notna().any():
    hb=[0,1,6,24,72,168,720,1e9]; hl=['<1h','1-6h','6-24h','1-3d','3-7d','1-4w','>4w']
    out['hours_to_sell_hist']=[dict(bucket=l, n=int(((cdf.hours_to_sell>=lo)&(cdf.hours_to_sell<hi)).sum())) for l,lo,hi in zip(hl,hb[:-1],hb[1:])]
# vaulting
if len(vault_df):
    vault_df = vault_df.merge(vault_prov[['wallet','symbol','block','origin','what','mint_price','mint_value','sold_tok','proceeds','avg_sell_price','held_tok','held_value','total_value','gain_vs_mint','card_value_now','gain_vs_card']], on=['wallet','symbol','block'], how='left')
    out['vault_kpi'] = dict(vaulters=int(vault_df.wallet.nunique()), mints=int(len(vault_df)), cards=float(vault_df.cards.sum()), tokens_minted=float(vault_df.minted.sum()), value_now=float(sum(vault_df.minted*vault_df.symbol.map(PRICE))), sold_any_pct=pct((vault_df.n_sell>0).mean()), sold_after_pct=pct(vault_df.sold_after.mean()), **vault_summary)
    vt = vault_df.groupby('symbol').agg(vaulters=('wallet','nunique'), mints=('wallet','count'), cards=('cards','sum'), minted=('minted','sum'), sold_pct=('sold_after','mean')).reset_index().sort_values('minted', ascending=False)
    out['vault_by_token'] = q(vt, ['symbol','vaulters','mints','cards','minted','sold_pct'])
    out['vault_rows'] = q(vault_df.sort_values('ts'), ['wallet','symbol','minted','cards','ts','origin','what','mint_price','mint_value','sold_tok','proceeds','avg_sell_price','held_tok','held_value','total_value','gain_vs_mint','card_value_now','gain_vs_card'])
    vw = vault_df.groupby('wallet').agg(cards=('cards','sum'), tokens=('symbol', lambda s: ','.join(sorted(set(s)))), mint_value=('mint_value','sum'), proceeds=('proceeds','sum'), held_value=('held_value','sum'), total_value=('total_value','sum'), gain_vs_mint=('gain_vs_mint','sum'), card_value_now=('card_value_now','sum'), gain_vs_card=('gain_vs_card','sum'), first_ts=('ts','min')).reset_index()
    vw['trading_pnl'] = vw.wallet.map(w_stats.total).fillna(0.0); vw['trading_volume'] = vw.wallet.map(w_stats.volume).fillna(0.0)
    out['vault_wallets'] = q(vw.sort_values('gain_vs_mint', ascending=False), ['wallet','cards','tokens','mint_value','proceeds','held_value','total_value','gain_vs_mint','card_value_now','gain_vs_card','trading_pnl','trading_volume','first_ts'])
    out['vault_pnl'] = dict(mint_value=float(vault_df.mint_value.sum()), proceeds=float(vault_df.proceeds.sum()), held_value=float(vault_df.held_value.sum()), total_value=float(vault_df.total_value.sum()),
        gain_vs_mint=float(vault_df.gain_vs_mint.sum()), card_value_now=float(vault_df.card_value_now.sum()), gain_vs_card=float(vault_df.gain_vs_card.sum()),
        wallets_up_vs_mint=int((vw.gain_vs_mint > 0).sum()), wallets_up_vs_card=int((vw.gain_vs_card > 0).sum()), wallets=int(len(vw)), avg_sell_discount=(float(1 - vault_df.proceeds.sum()/ (vault_df.sold_tok*vault_df.mint_price).sum()) if (vault_df.sold_tok*vault_df.mint_price).sum() > 0 else None))
else: out['vault_kpi'] = dict(vaulters=0, mints=0, cards=0, tokens_minted=0, value_now=0, sold_any_pct=0, sold_after_pct=0, **vault_summary); out['vault_by_token']=[]; out['vault_rows']=[]
out['mint_events'] = q(mint_df.sort_values('block'), ['symbol','to','amount','cards','block','ts','vaulted_by'])
out['vault_monthly'] = dict(months=list(vault_monthly.index), grail=[float(x) for x in vault_monthly.get('Grail', pd.Series(0, index=vault_monthly.index))], user=[float(x) for x in vault_monthly.get('User', pd.Series(0, index=vault_monthly.index))])
out['reserves'] = sorted(reserve_rows, key=lambda r: -r['cards'])
out['burns'] = q(burn_prov.sort_values('block'), ['symbol','redeemer','cards','ts','origin','src_app','src_dex','src_pack','src_mint','src_xfer','outcome','claim_id','closed_ts'])
# snipers
out['sniper_kpi'] = dict(snipe_events=int(len(snipes)), sniper_wallets=int((sniper_w.tag!='Early buyer').sum()), serial=int((sniper_w.tag=='Serial sniper').sum()), same_block=int(snipes.same_block.sum()), sold_24h_pct=pct(snipes.sold_within_24h.mean()), profitable_pct=pct((snipes.total>0).mean()), total_pnl=float(snipes.total.sum()))
out['sniper_by_token'] = sorted([dict(symbol=s, snipes=int(len(d)), same_block=int(d.same_block.sum()), spend=float(d.usdc.sum()), pnl=float(d.total.sum()), sold_24h_pct=pct(d.sold_within_24h.mean())) for s,d in snipes.groupby('symbol')], key=lambda r:-r['snipes'])
sniper_rows = sniper_w.reset_index().rename(columns={'actor':'wallet'})
sniper_rows = sniper_rows.join(w_stats[['primary_venue','volume','total']].rename(columns={'total':'total_pnl'}), on='wallet')
out['sniper_rows'] = q(sniper_rows[sniper_rows.tag!='Early buyer'].head(400), ['wallet','tag','tokens_sniped','symbols','same_block','min_delta_min','snipe_spend','pnl','sold_24h','primary_venue','volume','total_pnl'])
# top traders / worst traders
tt = traders.reset_index().rename(columns={'actor':'wallet'})
out['top_traders'] = q(tt.sort_values('volume', ascending=False).head(100), ['wallet','volume','n_swaps','n_tokens','realized','unrealized','total','holdings_value','primary_venue','usage','pack_buyer','redeemer'])
out['top_winners'] = q(tt.sort_values('total', ascending=False).head(50), ['wallet','volume','n_swaps','realized','unrealized','total','primary_venue','usage'])
out['top_losers'] = q(tt.sort_values('total').head(50), ['wallet','volume','n_swaps','realized','unrealized','total','primary_venue','usage'])
# grailist
gl = []
if len(cdf):
    for w, g in cdf.groupby('wallet'):
        ws = w_stats.loc[w] if w in w_stats.index else None
        gl.append(dict(wallet=w, n_launches=int(g.pack_series.nunique()), first_buy_ts=int(g.first_buy_ts.min()), roi=(float((g.sold_usdc.sum()+g.holding_value.sum())/g.spent.sum()-1) if g.spent.sum() > 0 else None), packs=int(g.packs.sum()), series=';'.join(sorted(set(g.pack_series))), spent=float(g.spent.sum()), redeemed_any=bool(g.redeemed.any()), sold_any=bool(g.sold.any()), avg_pct_sold=float(1 - (g.kept_value.sum()/g.red_value.sum())) if g.red_value.sum() > 0 else 0.0, kept_pct=float(g.kept_value.sum()/g.red_value.sum()) if g.red_value.sum() > 0 else 0.0,
            sold_usdc=float(g.sold_usdc.sum()), holding_value=float(g.holding_value.sum()), sniper=bool(g.sniper.any()), sniper_tag=(sniper_w.tag.get(w) if w in sniper_w.index else ''),
            trader_volume=float(ws.volume) if ws is not None else 0.0, trader_pnl=float(ws.total) if ws is not None else 0.0, primary_venue=(ws.primary_venue if ws is not None else 'No swaps'), usage=(ws.usage if ws is not None else 'No swaps'),
            tag=glist_tag(bool(g.redeemed.any()), (float(g.kept_value.sum()/g.red_value.sum()) if g.red_value.sum() > 0 else (float(g[g.redeemed].kept_pct.mean()) if g.redeemed.any() else 0.0)))))
out['grailist'] = sorted(gl, key=lambda r: -r['spent'])
_glp = pd.DataFrame(gl)
if len(_glp):
    out['glist_launch_hist'] = [dict(launches=int(k), wallets=int(v)) for k, v in _glp.n_launches.value_counts().sort_index().items()]
    _c = cdf.copy()
    _order = _c.groupby('pack_series').first_buy_ts.min().sort_values().index
    _t = _c.groupby(['pack_series', 'row_tag']).size().unstack(fill_value=0).reindex(_order)
    out['glist_by_launch'] = dict(launches=list(_t.index), series={k: [int(x) for x in _t[k]] for k in ['Flipper', 'Partial seller', 'Holder', 'Unredeemed'] if k in _t.columns})
    out['glist_money'] = dict(spent=float(_glp.spent.sum()), proceeds=float(_glp.sold_usdc.sum()), held=float(_glp.holding_value.sum()), repeat=int((_glp.n_launches >= 2).sum()), loyal=int(((_glp.n_launches >= 3) & (_glp.tag.isin(['Holder', 'Partial seller']))).sum()),
                             in_profit_pct=pct((_glp.roi.dropna() > 0).mean()) if _glp.roi.notna().any() else None, trading_pnl=float(_glp.trader_pnl.sum()))
out['grailist_kpi'] = dict(wallets=len(gl), flippers=sum(1 for r in gl if r['tag']=='Flipper'), holders=sum(1 for r in gl if r['tag']=='Holder'), unredeemed=sum(1 for r in gl if r['tag']=='Unredeemed'), partial=sum(1 for r in gl if r['tag']=='Partial seller'), snipers=sum(1 for r in gl if r['sniper']))
out['tokens'] = [dict(symbol=s, chain=CHAIN[s], quote=(TOK[s].get('peg_ticker') or 'USDC'), mcap=PRICE[s]*float(TOK[s]['circulating_supply'] or 0), name=NAME[s], price=PRICE[s], supply=float(TOK[s]['total_supply'] or 0), tags=TOK[s]['tags'], launch_block=(int(tokinfo.launch_block[s]) if pd.notna(tokinfo.launch_block[s]) else None), volume=float(sw[sw.symbol==s].usdc.sum()), traders=int(sw[sw.symbol==s].actor.nunique())) for s in TOK]
out['tokens'].sort(key=lambda r: -r['volume'])
os.makedirs('out', exist_ok=True)
# derived tables for ad-hoc queries
pos_df.to_parquet('data/parquet/derived_positions.parquet', index=False)
holders.to_parquet('data/parquet/derived_holders.parquet', index=False)
snipes.to_parquet('data/parquet/derived_snipes.parquet', index=False)
w_stats.reset_index().rename(columns={'actor':'wallet'}).drop(columns=['venues']).to_parquet('data/parquet/derived_wallets.parquet', index=False)
sw.drop(columns=['week'], errors='ignore').to_parquet('data/parquet/derived_swaps.parquet', index=False)
pd.DataFrame([dict(symbol=s_, chain=CHAIN[s_], price=PRICE[s_], circulating=float(TOK[s_]['circulating_supply'] or 0), total_supply=float(TOK[s_]['total_supply'] or 0), mcap=PRICE[s_]*float(TOK[s_]['circulating_supply'] or 0)) for s_ in TOK]).to_parquet('data/parquet/derived_tokens.parquet', index=False)
json.dump(out, open('out/analysis.json','w'), default=lambda o: None if (isinstance(o,float) and math.isnan(o)) else (o.item() if hasattr(o,'item') else str(o)))
pd.DataFrame(out['grailist']).to_csv('out/grailist.csv', index=False)
if len(cdf): cdf.to_csv('out/exclusive_mint_cohorts.csv', index=False)
sniper_rows.to_csv('out/snipers.csv', index=False)
tt.to_csv('out/traders.csv', index=False)
if len(vault_df): vault_df.to_csv('out/vaulters.csv', index=False)
print('written out/analysis.json', 'kpi', out['kpi']); print('unknown venues', unknown_to.head(12))
