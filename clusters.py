"""Wallet clusters ("bundles") per token, Grail team wallets already excluded by analysis.py.
Links: (a) launch-window first buys in the same block through the same front-end contract (>=3 wallets),
       (b) one transaction delivering the token to >=2 distinct wallets,
       (c) wallet-to-wallet token transfers outside swap txs.
Union-find over those links; a cluster of >=3 wallets counts as a bundle.
Usage: python clusters.py gVLAD,gSPEED,... [--window-min 5]"""
import sys, argparse, json
import pandas as pd, numpy as np
ap = argparse.ArgumentParser(); ap.add_argument('tokens'); ap.add_argument('--window-min', type=float, default=5.0); ap.add_argument('--min-size', type=int, default=3)
A = ap.parse_args(); TOKENS = A.tokens.split(',')
sw = pd.read_parquet('data/parquet/derived_swaps.parquet'); pos = pd.read_parquet('data/parquet/derived_positions.parquet')
hold = pd.read_parquet('data/parquet/derived_holders.parquet'); tok = pd.read_parquet('data/parquet/derived_tokens.parquet').set_index('symbol')
xf = pd.concat([pd.read_parquet('data/parquet/transfers.parquet'), pd.read_parquet('data/parquet/transfers_rh.parquet')], ignore_index=True)
for c in ('frm', 'to'): xf[c] = xf[c].str.lower()
lp = pd.concat([pd.read_parquet('data/parquet/lp.parquet'), pd.read_parquet('data/parquet/lp_rh.parquet')], ignore_index=True)
ZERO = '0x' + '0'*40
wallets_all = set(sw.actor) | set(hold.w)
infra = set(xf.frm) | set(xf.to)
infra = (infra - wallets_all) | {ZERO}          # anything that never acts as a trader/holder (pools, routers, vault, team) is infra

class UF:
    def __init__(self): self.p = {}
    def f(self, x):
        self.p.setdefault(x, x)
        while self.p[x] != x: self.p[x] = self.p[self.p[x]]; x = self.p[x]
        return x
    def u(self, a, b): self.p[self.f(a)] = self.f(b)

rows, detail = [], []
for s in TOKENS:
    S = sw[sw.symbol == s]; buys = S[S.side == 'buy']
    launch_ts = lp[(lp.symbol == s) & (lp.kind == 'mint')].ts.min()
    if pd.isna(launch_ts): launch_ts = S.ts.min()
    fb = buys.sort_values('block').drop_duplicates('actor')
    fb = fb[fb.ts <= launch_ts + A.window_min * 60]
    uf = UF(); reasons = {}
    # (a) same block + same front-end contract during the launch window
    for (blk, to), g in fb.groupby(['block', fb.tx_to.fillna('?')]):
        ws = list(g.actor.unique())
        if len(ws) >= A.min_size:
            for w in ws[1:]: uf.u(ws[0], w)
            for w in ws: reasons.setdefault(w, set()).add('same-block')
    # (b) a wallet (not a pool/router/vault) sending the token to 2..20 distinct wallets in one non-swap tx = distribution to sub-wallets
    swap_txs = set(S.tx)
    dist = xf[(xf.symbol == s) & ~xf.frm.isin(infra) & ~xf.to.isin(infra) & ~xf.tx.isin(swap_txs) & (xf.frm != xf.to)]
    for (tx, frm), g in dist.groupby(['tx', 'frm']):
        ws = sorted(set(g.to) | {frm})
        if 3 <= len(ws) <= 21:
            for w in ws[1:]: uf.u(ws[0], w)
            for w in ws: reasons.setdefault(w, set()).add('multi-wallet-tx')
    # (c) wallet-to-wallet transfers outside swap txs, ignoring hubs (a wallet with more than 6 distinct counterparties in this token)
    w2w = xf[(xf.symbol == s) & ~xf.frm.isin(infra) & ~xf.to.isin(infra) & ~xf.tx.isin(swap_txs) & (xf.frm != xf.to)]
    deg = pd.concat([w2w.groupby('frm').to.nunique(), w2w.groupby('to').frm.nunique()]).groupby(level=0).sum()
    hubs = set(deg[deg > 6].index)
    for r in w2w[~w2w.frm.isin(hubs) & ~w2w.to.isin(hubs)].itertuples():
        uf.u(r.frm, r.to); reasons.setdefault(r.frm, set()).add('transfer'); reasons.setdefault(r.to, set()).add('transfer')
    members = {}
    for w in list(uf.p): members.setdefault(uf.f(w), set()).add(w)
    clusters = [m for m in members.values() if len(m) >= A.min_size]
    cw = set().union(*clusters) if clusters else set()
    H = hold[hold.symbol == s]; held_total = H.amount.sum(); circ = tok.circulating[s]
    bought_tok = buys.groupby('actor').tokens.sum(); sold_tok = S[S.side == 'sell'].groupby('actor').tokens.sum(); held = H.set_index('w').amount
    launch_buys = buys[buys.ts <= launch_ts + A.window_min * 60]
    buyers = set(buys.actor)
    rows.append(dict(token=s, buyers=len(buyers), clusters=len(clusters), clustered_wallets=len(cw), clustered_pct_of_buyers=len(cw & buyers) / max(len(buyers), 1),
        launch_buy_tokens=launch_buys.tokens.sum(), launch_buy_by_clusters=launch_buys[launch_buys.actor.isin(cw)].tokens.sum(),
        launch_bundle_pct=launch_buys[launch_buys.actor.isin(cw)].tokens.sum() / max(launch_buys.tokens.sum(), 1e-9),
        all_buys_by_clusters_pct=bought_tok[bought_tok.index.isin(cw)].sum() / max(bought_tok.sum(), 1e-9),
        cluster_held=held[held.index.isin(cw)].sum(), cluster_held_pct_circ=held[held.index.isin(cw)].sum() / max(circ, 1e-9), cluster_held_pct_wallets=held[held.index.isin(cw)].sum() / max(held_total, 1e-9),
        largest_cluster=max((len(m) for m in clusters), default=0)))
    for m in sorted(clusters, key=len, reverse=True)[:8]:
        b = bought_tok[bought_tok.index.isin(m)].sum(); sd = sold_tok[sold_tok.index.isin(m)].sum(); h = held[held.index.isin(m)].sum()
        why = set().union(*(reasons.get(w, set()) for w in m))
        fbm = fb[fb.actor.isin(m)]
        detail.append(dict(token=s, size=len(m), bought=b, sold=sd, sold_pct=sd / max(b, 1e-9), holds=h, holds_pct_circ=h / max(circ, 1e-9),
            launch_wallets=int(fbm.actor.nunique()), first_block=int(fbm.block.min()) if len(fbm) else None, venues=','.join(sorted(set(fbm.venue))) if len(fbm) else '', links=','.join(sorted(why)), sample=sorted(m)[:3]))
R = pd.DataFrame(rows); D = pd.DataFrame(detail)
pd.set_option('display.width', 260); pd.set_option('display.float_format', lambda x: f'{x:,.2f}'); pd.set_option('display.max_colwidth', 60)
print(f'window {A.window_min} min, min cluster size {A.min_size}\n')
print(R[['token','buyers','clusters','clustered_wallets','clustered_pct_of_buyers','largest_cluster']].to_string(index=False))
print('\nLAUNCH-WINDOW BUNDLE SHARE (tokens bought in the window by clustered wallets / all window buys) and what they hold now')
print(R[['token','launch_buy_tokens','launch_buy_by_clusters','launch_bundle_pct','all_buys_by_clusters_pct','cluster_held','cluster_held_pct_circ','cluster_held_pct_wallets']].to_string(index=False))
print('\nLARGEST CLUSTERS')
print(D.drop(columns=['sample']).to_string(index=False))
R.to_csv('out/clusters_summary.csv', index=False); D.to_csv('out/clusters_detail.csv', index=False)
json.dump(dict(summary=json.loads(R.to_json(orient='records')), detail=json.loads(D.drop(columns=['sample']).to_json(orient='records'))), open('out/clusters.json', 'w'))
