"""Deep-dive on the top-N gTokens by market cap: sniper share, supply held by snipers,
top-20 holder sell behaviour, and aggregate cost basis of current holders.
Usage: python topn.py [N]   (reads data/parquet/derived_*.parquet written by analysis.py)"""
import sys, json
import pandas as pd, numpy as np
import argparse
ap = argparse.ArgumentParser(); ap.add_argument('n', nargs='?', type=int, default=5); ap.add_argument('--exclude', default=''); ap.add_argument('--include', default='')
A = ap.parse_args(); N = A.n
tok = pd.read_parquet('data/parquet/derived_tokens.parquet').sort_values('mcap', ascending=False)
excl = {x for x in A.exclude.split(',') if x}; incl = [x for x in A.include.split(',') if x]
top = incl + [s for s in tok.symbol if s not in excl and s not in incl][:max(0, N - len(incl))]
sw = pd.read_parquet('data/parquet/derived_swaps.parquet')
sn = pd.read_parquet('data/parquet/derived_snipes.parquet')
pos = pd.read_parquet('data/parquet/derived_positions.parquet')
hold = pd.read_parquet('data/parquet/derived_holders.parquet')
wal = pd.read_parquet('data/parquet/derived_wallets.parquet').set_index('wallet')
rows, top20_rows = [], []
for s in top:
    t = tok[tok.symbol == s].iloc[0]
    S = sw[sw.symbol == s]; buys = S[S.side == 'buy']; sells = S[S.side == 'sell']
    buyers = set(buys.actor)
    snipers = set(sn[sn.symbol == s].actor)
    sniper_buy_tokens = buys[buys.actor.isin(snipers)].tokens.sum()
    H = hold[hold.symbol == s].copy()
    held_total = H.amount.sum()
    held_by_snipers = H[H.w.isin(snipers)].amount.sum()
    # top-20 holders: sells vs inflows
    H = H.sort_values('amount', ascending=False)
    top20 = H.head(20).copy()
    bought = buys.groupby('actor').tokens.sum(); sold = sells.groupby('actor').tokens.sum()
    P = pos[pos.symbol == s].set_index('wallet')
    top20['bought'] = top20.w.map(bought).fillna(0); top20['sold'] = top20.w.map(sold).fillna(0)
    top20['redeemed'] = top20.w.map(P.redeemed).fillna(0); top20['xfer_in'] = top20.w.map(P.unpriced_in).fillna(0)
    top20['inflow'] = top20.bought + top20.redeemed + top20.xfer_in
    top20['sell_ratio'] = np.where(top20.inflow > 0, top20.sold / top20.inflow, 0)
    top20['is_sniper'] = top20.w.isin(snipers)
    t20_sell_ratio = top20.sold.sum() / max(top20.inflow.sum(), 1e-9)
    # cost basis of current holders
    ph = P[P.index.isin(H.w)]
    cost = (ph.value - ph.unrealized).clip(lower=0)   # remaining average-cost basis
    qty = ph.qty
    avg_cost = cost.sum() / qty.sum() if qty.sum() > 0 else None
    rows.append(dict(token=s, chain=t.chain, mcap=t.mcap, price=t.price, circulating=t.circulating,
        buyers=len(buyers), snipers=len(snipers), sniper_share_of_buyers=len(snipers)/max(len(buyers),1),
        sniper_share_of_buy_volume=sniper_buy_tokens / max(buys.tokens.sum(), 1e-9),
        holders=len(H), supply_held_by_wallets=held_total, sniper_held=held_by_snipers,
        sniper_held_pct_of_circ=held_by_snipers / max(t.circulating, 1e-9), sniper_held_pct_of_wallet_supply=held_by_snipers / max(held_total, 1e-9),
        top20_hold=top20.amount.sum(), top20_pct_of_circ=top20.amount.sum()/max(t.circulating,1e-9), top20_sold=top20.sold.sum(), top20_inflow=top20.inflow.sum(), top20_sell_ratio=t20_sell_ratio,
        top20_never_sold=int((top20.sold == 0).sum()), top20_snipers=int(top20.is_sniper.sum()),
        holders_cost_total=float(cost.sum()), holders_tokens_with_cost=float(qty.sum()), holders_avg_cost=avg_cost, holders_value_now=float(qty.sum()*t.price),
        holders_unrealized=float(qty.sum()*t.price - cost.sum()), holders_in_profit_pct=float(((ph.value - ph.unrealized) < ph.value).mean()) if len(ph) else None))
    top20_rows.append(top20.assign(symbol=s)[['symbol','w','amount','bought','redeemed','xfer_in','sold','sell_ratio','is_sniper']])
R = pd.DataFrame(rows)
pd.set_option('display.width', 250); pd.set_option('display.float_format', lambda x: f'{x:,.2f}')
print('TOKENS:', top, '\n')
print(R[['token','chain','mcap','price','buyers','snipers','sniper_share_of_buyers','sniper_share_of_buy_volume']].to_string(index=False))
print('\nSUPPLY HELD BY SNIPERS')
print(R[['token','circulating','supply_held_by_wallets','sniper_held','sniper_held_pct_of_circ','sniper_held_pct_of_wallet_supply']].to_string(index=False))
print('\nTOP-20 HOLDERS: SELL BEHAVIOUR (lowest sell ratio = least selling)')
print(R[['token','top20_hold','top20_pct_of_circ','top20_inflow','top20_sold','top20_sell_ratio','top20_never_sold','top20_snipers']].sort_values('top20_sell_ratio').to_string(index=False))
print('\nCURRENT HOLDERS: COST BASIS')
print(R[['token','holders','holders_tokens_with_cost','holders_cost_total','holders_avg_cost','price','holders_value_now','holders_unrealized','holders_in_profit_pct']].to_string(index=False))
R.to_csv('out/top_tokens_deepdive.csv', index=False)
pd.concat(top20_rows).to_csv('out/top_tokens_top20_holders.csv', index=False)
json.dump(json.loads(R.to_json(orient='records')), open('out/top_tokens_deepdive.json', 'w'))
