"""Did the snipers dump? Per token: sniper cohort's buys, sells, what they still hold, realized P&L,
and who has been doing the selling in the last N days (snipers vs everyone else).
Usage: python sniper_flows.py gVLAD,gSPEED,... [--days 4]"""
import argparse, json
import pandas as pd, numpy as np
ap = argparse.ArgumentParser(); ap.add_argument('tokens'); ap.add_argument('--days', type=float, default=4.0); A = ap.parse_args()
sw = pd.read_parquet('data/parquet/derived_swaps.parquet'); sn = pd.read_parquet('data/parquet/derived_snipes.parquet')
hold = pd.read_parquet('data/parquet/derived_holders.parquet'); pos = pd.read_parquet('data/parquet/derived_positions.parquet')
tok = pd.read_parquet('data/parquet/derived_tokens.parquet').set_index('symbol')
now = sw.ts.max(); cut = now - A.days * 86400
rows, daily = [], []
for s in A.tokens.split(','):
    S = sw[sw.symbol == s].copy(); snipers = set(sn[sn.symbol == s].actor)
    S['is_sniper'] = S.actor.isin(snipers); S['date'] = pd.to_datetime(S.ts, unit='s').dt.strftime('%m-%d')
    b = S[S.side == 'buy']; se = S[S.side == 'sell']
    sb, ss = b[b.is_sniper], se[se.is_sniper]
    H = hold[hold.symbol == s].set_index('w').amount; circ = tok.circulating[s]
    held = H[H.index.isin(snipers)]
    P = pos[(pos.symbol == s) & pos.wallet.isin(snipers)]
    recent = S[S.ts >= cut]; rs = recent[recent.side == 'sell']; rb = recent[recent.side == 'buy']
    px = S.sort_values(['block', 'li']); p_now = px.price.iloc[-1]; p_cut = px[px.ts <= cut].price.iloc[-1] if (px.ts <= cut).any() else px.price.iloc[0]; p_ath = px.price.max()
    rows.append(dict(token=s, snipers=len(snipers), bought_tok=sb.tokens.sum(), sold_tok=ss.tokens.sum(), sold_pct_of_bought=ss.tokens.sum()/max(sb.tokens.sum(), 1e-9),
        fully_out=int((~pd.Index(list(snipers)).isin(held[held > 0].index)).sum()), still_holding=int((held > 0).sum()), held_tok=held.sum(), held_pct_circ=held.sum()/circ,
        spent_usd=sb.usdc.sum(), sold_usd=ss.usdc.sum(), realized=P.realized.sum(), unrealized=P.unrealized.sum(),
        recent_sell_usd_snipers=rs[rs.is_sniper].usdc.sum(), recent_sell_usd_others=rs[~rs.is_sniper].usdc.sum(), sniper_share_of_recent_sells=rs[rs.is_sniper].usdc.sum()/max(rs.usdc.sum(), 1e-9),
        recent_net_tok_snipers=rb[rb.is_sniper].tokens.sum() - rs[rs.is_sniper].tokens.sum(), recent_net_tok_others=rb[~rb.is_sniper].tokens.sum() - rs[~rs.is_sniper].tokens.sum(),
        price_now=p_now, price_change_recent=p_now/p_cut - 1, drawdown_from_ath=p_now/p_ath - 1))
    d = S[S.ts >= now - 8*86400].groupby(['date', 'is_sniper', 'side']).agg(usd=('usdc', 'sum'), tok=('tokens', 'sum')).reset_index(); d['token'] = s; daily.append(d)
R = pd.DataFrame(rows); D = pd.concat(daily)
pd.set_option('display.width', 250); pd.set_option('display.float_format', lambda x: f'{x:,.3f}')
print('data through', pd.to_datetime(now, unit='s'), '| recent window', A.days, 'days\n')
print(R[['token','snipers','sold_pct_of_bought','fully_out','still_holding','held_tok','held_pct_circ','spent_usd','sold_usd','realized','unrealized']].to_string(index=False))
print('\nRECENT SELLING'); print(R[['token','price_now','price_change_recent','drawdown_from_ath','recent_sell_usd_snipers','recent_sell_usd_others','sniper_share_of_recent_sells','recent_net_tok_snipers','recent_net_tok_others']].to_string(index=False))
piv = D.pivot_table(index=['token', 'date'], columns=['is_sniper', 'side'], values='usd', fill_value=0)
print('\nDAILY USD (sniper=True/False x buy/sell)'); print(piv.round(0).to_string())
R.to_csv('out/sniper_flows.csv', index=False)
