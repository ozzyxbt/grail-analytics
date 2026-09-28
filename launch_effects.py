"""Last-N-day platform analysis: launch effects on total volume, day-of-week pattern, and tokens that rise with volume.
Usage: python launch_effects.py [days=90]"""
import sys, json
import pandas as pd, numpy as np
DAYS = int(sys.argv[1]) if len(sys.argv) > 1 else 90
sw = pd.read_parquet('data/parquet/derived_swaps.parquet'); tok = pd.read_parquet('data/parquet/derived_tokens.parquet').set_index('symbol')
lp = pd.concat([pd.read_parquet('data/parquet/lp.parquet'), pd.read_parquet('data/parquet/lp_rh.parquet')], ignore_index=True)
sw['date'] = pd.to_datetime(sw.ts, unit='s').dt.floor('D')
end = sw.date.max(); start = end - pd.Timedelta(days=DAYS - 1)
launch = lp[lp.kind == 'mint'].groupby('symbol').ts.min().pipe(lambda s: pd.to_datetime(s, unit='s').dt.floor('D'))
launch = launch[(launch >= start) & (launch <= end)].sort_values()
W = sw[(sw.date >= start) & (sw.date <= end)].copy()
daily = W.groupby('date').usdc.sum().reindex(pd.date_range(start, end), fill_value=0.0)
daily_tok = W.groupby(['date', 'symbol']).usdc.sum().unstack(fill_value=0.0).reindex(pd.date_range(start, end), fill_value=0.0)
traders = W.groupby('date').actor.nunique().reindex(pd.date_range(start, end), fill_value=0)

# ---- 1. launch effects
rows = []
for sym, d in launch.items():
    pre = daily[(daily.index >= d - pd.Timedelta(days=7)) & (daily.index < d)]
    post = daily[(daily.index > d) & (daily.index <= d + pd.Timedelta(days=7))]
    own = daily_tok[sym] if sym in daily_tok else pd.Series(0.0, index=daily.index)
    others_pre = (daily - own)[(daily.index >= d - pd.Timedelta(days=7)) & (daily.index < d)]
    others_post = (daily - own)[(daily.index > d) & (daily.index <= d + pd.Timedelta(days=7))]
    P = W[W.symbol == sym].sort_values(['block', 'li'])
    p0 = P.price.iloc[0] if len(P) else np.nan
    p7 = P[P.date <= d + pd.Timedelta(days=7)].price.iloc[-1] if len(P) else np.nan
    pnow = tok.price.get(sym, np.nan)
    rows.append(dict(token=sym, launch=d.date(), weekday=d.day_name(), launch_day_volume=daily.get(d, 0.0), own_launch_day=own.get(d, 0.0),
        pre7_avg=pre.mean(), post7_avg=post.mean(), post_vs_pre=(post.mean() / pre.mean() - 1) if pre.mean() > 0 else np.nan,
        others_pre7_avg=others_pre.mean(), others_post7_avg=others_post.mean(), spillover=(others_post.mean() / others_pre.mean() - 1) if others_pre.mean() > 0 else np.nan,
        own_7d_volume=own[(own.index >= d) & (own.index <= d + pd.Timedelta(days=7))].sum(), own_share_of_post7=own[(own.index > d) & (own.index <= d + pd.Timedelta(days=7))].sum() / max(post.sum(), 1e-9),
        first_price=p0, price_7d=p7, ret_7d=(p7 / p0 - 1) if p0 else np.nan, ret_to_now=(pnow / p0 - 1) if p0 else np.nan, traders_launch_day=int(traders.get(d, 0))))
L = pd.DataFrame(rows)

# ---- 2. day of week
dow = pd.DataFrame({'volume': daily, 'traders': traders}); dow['weekday'] = dow.index.day_name(); dow['dow'] = dow.index.dayofweek
base = daily.rolling(14, min_periods=5).median().shift(1)
dow['spike'] = daily > 1.5 * base
dow['is_launch'] = dow.index.isin(launch.values)
dow['spike_no_launch'] = dow.spike & ~dow.is_launch
D = dow.groupby(['dow', 'weekday']).agg(days=('volume', 'size'), avg_volume=('volume', 'mean'), median_volume=('volume', 'median'), avg_traders=('traders', 'mean'), spike_days=('spike', 'sum'), spikes_excl_launch=('spike_no_launch', 'sum'), launches=('is_launch', 'sum')).reset_index().sort_values('dow')
D['share_of_volume'] = D.avg_volume * D.days / (D.avg_volume * D.days).sum()
# same, excluding launch days and the 3 days after each launch
mask = pd.Series(True, index=daily.index)
for d in launch.values:
    mask[(daily.index >= d) & (daily.index <= d + pd.Timedelta(days=3))] = False
D2 = dow[mask].groupby('weekday').volume.mean().rename('avg_volume_excl_launch_windows')
D = D.merge(D2, left_on='weekday', right_index=True, how='left')

# ---- 3. tokens that rise with volume
# daily close price per token = last swap price of the day; forward-filled
px = W.sort_values(['block', 'li']).groupby(['date', 'symbol']).price.last().unstack().reindex(pd.date_range(start, end)).ffill()
ret = px.pct_change()
hi = daily >= daily.quantile(0.75); lo = daily <= daily.quantile(0.25)
tokrows = []
for s in px.columns:
    r = ret[s].dropna()
    if len(r) < 10: continue
    hi_r = r[hi.reindex(r.index).fillna(False)]; lo_r = r[lo.reindex(r.index).fillna(False)]
    v = daily_tok[s] if s in daily_tok else pd.Series(0.0, index=daily.index)
    corr_v = np.corrcoef(daily.reindex(r.index), r)[0, 1] if r.std() > 0 else np.nan          # price change vs platform volume
    corr_own = np.corrcoef(v.reindex(r.index), r)[0, 1] if r.std() > 0 and v.std() > 0 else np.nan
    # post-launch spillover: average 7-day return of this token after other tokens' launches
    spill = []
    for sym, d in launch.items():
        if sym == s: continue
        a = px[s].get(d - pd.Timedelta(days=1)); b = px[s].get(d + pd.Timedelta(days=7))
        if pd.notna(a) and pd.notna(b) and a > 0: spill.append(b / a - 1)
    tokrows.append(dict(token=s, days=len(r), avg_ret_high_vol_days=hi_r.mean(), avg_ret_low_vol_days=lo_r.mean(), diff=hi_r.mean() - lo_r.mean(), pct_up_on_high_vol=(hi_r > 0).mean() if len(hi_r) else np.nan,
        corr_platform_volume=corr_v, corr_own_volume=corr_own, avg_ret_after_others_launch=np.mean(spill) if spill else np.nan, n_launch_windows=len(spill),
        ret_period=(px[s].dropna().iloc[-1] / px[s].dropna().iloc[0] - 1) if px[s].notna().any() else np.nan, volume_period=v.sum()))
T = pd.DataFrame(tokrows).sort_values('diff', ascending=False)

# ---- weekly platform totals
wk = daily.resample('W-MON', label='left', closed='left').sum()
pd.set_option('display.width', 260); pd.set_option('display.float_format', lambda x: f'{x:,.3f}')
print(f'window {start.date()} .. {end.date()} ({DAYS} days), total volume {daily.sum():,.0f}\n')
print('LAUNCHES'); print(L.to_string(index=False))
print('\nDAY OF WEEK'); print(D[['weekday','days','avg_volume','median_volume','avg_volume_excl_launch_windows','share_of_volume','avg_traders','spike_days','spikes_excl_launch','launches']].to_string(index=False))
print('\nWEEKLY VOLUME'); print(wk.to_string())
print('\nTOKENS vs VOLUME (sorted by avg return on high-volume days minus low-volume days)'); print(T.to_string(index=False))
L.to_csv('out/launch_effects.csv', index=False); D.to_csv('out/day_of_week.csv', index=False); T.to_csv('out/tokens_vs_volume.csv', index=False)
json.dump(dict(daily=[dict(date=str(d.date()), volume=float(v), traders=int(traders[d]), spike=bool(dow.spike[d]), launch=[s for s, dd in launch.items() if dd == d]) for d, v in daily.items()],
               launches=json.loads(L.to_json(orient='records', date_format='iso')), dow=json.loads(D.to_json(orient='records')), tokens=json.loads(T.to_json(orient='records'))), open('out/launch_effects.json', 'w'))
