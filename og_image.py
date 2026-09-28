"""Render the social-share card (1200x630) in the dashboard's own style from out/analysis.json -> site/og.png"""
import json, os, time
from PIL import Image, ImageDraw, ImageFont
d = json.load(open('out/analysis.json')); K = d['kpi']
W, H = 1200, 630
GROUND, PANEL, LINE, INK, INK2, INK3, GOLD, GOOD, BAD = '#0e0f12', '#16181d', '#2a2e36', '#ece9e1', '#b0aea6', '#7c7e86', '#d9b04c', '#3ccb7a', '#ef6a60'
F = 'assets/fonts/'
def font(name, size, wght=None):
    f = ImageFont.truetype(F + name, size)
    if wght is not None:
        try: f.set_variation_by_axes([a if i else wght for i, a in enumerate(f.get_variation_axes() and [wght] + [ax['default'] for ax in f.get_variation_axes()[1:]])])
        except Exception: pass
    return f
def bric(size, wght=800):
    f = ImageFont.truetype(F + 'BricolageGrotesque.ttf', size)
    try:
        axes = f.get_variation_axes(); vals = []
        for ax in axes:
            n = ax['name'] if isinstance(ax['name'], str) else ax['name'].decode()
            vals.append(wght if n.lower().startswith('weight') else (ax['default']))
        f.set_variation_by_axes(vals)
    except Exception: pass
    return f
mono = lambda s: ImageFont.truetype(F + 'IBMPlexMono-Medium.ttf', s)
def sans(s, wght=400):
    f = ImageFont.truetype(F + 'IBMPlexSans-Regular.ttf', s)
    try:
        axes = f.get_variation_axes(); f.set_variation_by_axes([wght if (ax['name'] if isinstance(ax['name'], str) else ax['name'].decode()).lower().startswith('weight') else ax['default'] for ax in axes])
    except Exception: pass
    return f
def usd(v):
    return f'${v/1e6:.1f}M' if v >= 1e6 else (f'${v/1e3:.0f}K' if v >= 1e4 else f'${v:,.0f}')

img = Image.new('RGB', (W, H), GROUND); dr = ImageDraw.Draw(img)
# subtle slab grid on the right
for x in range(760, W, 40):
    dr.line([(x, 0), (x, H)], fill='#131519', width=1)
for y in range(0, H, 40):
    dr.line([(760, y), (W, y)], fill='#131519', width=1)
# gold rule + eyebrow
dr.rectangle([64, 64, 64 + 56, 64 + 4], fill=GOLD)
dr.text((64, 88), 'GRAIL · BASE + ROBINHOOD CHAIN · ON-CHAIN', font=mono(18), fill=INK3)
# title
dr.text((60, 124), 'Grail', font=bric(118, 800), fill=INK)
dr.text((60, 232), 'Slab Report', font=bric(118, 800), fill=GOLD)
dr.text((64, 372), 'Who made money, who sniped, who flipped their GLIST mint,', font=sans(26), fill=INK2)
dr.text((64, 404), 'and where the volume really comes from.', font=sans(26), fill=INK2)
# KPI slab
tiles = [('VOLUME', usd(K['volume']), INK), ('TRADERS', f"{K['traders']:,}", INK), ('IN PROFIT', f"{K['profitable_pct']:.0f}%", GOOD if K['profitable_pct'] >= 50 else BAD), ('GLIST', f"{d['grailist_kpi']['wallets']:,}", GOLD)]
x0, y0, tw, th, gap = 64, 468, 250, 98, 14
for i, (lab, val, col) in enumerate(tiles):
    x = x0 + i * (tw + gap)
    dr.rectangle([x, y0, x + tw, y0 + th], fill=PANEL, outline=LINE)
    dr.text((x + 16, y0 + 14), lab, font=mono(15), fill=INK3)
    dr.text((x + 16, y0 + 40), val, font=mono(38), fill=col)
# updated stamp
stamp = time.strftime('updated %Y-%m-%d %H:%M UTC', time.gmtime(d['meta']['generated']))
dr.text((W - 64 - dr.textlength(stamp, font=mono(16)), 64), stamp, font=mono(16), fill=INK3)
os.makedirs('site', exist_ok=True); img.save('site/og.png', optimize=True)
print('site/og.png', os.path.getsize('site/og.png') // 1024, 'KB')
