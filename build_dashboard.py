"""Build the dashboard twice from out/analysis.json:
  out/grail_dashboard.html  - artifact fragment (no document skeleton; the Claude artifact host wraps it)
  site/index.html           - full standalone document for static hosting (GitHub Pages), plus CSV downloads
"""
import os, shutil
import json as _json
_d = _json.load(open('out/analysis.json'))
for key, fn in (('deepdive', 'out/top_tokens_deepdive.json'), ('launch', 'out/launch_effects.json'), ('clusters', 'out/clusters.json')):
    try: _d[key] = _json.load(open(fn))
    except FileNotFoundError: _d[key] = None
data = _json.dumps(_d).replace('</script', '<\\/script')
tpl = open('template.html').read().replace('__DATA__', data)
open('out/grail_dashboard.html', 'w').write(tpl.replace('__CSV_LINKS__', '').replace('__AUTO_RELOAD__', '0'))
os.makedirs('site', exist_ok=True)
csvs = [f for f in sorted(os.listdir('out')) if f.endswith('.csv')]
for f in csvs: shutil.copy(os.path.join('out', f), os.path.join('site', f))
links = '<div class="dl">Downloads: ' + ' '.join(f'<a href="{f}" download>{f}</a>' for f in csvs) + '</div>'
import subprocess, sys
subprocess.run([sys.executable, 'og_image.py'], check=False)          # social card, regenerated with live numbers
SITE = os.environ.get('SITE_URL', 'https://ozzyxbt.github.io/grail-analytics/').rstrip('/') + '/'
DESC = 'On-chain analytics for Grail: who made money, Grail app vs aggregators, GLIST mint behaviour, card vaulting, snipers, launches and top tokens.'
head = ('<!doctype html>\n<html lang="en">\n<head>\n<meta name="viewport" content="width=device-width, initial-scale=1">\n'
        f'<meta name="description" content="{DESC}">\n'
        f'<link rel="canonical" href="{SITE}">\n'
        '<meta property="og:type" content="website">\n<meta property="og:site_name" content="Grail Slab Report">\n<meta property="og:title" content="Grail Slab Report">\n'
        f'<meta property="og:description" content="{DESC}">\n<meta property="og:url" content="{SITE}">\n<meta property="og:image" content="{SITE}og.png">\n'
        '<meta property="og:image:width" content="1200">\n<meta property="og:image:height" content="630">\n<meta property="og:image:alt" content="Grail Slab Report: on-chain analytics for Grail tokens">\n'
        '<meta name="twitter:card" content="summary_large_image">\n<meta name="twitter:title" content="Grail Slab Report">\n'
        f'<meta name="twitter:description" content="{DESC}">\n<meta name="twitter:image" content="{SITE}og.png">\n<meta name="theme-color" content="#0e0f12">\n')
body = tpl.replace('__CSV_LINKS__', links).replace('__AUTO_RELOAD__', '1')
# move the leading meta/title/link/style block into <head>
cut = body.index('</style>') + len('</style>')
open('site/index.html', 'w').write(head + body[:cut] + '\n</head>\n<body>' + body[cut:] + '\n</body>\n</html>\n')
open('site/.nojekyll', 'w').write('')
print('built out/grail_dashboard.html and site/index.html', len(tpl)//1024, 'KB')
