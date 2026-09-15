"""Build the dashboard twice from out/analysis.json:
  out/grail_dashboard.html  - artifact fragment (no document skeleton; the Claude artifact host wraps it)
  site/index.html           - full standalone document for static hosting (GitHub Pages), plus CSV downloads
"""
import os, shutil
data = open('out/analysis.json').read().replace('</script', '<\\/script')
tpl = open('template.html').read().replace('__DATA__', data)
open('out/grail_dashboard.html', 'w').write(tpl.replace('__CSV_LINKS__', ''))
os.makedirs('site', exist_ok=True)
csvs = [f for f in sorted(os.listdir('out')) if f.endswith('.csv')]
for f in csvs: shutil.copy(os.path.join('out', f), os.path.join('site', f))
links = '<div class="dl">Downloads: ' + ' '.join(f'<a href="{f}" download>{f}</a>' for f in csvs) + '</div>'
head = ('<!doctype html>\n<html lang="en">\n<head>\n<meta name="viewport" content="width=device-width, initial-scale=1">\n'
        '<meta name="description" content="On-chain user analytics for Grail on Base: who made money, Grail app vs aggregators, exclusive-mint behaviour, card vaulting, snipers and G-list wallets.">\n')
body = tpl.replace('__CSV_LINKS__', links)
# move the leading meta/title/link/style block into <head>
cut = body.index('</style>') + len('</style>')
open('site/index.html', 'w').write(head + body[:cut] + '\n</head>\n<body>' + body[cut:] + '\n</body>\n</html>\n')
open('site/.nojekyll', 'w').write('')
print('built out/grail_dashboard.html and site/index.html', len(tpl)//1024, 'KB')
