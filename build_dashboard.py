import json
data = open('out/analysis.json').read().replace('</script', '<\\/script')
html = open('template.html').read().replace('__DATA__', data)
open('out/grail_dashboard.html', 'w').write(html)
print('built', len(html)//1024, 'KB')
