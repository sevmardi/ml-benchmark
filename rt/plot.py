
from pylab import * #bad practice - Don't ever do this unless you are just messing around! 
import numpy as np


def show_most_informative_features(y1w, y1, y2w, y2, n=2):
	x =  np.arange(n)
	axes([0.025,0.025,0.95,0.95])
	bar(X, Y1, facecolor='#ccccff', edgecolor='white')
	bar(X, Y2, facecolor='#ffcccc', edgecolor='white')

	for x,y,w in zip(x,y1, y1w):
		text(x+0.6, 0.2, '%.2f' % y, ha='center', va= 'center', rotation='vertical', size=7)
		text(x+0.4, max(Y1) + 0.2, '%s' % w, ha='center', va= 'bottom', rotation='vertical', size=10)

	for x,y,w in zip(x,y2, y2w):
		text(x+0.6, -0.2, '%.2f' % y, ha='center', va= 'center', rotation='vertical', size=7)
		text(x+0.4, min(Y2) - 0.4, '%s' % w, ha='center', va= 'top', rotation='vertical', size=10)

	xlim(-1,n), xticks([])
	ylim(min(Y2) - 1,max(Y1) + 1), yticks([])

	savefig('bar_informative_features.png', dpi=500)
	show()


with open('rotten.varinfo.vw') as infile:
	d = {}
	for e, line in enumerate( infile.readlines() ):
		if e > 0:
			token = line.strip().split("\t")[0][2:].strip()
			value = line.strip().split("\t")[1].split(" ")[-1][:-1]
			d[token] = float(value)/float(100)

y1w = sorted(d, key=d.get, reverse=True)[:100]
y1 = [d[f] for f in sorted(d, key=d.get, reverse=True)[:100]]
y2w = sorted(d, key=d.get)[:100]
y2  = [d[f] for f in sorted(d, key=d.get)[:100]]

show_most_informative_features(y1w, y1, y2w, y2)





