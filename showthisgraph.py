#!/usr/local/bin/python2.7

import sys
import csv
import networkx as nx
import matplotlib as mp
import matplotlib.pyplot as plt
from multiprocessing import Pool
from multiprocessing import Process

def callShow(params):
	g, layout = params
	layout += "_layout"
	pos = getattr(nx, layout)(g)
	nx.draw(g,pos,node_color='#A0CBE2',edge_color='#BB0000',width=1,edge_cmap=plt.cm.Blues,with_labels=True)
	plt.show()

csv = csv.reader(sys.stdin, delimiter='|')
g = nx.DiGraph()
for line in csv:
	node = line[0].split(',')[0]
	print node
	edges = line[1:]
	print [e.split(',')[0]for e in edges]
	g.add_node(node)
	for e in edges:
		if e.split(',')[0] != '':
			g.add_edge(node, e.split(',')[0])

print g.nodes()
print g.edges()

pool = Pool(5)
#pool.map(callShow, [(g, type) for type in ["shell", "circular", "random", "spring", "spectral"]])
pool.map(callShow, [(g, "spectral")])
