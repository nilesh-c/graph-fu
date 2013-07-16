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
	plt.savefig('graphvis.pdf')

csv = csv.reader(sys.stdin, delimiter=',')
g = nx.DiGraph()
count = 0
for line in csv:
	node1 = line[0]
	node2 = line[1]
	g.add_edge(node1, node2)
	if count % 100000 == 0:
		print count, 'done!'
	count += 1

pool = Pool(5)
#pool.map(callShow, [(g, type) for type in ["shell", "circular", "random", "spring", "spectral"]])
pool.map(callShow, [(g, "spectral")])
