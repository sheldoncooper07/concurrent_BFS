"""
Parallel BFS.
"""
import time
from collections import defaultdict
from mpi4py import MPI
import sys

READY, START, DONE, EXIT, LOG, NONE = 0, 1, 2, 3, 4, 5
COMM = MPI.COMM_WORLD
SIZE = COMM.Get_size()
RANK = COMM.rank
STATUS = MPI.Status()

class Graph:
    def __init__(self):
        self.graph = defaultdict(list)

    def add_edge(self, node_one, node_two):
        self.graph[node_one].append(node_two)


    #this is not a text book definition breadth_first_search
    #it only makes each nodes visit their immediate neighbors
    def breadth_first_search(self, start, global_visited_array, visited_nodes):
        # visited_nodes = [False] * (len(self.graph) + 1)

        visited_nodes.append(start)
        
        for i in self.graph[start]:
            if i not in visited_nodes and global_visited_array[i] != True:
                visited_nodes.append(i)
        
        # print("###", visited_nodes)
GRAPH = Graph()

def get_min(_list):
    minimum = 0

    for i in xrange(len(_list)):
        if _list[i] < _list[minimum]:
            minimum = i

    return minimum

def split_nodes(node=0):
    GRAPH.graph[node] = sorted(GRAPH.graph[node],
                               key=lambda x: len(GRAPH.graph[x]),
                               reverse=True)
    distributed = [[], [], [], []]
    sumnbrs = [0] * 4

    for nbr in GRAPH.graph[node]:
        minpos = get_min(sumnbrs)
        distributed[minpos].append(nbr)
        sumnbrs[minpos] += len(GRAPH.graph[nbr])

    return distributed

def consumer():
    global COMM
    global GRAPH

    while True:
        task = COMM.recv(source=0, tag=MPI.ANY_TAG, status=STATUS)
        # print(RANK, task)
        tag = STATUS.Get_tag()

        #receiving START tag from parent would tell the child hardware node to start traversing their 
        #immediating neighbors. After it's done, they will send back a list of visited nodes to the parent hardwarenode
        if tag == START:
            visited_nodes = []
            for data in task['data']:
                GRAPH.breadth_first_search(data, task['visited'], visited_nodes)
                COMM.send(visited_nodes, dest=0, tag=DONE)

        elif tag == EXIT:
            break
        elif tag == NONE:
            COMM.send(None, dest=0, tag=DONE)

    COMM.isend(None, dest=0, tag=EXIT)


def producer(nodenum=0):
    global repository
    # print(repository)
    repository['visited'][nodenum] = True
    #split the neighbors of nodenum node into the number of hardware nodes
    #and send them to the hardware nodes for bfs traversal
    s_node = split_nodes()
    # print(s_node)
    for i in range(len(s_node)):
        repository['data'] = s_node[i]
        COMM.send(repository, dest = i+1, tag=START)

    
    counter = 0
    while counter <4:
        visited_nodes = COMM.recv(source=MPI.ANY_SOURCE,
                             tag=MPI.ANY_TAG)

        if(visited_nodes != None):
            print(visited_nodes)
            for each_node in visited_nodes:
                repository['visited'][each_node] = True
            counter +=1
    
    counter = 0
    for key in GRAPH.graph:
        if counter == len(GRAPH.graph):
            return
        if repository['visited'][key] == False:
            producer(key)
            
        
        counter +=1


if __name__ == '__main__':
    with open(sys.argv[1], 'r') as file_:
        for line in file_:
            idx = line.find(' ')
            GRAPH.add_edge(int(line[:idx]), int(line[idx + 1:]))

    if RANK == 0:
        repository = {'visited':[False]*len(GRAPH.graph),'data':[]}
        producer()

        for i in xrange(1, SIZE):
            COMM.send(None, dest=i, tag=EXIT)
    else:
        consumer()
