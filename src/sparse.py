"""
Parallel BFS
this version for testing sparse graphs
tested with a 20,000 node graph which it can traverse in around.
5 seconds. keep in mind this was tested on sub optimal hardware.
"""

#general imports
import time
from collections import defaultdict
from mpi4py import MPI
import sys

#graph class for storing core functions
class Graph:
    #initlization of class 
    def __init__(self):
        self.graph = defaultdict(list)
    #add an edge to the adj list
    def add_edge(self, node_one, node_two):
        self.graph[node_one].append(node_two)

    #core BFS algo
    def breadth_first_search(self, start, datalist):
        visited = [False] * (len(self.graph) + 1)
        queue = []
        #pop the starting node into the queue to start processing
        queue.append(start)
        visited[start] = True
        #keep traversing until the queue is empty
        while queue:
            start = queue.pop(0)
            datalist.append(start)
            # iterate through the given list of edges and log them
            for i in self.graph[start]:
                if not visited[i]:
                    queue.append(i)
                    #mark each one as visited
                    visited[i] = True

#general defines for message passing
START, DONE, EXIT, NONE = 0, 1, 2, 3
#VARS TO INTERFACE WITH MPI (Message Passing Interface)
COMM = MPI.COMM_WORLD
SIZE = COMM.Get_size()
RANK = COMM.rank
STATUS = MPI.Status()
GRAPH = Graph()

#find the minimum value within the given list of edges
def get_min(_list):
    minimum = 0
    #search for min edge
    for i in xrange(len(_list)):
        if _list[i] < _list[minimum]:
            minimum = i

    return minimum

#handles load balancing of edges to different pies for processing
def split_nodes(node=0):
    #sort the given nodes in reverse order
    GRAPH.graph[node] = sorted(GRAPH.graph[node],
                               key=lambda x: len(GRAPH.graph[x]),
                               reverse=True)
    #create an arr of 4 arrs (one for each node)
    distributed = [[], [], [], []]
    sumnbrs = [0] * 4
    #iterate through each edge's data and find min value
    for nbr in GRAPH.graph[node]:
        minpos = get_min(sumnbrs)
        distributed[minpos].append(nbr)
        #add len of the given edge's data to the given node's data list
        sumnbrs[minpos] += len(GRAPH.graph[nbr])
    #return the list to be used by the nodes
    return distributed

'''
consumer function, each of the 4 slave nodes runs this function and waits for data to be sent to it.
'''
def consumer():
    global COMM
    global GRAPH
    #while node is not being told to exit, keep running
    while True:
        task = COMM.recv(source=0, tag=MPI.ANY_TAG, status=STATUS)
        tag = STATUS.Get_tag()
        #if the node revives a message with a start tag, time to work.
        if tag == START:
            for data in task:
                datalist = []
                #call BFS on each edge being sent to the node
                GRAPH.breadth_first_search(data, datalist)
                #return data to the master node
                COMM.send(datalist, dest=0, tag=DONE)
        #if tag is EXIT, then it's time end
        elif tag == EXIT:
            break
        #if no data is sent to the pi then just send back DONE with no data
        elif tag == NONE:
            COMM.send(None, dest=0, tag=DONE)

    #when its time to exit, let the master node know
    COMM.send(None, dest=0, tag=EXIT)

'''
Producer Runs on the master node, Deligating taks to slave (or Consumer) nodes, then
collects the data being sent back.
'''
def producer():
    global COMM
    global SIZE
    nodenum = 0
    #number of edges on start node(we will always assume start of traversal at node 0)
    numofedges = len(GRAPH.graph[0])
    #edges of the start node (always 0)
    edges = GRAPH.graph[0]
    #loop through data and deligate
    for i in xrange(1, SIZE):
        #if there are less edges than pies then deligate them and then send
        #None to remaining nodes.
        if numofedges < SIZE - 1:
            #check to make sure we are not passed the number of inital edges
            if i <= numofedges:
                #send given edge to the corresponding node
                COMM.send([edges[nodenum]], dest=i, tag=START)
                nodenum += 1
            else:
                COMM.send(None, dest=i, tag=NONE)
        else:
            #if there are more edges than nodes, then we load balance and send
            #the list of edges to the corresponding node
            pi = 1
            for data in split_nodes():
                COMM.send(data, dest=pi, tag=START)
                pi += 1

    globalvec = []
    globalvec.append(0)
    s = set()
    s.add(0)
    finished = 0

    #Run as long for as long as there are nodes that have not returned data 
    while finished < SIZE -1:
        #recieve data from each node and proccess it.
        data = COMM.recv(source=MPI.ANY_SOURCE,
                         tag=MPI.ANY_TAG,
                         status=STATUS)
        #store the tag of the data being recieved
        tag = STATUS.Get_tag()
        #if the tag is DONE, then we proccess the data
        if tag == DONE:
            #make sure the data is not nothing.
            if data != None:
                #if it isnt cross check it with a set of all data that has been recieve
                #this is done at O(1) complexity
                for d in data:
                    if d not in s:
                        globalvec.append(d)

                    s.add(d)

            #tick up the finished counter when a nodes data is done being processed
            finished += 1

    #finally, print the results
    print globalvec

if __name__ == '__main__':
    #stores the file path being passed to the program
    filename = sys.argv[1]
    with open(filename, 'r') as file_:
        #parse graph data (stored as a pair ex: 0,1)
        for line in file_:
            idx = line.find(' ')
            GRAPH.add_edge(int(line[:idx]), int(line[idx + 1:]))
    #if the rank of the pi (or name ) is 0 then run the producer function
    if RANK == 0:
        producer()
        #when the producer is done, well then its time for the consumer nodes to be
        #done too, so we tell them to exit.
        for i in xrange(1, SIZE):
            COMM.send(None, dest=i, tag=EXIT)
    #else run the consumer function
    else:
        consumer()
