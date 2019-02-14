from collections import deque
from random import random, randint

class DirectedGraph():
    """Simple class for directed graphs. Nothing fancy.
    """
    def __init__(self):
        self.edges = []
        self.successors = {}
        self.predecessors = {}
        self.nodes = set()

    def add_edge(self, u, v):
        """Add the edge (u, v) to the graph.

        u : (hashable)
            source node
        v : (hashable)
            destination node
        """

        if u not in self.successors:
            self.successors[u] = []
        if v not in self.predecessors:
            self.predecessors[v] = []
        if v in self.successors[u]:
            raise Exception("Graph already contains edge.")

        self.edges.append((u,v))
        self.successors.get(u).append(v)
        self.predecessors.get(v).append(u)
        self.nodes.add(u)
        self.nodes.add(v)

    def get_successors(self, u):
        """Return nodes that are direct neighbours of u,
        i.e., all nodes v s.t. (u, v) exists.

        u : (hashable)
            node in the graph
        """
        return self.successors.get(u, [])

    def get_predecessors(self, u):
        """Return nodes that have u as direct neighbours,
        i.e., all nodes v s.t. (v, u) exists.

        u : (hashable)
            node in the graph
        """
        return self.predecessors.get(u, [])

    def get_nodes(self):
        """Return the set of all nodes in the graph.
        """
        return self.nodes

    def get_edges(self):
        """Return the set of all edges in the graph.
        """
        return self.edges


def random_dag(n_nodes, edge_threshold=0.5):
    """Create a random directed acyclic graph with n_nodes nodes.

    n_nodes : (int)
        number of nodes in the graph
    edge_threshold : (float)
        value between 0 and 1, take edge (i, j) if random() > edge_threshold
    """
    dag = DirectedGraph()

    edges = []
    for u in range(n_nodes):
        for v in range(u+1, n_nodes):
            if random() > edge_threshold:
                dag.add_edge(u, v)

    return dag


def is_topological_sort(graph, sequence):
    """Check if a given sequence is a topological sort of a directed graph.

    graph : (DirectedGraph)
        a directed graph
    sequence : (iterable[node])
        a sequence of nodes in the graph, will be checked if it fulfills the
        topological sorting condition
    """
    if set(sequence) != set(graph.get_nodes()):
        return False
    index = {v: i for (i, v) in enumerate(sequence)}
    return all(index[u] < index[v] for (u, v) in graph.get_edges())


def kahns_algo(graph):
    """Calculate a topological sort of a given directed graph using
    Kahn's algorithm [1].

    graph : (DirectedGraph)
        a directed graph

    raises : Exception
        if the graph contains a cycle

    References
    ----------
    [1] Kahn, Arthur B. "Topological sorting of large networks."
        Communications of the ACM 5.11 (1962): 558-562.
    """
    # initialize degree map
    degrees = {}
    zero_nodes = []
    top_sort = []

    for u in graph.get_nodes():
        pred = graph.get_predecessors(u)
        degrees[u] = len(pred)
        if len(pred) == 0:
            zero_nodes.append(u)

    while len(zero_nodes) > 0:
        u = zero_nodes.pop()
        top_sort.append(u)
        for v in graph.get_successors(u):
            degrees[v] -= 1
            if degrees[v] == 0:
                zero_nodes.append(v)

    if len(top_sort) < len(graph.get_nodes()):
        raise Exception("Graph contains a cycle")
    return top_sort

def topological_sort(graph):
    """
    Calculate a topological sort of the nodes in graph.

    graph : (DirectedGraph)
        a directed graph

    raises : Exception
        if the graph contains a cycle.
    """
    return kahns_algo(graph)

# TODO unittest
if __name__ == '__main__':
    N_MAX = 1000
    while True:
        n = randint(0, N_MAX)
        print(n)
        graph = random_dag(n, edge_threshold=random())
        top_sort = topological_sort(graph)
        assert is_topological_sort(graph, top_sort)

        # test cycles
        start = randint(0, N_MAX-1)
        end = randint(start+1, N_MAX)
        assert start < end
        for i in range(start, end):
            try:
                graph.add_edge(i, i+1)
            except:
                # ignore double edges
                pass
        try:
            graph.add_edge(end, start)
        except:
            pass

        try:
            topological_sort(graph)
            assert False
        except: # TODO exception type
            # happy path
            pass
