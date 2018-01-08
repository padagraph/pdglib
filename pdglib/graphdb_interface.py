# -*- coding:utf8 -*-
from abc import ABCMeta, abstractmethod
from collections import namedtuple



# Types de nœuds 
User = namedtuple("User", "login password")
Graph = namedtuple("Graph", "name description")
Schema = namedtuple("Schema", "name lastmodified")
NodeType = namedtuple("NodeType", "name properties_types")
EdgeType = namedtuple("EdgeType", "name properties_types")
Node = namedtuple("Node", "id label properties")
Edge = namedtuple("Edge", "id label properties source target")
Comment = namedtuple("Comment", "content on by")
LogEntry = namedtuple("LogEntry", "who did what on_what when previous_state")


# Erreurs


class GraphError(Exception):
    pass

class ApiError(Exception):
    pass

class GraphExistsError(GraphError):
    pass

class ExistsError(GraphError):
    pass

class GraphNameError(GraphError):
    pass
    
class GraphNotFoundError(GraphError):
    pass

class NodeNotFoundError(GraphError):
    pass
    
class UserLoginExistsError(GraphError):
    pass

class UserPasswordInvalidError(GraphError):
    pass

class UserLoginNotFoundError(GraphError):
    pass

class NodeTypeNotFoundError(GraphError):
    pass

class EdgeTypeNotFoundError(GraphError):
    pass

class EdgeNotFoundError(GraphError):
    pass


# Interface to be implemented
class iGraphDB(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def create_database(self, **kwargs):
        """
        initialize the backend
        """
        pass

    @abstractmethod
    def open_database(self):
        """
        Open a connection to a backend
        """
        pass

    @abstractmethod
    def close_database(self):
        """
        Close connection to backend
        """
        pass

    @abstractmethod
    def get_db_metadata(self):
        """
        @return a dict containing the DB metadata
        """
        pass

    @abstractmethod
    def create_user(self, login, hashed_password):
        """
        Create a new user
        @return the new uid or raise an TODO exception
        """
        pass
        
    @abstractmethod
    def user_exists(self, login):
        """
        :returns: True if a user exists with this login
        """
        pass

    @abstractmethod
    def get_user(self, login):
        """
        get user from a login
        :raise : UserLoginNotFoundError
        """
        pass


    @abstractmethod
    def create_graph(self, user, graph_name, graph_description):
        """
        create a new graph with a specific owner
        @return the new gid
        :raise: GraphExistsError, GraphNameError
        """
        pass

    @abstractmethod
    def destroy_graph(self, graph_name):
        """
        DANGEROUS
        fully erase a whole graph from the db
        """
        pass

    @abstractmethod
    def get_graphs(self):
        """
        get a list of available graphs
        @return a list of dict with 'owner', 'desc' keys
        """
        pass

    @abstractmethod
    def get_graph(self, name):
        """
        get a graph by its name
        """
        pass

    @abstractmethod
    def update_graph(self, user, graph, properties):
        """
        overwrite the description of a graph
        """
        pass

    @abstractmethod
    def complete_label(self, graph_name, what, prefix):
        pass

    @abstractmethod
    def create_node_type(self, user, gid, name, properties, description=""):
        """
        create a new node type in a schema with a name and a list of properties
        """
        pass

    @abstractmethod
    def get_node_type(self, uuid):
        """
        get a nodetype by its id
        """
        pass

    @abstractmethod
    def find_node_type(self, gid, name):
        """
        find a nodetype by its name in a specific graph
        """
        pass

    @abstractmethod
    def update_nodetype(self, nodetype_id, properties, description):
        """
        add properties that are not already defined
        change description
        return nodetype as dict
        """
        pass

    @abstractmethod
    def create_edge_type(self, user, gid, name, properties, description=""):
        """
        create a new edge type in a schema with a name and a list of properties
        return edgetype as dict
        """
        pass

    @abstractmethod
    def get_edge_type(self, uuid):
        """
        find a edge type by its id 
        """
        pass

    @abstractmethod
    def find_edge_type(self, gid, name):
        """
        find a edgetype by its name in a specific graph
        """
        pass

    @abstractmethod
    def update_edgetype(self, edgetype_id, properties, description):
        """
        add properties that are not already defined
        return edgetype as dict
        """
        pass



    @abstractmethod
    def create_node(self, user, graph, node_type, label, properties):
        """
        create a new node in a graph with a type, a label and a list of properties
        """
        pass
    
    @abstractmethod
    def get_node(self, nid):
        """
        find a node by its id
        :raise: NodeNotFoundError
        """
        pass

    @abstractmethod
    def get_nodes(self, graphname, nid):
        """
        find a list of nodes by their ids
        :raise: NodeNotFoundError
        """
        pass

    @abstractmethod
    def find_nodes(self, gid, nodetype_name, start=0, size=100, **properties):
        """
        find a node  by its properties in a specific graph
        """
        pass

    @abstractmethod
    def delete_node(self, nid):
        """
        delete a node in a specific graph
        :raise: NodeNotFoundError
        """
        pass

    @abstractmethod
    def change_node_properties(self, user, node_uuid, properties):
        """
        add or change a property of a node.
        """
        pass
        
    @abstractmethod
    def change_edge_properties(self, user, edge_uuid, properties):
        """
        add or change a property of an edge.
        """
        pass


    @abstractmethod
    def create_edge(self, user, edge_type, label, properties, source, target):
        """
        create a new edge in a graph with a label and a list of properties
        """
        pass

    @abstractmethod
    def find_edges(self, graph_name, edgetype_name, start=0, size=100, **properties):
        """
        find a edge  by its label in a graph
        """
        pass

    @abstractmethod
    def delete_edge(self, edge_uuid):
        """
        delete an edge
        """
        pass

    @abstractmethod
    def add_comment(self, user, obj, content):
        """
        add a comment by an user on any object.
        """
        pass

    @abstractmethod
    def get_log(self, length=20, obj=None):
        """
        return a list of log entries, 
        obj can be None for global log, a graph for a graph log or any edge or node
        """
        pass

    
    @abstractmethod
    def get_graph_neighbors(self,gid, node_id, filter_edges=None, filter_nodes=None, filter_properties=None, mode='ALL', start=0, size=100):
        u"""
            returns node's neighbors
            @param node_id : uuid of source node
            @param filter_edges|nodes : list of accepted edge types or node types
            @param filter_properties : non utilisé pour l'instant
            @param mode : direction of links to follow 'ALL', 'IN' or 'OUT'
            @return a list of (edge, node2) tuples
        """
        
        pass
        
    @abstractmethod
    def batch_create_nodes(self, user, graph_name, data):
        pass

    @abstractmethod
    def batch_create_edges(self, user, graph_name, data):
        pass

    @abstractmethod
    def get_edges(self, graph_name, edges_uuids):
        pass

    @abstractmethod
    def get_edge_list(self, graph_name, nodes_uuids):
        pass

    #@abstractmethod
    #def get_subgraph(self, graph_name, nodes_uuids):
        #pass
    
    @abstractmethod
    def proxemie(self, graph_name, p0, weights=None, filter_edges=None, filter_nodes=None, limit=50, n_step=3):
        u"""
        fonction de proxémie qui filtre sur les edgetypes et nodetypes
        """
        pass
