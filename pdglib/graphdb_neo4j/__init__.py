# -*- coding:utf8 -*-
import datetime


from utils import *

from user import User
from graphnode import GraphNode
from types import NodeType, EdgeType
from graphdata import UNode, UEdge
import graphdb_interface  as I

import labels

def translate_exceptions(f):
    def task(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except SchemaErrors.ConstraintAlreadyExists as e:
            print "warning: ",e.message
        except Errors.CypherError as e:
            if 'already exists with' in e.message:
                raise I.ExistsError(e.message)
        raise
    return task


LABEL_KEYS = (
        (labels.GraphNode.label, 'name'),
        (labels.User.label, 'username'),
        (labels.User.label, 'email'),
        (labels.NodeType.label, 'uuid'),
        (labels.NodeType.label, NodeType.label_key),
        (labels.EdgeType.label, 'uuid'),
        (labels.EdgeType.label, EdgeType.label_key),
        (labels.UNode.label, 'uuid'),
        (labels.UEdge.label, 'uuid'),
      )

IDX_LABEL_KEYS = (
        (labels.NodeType.label, 'name'),
        (labels.EdgeType.label, 'name'),
        (labels.Properties.label, 'label')
      )

CONSTRAINTS = ["CREATE CONSTRAINT ON (n:%s) ASSERT n.%s IS UNIQUE" % couple for couple in LABEL_KEYS]
INDEXES = ["CREATE INDEX ON :%s(%s)" % couple for couple in IDX_LABEL_KEYS]

MAX_NODES_COUNT = 500


class GraphDB(I.iGraphDB):
    """
    root class to fetch data from a N4J DB
    """

    def __init__(self, url):
        self.url = url
        self.open_database()

    @translate_exceptions
    def create_database(self, **kwargs):
        if self.db is None:
            self.open_database()
        for constraint in CONSTRAINTS:
            self.db.cypher.execute(constraint)
        for idx in INDEXES:
            self.db.cypher.execute(idx)
        

    @translate_exceptions
    def open_database(self):
        if not hasattr(self, 'db'):
            self.db = Graph(self.url)
        if not hasattr(self.db, 'cypher'):
            raise I.GraphError( "Missing Cypher")

    @translate_exceptions
    def close_database(self):
        self.db = None

    @translate_exceptions
    def get_db_metadata(self):
        return {'url': self.url}


    """ up/down votes """
    @translate_exceptions
    def get_user_updownvote_graph(self, login, gid):
        user = User.get_by_login(self.db, login)
        graph = GraphNode.get_by_name(self.db, gid)
        return user.get_vote_graph(gid)

    @translate_exceptions
    def toggle_user_updownvote_graph(self, login, gid, direction):

        direction in ('up','down')
        user = User.get_by_login(self.db, login)
        graph = GraphNode.get_by_name(self.db, gid)
        vote = user.get_vote_graph(gid)      

        if direction == 'up':
            if vote['upvote'] :
                user.rm_upvote_graph(gid)
                vote['upvote'] =  False
            else:
                user.upvote_graph(gid)
                vote['upvote'] =  True
                vote['downvote'] =  False
        if direction == 'down':
            if vote['downvote'] :
                user.rm_downvote_graph(gid)
                vote['downvote'] =  False
            else:
                user.downvote_graph(gid)
                vote['upvote'] =  False
                vote['downvote'] =  True
            
        vote.update( {
            'username' : login,
            'graph' : gid,
            'action' : '%svote' % direction,
            'time' : datetime.datetime.now().isoformat()
          })
        return vote
        
    @translate_exceptions
    def rm_upvote_graph(self, login, graphname):
        user = self.get_user(login)
        graph = GraphNode.get_by_name(self.db, graphname)
        
        pass
        

    """ Users """

    @translate_exceptions
    def create_user(self, login, email, hashed_password, active=False):
        u = User(login, email, hashed_password, active)
        self.db.create(u.node)
        
        return u.get_login()
    
    @translate_exceptions
    def get_user(self, login):
        try: 
            u = User.get_by_login(self.db, login)
            return u.as_dict()
        except :
            return None

    @translate_exceptions
    def get_users_count(self):
        return User.get_count(self.db)

            
    @translate_exceptions
    def list_graphs(self, page=1, offset=5, order_by="name", meta=False, reverse=False, root_page_url="./" ):

        order_by = "name"
        
        page = page if page > 0 else 1

        start = (page - 1) * offset
        end = offset * page        

        import time
        one = time.time()

        data = self.get_listing(page, offset)
        print data
        count = data['count'] 
		
        data['offset'] = offset
        data['prev'] = root_page_url + "/page/%s"  % (page - 1) if page > 1 else None
        data['next'] =  root_page_url + "/page/%s"  % (page + 1) if end < count  else None
        data['time'] = time.time() - one
        
        return data

    @translate_exceptions
    def get_graphs(self):
        graphs = GraphNode.get_all(self.db)
        return graphs

    @translate_exceptions
    def get_graphs_count(self):
        return GraphNode.get_count(self.db)

    @translate_exceptions
    def create_graph(self, login, name, properties ):
        """
        :param login: user login
        :param name: graph name
        :param description: graph description
        """
        user = User.get_by_login(self.db, login)
        g = GraphNode(user, name)
        self.db.create(g.node, g.own_rel)
        self.update_graph(user, name, properties)
        return name

    @translate_exceptions
    def destroy_graph(self, graph_name):
        return GraphNode.get_by_name(self.db, graph_name).destroy()


    @translate_exceptions
    def update_graph(self, user, graph_name, properties):
        graph = GraphNode.get_by_name(self.db, graph_name)
        graph.set_properties(properties)
        graph.node.push()
        return graph
        #TODO: log user action

    @translate_exceptions
    def get_graph(self, name):
        return GraphNode.get_by_name(self.db, name).as_dict()

    @translate_exceptions
    def get_graph_infos(self, name):
        return GraphNode.get_by_name(self.db, name).as_dict()

    @translate_exceptions
    def get_graph_metadata(self, name):
        return GraphNode.get_by_name(self.db, name).get_metadata()


    @translate_exceptions 
    def proxemie(self, graph_name, p0, weights=None, filter_edges=None, filter_nodes=None, limit=50, n_step=3):
        graph = GraphNode.get_by_name(self.db, graph_name)
        return graph.proxemieMC(p0, weights=weights, filter_edges=filter_edges, filter_nodes=filter_nodes, limit=limit, n_step=n_step,n_walk=10000)
        #return graph.proxemie(p0, weights=weights, filter_edges=filter_edges, filter_nodes=filter_nodes, limit=limit, n_step=n_step)


    @translate_exceptions
    def complete_label(self, graph_name, what, prefix, start=0, size=100):
        graph = GraphNode.get_by_name(self.db, graph_name)
        return graph.complete_label(what, prefix, start, size)
        
    @translate_exceptions
    def create_node_type(self, username, graph_name, name, properties, description=""):
        graph = GraphNode.get_by_name(self.db, graph_name)
        nodetype = NodeType(graph, name, properties, description)
        self.db.create(nodetype.node, nodetype.belongs_rel)
        
        #TODO: log user action

        return nodetype.as_dict()

    @translate_exceptions
    def get_node_types(self, gid):
        """ returns node types that belongs to a graph gid
        : param gid : graph name
        
        """
        graph = GraphNode.get_by_name(self.db, gid)
        return graph.get_node_types()
        
    @translate_exceptions
    def get_node_type(self, gid, uuid):
        return NodeType.get_by_uuid(self.db, uuid).as_dict()
               
    @translate_exceptions
    def find_node_type(self, graph_name, type_name):
        graph = GraphNode.get_by_name(self.db, graph_name)
        return NodeType.get_by_name(self.db, graph, type_name).as_dict()


    @translate_exceptions
    def update_nodetype(self, nodetype_id, properties, description):
        nodetype = NodeType.get_by_uuid(self.db, nodetype_id)
        nodetype.update(properties, description)
        return nodetype.as_dict()

    @translate_exceptions
    def create_node(self, user_id, graph_name, nodetype_uuid, properties):
        """
        """
        node = UNode(self.db, user_id, graph_name, nodetype_uuid, properties)
        
        #TODO: log user action

        return node.uuid
        
    @translate_exceptions
    def get_node(self, gid, node_id):
        # not safe for unmanaged UNode 
        return  UNode.get_by_id(self.db, node_id).as_dict()

    @translate_exceptions
    def get_nodes(self, gid, uuids):
        assert(len(uuids) <= MAX_NODES_COUNT)
        return [node.as_dict() for node in UNode.get_nodes_by_id(self.db, gid, uuids)]
        
    @translate_exceptions
    def get_starred_node_uuids(self, graphname):
        return [node for node in GraphNode.get_by_name(self.db, graphname).get_starred_node_uuids()]

    @translate_exceptions
    def get_starred_node_count(self, graphname):
        return GraphNode.get_by_name(self.db, graphname).get_starred_node_count()

    @translate_exceptions
    def set_nodes_starred(self, gid, nodes_uuids, starred ):
        graph = GraphNode.get_by_name(self.db, gid)
        if starred :
            graph.star(nodes_uuids)
        else:
            graph.unstar(nodes_uuids)


        
    @translate_exceptions
    def find_nodes(self, graph_name, nodetype_uuid, properties, start=0, size=100):
        graph = GraphNode.get_by_name(self.db, graph_name)
        nodetype = NodeType.get_by_uuid(self.db, nodetype_uuid) 
        return [x.as_dict() for x in UNode.find_in_graph(self.db, graph_name, nodetype.uuid, properties, start, size)]

    @translate_exceptions
    def iter_nodes(self, graph_name, nodetype_uuid, properties, size=1000):
        """
        like find nodes makes a complete iteration of the nodes matching node_type and properties
            :see: find_nodes
        """
        start=0
        while True:
            nodes = list( self.find_nodes(graph_name, nodetype_uuid, properties, start, size))
            if not len(nodes) :
                break
            start += size
            for node in nodes:
                yield node

    @translate_exceptions
    def delete_node(self, username, gid, node_id):
        """
        Delete a node
        * Node should belongs to gid 
        * username can write in gid default == true 
        :param :
        """

        unode = UNode.get_by_id(self.db, node_id)
        unode.delete(gid)

    @translate_exceptions
    def change_node_properties(self, username, node_id, properties):

        unode = UNode.get_by_id(self.db, node_id)
        unode.update_properties(properties, username)

    @translate_exceptions
    def create_edge_type(self, username, graph_name, name, properties, description=""):
        graph = GraphNode.get_by_name(self.db, graph_name)
        edgetype = EdgeType(graph, name, properties, description)
        self.db.create(edgetype.node, edgetype.belongs_rel)
        
        #TODO: log user action

        return edgetype.as_dict()

    @translate_exceptions
    def get_edge_types(self, gid):
        """ returns node types that belongs to a graph gid
        : param gid : graph name
        
        """
        graph = GraphNode.get_by_name(self.db, gid)
        return graph.get_edge_types()

    @translate_exceptions
    def get_edge_type(self, uuid):
        return EdgeType.get_by_uuid(self.db, uuid).as_dict()


    @translate_exceptions
    def find_edge_type(self, graph_name, type_name):
        graph = GraphNode.get_by_name(self.db, graph_name)
        return EdgeType.get_by_name(self.db, graph, type_name).as_dict()
 
    @translate_exceptions
    def update_edgetype(self, edgetype_id, properties, description):
        edgetype = EdgeType.get_by_uuid(self.db, edgetype_id)
        edgetype.update(properties, description)
        
        return edgetype.as_dict()

  
    @translate_exceptions
    def create_edge(self, user_id, graph_name, edgetype_uuid, properties, source_id, target_id):
        uedge = UEdge(self.db, user_id, graph_name, edgetype_uuid, properties, source_id, target_id)
        return uedge.uuid
        
    @translate_exceptions
    def get_edge(self, edge_id):
        return UEdge.get_by_id(self.db, edge_id).as_dict()

    @translate_exceptions
    def find_edges(self, graph_name, edgetype_name, properties, start=0, size=100):
        graph = GraphNode.get_by_name(self.db, graph_name)
        edgetype = EdgeType.get_by_name(self.db, graph, edgetype_name)
        return [x.as_dict() for x in UEdge.find_in_graph(self.db, graph, edgetype, properties, start, size)]

    @translate_exceptions
    def delete_edge(self, username, gid, edge_id):
        """
        Delete a edge
        * Edge should belongs to gid 
        * username can write in gid default == true
        :param :
        """

        uedge = UEdge.get_by_id(self.db, edge_id)
        uedge.delete(gid)
        # = TODO = 
        #log user action:

    @translate_exceptions
    def change_edge_properties(self, user, edge_id, properties):
        uedge = UEdge.get_by_id(self.db, edge_id)
        uedge.update_properties(properties, user)
    
    @translate_exceptions
    def batch_create_nodes(self, user, graph_name, data):
        return UNode.batch_create(self.db, user, graph_name, data)

    @translate_exceptions
    def batch_create_edges(self, user, graph_name, data):
        return UEdge.batch_create(self.db, user, graph_name, data)

            
            
    @translate_exceptions
    def get_graph_neighbors(self,gid, node_id, filter_edges=None, filter_nodes=None, filter_properties=None, mode='ALL', start=0, size=100):
        unode = UNode.get_by_id(self.db, node_id)
        return unode.get_count_neighbors('get', filter_edges, filter_nodes, mode, start=start, size=size)

    @translate_exceptions
    def count_neighbors(self, gid, node_id, filter_edges=None, filter_nodes=None, filter_properties=None, mode='ALL'):
        unode = UNode.get_by_id(self.db, node_id)
        return unode.get_count_neighbors('count', filter_edges, filter_nodes, mode)

    @translate_exceptions
    def get_edge_list(self, graph_name, nodes_uuids):
        return GraphNode.get_by_name(self.db, graph_name).get_subgraph(nodes_uuids)

    @translate_exceptions
    def get_edges(self, graph_name, edges_uuids):
        return GraphNode.get_by_name(self.db, graph_name).get_edge_list(edges_uuids)

    def add_comment(self, *args): pass # TODO
    def edit_node(self, *args): pass # TODO
    def get_log(self, *args): pass # TODO
    def change_property(self, *args): pass # TODO
    def user_exists(self, *args): pass # TODO

    def get_listing(self, page, page_size):
        import json
        import timeit
        print "ask listing", timeit.default_timer()

        resource = py2neo.ServerPlugin(self.db, 'Listing').resources.get('graphListing')
        args = {
            'page':page, 
            'pageSize':page_size
            }
        response = resource.post(args)
        result = json.loads(response.content)
        print "end listing", timeit.default_timer()
        return result

