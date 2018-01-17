# -*- coding:utf8 -*-
from abc import ABCMeta, abstractmethod
from collections import namedtuple
import igraph
import re
import datetime
from  uuid import uuid4

import pdglib.graphdb_interface
from pdglib.graphdb_interface import GraphExistsError, GraphError, GraphNameError, NodeNotFoundError, EdgeNotFoundError, UserLoginNotFoundError, iGraphDB

from cello.providers.igraphGraph import IgraphGraph
from cello.graphs.prox import ProxSubgraph, ProxExtract

# Types de nœuds 
#User = namedtuple("User", "login password")
#Graph = namedtuple("Graph", "name description")
#Schema = namedtuple("Schema", "name lastmodified")
#NodeType = namedtuple("NodeType", "name properties_types")
#EdgeType = namedtuple("EdgeType", "name properties_types")
#Node = namedtuple("Node", "id label properties")
#Edge = namedtuple("Edge", "id label properties source target")
#Comment = namedtuple("Comment", "content on by")
#LogEntry = namedtuple("LogEntry", "who did what on_what when previous_state")


class IGraphDB(iGraphDB):

    def __init__(self, conf):
        """ Function doc
        :param : 
        """
        self.graphs = {}
        self.conf = conf
        
    @staticmethod
    def generate_uuid():
        return uuid4().hex
        
    def __repr__(self):
        """ Function doc
        :param : 
        """
        names = ", ".join([ k for k in self.graphs ])
        return " ".join([u'iGraphDB', '@%s' % names ]) 
        
    def open_database(self):
        
        print( self.get_db_metadata() )

    def close_database(self):
        self.graph = None
        
    def create_database(self, **kwargs):
        """
        initialize the backend
        """
        self.graph = IgraphGraph()
        self.graphs = {}

    def get_db_metadata(self):
        """ returns dict containg db meta """
        graphs = self.get_graphs()
        return {
            'graphs' : graphs,
            'count' : len(graphs)
        }
    
    def save(self):
        """ write graph to path """
        pass

    def list_graphs(self, page=1, offset=5, order_by="name", meta=False, reverse=False, root_page_url="./" ):

        order_by = "name"
        
        page = page if page > 0 else 1

        start = (page - 1) * offset
        end = offset * page        

        import time
        one = time.time()
        data = self.get_db_metadata()
        
        print data
        count = data['count']
		
        data['offset'] = offset
        data['prev'] = root_page_url + "/page/%s"  % (page - 1) if page > 1 else None
        data['next'] =  root_page_url + "/page/%s"  % (page + 1) if end < count  else None
        data['time'] = time.time() - one
        
        return data


    def get_graphs(self):
        """ return list of graphs ( owner, desc, creation) """
        return self.graphs.keys()

    def get_graph(self, gid):

        graph = self.graphs.get(gid)

        if not graph : 
            path = self.conf.get(gid)
            
            if path is None : raise GraphError('no such graph %s' % gid) 

            if self.conf.get(gid,None) : 
                print "opening graph %s@%s" %(gid, path)
                graph = IgraphGraph.Read(path)
                self.graphs[gid] = graph

        if graph : return graph
        
        raise GraphError('%s' % gid)

        
    def get_graph_infos(self, gid):
        """ return meta data for a graph """
        return self.get_graph_metadata(gid)
        
    def get_graph_metadata(self, gid) :
        """ return meta data for a graph """
        g = self.get_graph(gid)
        return {  k:g[k] for k in  g.attributes() } if g else {'gid': gid}

    def get_node_types(self, gid):
        g = self.get_graph(gid)
        return g['nodetypes']
            
    def get_edge_types(self, gid):
        g = self.get_graph(gid)
        return g['edgetypes']
    
        
    def create_graph(self, username, graph_name, graph_description):
        """
        create a new graph with a specific owner
        @return the new gid
        """
        if graph_name is None:
            raise GraphNameError("name can't be null")

        if graph_name in self.graphs:
            raise GraphExistsError(graph_name)

        self.graphs[graph_name] = {'owner' : username,
                                   'desc' : graph_description,
                                   'creation_date' : datetime.datetime.now()}

    def update_graph(self, user, graph, properties):
        """
        overwrite the description of a graph
        """
        # unimplemented
        pass

    def find_graph(self, name, user=None):
        """
        find a graph by its name, with or without specifying its owner
        """
        pass

    def update_graph_description(self, user, graph, new_description):
        """
        overwrite the description of a graph
        """
        pass


    def create_user(self, login, hashed_password):
        """
        Create a new user
        @return the new uid or raise an TODO exception
        """
        user = None
        if  self.user_exists(login) == False:
            
            self.graph.add_vertex( login, **{ 'node_type':"user", 'login': login, 'password' : hashed_password})
            user =  self.graph.vs.find(login=login, node_type="user")
        else:
            raise UserLoginExistsError(login)
            

        print "created user %s " % user
        return user

    def _get_user(self, login):
        user = None
        if len(self.graph.vs) and 'node_type' in self.graph.vs.attribute_names() :
            users = self.graph.vs.select(name=login, node_type="user")
            if len(users): 
                return users[0]
    
    def user_exists(self, login):
        return self._get_user(login) is not None
        
    def get_user(self, login):
        """
        get uid from a login
        """
        user = self._get_user(login)
        if user:
            return user
        raise UserLoginNotFoundError(login)

    def create_schema(self, user, name, graph=None):
        """
        create a new schema, optionally linking it with a graph
        """
        pass

    def find_schema(self, name, user=None):
        """
        find a schema by its name, with or without specifying its owner's uid
        """
        pass

    def set_schema_for_graph(self, user, schema, graph):
        """
        link a schema to a graph
        """
        pass

    def create_node_type(self, user, schema, name, properties):
        """
        create a new node type in a schema with a name and a list of properties
        """
        pass

    def get_node_type(self, schema, name):
        """
        find a node type by its name in a specific schema
        """
        pass
        
    def update_nodetype(self, nodetype_id, properties, description):
        """
        add properties that are not already defined
        change description
        return nodetype as dict
        """
        pass

        
    def _select_node(self, uuid ):
        """
        :param uuid: uniq node id
        :returns : igraph node
        :raise: NodeNotFoundError
        """
        nodes = self.graph.vs.select(uuid=uuid)
        if len(nodes):
            return nodes[0]

        raise NodeNotFoundError(uuid)
        
    def node_data(self, node):
        data = {}
        if node :
            data = node.attributes()
            data['_index'] = node.index
            data['_degree'] = node.degree()
        return data
        
    def get_node(self, gid, uuid):
        """
        find a node  by its uuid
        """
        graph = self.get_graph(gid)
        nodes = graph.vs.select(uuid=uuid)

        if nodes and len(nodes) : return nodes[0].attributes() 
        
        return None
    
    
    def get_nodes(self, gid, nids):
        """
        find a list of nodes by their ids
        :raise: NodeNotFoundError
        """
        nodes = []
        graph = self.get_graph(gid)
        vs = graph.vs.select(nids)
        for v in vs:
            nodes.append(self.node_data(v))
        return nodes

    def find_nodes(self, gid, nodetype_uuid, properties, start=0, size=100):
        return []



    def delete_node(self, uuid):
        """ TODO   """
        node = self._select_node(uuid)
        self.graph.delete_vertices(node)

    def create_node(self, username, graph, node_type, label, properties):
        """
        create a new node in a graph with a type, a label and a list of properties
        """
        props = { 'user':username,
                  'graph' : graph,
                  'node_type': node_type
                }
        props.update( { k:v for k,v in properties })
        props['uuid'] = self.generate_uuid()
        
        self.graph.add_vertex( label.encode('utf8'), **props )
        
        return props['uuid']

   

    def change_node_properties(self, username, node_uuid, properties):
        """
        add or change a property of a node.
        """
        props = { 'user':username }
        
        props.update( { k:v for k,v in properties })

        vtx = self._select_node(uuid)

        for k,v in props.iteritems():
            vtx[k] = v

    def count_neighbors(self, gid, node_id, **kwargs):
        vs = self.get_graph_neighbors(gid, node_id, size=0, **kwargs)
        return len(vs)
        
    def get_graph_neighbors(self, gid, node_id, filter_edges=None, filter_nodes=None, filter_properties=None, mode='ALL', start=0, size=100):
        u"""
            returns node's neighbors
            @param node_id : uuid of source node
            @param filter_edges|nodes : list of accepted edge types or node types
            @param filter_properties : non utilisé pour l'instant
            @param mode : direction of links to follow 'ALL', 'IN' or 'OUT'
            @return a list of (edge, node2) tuples
        """
        g = self.graphs.get(gid)
        vs = g.vs.select(uuid=node_id)
        rs = []
        mode = { "OUT": 1 , "IN": 2 , "ALL": 3 }.get(mode, 3)
        
        if len(vs):
            v = vs[0]
            edges = [g.es[e] for e in  g.incident(v.index, mode = mode )]
            for e in edges:
                mode = "ALL"
                vertex = v.index
                if e.source != e.target :
                    if e.source == v.index :
                        mode = "OUT"
                        vertex = e.target
                    elif e.target == v.index :
                        mode = "IN"
                        vertex = e.source
                rs.append(( e.attributes(), mode, g.vs[vertex].attributes() ))
        return rs if size == 0 else rs[start:start+size]
        
    def get_starred_node_uuids(self, gid):
        g = self.graphs.get(gid)

        return []
        

    def create_edge_type(self, user, schema, name, properties):
        """
        create a new edge type in a schema with a name and a list of properties
        """
        pass

    def get_edge_type(self, schema, name):
        """
        find a edge type by its name in a specific schema
        """
        pass

    def update_edgetype(self, edgetype_id, properties, description):
        """
        add properties that are not already defined
        return edgetype as dict
        """
        
        raise Error("unimplemented")
    

    def create_edge(self, user, edge_type, label, properties, source, target):
        """
        create a new edge in a graph with a label and a list of properties
        source and target are unicode uuid
        """
        props = { 'user':user,
                  'edge_type': edge_type,
                  'label' : label
                }
        
        props.update( { k:v for k,v in properties })
        src = self._select_node(source).index
        tgt = self._select_node(target).index
        
        self.graph.add_edge( src, tgt,  **props )

    def get_edge_list(self,gid, uuid):
        graph = self.get_graph(gid)
        
    def get_edge(self,gid, uuid):
        graph = self.get_graph(gid)
    
    def get_edges(self,gid, uuid):
        graph = self.get_graph(gid)

    def delete_edge(self,gid, uuid):
        edge = self._select_edge(uuid)
        self.graph.delete_edges(node)

    def _select_edge(self, uuid ):
        """
        :param uuid: uniq edge id
        :returns : igraph edge
        :raise: EdgeNotFoundError
        """
        edges = self.graph.es.select(uuid=uuid)
        if len(edges):
            return edges[0]

        raise EdgeNotFoundError(uuid)
    
    def change_edge_properties(self, username, edge_uuid, properties):
        """
        add or change a property of an edge.
        """
        props = { 'user':username }
        
        props.update( { k:v for k,v in properties })

        edge = self._select_edge(uuid)
        for k,v in props.iteritems():
            edge[k] = v

    def find_edges(self, graph_name, edgetype_name, start=0, size=100, **properties):
        """
        find a edge  by its label in a graph
        """
        edges = []
        try: 
            edges = self.graph.es.find(**kwargs)
        except ValueError: pass
        return edges

    def find_node_type(self, graph_name, type_name): pass
    def find_edge_type(self, graph_name, type_name): pass

    def add_comment(self, user, obj, content):
        """
        add a comment by an user on any object.
        """
        pass

    def get_log(self, length=20, obj=None):
        """
        return a list of log entries, 
        obj can be None for global log, a graph for a graph log or any edge or node
        """
        pass

    def batch_create_nodes(self, user, graph_name, data):
        pass

    def batch_create_edges(self, user, graph_name, data):
        pass

    def get_subgraph(self, graph_name, nodes_uuids):
        g = self.graph
        _uuids = set(nodes_uuids)
        subset = [ e.index for e in g.vs if e['uuid'] in _uuids  ]
        return g.subgraph(subset)
    
    def proxemie(self, gid, p0, weights=None, filter_edges=None, filter_nodes=None, limit=50, n_step=3):
        """
        fonction de proxémie qui filtre sur les edgetypes et nodetypes
        """

        extract = ProxExtract()
        g = self.get_graph(gid)
        uuids = { v['uuid'] : v.index for v in g.vs }
        
        pz = [ p for p in p0 ]
        pz = [ uuids[p] for p in pz ]
        s = extract(g, pzeros=pz, cut=limit)
        return s

    def complete_label(self, gid, what, prefix, start=0, size=100):
        g = self.get_graph(gid)
        m = []
        for v in g.vs:
            if re.search( ".*%s.*" % prefix, v['properties']['label'] , re.IGNORECASE ):
                m.append({
                    "label": v['properties']['label'], 
                    "nodetype":v['nodetype'] , 
                    "uuid": v['uuid'], 
                 })
        t = len(m)
        m = m[start:size]
        return m
         
    def destroy_graph(self, graph_name): pass
    

