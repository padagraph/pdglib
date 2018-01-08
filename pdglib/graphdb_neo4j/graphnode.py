# -*- coding:utf8 -*-

from utils import *
from graphdata import UNode, UEdge
from types import NodeType, EdgeType
import labels
from labels import label_names



def edge_to_dict( uuid, source, properties, edgetype, target ):
     return {
                'uuid' : uuid,
                'edgetype' : edgetype,
                'source' : source,
                'target' : target,
                'properties' : properties
            }


class GraphNode():
    """
    Node of type :Graph in N4J
    """
    
    @staticmethod
    def validate_name(name):
        try:
            return not any([ud.category(unicode(c)).startswith('P') for c in name])
        except:
            return False

    def __init__(self, usernode, name):

        if usernode and name:
            if not GraphNode.validate_name(name):
                raise I.GraphNameError("Invalid name %s " % name )
            self.node = Node(labels.GraphNode.label, name=name)
            self.own_rel = Relationship(self.node, labels.GraphNode.label_owned_by, usernode.node, creation_time=get_time())
    

    @staticmethod
    def get_by_name(db, name):
        if not GraphNode.validate_name(name):
            raise I.GraphNameError("Invalid name '%s'" % name)
            
        graph = GraphNode(None, None)
        graph.node = db.find_one(labels.GraphNode.label, 'name', name)
        if graph.node is None:
            raise I.GraphNotFoundError(name)
        graph.own_rel = graph.node.match(labels.GraphNode.label_owned_by).next()
        if graph.own_rel is None:
            raise I.GraphError("Graph %s has no owner %s" % name )
        return graph

    @staticmethod
    def get_all(db):
        return [dict(x.properties) for x in db.find(labels.GraphNode.label)]
        
    @staticmethod
    def get_count(db):
        cypher =  """
        MATCH (g:%(graphnode)s ) 
        RETURN count(g) as count
        """ % label_names

        params = {}
        row = db.cypher.execute(cypher, params)
        return row[0]['count']
        
    @staticmethod
    def get_type_property_names(props):
        return [  prop['name'] for prop in props ]
        
        
    def get_name(self):
        return self.node['name']

    def get_entities(self):
        return [self.node, self.own_rel]

    def get_owners(self):
        """ return owners usernames """
        own_rels = self.get_entities()
        return own_rels[1].end_node.properties['username']

    def set_properties(self, props):

        for k in ('description', 'pad_url', 'image', 'seed'):
            self.node[k] = props.get(k,"")

        tags = props.get('tags', [])
        self.node['tags'] = list(set(tags)) if tags and len(tags) else None    
        
    def get_node_types(self):
        rels = self.node.match(labels.NodeType.label_belongs)
        rels = [  r.nodes[0].properties for r in rels ]
        return self._get_types_properties( rels )

    def get_edge_types(self):
        rels = self.node.match(labels.EdgeType.label_belongs) 
        rels =  [  r.nodes[0].properties for r in rels ]
        return self._get_types_properties( rels )

    def _get_types_properties(self, rels):
        types =  [ dict(t) for t in rels ]
        for i, t in enumerate(types):
            types[i]["properties"] = json.loads(t.get("properties", "{}"))
        
        return types

        
    def get_edge_count(self):
        
        cypher =  """
        MATCH (:%(graphnode)s {name:{gid}}) -[e:%(uedge_belongs)s]- (:%(uedge)s)
        RETURN count(e) as count
        """ % label_names

        params = {
            'gid':self.node['name'],
        }

        db = self.node.graph
        return [{ 'edge_count': row['count']}
            for row in db.cypher.execute(cypher, params)][0]

    def _get_vote_count(self, vote ):

        if vote in ('up', 'down' ):
            if vote == 'up' :
                direction = label_names['user_upvote_graph']

            if vote == 'down' :
                direction = label_names['user_downvote_graph']

            _labels = dict(label_names)
            _labels['direction'] = direction
            
            cypher =  """
            MATCH (:%(graphnode)s {name:{gid}}) <-[e:%(direction)s]- (:%(user)s)
            RETURN count(e) as count
            """ % _labels

            params = {
                'gid':self.node['name'],
            }

            db = self.node.graph
            return [row['count'] for row in db.cypher.execute(cypher, params)][0]
                    
        raise ValueError('Invalid vote direction %s' % vote)
        
    def get_upvote_count(self):
        return self._get_vote_count('up')
        
    def get_downvote_count(self):
        return self._get_vote_count('down')
        
    def get_node_count(self):
            
        cypher =  """
        MATCH (:%(graphnode)s {name:{gid}}) <-[n:%(unode_belongs)s]- (:%(unode)s)
        RETURN count(n) as count
        """ % label_names

        params = {
            'gid':self.node['name'],
        }

        db = self.node.graph
        return [{ 'node_count': row['count'] }
            for row in db.cypher.execute(cypher, params)][0]

        
    def get_types_meta(self, otype):
        """
        :param otype: 'node' | 'edge'
        :returns:
                { name : type name ,
                  uuid : uuid,
                  count: edge/node count for this type
                }
        """
        if otype not in ('node', 'edge'):
            raise ValueError( 'no meta for %s' % otype )
            
        if otype == 'edge':
            #cypher =  """
            #MATCH (:%(graphnode)s {name:{gid}}) <-[]- (n:%(edgetype)s) <-[]- (u:%(uedge)s)
            #RETURN n.name as name, n.uuid as uuid, count(u) as count
            #""" % label_names
            
            cypher =  """
            MATCH (:%(graphnode)s {name:{gid}}) <-[e:%(uedge_belongs)s]- (:%(uedge)s) -[]-> (n:%(edgetype)s)
            RETURN n.name as name, n.uuid as uuid, count(e) as count
            """ % label_names

        else :
            cypher =  """
            MATCH (:%(graphnode)s {name:{gid}}) <- []- (n:%(nodetype)s) <-[]- (u:%(unode)s)
            RETURN n.name as name, n.uuid as uuid, count(u) as count
            """ % label_names
        
        params = {
            'gid':self.node['name'],
        }
        db = self.node.graph
        return [{ 'name': row['name'], 'uuid': row['uuid'], 'count': row['count']}
                for row in db.cypher.execute(cypher, params)]

    def get_starred_node_uuids(self, limit=200):
            
        cypher =  """
        MATCH (g:%(graphnode)s {name:{gid}}) -[e:%(starred)s]-> (n:%(unode)s)
        WHERE NOT coalesce(n.deleted, false)
        RETURN n.uuid as uuid
        """ % label_names

        r = [ row['uuid'] for row in self.node.graph.cypher.execute(cypher, {'gid': self.node['name']})]        
        return list(set(r))

    def get_starred_node_count(self):
            
        cypher =  """
        MATCH (g:%(graphnode)s {name:{gid}}) -[e:%(starred)s]-> (n:%(unode)s)
        WHERE NOT coalesce(n.deleted, false)
        RETURN count(n) as count
        """ % label_names

        r = [ row['count'] for row in self.node.graph.cypher.execute(cypher, {'gid': self.node['name']})]        
        return r[0]

        
    def star(self, nodes_uuids):
        """ mark nodes as starred"""
        
        cypher =  """
        MATCH (g:%(graphnode)s {name:{gid}})
        WITH g MATCH  (g) <-- (n:%(unode)s)
        WHERE n.uuid IN {uuids}
        AND NOT coalesce(n.deleted, false)
        CREATE (g) -[:%(starred)s]-> (n)
        """ % label_names

        self.node.graph.cypher.execute(cypher, {'uuids': nodes_uuids, 'gid': self.node['name']})
        
        
    def unstar(self, nodes_uuids):
        """ unmark starred nodes"""
            
        cypher =  """
        MATCH (g:%(graphnode)s {name:{gid}}) -[e:%(starred)s]-> (n:%(unode)s)
        WHERE n.uuid IN {uuids}
        AND NOT coalesce(n.deleted, false)
        
        DELETE e
        """ % label_names

        self.node.graph.cypher.execute(cypher, {'uuids': nodes_uuids, 'gid': self.node['name']})


    def complete_label(self, what, prefix, start=0, size=100):
        """
            Label completion returns only label and uuid

            return :
                label
                uuid
                nodetype
                # TODO neighbors count 
                # TODO properties
                # TODO last edit time
        """
        
        if what == 'node'  : what = 'unode'
        if what == 'edge'  : what = 'uedge'
        what in ('unode', 'uedge', 'nodetype', 'edgetype')
        objecttype = label_names[what]
            
        if what in ('node', 'edge', 'unode', 'uedge'):
            prop = 'label'
        elif what in ('nodetype', 'edgetype'):
            prop = 'name'
        else:
            raise I.GraphError("No complete on %s, %s" % (what, prefix) )

        _label_names = dict(label_names)
        _label_names['propname']= prop
        
        cypher = """
        MATCH (np:%(properties)s) <-- (n:%(unode)s) --> (:%(graphnode)s {name:{graphname}})
        WHERE np.%(propname)s STARTS WITH {prefix}
        WITH np
        MATCH (np) <-- (n:%(unode)s) --> (nt:%(nodetype)s)
        WHERE  NOT coalesce(n.deleted, false)
        RETURN nt.name, np.label, n.uuid ORDER BY np.label SKIP {start} LIMIT {size}
        """ % _label_names
        
        db = self.node.graph
        params = {
            'graphname':self.node['name'],
            'prefix': prefix,
            'type': objecttype,
            'start' : start,
            'size' : size,
        }

        result = [{ 'nodetype': r['nt.name'],
                    'label':r['np.label'],
                    'uuid':r['n.uuid']
                  } for r in db.cypher.execute(cypher, params )]

        return result

    def get_subgraph(self, nodes_uuids):
        import timeit
        print "SUBG1", timeit.default_timer()
        cypher = """
        
        MATCH (n1:%(unodelabel)s)
        USING INDEX n1:UNode(uuid)
        WHERE n1.uuid IN {uuids}
        AND NOT coalesce(n1.deleted, false)
        MATCH (n1) <-[:%(from)s]- (e:%(uedgelabel)s) -[:%(to)s]-> (n2:%(unodelabel)s)
        USING INDEX n2:UNode(uuid)
        WHERE n2.uuid IN {uuids}
        AND NOT coalesce(n2.deleted, false)
        AND (e) -[:%(ingraphrel)s]-> (:%(graphnodelabel)s {name:{graph_name}})
        MATCH (e) -[:%(totype)s]-> (et:%(edgetypelabel)s),
              (e) -[:%(hasprops)s] -> (p:%(properties)s)
        WHERE NOT coalesce(e.deleted, false)
        RETURN n1.uuid as src , e.uuid as uuid, p as props, n2.uuid as tgt, et.uuid as edgetype
        """ % {'unodelabel': labels.UNode.label,
               'uedgelabel': labels.UEdge.label,
               'hasprops': labels.UEdge.has_properties,
               'properties': labels.UEdge.properties,
               'from': labels.UEdge.label_source,
               'to': labels.UEdge.label_target,
               'totype': labels.UEdge.label_type,
               'edgetypelabel': labels.EdgeType.label,
               'ingraphrel': labels.UEdge.label_belongs,
               'graphnodelabel': labels.GraphNode.label}

        

        r = [ edge_to_dict( row.uuid, row['src'],  dict(row.props.properties), row['edgetype'], row['tgt'] )
            for row in self.node.graph.cypher.execute(cypher, {'uuids': nodes_uuids, 'graph_name': self.node['name']})]

        print "SUBG1",len(nodes_uuids),len(r), timeit.default_timer()
        return r

    def get_edge_list(self, edge_uuids):
        cypher = """
        MATCH (e:%(uedgelabel)s) 
        USING INDEX e:%(uedgelabel)s(uuid)
        WHERE e.uuid IN {uuids}
        AND NOT coalesce(e.deleted, false)
        MATCH (n1:%(unodelabel)s) <-[:%(from)s]- (e) -[:%(to)s]-> (n2:%(unodelabel)s),
        (e) -[:%(totype)s]-> (et:%(edgetypelabel)s),
        (e) -[:%(hasprops)s] -> (p:%(properties)s)
        WHERE (e) -[:%(ingraphrel)s]-> (:%(graphnodelabel)s {name:{graph_name}})
        AND NOT coalesce(n1.deleted, false)
        AND NOT coalesce(n2.deleted, false)
        RETURN n1.uuid as src , e.uuid as uuid, p as props, n2.uuid as tgt, et.uuid as edgetype
        """ % {'unodelabel': labels.UNode.label,
               'uedgelabel': labels.UEdge.label,
               'hasprops': labels.UEdge.has_properties,
               'properties': labels.UEdge.properties,
               'from': labels.UEdge.label_source,
               'to': labels.UEdge.label_target,
               'totype': labels.UEdge.label_type,
               'edgetypelabel': labels.EdgeType.label,
               'ingraphrel': labels.UEdge.label_belongs,
               'graphnodelabel': labels.GraphNode.label}

        rows =  [edge_to_dict( row.uuid,  row['src'],  dict(row.props.properties), row['edgetype'], row['tgt'] )
                for row in self.node.graph.cypher.execute(cypher, {'uuids': edge_uuids, 'graph_name': self.node['name']})]
        return rows

        
    def destroy(self):
        import json
        resource = py2neo.ServerPlugin(self.node.graph, 'Destroyer').resources.get('deleteGraph')
        args = { 'graphname': self.get_name()}
        response = resource.post(args)
        result = json.loads(response.content)
        return result 


    def proxemie(self, p0, weights=None, filter_edges=None, filter_nodes=None, limit=50, n_step=3):
        import json
        import timeit
        print "Proxit", timeit.default_timer()
        if weights is None:
            weights = []
        if filter_edges is None:
            filter_edges = []
        if filter_nodes is None:
            filter_nodes = []

        resource = py2neo.ServerPlugin(self.node.graph, 'Prox').resources.get('filteredProxMC')
        args = {
            'p0':p0, 
            'weights':weights,
            'step': n_step, 
            'limit':limit,
            'filter_nodes': filter_nodes,
            'filter_edges': filter_edges
            }
        response = resource.post(args)
        result = json.loads(response.content)
        print "Proxit", timeit.default_timer()
        return result

    def proxemieMC(self, p0, weights=None, filter_edges=None, filter_nodes=None, limit=50, n_step=3, n_walk=10000):
        import json
        import timeit
        print "PMC", timeit.default_timer()

        if weights is None:
            weights = []
        if filter_edges is None:
            filter_edges = []
        if filter_nodes is None:
            filter_nodes = []

        resource = py2neo.ServerPlugin(self.node.graph, 'Prox').resources.get('filteredProxMC')
        args = {
            'p0':p0, 
            'weights':weights,
            'step': n_step, 
            'limit':limit,
            'filter_nodes': filter_nodes,
            'filter_edges': filter_edges,
            'nwalks': n_walk,
            }
        response = resource.post(args)
        result = json.loads(response.content)
        print "PMC", timeit.default_timer()
        return result


    def get_metadata(self):

        import time
        one = time.time()
        data = self.as_dict()
        data['meta']['edgetypes'] = self.get_types_meta('edge')
        data['meta']['nodetypes'] = self.get_types_meta('node')
        data['meta']['time'] = time.time() - one

        return data


    def as_dict(self):
        #return dict(self.node.properties)
        orempty = lambda x : self.node[x] if self.node[x] else ""

        properties = {
            'name' : self.get_name(),
            'description' : orempty('description'),
            'image' : orempty('image'),
            'tags' : self.node['tags'] if self.node['tags'] else [],
        }
        
        attributes = {
            'upvotes'    : self.get_upvote_count(),
            'downvotes'  : self.get_downvote_count(),
            'star_count' : self.get_starred_node_count(),
            'owner'      : self.get_owners(),
        }
        attributes['votes'] = attributes['upvotes'] - attributes['downvotes']
        attributes.update(self.get_edge_count())
        attributes.update(self.get_node_count())

        return {
            'edgetypes' : self.get_edge_types(),
            'nodetypes' : self.get_node_types(),
            'properties' : properties,
            'meta' : attributes 
        }

        


