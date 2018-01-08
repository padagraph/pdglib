# -*- coding:utf8 -*-

from utils import *
from types import EdgeType, NodeType 
import labels


class UNode():
    """
    Node describing :UserNode in users' graphs
    """
    
    #label = 'UNode'
    #plabel = 'Properties'
    #label_belongs = 'NodeInGraph'
    #label_type = 'OfNodeType'
    #label_edit = 'Edit'
    #label_set_by = 'SetByUser'

    def __init__(self, connexion, user=None, graph=None, nodetype=None, properties=None, inTx=False):

        if None not in (user, graph, nodetype, properties):
            # creating a new UNode
            req = """
            MATCH (g:%(graph)s {name:{graphname}}) <-[:%(typebelongs)s]- (nt:%(nodetype)s {uuid:{nt_uuid}})
            MATCH (u:%(user)s {username: {user}})
            CREATE (n:%(unode)s {uuid:{n_uuid}})
            CREATE (n) -[:%(hasprops)s {timestamp: timestamp()}]-> (p:%(properties)s {props})
            CREATE (n) -[:%(ingraph)s]-> (g)
            CREATE (n) -[:%(oftype)s]-> (nt)
            CREATE (p) -[:%(setby)s]-> (u)
            """ %  {'graph': labels.GraphNode.label,
                    'typebelongs': labels.NodeType.label_belongs,
                    'nodetype': labels.NodeType.label,
                    'user': labels.User.label,
                    'unode': labels.UNode.label,
                    'properties': labels.UNode.properties,
                    'hasprops': labels.UNode.has_properties,
                    'ingraph': labels.UNode.label_belongs,
                    'oftype': labels.UNode.label_type,
                    'setby': labels.User.setby}
            self.uuid = uuid()
            self.starred = False
            self.properties = properties
            self.graphname = graph
            self.nodetype = nodetype
            self.db = connexion
            params = {'graphname': graph,
                      'nt_uuid': nodetype,
                      'user': user,
                      'n_uuid': self.uuid,
                      'props': properties}
            if inTx:
                # in a transaction, just append the request
                connexion.append(req, params)
                self.db = connexion.graph
            else:
                connexion.cypher.execute(req, params)
        else:
            self.db = connexion
            self.uuid = None
            self.starred = False
            self.deleted = False
            self.properties = {}

    def get_graph_name(self):
        return self.graphname

    def update_properties(self, properties, userid):
        req = """
        MATCH (u:%(user)s {username:{userid}})
        MATCH (n:%(unode)s {uuid:{uuid}}) -[prev_rel:%(has_properties)s]-> (prev_state:%(properties)s)
        CREATE (p:%(properties)s {props}) <-[new_rel:%(has_properties)s {timestamp: timestamp()}]- (n)
        CREATE (prev_state) -[:%(log)s {timestamp: prev_rel.timestamp}]-> (p)
        CREATE (p) -[:%(setby)s]-> (u)
        DELETE prev_rel
        """ %{'unode': labels.UNode.label,
              'user': labels.User.label,
              'has_properties': labels.UNode.has_properties,
              'properties': labels.UNode.properties,
              'log': labels.UNode.log,
              'setby': labels.User.setby}

        self.db.cypher.execute(req, {'uuid': self.uuid, 'props': properties, 'userid': userid})

        
    def get_count_neighbors(self, action, filter_edges=None, filter_nodes=None, mode='ALL', start=0, size=100):
        """
            :param action : 'get' or 'count'
            :param filter_edges: edge types uuids
            :param filter_nodes: node types uuids
        """
        MODES = ('ALL', 'IN', 'OUT')
        if mode not in MODES :
            raise I.ApiError( "'%s' not in %s " % (mode, MODES) )
            
        if mode == 'IN':
            filter_mode = "AND () -[rel_to_edge:%s]- ()" % (UEdge.label_target)
        elif mode == 'OUT':
            filter_mode = "AND () -[rel_to_edge:%s]- ()" % (UEdge.label_source)
        else:
            filter_mode = ""

        if action == "get":
            return_action = "RETURN e, n2, nt.uuid, et.uuid, rel_to_edge, p.label as label, p, pe SKIP {start} LIMIT {size}"
        #    return_action = "RETURN e, n2, nt.uuid, et.uuid, rel_to_edge SKIP {start} LIMIT {size}"
        elif action == "count":
            return_action = "RETURN count(n2) as count"

        cypher = """
        MATCH (n:%(unodelabel)s {uuid:{node_id}}) <-[rel_to_edge]- (e:%(uedgelabel)s) -[]-> (n2:%(unodelabel)s) ,
        (e) -[:%(hasedgetype)s]-> (et:%(edgetypelabel)s),
        (e) -[:%(has_properties)s]-> (pe:%(edge_properties)s),
        
        (n2) -[:%(hasnodetype)s]-> (nt:%(nodetypelabel)s),
        (n2) -[:%(has_properties)s]-> (p:%(node_properties)s)

        WHERE (( {filter_edges} IS NULL  ) OR et.name IN {filter_edges} 
        AND ( {filter_nodes} IS NULL ) OR nt.name IN {filter_nodes})
        AND NOT coalesce(n.deleted, false)
        AND NOT coalesce(n2.deleted, false)
        AND NOT coalesce(e.deleted, false)
        %(filter_mode)s
        %(return_action)s
        """ % {'unodelabel': labels.UNode.label,
               'uedgelabel': labels.UEdge.label,
               'hasedgetype': labels.UEdge.label_type,
               'hasnodetype': labels.UNode.label_type,
               'edgetypelabel': labels.EdgeType.label,
               'has_properties': labels.UNode.has_properties,
               'node_properties': labels.UNode.properties,
               'edge_properties': labels.UEdge.properties,
               'filter_mode': filter_mode,
               'return_action' : return_action,
               'nodetypelabel': labels.NodeType.label}
        params = {'node_id': self.uuid,
                  'filter_edges': filter_edges,
                  'filter_nodes': filter_nodes,
                  'start': start,
                  'size': size
                }
                
        res = self.db.cypher.execute(cypher, params)
        
        if action == "get":
            result = []
            for row in res  :
                rel = row['rel_to_edge'].type
                if rel == labels.UEdge.label_source:
                    direction = 'OUT'
                elif rel == labels.UEdge.label_target:
                    direction = 'IN'
                else : 
                    direction = 'ALL'
                props = dict(row['p'].properties)
                props['label'] = row.label

                result.append((
                        { 'uuid' : row['e'].properties['uuid'],
                          'edgetype'  : row['et.uuid'],
                          'properties' : row['pe'].properties,
                          'weight' : row['e']['weight'],
                        },
                        direction,
                        {   'nodetype':   row['nt.uuid'],
                            'properties': props,
                            'uuid': row['n2']['uuid'],
                            'label': row['label']
                        }))
                        
            return result
                
        elif action =="count":
            return res[0]['count']

               

  
    @staticmethod
    def get_nodes_by_id(db, graphname, uuids):
        req = """
        UNWIND {uuids} AS id
        MATCH (n:%(unode)s {uuid:id}) -[:%(belongsto)s]-> (g:%(graph)s),
        (n) -[:%(oftype)s]-> (nt:%(nodetype)s),
        (n) -[:%(hasprops)s] -> (p:%(properties)s)
        WHERE NOT coalesce(n.deleted, false)
        OPTIONAL MATCH (g) -[s:%(node_starred)s]-> (n)
        RETURN n.uuid as uuid, p.label as label, g.name as graphname, nt.uuid as nodetype, p, s as star
        """ %{'unode': labels.UNode.label,
              'belongsto': labels.UNode.label_belongs,
              'graph': labels.GraphNode.label,
              'node_starred' : labels.GraphNode.label_has_star,
              'oftype': labels.UNode.label_type,
              'nodetype': labels.NodeType.label,
              'hasprops': labels.UNode.has_properties,
              'properties': labels.UNode.properties}

        results = []
        response = db.cypher.execute(req, {'uuids': uuids})
        for row in response:
            node = UNode(db)
            node.uuid = row.uuid
            node.label = row.label
            node.starred = row.star != None
            node.graphname = row.graphname
            node.properties = dict(row.p.properties)
            node.nodetype = row.nodetype
            results.append(node)
        return results


    @staticmethod
    def get_by_id(db, node_uuid):
        nodes = UNode.get_nodes_by_id(db, "", [node_uuid])
        return nodes[0]


    @staticmethod
    def find_in_graph(db, graph, nodetype, properties, start=0, size=100):
        #OPTIONAL MATCH (g) -[s:%(node_starred)s]-> (n)
        req = """
        MATCH (p:%(properties)s) <-[:%(has_properties)s]- (n:%(unode)s) -[:%(ingraph)s]-> (g:%(graph)s {name:{graphname}})
        WHERE all(kv IN {props} WHERE p[head(kv)] = last(kv))
        AND NOT coalesce(n.deleted, false)
        RETURN n.uuid as uuid, p.label as label, p, s as starred
        SKIP {start} LIMIT {size}
        """ %{'properties': labels.UNode.properties,
              'has_properties': labels.UNode.has_properties,
              'unode': labels.UNode.label,
              'ingraph': labels.UNode.label_belongs,
              'graph': labels.GraphNode.label,
              'node_starred' : labels.GraphNode.label_has_star 
              }

        params = {'graphname': graph,
                  'nodetype': nodetype,
                  'props': properties.items(),
                  'start': start,
                  'size': size}
        results = []
        for row in db.cypher.execute(req, params):
            node = UNode(db)
            node.uuid = row.uuid
            node.label = row.label
            node.starred = row.starred != None
            node.graphname = graph
            node.properties = row.p.properties
            node.nodetype = nodetype
            results.append(node)
        return results

    @staticmethod
    def batch_create(db, user, graph_name, data):
        tx = db.cypher.begin()
        uuids = []
        for rank,params in enumerate(data):
            node = UNode(tx,user,graph_name,params["nodetype"], params['properties'],inTx=True)
            uuids.append((rank,node.uuid))
            tx.process()
        tx.commit()
        return uuids

    def delete(self, gid):
        from graphnode import GraphNode
        # delete from graph 
        req = """
        MATCH (g:%(graph)s {name:{gid}}) <-[:%(belong)s]- (n:%(unode)s {uuid:{node_id}})
        SET n.deleted = TRUE
        """ %{'graph': labels.GraphNode.label,
              'belong': labels.UNode.label_belongs,
              'unode': labels.UNode.label}

        params = {'node_id': self.uuid, 'gid': gid}
        res = self.db.cypher.execute(req, params)

    def as_dict(self):
        d = {} 
        d['nodetype'] = self.nodetype
        d['uuid'] = self.uuid
        #d['deleted'] = self.deleted
        d['starred'] = self.starred
        
        props = dict(self.properties)
        d['properties'] = props
        
        return d

# TODO: splitter les properties, gérer les change_x_properties du __init__.py
class UEdge():
    """
    Node describing :Edge in users' graphs (hyperedges in n4j)
    """

    def __init__(self, connexion, user=None, graph=None, edgetype=None, properties=None, source=None, target=None, inTx=False):
        self.db = connexion
        self.properties = properties
        self.graphname = graph
        self.edgetype = edgetype
        self.source = source
        self.target = target
        if None not in (graph, edgetype, properties):
            req = """
            MATCH (g:%(graph)s {name:{graph}}),
            (u:%(user)s {username:{user}}),
            (src:%(node)s {uuid:{src}}),
            (tgt:%(node)s {uuid:{tgt}}),
            (et:%(edgetype)s {uuid: {edgetype}})
            CREATE (e:%(edge)s {uuid:{uuid}})
            CREATE (e) -[:%(ingraph)s]-> (g)
            CREATE (e) -[:%(oftype)s]-> (et)
            CREATE (e) -[:%(fromnode)s]-> (src)
            CREATE (e) -[:%(tonode)s]-> (tgt)
            CREATE (e) -[:%(hasProps)s {timestamp:timestamp()}]-> (p:%(properties)s {props})
            CREATE (p) -[:%(setby)s]-> (u)
            """ %{'graph': labels.GraphNode.label,
                  'user': labels.User.label,
                  'node': labels.UNode.label,
                  'edgetype': labels.EdgeType.label,
                  'edge': labels.UEdge.label,
                  'ingraph': labels.UEdge.label_belongs,
                  'oftype': labels.UEdge.label_type,
                  'fromnode': labels.UEdge.label_source,
                  'tonode': labels.UEdge.label_target,
                  'hasProps': labels.UEdge.has_properties,
                  'properties':labels.UEdge.properties,
                  'setby': labels.User.setby}

            self.uuid = uuid()
            params = {'graph':graph,
                      'user': user,
                      'src': source,
                      'tgt': target,
                      'edgetype': edgetype,
                      'uuid': self.uuid,
                      'props': properties}
            if inTx:
                self.db = connexion.graph
                connexion.append(req, params)
            else:
                connexion.cypher.execute(req, params)

    def get_graph_name(self):
        return self.graphname
    
    def update_properties(self, properties, userid):
        req = """
        MATCH (u:%(user)s {username:{userid}})
        MATCH (n:%(uedge)s {uuid:{uuid}}) -[prev_rel:%(has_properties)s]-> (prev_state:%(properties)s)
        CREATE (p:%(properties)s {props}) <-[new_rel:%(has_properties)s {timestamp: timestamp()}]- (n)
        CREATE (prev_state) -[:%(log)s {timestamp: prev_rel.timestamp}]-> (p)
        CREATE (p) -[:%(setby)s]-> (u)
        DELETE prev_rel
        """ %{'uedge': labels.UEdge.label,
              'user': labels.User.label,
              'has_properties': labels.UEdge.has_properties,
              'properties': labels.UEdge.properties,
              'log': labels.UEdge.log,
              'setby': labels.User.setby}
        self.db.cypher.execute(req, {'uuid': self.uuid, 'props': properties, 'userid': userid})

    @staticmethod
    def get_by_id(db, node_uuid):
        uedge = UEdge(db)
        req = """
        MATCH (e:%(edge)s {uuid:{uuid}}),
        (e) -[:%(ingraph)s]-> (g:%(graph)s),
        (e) -[:%(totype)s]-> (et:%(edgetype)s),
        (e) -[:%(has_properties)s]-> (p:%(properties)s),
        (src:%(node)s) <-[:%(tosrc)s]- (e) -[:%(totgt)s]-> (tgt:%(node)s)
        WHERE NOT coalesce(e.deleted, false)
        RETURN 
        g.name as graphname, 
        et.uuid as edgetype,
        p,
        src.uuid as source,
        tgt.uuid as target
        """%{'edge': labels.UEdge.label,
             'ingraph': labels.UEdge.label_belongs,
             'graph': labels.GraphNode.label,
             'totype': labels.UEdge.label_type,
             'edgetype': labels.EdgeType.label,
             'has_properties': labels.UEdge.has_properties,
             'properties': labels.UEdge.properties,
             'node': labels.UNode.label,
             'tosrc': labels.UEdge.label_source,
             'totgt': labels.UEdge.label_target}
        try:
            result = db.cypher.execute(req, {'uuid':node_uuid}).one
            uedge.properties = result.p.properties
            uedge.graphname = result.graphname
            uedge.edgetype = result.edgetype
            uedge.source = result.source
            uedge.target = result.target
            uedge.uuid = node_uuid
        except:
            raise I.EdgeNotFoundError(node_uuid)
        return uedge
    


    @staticmethod
    def find_in_graph(db, graph, edgetype, properties, start=0, size=100):
        req = """
        MATCH (p:%(properties)s) <-[:%(has_properties)s]- (e:%(edge)s) -[:%(ingraph)s]-> (:%(graph)s {name:{graphname}})
        WHERE (e) -[:%(oftype)s]-> (:%(edgetype)s {uuid:{edgetype}}) 
        AND all(kv in {props} 
        AND p[head(kv)] = last(kv)
        AND NOT coalesce(e.deleted, false)
        MATCH (src:%(node)s) <-[:%(tosrc)s]- (e) -[:%(totgt)s]-> (tgt:%(node)s)
        RETURN 
        e.uuid as uuid,
        p,
        src.uuid as source
        tgt.uuid as target
        SKIP {start} LIMIT {size}
        """ % {'properties': labels.UEdge.properties,
               'has_properties': labels.UEdge.has_properties,
               'edge': labels.UEdge.label,
               'ingraph': labels.UEdge.label_belongs,
               'graph': labels.GraphNode.label,
               'oftype': labels.UEdge.label_type,
               'edgetype': labels.EdgeType.label,
               'node': labels.UNode.label,
               'tosrc': labels.UEdge.label_source,
               'totgt': labels.UEdge.label_target}

        params = {'graphname': graph,
                  'props': properties.items(),
                  'start': start,
                  'size': size,
                  'edgetype': edgetype}

        result = []
        for row in db.cypher.execute(req, params):
            uedge = UEdge(db)
            uedge.properties = row.p.properties
            uedge.graphname = graph
            uedge.edgetype = edgetype
            uedge.source = row.source
            uedge.target = row.target
            uedge.uuid = row.uuid
            result.append(uedge)
        return result

    @staticmethod
    def batch_create(db, user, graph_name, data):
        tx = db.cypher.begin()
        uuids = []
        for rank, params in enumerate(data):
            edge = UEdge(tx, user, graph_name, params['edgetype'], params['properties'], params['source'], params['target'], inTx=True)
            uuids.append((rank,edge.uuid))
            tx.process()
        tx.commit()
        return uuids

    def delete(self, gid):
        req = """
        MATCH (g:%(graph)s {name:{gid}}) <-[:%(belong)s]- (e:%(uedge)s {uuid:{node_id}})
        MATCH (from) <-[:%(from)s]- (e) -[:%(to)s]-> (to)
        SET e.deleted = TRUE
        SET from.deleted = TRUE
        SET to.deleted = TRUE
        """ % {'graph': labels.GraphNode.label,
               'belong': labels.UEdge.label_belongs,
               'uedge': labels.UEdge.label,
               'from': labels.UEdge.label_source,
               'to': labels.UEdge.label_target
               }

        params = {'node_id': self.uuid, 'gid': gid}
        res = self.db.cypher.execute(req, params)


    def as_dict(self):
        props = {}
        props['edgetype'] = self.edgetype
        props['uuid'] = self.uuid
        props['source'] = self.source
        props['target'] = self.target
        props['properties'] = dict(self.properties)
        return props
