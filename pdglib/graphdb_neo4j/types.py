# -*- coding:utf8 -*-

from utils import *

class Type():
    def __init__(self):
        pass
    
    def update_desc(self, desc):

        # TODO ensure not script injection
        
        self.node['description'] = desc
        self.node.push()

        
    def update(self, new_properties, description):
        added = []
        old_properties = json.loads(self.node['properties'])
        for key,value in new_properties.iteritems():
            if not key in old_properties:
                old_properties[key] = value
                added.append(key)

        changed = False
    
        if len(added) > 0 :
            self.node['properties'] = json.dumps(old_properties)
            changed = True
            
        if description != self.node['properties']:
            self.node['description'] = description
            changed = True
            
        if changed :
            self.node.push()

    def get_name(self):
        return self.node['name']

    def as_dict(self):
        xtype = dict(self.node.properties)
        xtype['properties'] = json.loads(xtype['properties'])
        return xtype


class NodeType(Type):
    """
    Node describing :NodeType in N4J
    """

    label = 'NodeType'
    label_belongs = 'NodeBelongsTo'
    label_key = '_uniq_key'

    def __init__(self, graph, name, properties, description=""):

        Type.__init__(self)
        
        if not None in (graph, name):
            self.uuid = uuid()
            if properties is None:
                properties = {}

            if type(properties) != dict:
                raise I.ApiError('NodeType properties should be a dict')
            key = '_' + graph.get_name() + '_' + name
            self.node = Node(self.label, name=name, uuid=self.uuid, description=description, properties=json.dumps(properties))
            self.belongs_rel = Relationship(self.node, self.label_belongs, graph.node)
            self.node[self.label_key] = key



    @staticmethod
    def get_by_uuid(db, nt_uuid): 
        nodetype = NodeType(None, None, None)
        nodetype.node = db.find_one(NodeType.label, 'uuid', nt_uuid)
        if nodetype.node is None:
            raise I.NodeTypeNotFoundError(nt_uuid)
        nodetype.belongs_rel = nodetype.node.match(NodeType.label_belongs).next()
        if nodetype.belongs_rel is None:
            raise I.GraphError("NodeType '%s' has no belongs_rel" % nt_uuid)
        nodetype.uuid = nodetype.node['uuid']
        nodetype.name = nodetype.node['name']
        nodetype.description = nodetype.node['description']
        nodetype.properties = nodetype.node['properties'] # == str(json)
        return nodetype
    
    @staticmethod
    def get_by_name(db, graph, name): 
        nodetype = NodeType(None, None, None)
        nodetype.node = None
        for rel in graph.node.match_incoming(NodeType.label_belongs):
            if rel.start_node['name'] == name:
                nodetype.node = rel.start_node
                nodetype.belongs_rel = rel
        if nodetype.node is None:
            raise I.NodeTypeNotFoundError(name)
        nodetype.uuid = nodetype.node['uuid']
        nodetype.name = nodetype.node['name']
        nodetype.description = nodetype.node['description']
        nodetype.properties = nodetype.node['properties'] # == str(json)
        return nodetype

class EdgeType(Type):
    """
    Node describing :EdgeType in N4J
    """

    label = 'EdgeType'
    label_belongs = 'EdgeBelongsTo'
    label_key = '_uniq_key'

    def __init__(self, graph, name, properties,  description=""):

        Type.__init__(self)
        
        if graph and name:
            self.uuid = uuid()
            if properties is None:
                properties = {}
            if type(properties) != dict:
                raise I.ApiError('EdgeType properties should be a dict')
            key = '_' + graph.get_name() + '_' + name
            self.node = Node(self.label, name=name, uuid=self.uuid, description=description, properties=json.dumps(properties))
            self.belongs_rel = Relationship(self.node, self.label_belongs, graph.node)
            self.node[self.label_key] = key
    




    @staticmethod
    def get_by_uuid(db, type_id): 
        edgetype = NodeType(None, None, None)
        edgetype.node = db.find_one(EdgeType.label, 'uuid', type_id)
        if edgetype.node is None:
            raise I.EdgeTypeNotFoundError("EdgeType type_id:%s" % type_id)
        edgetype.belongs_rel = edgetype.node.match(EdgeType.label_belongs).next()
        if edgetype.belongs_rel is None:
            raise I.GraphError("EdgeType '%s' belongs_rel is None" % type_id)
        return edgetype
    
    @staticmethod
    def get_by_name(db, graph, name):
        ""
        edgetype = EdgeType(None, None, None)
        edgetype.node = None
        for rel in graph.node.match_incoming(EdgeType.label_belongs):
            if rel.start_node['name'] == name:
                edgetype.node = rel.start_node
                edgetype.belongs_rel = rel
        if edgetype.node is None:
            raise I.EdgeTypeNotFoundError("EdgeType %s in graph" % name)
        return edgetype

