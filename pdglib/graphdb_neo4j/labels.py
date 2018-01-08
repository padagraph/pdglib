
    
class Properties():
    label = 'Properties'

class UNode():
    label = 'UNode'
    properties = Properties.label
    label_belongs = 'NodeInGraph'
    label_type = 'OfNodeType'
    label_edit = 'Edit'
    has_properties = 'HasProperties'
    log = "Log"

class UEdge():
    """
    Node describing :Edge in users' graphs (hyperedges in n4j)
    """
    label = 'UEdge'
    label_belongs = 'EdgeInGraph'
    label_source = 'EdgeFromNode'
    label_target = 'EdgeToNode'
    label_type = 'OfEdgeType'
    has_properties = 'HasProperties'
    properties = Properties.label
    log = "Log"


class GraphNode():
    label = 'Graph'
    label_owned_by = 'OwnedBy'
    label_has_star = 'HasStarNode'
    
class NodeType():
    """
    Node describing :NodeType in N4J
    """

    label = 'NodeType'
    label_belongs = 'NodeBelongsTo'
    label_key = '_uniq_key'


class EdgeType():
    """
    Node describing :EdgeType in N4J
    """

    label = 'EdgeType'
    label_belongs = 'EdgeBelongsTo'
    label_key = '_uniq_key'

class User():
    """
    Node of type :User in our N4J DB
    creating a new User object create a new User,
    to fetch existing user, use User.get_by_uid()
    """
    label = 'User'
    setby = 'SetBy'

    upvote_graph   = 'UpVoteGraph'
    upvote_node    = 'UpVoteUNode'
    upvote_edge    = 'UpVoteUEdge'
    
    downvote_graph = 'DownVoteGraph'
    downvote_node  = 'DownVoteUNode'
    downvote_edge  = 'DownVoteUEdge'


label_names = {
          'user' : User.label,
          'user_upvote_graph' : User.upvote_graph,
          'user_downvote_graph' : User.downvote_graph,
          'user_upvote_node' : User.upvote_node,
          'user_downvote_node' : User.downvote_node,
          'user_upvote_edge' : User.upvote_edge,
          'user_downvote_edge' : User.downvote_edge,
          
          'graphnode' : GraphNode.label,

          'nodetype'  : NodeType.label,
          'unode'     : UNode.label,
          'unode_belongs': UNode.label_belongs,
          
          'edgetype'  : EdgeType.label,
          'uedge'     : UEdge.label,
          'uedge_belongs': UEdge.label_belongs,

          'properties': Properties.label, 
        
          'starred'   : GraphNode.label_has_star,
        }