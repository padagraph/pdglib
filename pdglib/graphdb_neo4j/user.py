# -*- coding:utf8 -*-
from utils import *
import labels
from labels import label_names

class User():
    """
    Node of type :User in our N4J DB
    creating a new User object create a new User,
    to fetch existing user, use User.get_by_uid()
    """

    def __init__(self, username, email, password, active=False):
        if username and email and password:
            #uid = uuid()
            self.node = Node(labels.User.label , username=username, password=password, email=email.lower(), active=active)
     
    @staticmethod
    def get_by_login(db, username):
        user = User(None, None, None)
        user.node = db.find_one(labels.User.label, 'username', username)
        if not user.node:
            raise I.UserLoginNotFoundError('username: %s' % username)
        return user

    @staticmethod
    def get_by_email(db, email):
        user = User(None, None, None)
        user.node = db.find_one(labels.User.label, 'email', email.lower())
        if not user.node:
            return None
        return user
        
    @staticmethod
    def get_count(db):
            
        cypher =  """
            MATCH  (u:%(user)s)
            RETURN count(u) as count
            """ % label_names
        
        params = {}
        row = db.cypher.execute(cypher, params)
        return row[0]['count']

    

    def get_login(self):
        return self.node.properties['username']
            
    def activate(self):
        self.node['active'] = True
        self.node.push()
        return True

    def update_password(self, hash_password):
        self.node['password'] = hash_password 
        self.node.push()
        return True

    def get_vote_graph(self, gid):
        
        cypher =  """
        MATCH (u:%(user)s {username:{username}})
        MATCH (g:%(graphnode)s {name:{gid}})
        MATCH (u) -[e:%(user_upvote_graph)s|:%(user_downvote_graph)s]-> (g)
        RETURN e 
        """ % label_names

        username = self.node['username']
        params = {'username': username, 'gid': gid}

        db = self.node.graph
        res = db.cypher.execute(cypher, params)
        types = [row['e'].type for row in res ]

        return {
            'gid'      : gid,
            'username' : username,
            'upvote'   : labels.User.upvote_graph in types ,
            'downvote' : labels.User.downvote_graph in types ,
        }
        
        
        
    def upvote_graph(self, gid):
        
        self.rm_downvote_graph(gid)
        
        cypher =  """
        MATCH (u:%(user)s {username:{username}})
        MATCH (g:%(graphnode)s {name:{gid}})
        CREATE (u) -[e:%(user_upvote_graph)s {timestamp: timestamp()}]-> (g)
        """ % label_names

        params = {'username': self.node['username'], 'gid': gid}

        self.node.graph.cypher.execute(cypher, params)

        
    def rm_upvote_graph(self, gid):
        
        cypher =  """
        MATCH (u:%(user)s {username:{username}}) -[e:%(user_upvote_graph)s ]-> (g:%(graphnode)s {name:{gid}})
        DELETE (e) 
        """ % label_names

        params = {'username': self.node['username'], 'gid': gid}
        self.node.graph.cypher.execute(cypher, params)
        
    def downvote_graph(self, gid):

        self.rm_upvote_graph(gid)
        
        cypher =  """
        MATCH (u:%(user)s {username:{username}})
        MATCH (g:%(graphnode)s {name:{gid}})
        CREATE (u) -[e:%(user_downvote_graph)s {timestamp: timestamp()}]-> (g)
        """ % label_names

        params = {'username': self.node['username'], 'gid': gid}

        self.node.graph.cypher.execute(cypher, params)
        
    def rm_downvote_graph(self, gid):
        
        cypher =  """
        MATCH (u:%(user)s {username:{username}}) -[e:%(user_downvote_graph)s ]-> (g:%(graphnode)s {name:{gid}})
        DELETE (e) 
        """ % label_names

        params = {'username': self.node['username'], 'gid': gid}
        self.node.graph.cypher.execute(cypher, params)
        
        

    def as_dict(self):
        properties = dict(self.node.properties)
        return properties


