# -*- coding: utf-8 -*-
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#
# This example assumes the following schema:
#
# <Keyspaces>
#     <Keyspace Name="UserData">
#         <ColumnFamily CompareWith="BytesType" Name="Users"/>
#         <ColumnFamily CompareWith="BytesType" Name="UserViews"/>
#     </Keyspace>
# </Keyspaces>
#

from pprint import pprint

from lazyboy import *
from lazyboy.key import Key
from record import UserKey, User

connection.add_pool('UserData', ['localhost:9160'])

class AllUsersViewKey(Key):
    """This is a key class which defaults to storage in the UserViews CF
    in the UserData Keyspace."""
    def __init__(self, key=None):
        Key.__init__(self, "UserData", "UserViews", key)


# You should subclass View to provide defaults for your view.
class AllUsersView(View):
    def __init__(self):
        View.__init__(self)
        # This is the key to the CF holding the view
        self.key = AllUsersViewKey(key='row_a')
        # Records referenced in the view will be instances of this class.
        self.record_class = User
        # This is the key for the record.
        self.record_key = UserKey()


# Instantiate the view
auv = AllUsersView()

_users = ({'username': 'jsmith', 'email': 'jsmith@example.com'},
          {'username': 'jdoe', 'email': 'jdoe@example.com'},
          {'username': 'jxdoe', 'email': 'jxdoe@example.com'})

# Add records to the view
for _user in _users:
    auv.append(User(_user).save())

# When the view is iterated, the records referenced in it's keys are
# returned.
pprint(tuple(auv))
# -> ({'username': 'jxdoe', 'email': 'jxdoe@example.com'}
#     {'username': 'jdoe', 'email': 'jdoe@example.com'},
#     {'username': 'jsmith', 'email': 'jsmith@example.com'})
