# -*- coding: utf-8 -*-
#
# Lazyboy examples
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#
# This example assumes the following schema:
#
# <Keyspaces>
#     <Keyspace Name="UserData">
#         <ColumnFamily CompareWith="BytesType" Name="Users"/>
#     </Keyspace>
# </Keyspaces>
#


from lazyboy import *
from lazyboy.key import Key


# Define your cluster(s)
connection.add_pool('UserData', ['localhost:9160'])

# This can be used for convenience, rather than repeating the Keyspace
# and CF.
class UserKey(Key):
    """This is a key class which defaults to storage in the Users CF
    in the UserData Keyspace."""
    def __init__(self, key=None):
        Key.__init__(self, "UserData", "Users", key)



# Subclass Record to create an object of the correct type.
class User(record.Record):
    """A class representing a user in Cassandra."""

    # Anything in here _must_ be set before the object is saved
    _required = ('username',)

    def __init__(self, *args, **kwargs):
        """Initialize the record, along with a new key."""
        record.Record.__init__(self, *args, **kwargs)
        self.key = UserKey()


# Create an empty object
u = User()

# Set the key. A UUID will be generated for you
print u.key
# -> {'keyspace': 'UserData', 'column_family': 'Users',
#     'key': 'da6c8e19174f40cfa6d0b65a08eef62f',
#      'super_column': None}

# If you want to store records in a SuperColumn, set key.super_column:
superkey = u.key.clone(super_column="scol_a")

data = {'username': 'ieure', 'email': 'ian@digg.com'}

# The object is a dict. All these are equivalent.
u.update(data)
u.update(data.items())
u.update(**data)
for k in data:
    u[k] = data[k]

# Arguments to __init__ are passed to update()
u_ = User(data)
print u_           # -> {'username': 'ieure', 'email': 'ian@digg.com'}

# You can see if it's been modified.
print u.is_modified()           # -> True

# Save to Cassandra
u.save()           # -> {'username': 'ieure', 'email': 'ian@digg.com'}

print u.is_modified()           # -> False

# Load it in a new instance.
#u_ = User().load(key)
u_ = User().load(u.key.clone())    # PATCH

print u_           # -> {'username': 'ieure', 'email': 'ian@digg.com'}

print u.is_modified()           # -> False
del u['username']
print u.valid()                 # -> False
print u.missing()               # -> ('username',)
try:
    u.save()        # -> ('Missing required field(s):', ('username',))
except Exception, e:
    print e

# Discard modifications
u.revert()
print u.is_modified()           # -> False
print u.valid()                 # -> True
