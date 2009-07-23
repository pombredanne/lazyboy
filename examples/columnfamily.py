# -*- coding: utf-8 -*-
#
# Lazyboy examples
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#
# This example assumes the following schema:
#
# <Tables>
#     <Table Name="UserData">
#         <ColumnFamily ColumnSort="Name" Name="Users"/>
#     </Table>
# </Tables>
#


from lazyboy import *


# Define your cluster(s)
connection.add_pool('UserData', ['localhost:9160'])


# Subclass ColumnFamily to create an object of the correct type.
class User(columnfamily.ColumnFamily):
    """A class representing a user in Cassandra."""

    # _key is the key template. It's values are given to
    # PrimaryKey.__init__ as keyword arguments any time a PK is
    # instantiated for this object.
    _key = {'table': 'UserData',        # The table to store in
            'family': 'Users'}          # The ColumnFamily name to store on

    # Anything in here _must_ be set before the object is saved
    _required = ('username',)


# Create an empty object
u = User()

# A PrimaryKey is generated for you:
print u.pk
# -> {'table': 'UserData', 'superkey': None,
#     'key': 'da6c8e19174f40cfa6d0b65a08eef62f',
#     'family': 'Users', 'supercol': None}

data = {'username': 'ieure', 'email': 'ian@digg.com'}

# The object is a dict. All these are equivalent.
u.update(data)
u.update(data.items())
u.update(**data)
for k in data:
    u[k] = data[k]

# Arguments to __init__ are passed to update()
u = User(data)
print u            # -> {'username': 'ieure', 'email': 'ian@digg.com'}

# You can see if it's been modified.
print u.is_modified()           # -> True

# Save to Cassandra
u.save()           # -> {'username': 'ieure', 'email': 'ian@digg.com'}

print u.is_modified()           # -> False

# Load it in a new instance.
u_ = User().load(u.pk.key)
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

