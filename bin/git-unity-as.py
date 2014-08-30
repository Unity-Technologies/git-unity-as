#!/usr/bin/python

import psycopg2
import psycopg2.extras
import argopen
import time
import sys

try:
    conn = psycopg2.connect("dbname='assetservertest' user='admin' host='localhost' password='unity' port='10733'")
except:
    print "I am unable to connect to the database"

query_assetversions="""
SELECT av.created_in AS changeset, guid2hex(a.guid) AS guid, guid2hex(get_asset_guid_safe(av.parent)) AS parent, av.name, av.assettype 
FROM assetversion av, asset a
WHERE av.asset=a.serial
AND av.created_in < %d
ORDER BY av.serial
"""
query_assetversiondetails="""
SELECT vc.changeset, cs.description AS log, extract(epoch FROM commit_time)::int AS date, a.serial, guid2hex(a.guid) AS guid,
       av.name, guid2hex(get_asset_guid_safe(av.parent)) AS parent, av.assettype, av.serial AS version
FROM variant v, variantinheritance vi, variantcontents vc, changeset cs, changesetcontents cc, ASsetversion av, ASset a
WHERE v.name = 'work' 
AND vi.child = v.serial
AND vc.variant = vi.parent
AND cs.serial=vc.changeset
AND cs.serial=cc.changeset
AND cc.assetversion=av.serial
AND av.asset=a.serial
AND vc.changeset = %d
ORDER BY vc.changeset
"""
query_changesets="""
SELECT cs.serial as id, cs.description, cs.commit_time as date, 
  CASE WHEN p.email = 'none' OR p.email IS NULL THEN ' <' || p.username || '@' || p.username || '>'
       ELSE COALESCE(p.realname, p.username) || ' <' || p.email || '>'
  END AS author
FROM   (
         SELECT person.serial, person.username, users.realname, users.email
         FROM   person
         JOIN   all_users__view AS users ON person.username = users.username
       ) AS p,
       changeset cs
WHERE p.serial = cs.creator
AND cs.serial >= %d
"""

query_streams="""
SELECT assetversion,tag,lobj
FROM stream, assetcontents 
WHERE stream = lobj AND assetversion = %d
"""

cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

def export_data(data):
    print "data %d" % len(data)
    stdout.write(data)

def inline_data(stream, path, code = 'M', mode = '644'):
    # TODO: Read the stream!
    data = "StreamData%d" % stream 
    print "%s %s inline \"%s\"" % (code, mode, path)
    export_data(data + "\n")

def new_guid_item(name, parent):
    item = { 'name': name, 'parent': parent }
    return item

guid_map = {}
settings_guid="00000000000000000000000000000000"
trash_guid="ffffffffffffffffffffffffffffffff"

guid_map[settings_guid]=new_guid_item("ProjectSettings", None)
        
def guid_path(guid, new_parent = None, name = None):
    if(guid_map.has_key(guid) == False):
        if(name is not None):
            parent = None
            if(new_parent is not None):
                parent = new_parent
            elif(name.endswith(".asset")):
                parent = settings_guid

            guid_map[guid] = new_guid_item(name, parent)
        else:
            return "";
    elif(new_parent is not None):
        guid_map[guid] = new_guid_item(name, new_parent)

    def build_path(parent_guid, path = ""):
        node=guid_map[parent_guid]

        if(len(path) == 0):
            path = node['name']
        else:
            path = node['name'] + "/" + path

        if(node['parent'] is not None):
            path = build_path(node['parent'], path)
    
        return path

    return build_path(guid)

def get_streams(asset_version):
    cur.execute(query_streams % asset_version);
    streams = cur.fetchall()
    stream_ar = []
    for stream in streams:
        stream_ar.append({ 'tag': stream['tag'], 'lobj': stream['lobj'] })

    return stream_ar

def get_ops(asset_name, asset_version, asset_guid, parent_guid):
    ops=[]
    path=''
    streams = get_streams(asset_version)

    def create_op(op_name, op_path, stream_tag, stream_id):
        if(stream_tag == "asset.meta"):
            op_path += ".meta"
        return [op_name, op_path, stream_id]
    
    if(guid_map.has_key(asset_guid)):
        guid_item=guid_map[asset_guid]
        old_path=guid_path(asset_guid)
        if(guid_item['parent'] != parent_guid or guid_item['name'] != asset_name):
            if(guid_item['parent'] != trash_guid):
                for stream in streams:
                    ops.append(create_op('D', old_path, stream['tag'], stream['lobj']))

            path=guid_path(asset_guid, parent_guid, asset_name) 
        else:
            path=old_path
    else:
        path=guid_path(asset_guid, parent_guid, asset_name) 

    if(parent_guid != trash_guid):
        for stream in streams:
            ops.append(create_op('M', path, stream['tag'], stream['lobj']))

    return ops 

last_mark = 4001 if len(sys.argv) < 2 else int(sys.argv[1])
stdout = argopen.argopen('-', 'wb')

# First build GUID list of assets up until the specified changeset
cur.execute(query_assetversions % last_mark)
versions = cur.fetchall()
for version in versions:
    guid_path(version['guid'], version['parent'], version['name'])

cur.execute(query_changesets % last_mark)
changesets = cur.fetchall()
for changeset in changesets:
    mark=changeset['id']
    date = 0
    if(changeset['date']):
        date = changeset['date'].strftime('%s')

    author=changeset['author']
    comment=changeset['description']

    print "commit refs/heads/master"
    print "mark :%d" % mark
    print "author %s %s -0700" % (author, date)
    print "committer %s %s -0700" % (author, date)
    export_data(comment)

    if(last_mark == mark):
        print "deleteall"
    else:
        print "from :%d" % last_mark

    cur.execute(query_assetversiondetails % mark)
    versions = cur.fetchall()

    for version in versions:
        ops = get_ops(version['name'], version['version'], version['guid'], version['parent'])
        for op in ops:
            op_name=op[0]
            path=op[1] 
            stream=op[2]

            def M():
                inline_data(stream, path)

            def D():
                print "D %s" % path

            options = { 'M': M, 'D': D }
            options[op_name]()

    last_mark=mark

    



