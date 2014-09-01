#!/usr/bin/python

import psycopg2
import psycopg2.extras
import psycopg2.extensions
import time
import sys
import getopt
from os.path import expanduser
import subprocess
import sys

if sys.platform == "win32":
    import os, msvcrt
    msvcrt.setmode(sys.stdout.fileno(), os.O_BINARY)

###### SQL queries
# query a simple list of asset versions used to build the guid_map
query_assetversions="""SELECT av.created_in AS changeset, guid2hex(a.guid) AS guid, guid2hex(get_asset_guid_safe(av.parent)) AS parent, av.name, av.assettype FROM assetversion av, asset a WHERE av.asset=a.serial AND av.created_in < %d ORDER BY av.serial"""

# query for full asset version details of a given changeset to translate into git commits
query_assetversiondetails="""SELECT vc.changeset, cs.description AS log, extract(epoch FROM commit_time)::int AS date, a.serial, guid2hex(a.guid) AS guid, av.name, guid2hex(get_asset_guid_safe(av.parent)) AS parent, av.assettype, av.serial AS version FROM variant v, variantinheritance vi, variantcontents vc, changeset cs, changesetcontents cc, ASsetversion av, ASset a WHERE v.name = 'work' AND vi.child = v.serial AND vc.variant = vi.parent AND cs.serial=vc.changeset AND cs.serial=cc.changeset AND cc.assetversion=av.serial AND av.asset=a.serial AND vc.changeset = %d ORDER BY vc.changeset"""

# list of changesets, greater than the specified commit
query_changesets="""SELECT cs.serial as id, cs.description, cs.commit_time as date, CASE WHEN p.email = 'none' OR p.email IS NULL THEN ' <' || p.username || '@' || p.username || '>' ELSE COALESCE(p.realname, p.username) || ' <' || p.email || '>' END AS author FROM (SELECT person.serial, person.username, users.realname, users.email FROM person JOIN all_users__view AS users ON person.username = users.username) AS p, changeset cs WHERE p.serial = cs.creator AND cs.serial >= %d"""

# list of all large object streams associated with a given asset version
query_streams="""SELECT assetversion,tag,lobj FROM stream, assetcontents WHERE stream = lobj AND assetversion = %d""" 
###### END SQL queries

###### Globals
# Special guids
settings_guid="00000000000000000000000000000000"
trash_guid="ffffffffffffffffffffffffffffffff"

# Mappings for asset paths
guid_map = {}
# We have to manually initialize the "ProjectSettings" path as it isn't acutally recorded in asset version history
guid_map[settings_guid] = { 'name': "ProjectSettings", 'parent': None }

# Directory where repositories for asset server databases are created
repo_root="%s/Library/AssetServer/Cloud/Chameleon" % expanduser("~")
###### End Globals

# Helper function to write the data header + given data to stdout
def export_data(out, data):
    out.write("data %d\n" % len(data))
    out.write(data)

# Helper function to write the data header + buffer binary data for a given stream to stdout
def inline_data(out, stream, path, code = 'M', mode = '644', nodata = False):
    out.write("%s %s inline \"%s\"\n" % (code, mode, path))

    if(nodata == True):
        out.write("data 1\n")
        out.write("-\n")
        return

    obj=psycopg2.extensions.lobject(conn, stream,'b')
    size=obj.seek(0,2)
    obj.seek(0,0)
    out.write("data %d\n" % size)
    bytes_read=0

    while bytes_read < size:
        buff_size = min(size - bytes_read, 2048)
        out.write(obj.read(buff_size))
        bytes_read += buff_size

# Create and return a object to be stored in the guid_map hash
def new_guid_item(name, parent):
    return { 'name': name, 'parent': parent }
        
# Get the full path for a given guid object, or move/rename an existing object
def guid_path(guid, new_parent = None, name = None):

    if(guid_map.has_key(guid) == False):
        if(name is not None):

            # Special case for ProjectSettings/*.asset
            if(name.endswith(".asset") and new_parent is None):
                parent = settings_guid
            else:
                parent = new_parent

            guid_map[guid] = new_guid_item(name, parent)
        else:
            return "";
    elif(new_parent is not None):
        guid_map[guid] = new_guid_item(name, new_parent)

    # recursive function to build a qualified path for a given guid
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

# Get a list of large object id's and associated tags for a given asset version
def get_streams(asset_version):
    cur.execute(query_streams % asset_version);
    streams = cur.fetchall()
    stream_ar = []
    for stream in streams:
        stream_ar.append({ 'tag': stream['tag'], 'lobj': stream['lobj'] })

    return stream_ar

# Get a list of commands to be sent to git fast-import
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

# Get a reference to the initial changeset id
def get_initial_changeset():
    query="""select serial from changeset order by serial limit 1"""
    cur.execute(query)
    return int(cur.fetchone()['serial'])

def git_export(out, export_mark = 0, opts = { 'no-data': False }):

    if(isinstance(export_mark, dict)):
        opts = export_mark
        export_mark = 0

    init_mark = get_initial_changeset()
    init_branch = False

    # We don't actually want to export the initial changeset, as it is administrative
    if(export_mark <= init_mark):
        init_branch = True
        export_mark = init_mark + 1

    last_mark = export_mark

    # First build GUID list of assets up until the specified changeset
    cur.execute(query_assetversions % export_mark)
    versions = cur.fetchall()
    for version in versions:
        guid_path(version['guid'], version['parent'], version['name'])

    # Create a commit for each changeset
    cur.execute(query_changesets % export_mark)
    changesets = cur.fetchall()
    for changeset in changesets:
        mark = changeset['id']
        date = changeset['date'].strftime('%s')

        author = changeset['author']
        comment = changeset['description']

        out.write("commit refs/heads/master\n")
        out.write("mark :%d\n" % mark)
        out.write("author %s %s -0700\n" % (author, date))
        out.write("committer %s %s -0700\n" % (author, date))
        export_data(out, comment)

        # Emit a deletall to reset the branch if we're starting from the beginnning of changeset history
        if(init_branch):
            init_branch = False
            out.write("deleteall\n")
        else:
            out.write("from :%d\n" % last_mark)

        # emmit file operations and version data for the current changeset
        cur.execute(query_assetversiondetails % mark)
        versions = cur.fetchall()

        for version in versions:
            ops = get_ops(version['name'], version['version'], version['guid'], version['parent'])
            for op in ops:
                op_name=op[0]
                path=op[1] 
                stream=op[2]

                def M():
                    inline_data(out, stream, path, nodata=opts['no-data'])

                def D():
                    out.write("D %s\n" % path)

                options = { 'M': M, 'D': D }
                options[op_name]()

        last_mark=mark


def db_connect(dbname, user, password, host = 'localhost', port = 10733):
    conn_str = "dbname='%s' user='%s' host='%s' password='%s' port='%d'" % (dbname, user, host, password, port)
    try:
        global conn, cur
        conn = psycopg2.connect(conn_str)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    except:
        print "Unable to connect to DB"
        sys.exit()

def help(code):
    print 'git-unity-as.py [changelist] --db <name> [--no-data] [--stdout] [--username=<name>]\n'
    print '                [--password=<password>] [--host=<host>] [--port=<num>]'
    sys.exit(code)

process = None
def get_export_pipe(type = 'git'):
    def git():
        process = subprocess.Popen('git fast-import', stdin=subprocess.PIPE, shell=True)
        return process.stdin

    def stdout():
        return sys.stdout

    options = { 'git': git, 'stdout': stdout }
    return options[type]()

#### MAIN
def main(argv):

    export_opts = { 'no-data': False }
    export_type = 'git'
    dbname = None
    user = None
    password = None
    host = 'localhost'
    port = 10733
    
    try:
        opts, args = getopt.getopt(argv,'h',["no-data","stdout","db=","username=","password=","host=","port="])
    except getopt.GetoptError:
       help(2)
    for opt, arg in opts:
        if opt == '-h':
            help(0)
        elif opt in ("--no-data"):
            export_opts['no-data'] = True
        elif opt in ("--stdout"):
            export_type = 'stdout'
        elif opt in ("--db"):
            dbname = arg
        elif opt in ("--username"):
            user = arg
        elif opt in ("--password"):
            password = arg
        elif opt in ("--host"):
            host = arg
        elif opt in ("--port"):
            port = arg

    if(dbname == None):
        help(2)

    db_connect(dbname, user, password, host, port)

    mark = 0
    if(len(args) > 0):
        mark = int(args[0])

    out = get_export_pipe(export_type)
    git_export(out, mark, export_opts)

    # Allow the git sub process to clean up and exit
    if(process is not None):
        process.wait()

if __name__ == "__main__":
    main(sys.argv[1:])

