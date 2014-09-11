#!/usr/bin/python

import psycopg2
import psycopg2.extras
import psycopg2.extensions
import time
import sys
import os
import getopt
from os import path
import subprocess
import sys
import json

class cd:
    """Context manager for changing the current working directory"""
    def __init__(self, newPath):
        self.newPath = newPath

    def __enter__(self):
        self.savedPath = os.getcwd()
        os.chdir(self.newPath)

    def __exit__(self, etype, value, traceback):
        os.chdir(self.savedPath)

if sys.platform == "win32":
    import os, msvcrt
    msvcrt.setmode(sys.stdout.fileno(), os.O_BINARY)

###### SQL queries
# query a simple list of asset versions used to build the guid_map
query_assetversions="""SELECT av.created_in AS changeset, guid2hex(a.guid) AS guid, guid2hex(get_asset_guid_safe(av.parent)) AS parent, av.name, av.assettype FROM assetversion av, asset a WHERE av.asset=a.serial AND av.created_in <= %d ORDER BY av.serial"""

# query for full asset version details of a given changeset to translate into git commits
query_assetversiondetails="""SELECT vc.changeset, cs.description AS log, extract(epoch FROM commit_time)::int AS date, a.serial, guid2hex(a.guid) AS guid, av.name, guid2hex(get_asset_guid_safe(av.parent)) AS parent, at.description as assettype, av.serial AS version FROM variant v, variantinheritance vi, variantcontents vc, changeset cs, changesetcontents cc, assetversion av, asset a, assettype at WHERE v.name = 'work' AND vi.child = v.serial AND vc.variant = vi.parent AND cs.serial=vc.changeset AND cs.serial=cc.changeset AND cc.assetversion=av.serial AND av.asset=a.serial AND av.assettype=at.serial AND vc.changeset = %d ORDER BY av.serial"""

# gets a user list with associated email addresses, if any
query_users="""select person.serial, person.username, split_part(pg_shdescription.description, ':'::text, 1) AS realname, split_part(pg_shdescription.description, ':'::text, 2) AS email FROM person LEFT JOIN pg_user ON person.username = pg_user.usename LEFT JOIN pg_shdescription ON pg_user.usesysid = pg_shdescription.objoid"""

# list of changesets, greater than the specified commit
query_changesets="""SELECT cs.serial as id, cs.description, cs.commit_time as date, CASE WHEN p.email = 'none' OR p.email IS NULL THEN ' <' || p.username || '@' || p.username || '>' ELSE COALESCE(p.realname, p.username) || ' <' || p.email || '>' END AS author FROM (""" + query_users + """) AS p, changeset cs WHERE p.serial = cs.creator AND cs.serial > %d"""

# list of all large object streams associated with a given asset version
query_streams="""SELECT assetversion,tag,lobj FROM stream, assetcontents WHERE stream = lobj AND tag = ANY(ARRAY['asset'::name, 'asset.meta'::name]) AND assetversion = %d""" 

# function called by trigger for auto update
query_func_auto_update="""CREATE OR REPLACE FUNCTION git_unity_as ()
  RETURNS integer AS $$
import subprocess
return subprocess.call(['/opt/unity_asset_server/bin/git-unity-as.py','--db=assetservertest','--username=admin','--password=unity'])
return 0
$$ LANGUAGE plpythonu;"""

###### END SQL queries

###### Globals
# Special guids
settings_guid="00000000000000000000000000000000"
trash_guid="ffffffffffffffffffffffffffffffff"

# Mappings for asset paths
guid_map = {}
# We have to manually initialize the "ProjectSettings" path as it isn't acutally recorded in asset version history
guid_map[settings_guid] = { 'name': "ProjectSettings", 'parent': None }

###### End Globals

# Helper function to write the data header + given data to stdout
def export_data(out, data):
    out.write("data %d\n" % len(data))
    out.write(data)

# Helper function to write the data header + buffer binary data for a given stream to stdout
def inline_data(out, stream, path, code = 'M', mode = '644', nodata = False, data = None):
    out.write("%s %s inline \"%s\"\n" % (code, mode, path))

    # data provided, do not lookup stream
    if data != None:
        export_data(out, data)
        return

    obj=psycopg2.extensions.lobject(conn, stream, 'b')
    size=obj.seek(0,2)
    obj.seek(0,0)
    out.write("data %d\n" % size)
    bytes_read=0

    if(nodata == True):
        out.write("stream %d\n" % stream)
        return

    while bytes_read < size:
        buff_size = min(size - bytes_read, 2048)
        out.write(obj.read(buff_size))
        bytes_read += buff_size


# Create and return a object to be stored in the guid_map hash
def new_guid_item(name, parent):
    return { 'name': name, 'parent': parent }
        
# Get the full path for a given guid object, or move/rename an existing object
def guid_path(guid, new_parent = None, new_name = None):
    if guid_map.has_key(guid):
        if new_parent != None:
            guid_map[guid]['parent'] = new_parent
        if new_name != None:
            guid_map[guid]['name'] = new_name
    else:
        # Special case for ProjectSettings/*.asset
        if(new_name.endswith(".asset") and new_parent is None):
            new_parent = settings_guid
        guid_map[guid] = new_guid_item(new_name, new_parent)

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
def get_streams(asset_type, asset_guid, asset_version):
    stream_ar = []
    if asset_type == 'dir':
        meta="""fileFormatVersion: 2\nguid: %s\nfolderAsset: yes\nDefaultImporter:\n  userData: \n""" % asset_guid
        stream_ar.append({ 'type': asset_type, 'tag': 'asset.meta', 'data': meta })
    else:
        cur.execute(query_streams % asset_version);
        streams = cur.fetchall()
        for stream in streams:
            stream_ar.append({ 'type': asset_type, 'tag': stream['tag'], 'lobj': stream['lobj'] })

    return stream_ar

# Get a list of commands to be sent to git fast-import
def get_ops(asset_type, asset_name, asset_version, asset_guid, parent_guid):
    ops=[]
    path=''
    streams = get_streams(asset_type, asset_guid, asset_version)

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
                    ops.append(create_op('D', old_path, stream['tag'], ''))

            path=guid_path(asset_guid, parent_guid, asset_name) 
        else:
            path=old_path
    else:
        path=guid_path(asset_guid, parent_guid, asset_name) 

    if(parent_guid != trash_guid):
        for stream in streams:
            if stream['type'] == 'dir':
                ops.append(create_op('dir', path, stream['tag'], stream['data']))
            else:
                ops.append(create_op('M', path, stream['tag'], stream['lobj']))

    return ops 

# Get a reference to the initial changeset id
def get_initial_changeset():
    query="""select serial from changeset order by serial limit 1"""
    cur.execute(query)
    return int(cur.fetchone()['serial'])

def sort_versions(versions):
    lastidx = len(versions) - 1;

    # Move folders to delete below any of their children
    for x in range(0, lastidx):
        parent = versions[x]
        if parent['parent'] != trash_guid:
            continue
        
        for y in range(lastidx, x, -1):
            child_guid = versions[y]['guid']
            if guid_map.has_key(child_guid) and guid_map[child_guid]['parent'] == parent['guid']:
                del versions[x]
                versions.insert(y, parent)
                break
    
    return versions

def git_export(out, last_mark = 0, opts = { 'no-data': False }):

    if(isinstance(last_mark, dict)):
        opts = last_mark
        last_mark = 0

    init_mark = get_initial_changeset()
    init_branch = False

    if(last_mark <= init_mark):
        init_branch = True
        last_mark = init_mark

    # First build GUID list of assets up until the specified changeset
    cur.execute(query_assetversions % last_mark)
    versions = cur.fetchall()
    for version in versions:
        guid_path(version['guid'], version['parent'], version['name'])

    # Create a commit for each changeset
    cur.execute(query_changesets % last_mark)
    changesets = cur.fetchall()
    sys.stderr.write("Last exported changeset: %d\n" % last_mark)
    sys.stderr.write("New changesets to export: %d\n" % len(changesets))
    i = 0
    for changeset in changesets:
        i = i + 1
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
        elif(i == 1):
            out.write("from refs/heads/master^0\n")
        else:
            out.write("from :%d\n" % last_mark)

        # emit file operations and version data for the current changeset
        cur.execute(query_assetversiondetails % mark)
        versions = sort_versions(cur.fetchall())
        ops = []

        for version in versions:
                ops = get_ops(version['assettype'], version['name'], version['version'], version['guid'], version['parent'])

                for op in ops:
                    op_name=op[0]
                    path=op[1] 
                    stream=op[2]

                    def Dir():
                        inline_data(out, -1, path, data=stream)

                    def M():
                        inline_data(out, stream, path, nodata=opts['no-data'])

                    def D():
                        out.write("D %s\n" % path)

                    options = { 'M': M, 'D': D, 'dir': Dir }
                    options[op_name]()

        edited_comment = comment.replace('\n', ' ')
        if(len(edited_comment) > 100):
            edited_comment = edited_comment[:100] + "..."

        out.write("progress Processed changeset %d, %d file(s): %s\n" % (mark, len(versions), edited_comment))
        last_mark=mark

        # Track last successful changeset import
        if(opts['export-type'] == 'git'):
            save_config({ 'last_mark': last_mark })
        
    out.write("progress Done.\n")
    return last_mark

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
    print 'git-unity-as.py --db <name> [--no-data] [--stdout] [--reimport] [--repopath=<path>]'
    print '                [--username=<name>] [--password=<password>] [--host=<host>] [--port=<num>]'
    sys.exit(code)

process = None
def get_export_pipe(type = 'git'):
    def git():
        sed = subprocess.Popen("sed 's/^progress //'", stdin=subprocess.PIPE, shell=True)
        process = subprocess.Popen('git fast-import --quiet', stdin=subprocess.PIPE, stdout=sed.stdin, shell=True)
        return process.stdin

    def stdout():
        return sys.stdout

    options = { 'git': git, 'stdout': stdout }
    return options[type]()

def init_repo_root():
    if(path.exists('.git') == False):
        subprocess.call(['git','init'])

    conf = load_config()
    return conf.get('last_mark', -1)
        
def conf_path():
    return path.normpath(path.join(os.getcwd(), '.git/git-unity-as.conf'))

def load_config():
    conf = {}
    if(path.exists(conf_path())):
        with open(conf_path()) as f:
            conf = json.loads(f.read())

    return conf

def save_config(obj):
    conffile = conf_path()
    conf = load_config()
    conf = dict(conf.items() + obj.items())

    f = open(conffile, 'w')
    f.write(json.dumps(conf, sort_keys = True, indent = 2, ensure_ascii=True))
    f.close()

#### MAIN
def main(argv):

    export_opts = { 'no-data': False, 'export-type': 'git' }
    dbname = None
    user = None
    password = None
    host = 'localhost'
    port = 10733
    custom_path = False 
    reimport = False

    # Directory where repositories for asset server databases are created
    if(sys.platform == "win32"):
        # TODO: verify and test this
        repo_root=path.expandvars("%ProgramFiles%\Unity\AssetServer\Cloud\Chameleon")
    else:
        repo_root=path.expanduser("~/data/UnityCloud/Chameleon")

    try:
        opts, args = getopt.getopt(argv,'h',["no-data","stdout","reimport","db=","username=","password=","host=","port=","repopath="])
    except getopt.GetoptError:
       help(2)
    for opt, arg in opts:
        if opt == '-h':
            help(0)
        elif opt in ("--no-data"):
            export_opts['no-data'] = True
        elif opt in ("--stdout"):
            export_opts['export-type'] = 'stdout'
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
        elif opt in ("--reimport"):
            reimport = True
        elif opt in ("--repopath"):
            custom_path = True
            repo_root = path.normpath(arg)

    if(dbname == None):
        help(2)

    db_connect(dbname, user, password, host, port)

    if(custom_path == False):
        repo_root = path.join(repo_root, dbname)

    if(path.exists(repo_root) == False):
        print "Creating output dir '%s'" % repo_root
        try:
            os.makedirs(repo_root)
        except:
            print "Could not create output dir '%s'" % repo_root
            sys.exit(2)

    with cd(repo_root):
        last_mark = init_repo_root()
        if(reimport == True): last_mark = 0
        out = get_export_pipe(export_opts['export-type'])
        last_mark = git_export(out, last_mark, export_opts)

    # Allow the git sub process to clean up and exit
    if(process is not None):
        process.wait()

if __name__ == "__main__":
    main(sys.argv[1:])

