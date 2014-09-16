#!/usr/bin/python

import psycopg2
import psycopg2.extras
import psycopg2.extensions
import sys
import os
import argparse
import traceback
import ConfigParser

###### SQL queries
# query a simple list of asset versions used to build the GUID_MAP
QUERY_ASSETVERSIONS = """SELECT av.created_in AS changeset, guid2hex(a.guid) AS guid, guid2hex(get_asset_guid_safe(av.parent)) AS parent, av.name, av.assettype FROM assetversion av, asset a WHERE av.asset = a.serial AND av.created_in <= %d ORDER BY av.serial"""

# query for full asset version details of a given changeset to translate into git commits
QUERY_ASSETVERSIONDETAILS = """SELECT vc.changeset, cs.description AS log, extract(epoch FROM commit_time)::int AS date, a.serial, guid2hex(a.guid) AS guid, av.name, guid2hex(get_asset_guid_safe(av.parent)) AS parent, at.description as assettype, av.serial AS version FROM variant v, variantinheritance vi, variantcontents vc, changeset cs, changesetcontents cc, assetversion av, asset a, assettype at WHERE v.name = 'work' AND vi.child = v.serial AND vc.variant = vi.parent AND cs.serial=vc.changeset AND cs.serial=cc.changeset AND cc.assetversion=av.serial AND av.asset=a.serial AND av.assettype=at.serial AND vc.changeset = %d ORDER BY av.serial"""

# gets a user list with associated email addresses, if any
QUERY_USERS = """select person.serial, person.username, split_part(pg_shdescription.description, ':'::text, 1) AS realname, split_part(pg_shdescription.description, ':'::text, 2) AS email FROM person LEFT JOIN pg_user ON person.username = pg_user.usename LEFT JOIN pg_shdescription ON pg_user.usesysid = pg_shdescription.objoid"""

# list of changesets, greater than the specified commit
QUERY_CHANGESETS = """SELECT cs.serial as id, cs.description, cs.commit_time as date, CASE WHEN p.email = 'none' OR p.email IS NULL THEN ' <' || p.username || '@' || p.username || '>' ELSE COALESCE(p.realname, p.username) || ' <' || p.email || '>' END AS author FROM (""" + QUERY_USERS + """) AS p, changeset cs WHERE p.serial = cs.creator AND cs.serial > %d"""

# list of all large object streams associated with a given asset version
QUERY_STREAMS = """SELECT assetversion,tag,lobj FROM stream, assetcontents WHERE stream = lobj AND tag = ANY(ARRAY['asset'::name, 'asset.meta'::name]) AND assetversion = %d""" 

###### END SQL queries

###### Globals
# Special guids
SETTINGS_GUID = "00000000000000000000000000000000"
TRASH_GUID = "ffffffffffffffffffffffffffffffff"

# Mappings for asset paths
GUID_MAP = {}
# We have to manually initialize the "ProjectSettings" path as it isn't acutally recorded in asset version history
GUID_MAP[SETTINGS_GUID] = { 'name': "ProjectSettings", 'parent': None }

# Connection and cursor, to be assigned
CONN = None
CUR = None
###### End Globals

# Helper function to write the data header + given data to stdout
def export_data(out, data):
    out.write("data %d\n" % len(data))
    out.write(data)

# Helper function to write the data header + buffer binary data for a given stream to stdout
def inline_data(out, stream, path, code='M', mode='644', nodata=False, data=None):
    out.write("%s %s inline \"%s\"\n" % (code, mode, path))

    # data provided, do not lookup stream
    if data != None:
        export_data(out, data)
        return

    obj = psycopg2.extensions.lobject(CONN, stream, 'b')
    size = obj.seek(0,2)
    obj.seek(0,0)
    out.write("data %d\n" % size)
    bytes_read = 0

    if nodata == True:
        out.write("stream %d\n" % stream)
        return

    while bytes_read < size:
        buff_size = min(size - bytes_read, 2048)
        out.write(obj.read(buff_size))
        bytes_read += buff_size


# Create and return a object to be stored in the GUID_MAP hash
def new_guid_item(name, parent):
    return { 'name': name, 'parent': parent }
        
# Get the full path for a given guid object, or move/rename an existing object
def guid_path(guid, new_parent=None, new_name=None):
    if GUID_MAP.has_key(guid):
        if new_parent != None:
            GUID_MAP[guid]['parent'] = new_parent
        if new_name != None:
            GUID_MAP[guid]['name'] = new_name
    else:
        # Special case for ProjectSettings/*.asset
        if new_name != None and new_name.endswith(".asset") and new_parent is None:
            new_parent = SETTINGS_GUID
        GUID_MAP[guid] = new_guid_item(new_name, new_parent)

    # recursive function to build a qualified path for a given guid
    def build_path(parent_guid, path=""):
        node = GUID_MAP[parent_guid]

        if len(path) == 0:
            path = node['name']
        else:
            path = node['name'] + "/" + path

        if node['parent'] is not None:
            path = build_path(node['parent'], path)
    
        return path

    return build_path(guid)

# Get a list of large object id's and associated tags for a given asset version
def get_streams(asset_type, asset_guid, asset_version):
    stream_ar = []
    if asset_type == 'dir':
        meta = """fileFormatVersion: 2\nguid: %s\nfolderAsset: yes\nDefaultImporter:\n  userData: \n""" % asset_guid
        stream_ar.append({ 'type': asset_type, 'tag': 'asset.meta', 'data': meta })
    else:
        CUR.execute(QUERY_STREAMS % asset_version)
        streams = CUR.fetchall()
        for stream in streams:
            stream_ar.append({ 'type': asset_type, 'tag': stream['tag'], 'lobj': stream['lobj'] })

    return stream_ar

# Get a list of commands to be sent to git fast-import
def get_ops(asset_type, asset_name, asset_version, asset_guid, parent_guid):
    ops = []
    new_path = ''
    rename = False
    streams = get_streams(asset_type, asset_guid, asset_version)

    def create_op(op_name, op_path, stream_tag, stream_id):
        if stream_tag == "asset.meta":
            op_path += ".meta"
            if op_name == 'R':
                stream_id += ".meta"
        return [op_name, op_path, stream_id]
    
    if GUID_MAP.has_key(asset_guid):
        guid_item = GUID_MAP[asset_guid]
        old_path = guid_path(asset_guid)
        old_parent_guid = guid_item['parent']
        old_name = guid_item['name']
        if old_parent_guid != SETTINGS_GUID and (old_parent_guid != parent_guid or old_name != asset_name):
            new_path = guid_path(asset_guid, parent_guid, asset_name) 
            if old_parent_guid != TRASH_GUID:

                if '(DEL_' in old_path:
                    raise StandardError("Tried to rename or delete a file in Trash: parent_guid:%s asset_guid:%s path:%s" % (guid_item['parent'], asset_guid, old_path))

                for stream in streams:
                    if parent_guid == TRASH_GUID:
                        ops.append(create_op('D', old_path, stream['tag'], ''))
                    else:
                        rename = True

                        # When renaming a directory, include a rename command for the base directory as well as the meta file
                        if asset_type == 'dir':
                            ops.append(create_op('R', old_path, 'asset', new_path))
                        ops.append(create_op('R', old_path, stream['tag'], new_path))

        else:
            new_path = old_path
    else:
        new_path = guid_path(asset_guid, parent_guid, asset_name) 

    if parent_guid != TRASH_GUID:
        for stream in streams:
            if stream['type'] == 'dir':
                if rename == False:
                    ops.append(create_op('dir', new_path, stream['tag'], stream['data']))
            else:
                ops.append(create_op('M', new_path, stream['tag'], stream['lobj']))

    return ops 

# Get a reference to the initial changeset id
def get_initial_changeset():
    query = """select serial from changeset order by serial limit 1"""
    CUR.execute(query)
    return int(CUR.fetchone()['serial'])


# Moves folders to delete below any of their children
def sort_versions(versions):
    cnt = len(versions)
    x = 0 
    while x < cnt:
        parent = versions[x]
        if parent['parent'] == TRASH_GUID:
            for y in range(cnt - 1, x, -1):
                child_guid = versions[y]['guid']
                if GUID_MAP.has_key(child_guid) and GUID_MAP[child_guid]['parent'] == parent['guid']:
                    del versions[x]
                    versions.insert(y, parent)
                    x = x - 1
                    break

        x = x + 1
    
    return versions

def git_export(out, args):
    last_mark = 0
    if not args.init:
        conf = get_config()
        if conf.has_option(args.db, 'last_mark'):
            last_mark = conf.getint(args.db, 'last_mark')

    init_mark = get_initial_changeset()
    init_branch = False

    if last_mark <= init_mark:
        init_branch = True
        last_mark = init_mark
    
    # First build GUID list of assets up until the specified changeset
    CUR.execute(QUERY_ASSETVERSIONS % last_mark)
    versions = CUR.fetchall()
    for version in versions:
        guid_path(version['guid'], version['parent'], version['name'])

    # Create a commit for each changeset
    CUR.execute(QUERY_CHANGESETS % last_mark)
    changesets = CUR.fetchall()
    sys.stderr.write("Last exported changeset: %d\n" % last_mark)
    sys.stderr.write("New changesets to export: %d\n" % len(changesets))
    i = 0
    for changeset in changesets:
        i = i + 1
        mark = changeset['id']
        date = changeset['date'].strftime('%s')

        author = changeset['author']
        comment = changeset['description']

        out.write("commit refs/heads/%s\n" % args.branch)
        out.write("mark :%d\n" % mark)
        out.write("author %s %s -0700\n" % (author, date))
        out.write("committer %s %s -0700\n" % (author, date))
        export_data(out, comment)

        # Emit a deletall to reset the branch if we're starting from the beginnning of changeset history
        if init_branch:
            init_branch = False
            out.write("deleteall\n")
        elif i == 1:
            out.write("from refs/heads/master^0\n")
        else:
            out.write("from :%d\n" % last_mark)

        # emit file operations and version data for the current changeset
        CUR.execute(QUERY_ASSETVERSIONDETAILS % mark)
        versions = sort_versions(CUR.fetchall())
        ops = []

        for version in versions:
            ops = get_ops(version['assettype'], version['name'], version['version'], version['guid'], version['parent'])

            for op in ops:
                op_name = op[0]
                path = op[1] 
                stream = op[2]

                def Dir():
                    inline_data(out, -1, path, data=stream)

                def M():
                    inline_data(out, stream, path, nodata=args.nodata)

                def D():
                    out.write("D \"%s\"\n" % path)

                def R():
                    out.write("R \"%s\" \"%s\"\n" % (op[1], op[2]))

                options = { 'M': M, 'D': D, 'R': R, 'dir': Dir }
                options[op_name]()

        edited_comment = comment.replace('\n', ' ')
        if len(edited_comment) > 100:
            edited_comment = edited_comment[:100] + "..."

        out.write("progress Processed changeset %d, %d file(s): %s\n" % (mark, len(versions), edited_comment))
        last_mark = mark

        # Track last successful changeset import
        get_config().set(args.db, 'last_mark', str(last_mark))
        
    out.write("progress Done.\n")
    return last_mark

def db_connect(dbname, user, password, host='localhost', port=10733):
    conn_str = "dbname='%s' user='%s' host='%s' password='%s' port='%d'" % (dbname, user, host, password, port)
    try:
        global CONN, CUR
        CONN = psycopg2.connect(conn_str)
        CUR = CONN.cursor(cursor_factory=psycopg2.extras.DictCursor)
    except:
        print "Unable to connect to DB"
        sys.exit()

CONFIG = None
CONFIG_FILE = os.path.join(os.path.expanduser("~"), ".git-unity-as")
def config_init(db_name=None):
    global CONFIG
    if db_name != None:
        CONFIG = ConfigParser.SafeConfigParser() 
        CONFIG.read(CONFIG_FILE)
        if not CONFIG.has_section(db_name):
            CONFIG.add_section(db_name)
    return CONFIG

def get_config():
    return CONFIG

def save_config():
    if CONFIG != None:
        with open(CONFIG_FILE, 'wb') as configfile:
            CONFIG.write(configfile)

#### MAIN
def main():
    desc = """Exports a Unity Asset Server database to a git fast-import stream. Typically you would pipe the output to
    git from a valid git repository, like so: %s | git fast-import""" % os.path.basename(__file__)

    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('db', help='Asset Server database name.')
    parser.add_argument('--username', required=True, help='Database user with read access to specified database.')
    parser.add_argument('--password', help='Password for specified database user.')
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', type=int, default=10733)
    parser.add_argument('--init', action='store_true', help='Resets and exports from the initial changeset.')
    parser.add_argument('--no-data', dest='nodata', action='store_true', help='Do not output asset version data (for debugging).')
    parser.add_argument('--branch', '-b', default='master', help='Target export to specified branch. Default is \'master\'')
    args = parser.parse_args()

    # Establish database connection
    db_connect(args.db, args.username, args.password, args.host, args.port)
    config_init(args.db)

    try:
        git_export(sys.stdout, args)
        save_config()
    except StandardError as e:
        print "ERROR: %s" % e
        print traceback.format_exc()
        sys.exit(2)

if __name__ == "__main__":
    main()

