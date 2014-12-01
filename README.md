git-unity-as
============
git fast-import converter for Unity Asset Server databases. Tracks changelists for incremental exports.

Requirements
============
Developed with Python 2.7; relies on the psycopg2 Python module.

MacOS
-----
1. Install [postgresql](http://www.postgresql.org/download/macosx/). I recommend the hombrew approach ("brew install postgresql")
2. Install psycopg2 python module. "sudo pip install psycopg2" (if you don't have pip, "sudo easy_install pip" first)

Windows
-------
* Try [this](http://www.stickpeople.com/projects/python/win-psycopg/)

Usage
=====
### Basic example
```
mkdir myassetserverproject-git
cd myassetserverproject-git
git init
git-unity-as.py --username USERNAME DBNAME | git fast-import
git checkout master
```

### git-unity-as.py options
|Option                     |Description 
|---------------------------|----
| DBNAME                    |Name of the AssetServer database to export
|-h, --help                 |show extended help message and exit
|--username USERNAME        |(Required) Database user with read access to specified database.
|--password PASSWORD        |Password for specified database user.
|--host HOST                |AssetServer database host (default: localhost)
|--port PORT                |AssetServer database port (default: 10733)
|--init                     |Resets and exports from the initial changeset.
|--no-data                  |Do not output asset version data (for debugging).
|--branch BRANCH, -b BRANCH |Target export to specified branch. (default: master)

## Incremental updates
By default, git-unity-as.py will save the last changelist exported from the given database, 
so back-to-back imports will be incremental by default. Use --init to re-export the entire database.

Contributors
============
Email stephenp@unity3d.com with feedback/suggestions.




