# opentraveller-carto

#######
#######
#######
CONTEXT
#######

opentraveller-carto is a global map style dedicated to outdoor activities based on osm data.

opentraveller-carto is directly derived from openstreetmap-carto project and the same license applies (see LICENSE).

This map style has been created by Julien Sentier - julien@opentraveller.net

Opentraveller map style is visible on https://opentraveller.net/v3/?baselayers=opentraveller


#######
#######
#######
LICENCE
#######

opentraveller-carto is directly derived from openstreetmap-carto project and the same license applies:

https://github.com/gravitystorm/openstreetmap-carto/blob/master/LICENSE.txt
"
This software and associated documentation files (the "Software") is
released under the CC0 Public Domain Dedication, version 1.0, as
published by Creative Commons. To the extent possible under law, the
author(s) have dedicated all copyright and related and neighboring
rights to the Software to the public domain worldwide. For avoidance
of doubt, this includes the cartographic design. The Software is
distributed WITHOUT ANY WARRANTY.

If you did not receive a copy of the CC0 Public Domain Dedication
along with the Software, see
<http://creativecommons.org/publicdomain/zero/1.0/>
"

#########################
#########################
#########################
INSTALL MAPNIK TILESERVER
#########################

To install a tileserver based on mapnik rendering refer to:
    o LinuxBabe: How to Set Up OpenStreetMap Tile Server on Ubuntu 20.04
      https://www.linuxbabe.com/ubuntu/openstreetmap-tile-server-ubuntu-20-04-osm/amp
    o switch2osm
      https://switch2osm.org/serving-tiles/manually-building-a-tile-server-20-04-lts/


###############
###############
###############
IMPORT OSM DATA
###############

Create osm user and gis2 database
> sudo -u postgres -i
    create otadmin superuser
    postgres> createuser -s -P otadmin
    enter password ‘xxxxxxxx’
    confirm password ‘xxxxxxxx’
    create osm user
    postgres> createuser osm
    create gis2 database
    postgres> createdb -E UTF8 -O osm gis2
    postgres> psql -c "CREATE EXTENSION postgis;" -d gis2
    postgres> psql -c "CREATE EXTENSION hstore;" -d gis2
    postgres> psql -c "ALTER TABLE spatial_ref_sys OWNER TO osm;" -d gis2
    exit
    postgres> exit

Install pip for python and python packages
> cd /home/osm/src
> wget https://bootstrap.pypa.io/pip/2.7/get-pip.py
> python2 get-pip.py
> sudo pip install psycopg2==2.7.5 --ignore-installed

install osm2pgsql
> sudo apt install osm2pgsql

create pgpass to automatically log the postgresql db when using osm2pgsql function
> cd ~
> touch .pgpass
> nano .pgpass
edit .pgpass and add the below line (replace xxxxx by your password)	 
‘
localhost:5432:*:otadmin:xxxxxxxx
‘
save and quit
> chmod 0600 .pgpass

> cd importosmdata

update file ‘importosmdata_geofabriklist.txt’ with the list of the osmdata pbf file you want to import
> nano importosmdata_geofabriklist.txt
‘
#################################################
# EUROPE
# http://download.geofabrik.de/europe.html
#
# MONACO - ANDORRA ################################################
http://download.geofabrik.de/europe/andorra-latest.osm.pbf
http://download.geofabrik.de/europe/monaco-latest.osm.pbf
#
# FRANCE ##########################################################
#https://download.geofabrik.de/europe/france/midi-pyrenees-latest.osm.pbf
‘

update file importosmdata_param.txt, in particular update OSM2PGSQL_number-processes parameters
> nano importosmdata_param.txt
‘
# param file to be used as argument for createandimportcontours_v1.py
# script to be run with 
#    ./importosmdata_v1.py importosmdata_param.txt

# hgt files
GEOFABRIK_LIST = ./importosmdata_geofabriklist.txt

# pgsql identification
PGSQL_HOST = localhost
PGSQL_PORT = 5432
PGSQL_USER = otadmin
PGSQL_PASSWORD = pwd98
PGSQL_DB = gis2

# osm2pgsql --number-processes  --> number of CPU cores on your server
OSM2PGSQL_number-processes = 4
‘

execute script to import data
> ./importosmdata_v1.py importosmdata_param.txt

Copy mapnik xml style file in openstreetmap-carto directory (see INSTALL MAPNIK TILESERVER)

Configure Renderd with the link to opentraveller xml stylesheets copied in openstreetmap-carto directory (see INSTALL MAPNIK TILESERVER)

Rerun tileserver (see INSTALL MAPNIK TILESERVER)


############################
############################
############################
ADD CONTOURS AND HILLSHADING
############################

to add relief contour lines and hillshading refer to opentopomap project https://github.com/der-stefan/OpenTopoMap/tree/master/mapnik (see mapnik/




