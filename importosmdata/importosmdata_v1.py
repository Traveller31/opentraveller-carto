#!/usr/bin/python2
#
# the purpose of this script is to import osm data into pgsql database
#
# to run script in the terminal
# > ./importosmdata_v1.py importosmdata_param.txt
# 
# to run script in background
# https://janakiev.com/til/python-background/
# > nohup ./backup_v1.py &
# > ps ax | grep backup_v1.py
# > pkill -f backup_v1.py
#
#
# PREREQUISITES
#
# use .pgpass file so as to avoid to use password in the osm2pgsql
#	> cd ~
#	> touch .pgpass
#	> nano .pgpass
#	> localhost:5432:gis2:otadmin:xxxxxx
#	> chmod 0600 .pgpass
#
# create osm user and gis2 database if needed
# > sudo -u postgres -i
#	create osm user
#	postgres> createuser osm
#	postgres> createdb -E UTF8 -O osm gis2
#	postgres> psql -c "CREATE EXTENSION postgis;" -d gis2
#	postgres> psql -c "CREATE EXTENSION hstore;" -d gis2
#	postgres> psql -c "CREATE EXTENSION pgrouting;" -d gis2
#	postgres> psql -c "ALTER TABLE spatial_ref_sys OWNER TO osm;" -d gis2
#	postgres> exit
#
#
# RECORD OF REVISION
#
# v1 13/04/2022 Julien SENTIER

import sys
import psycopg2
import time
# import math
# import os
import subprocess
# import shutil
import re

def log(file, txt, boolNewFile=False):
	if boolNewFile:
		f = open(file, "w")
		f.close()
	f = open(file, "a")
	f.write(txt+"\n")
	f.close()
	print txt

def readparam():
	if(len(sys.argv) < 2):
		log("log.txt",'    ERROR: no param argument')
		sys.exit()
	else:
		param = {}
		paramfile = sys.argv[1]
		log("log.txt",'    read param from '+paramfile)
		# Using readlines()
		file1 = open(paramfile, 'r')
		Lines = file1.readlines()
		for line in Lines:
			line = line.strip()
			if len(line) > 0:
				if line[0] != '#':
					tline = line.split('=')
					if len(tline) == 2:
						log("log.txt",'        \'' + tline[0].strip() + '\': \'' + tline[1].strip() + '\'')
						param[tline[0].strip()] = tline[1].strip()
		return param

def readgeofabriklist(param):
	#file = param['HGT_ROOT'] + '/' + param['HGT_LIST']
	log("log.txt",'    read geofabrik pbf file list from '+param['GEOFABRIK_LIST'])
	file1 = open(param['GEOFABRIK_LIST'], 'r')
	Lines = file1.readlines()
	geofabriklist = []
	for line in Lines:
		line = line.strip()
		if len(line) > 0:
			if line[0] != '#':
				geofabriklist.append(line)
	log("log.txt",'        ' + str(len(geofabriklist)) + ' geofabrik files listed')
	return geofabriklist

def shellexec(cmd, showtime, log_prefix):
	log("log.txt", log_prefix + '> ' + cmd)
	if showtime:
		log("log.txt", log_prefix + '    start: '+str(time.strftime("%Y-%m-%d %H:%M:%S")))
	o = subprocess.check_output([cmd],shell=True)
	if showtime:
		log("log.txt", log_prefix + '    end: '+str(time.strftime("%Y-%m-%d %H:%M:%S")))
	return o

def sqlexec(cur, sql, showtime, log_prefix):
	log("log.txt", log_prefix + 'sql> ' + sql)
	if showtime:
		log("log.txt", log_prefix + '    start: '+str(time.strftime("%Y-%m-%d %H:%M:%S")))
	cur.execute(sql)
	if showtime:
		log("log.txt", log_prefix + '    end: '+str(time.strftime("%Y-%m-%d %H:%M:%S")))
	return cur

def prepare_db(param):
	conn = psycopg2.connect("dbname="+param['PGSQL_DB']+" user="+param['PGSQL_USER']+" password="+param['PGSQL_PASSWORD']+" host="+param['PGSQL_HOST']+" port="+param['PGSQL_PORT'])
	cur = conn.cursor()

	# sql = "CREATE EXTENSION IF NOT EXISTS postgis"
	# sqlexec(cur, sql, False, '    ')
	# sql = "SELECT postgis_full_version()"
	# cur = sqlexec(cur, sql, False, '    ')
	# data = cur.fetchone()
	# while data is not None:
	#	log("log.txt", '        ' + str(data))
	#	data = cur.fetchone()
	# sql = "CREATE EXTENSION IF NOT EXISTS hstore"
	# sqlexec(cur, sql, False, '    ')

	if 'CREATENEWTABLES' in param.keys():
		if param['CREATENEWTABLES'].lower() == 'true':
			log("log.txt",'')
			sql = "DROP TABLE IF EXISTS gis_master;"
			sqlexec(cur, sql, False, '    ')
			sql = "DROP TABLE IF EXISTS public.planet_otm_line;"
			sqlexec(cur, sql, False, '    ')
			sql = "DROP TABLE IF EXISTS public.planet_otm_point;"
			sqlexec(cur, sql, False, '    ')
			sql = "DROP TABLE IF EXISTS public.planet_otm_polygon;"
			sqlexec(cur, sql, False, '    ')
			sql = "DROP TABLE IF EXISTS public.planet_otm_roads;"
			sqlexec(cur, sql, False, '    ')

	log("log.txt",'')
	sql = "CREATE TABLE IF NOT EXISTS gis_master(id  serial PRIMARY KEY, pbffilename text, status text, log text, timestamp int);"
	sqlexec(cur, sql, False, '    ')
	sql = "CREATE INDEX IF NOT EXISTS gis_master_pbffilename_idx ON gis_master (pbffilename);"
	sqlexec(cur, sql, False, '    ')
	sql = "CREATE INDEX IF NOT EXISTS gis_master_status_idx ON gis_master (status);"
	sqlexec(cur, sql, False, '    ')

	log("log.txt",'')
	sql = "		\
		CREATE TABLE IF NOT EXISTS public.planet_otm_line (		\
			otm_id_master int,		\
			otm_id  bigserial PRIMARY KEY,		\
		    osm_id bigint,		\
		    access text,		\
		    \"addr:housename\" text,		\
		    \"addr:housenumber\" text,		\
		    \"addr:interpolation\" text,		\
		    admin_level text,		\
		    aerialway text,		\
		    aeroway text,		\
		    amenity text,		\
		    barrier text,		\
		    bicycle text,		\
		    bridge text,		\
		    boundary text,		\
		    building text,		\
		    construction text,		\
		    covered text,		\
		    foot text,		\
		    highway text,		\
		    historic text,		\
		    horse text,		\
		    junction text,		\
		    landuse text,		\
		    layer integer,		\
		    leisure text,		\
		    lock text,		\
		    man_made text,		\
		    military text,		\
		    name text,		\
		    \"natural\" text,		\
		    oneway text,		\
		    place text,		\
		    power text,		\
		    railway text,		\
		    ref text,		\
		    religion text,		\
		    route text,		\
		    service text,		\
		    shop text,		\
		    surface text,		\
		    tourism text,		\
		    tracktype text,		\
		    tunnel text,		\
		    water text,		\
		    waterway text,		\
		    way_area real,		\
		    z_order integer,		\
		    tags public.hstore,		\
		    way public.geometry(LineString,3857),		\
		    otm_tags public.hstore,		\
		    otm_ref text,		\
		    otm_name text		\
		);		\
	"
	sqlexec(cur, sql, False, '    ')
	sql = "CREATE INDEX IF NOT EXISTS planet_otm_line_otm_id_master_idx ON public.planet_otm_line USING btree (otm_id_master);"
	sqlexec(cur, sql, False, '    ')
	sql = "CREATE INDEX IF NOT EXISTS planet_otm_line_osm_id_idx ON public.planet_otm_line USING btree (osm_id);"
	sqlexec(cur, sql, False, '    ')
	sql = "CREATE INDEX IF NOT EXISTS planet_otm_line_way_idx ON public.planet_otm_line USING gist (way);"
	sqlexec(cur, sql, False, '    ')
	
	sql = "CREATE INDEX IF NOT EXISTS planet_otm_line_ferry ON planet_otm_line USING GIST (way) WHERE route = 'ferry' AND osm_id > 0;"
	sqlexec(cur, sql, False, '    ')
	sql = "CREATE INDEX IF NOT EXISTS planet_otm_line_label ON planet_otm_line USING GIST (way) WHERE name IS NOT NULL OR ref IS NOT NULL;"
	sqlexec(cur, sql, False, '    ')
	sql = "CREATE INDEX IF NOT EXISTS planet_otm_line_river ON planet_otm_line USING GIST (way) WHERE waterway = 'river';"
	sqlexec(cur, sql, False, '    ')
	sql = "CREATE INDEX IF NOT EXISTS planet_otm_line_waterway ON planet_otm_line USING GIST (way) WHERE waterway IN ('river', 'canal', 'stream', 'drain', 'ditch');"
	sqlexec(cur, sql, False, '    ')

	log("log.txt",'')
	sql = "		\
		CREATE TABLE IF NOT EXISTS public.planet_otm_point (	\
			otm_id_master int,		\
		    osm_id bigint,	\
		    access text,	\
		    \"addr:housename\" text,	\
		    \"addr:housenumber\" text,	\
		    admin_level text,	\
		    aerialway text,	\
		    aeroway text,	\
		    amenity text,	\
		    barrier text,	\
		    boundary text,	\
		    building text,	\
		    highway text,	\
		    historic text,	\
		    junction text,	\
		    landuse text,	\
		    layer integer,	\
		    leisure text,	\
		    lock text,	\
		    man_made text,	\
		    military text,	\
		    name text,	\
		    \"natural\" text,	\
		    oneway text,	\
		    place text,	\
		    power text,	\
		    railway text,	\
		    ref text,	\
		    religion text,	\
		    shop text,	\
		    tourism text,	\
		    water text,	\
		    waterway text,	\
		    tags public.hstore,	\
		    way public.geometry(Point,3857)	\
		);		\
	"
	sqlexec(cur, sql, False, '    ')
	sql = "CREATE INDEX IF NOT EXISTS planet_otm_point_otm_id_master_idx ON public.planet_otm_point USING btree (otm_id_master);"
	sqlexec(cur, sql, False, '    ')
	sql = "CREATE INDEX IF NOT EXISTS planet_otm_point_osm_id_idx ON public.planet_otm_point USING btree (osm_id);"
	sqlexec(cur, sql, False, '    ')
	sql = "CREATE INDEX IF NOT EXISTS planet_otm_point_way_idx ON public.planet_otm_point USING gist (way);"
	sqlexec(cur, sql, False, '    ')

	sql = "CREATE INDEX IF NOT EXISTS planet_otm_point_place ON planet_otm_point USING GIST (way) WHERE place IS NOT NULL AND name IS NOT NULL;"
	sqlexec(cur, sql, False, '    ')	

	log("log.txt",'')
	sql = "		\
		CREATE TABLE IF NOT EXISTS public.planet_otm_polygon (	\
			otm_id_master int,		\
		    osm_id bigint,	\
		    access text,	\
		    \"addr:housename\" text,	\
		    \"addr:housenumber\" text,	\
		    \"addr:interpolation\" text,	\
		    admin_level text,	\
		    aerialway text,	\
		    aeroway text,	\
		    amenity text,	\
		    barrier text,	\
		    bicycle text,	\
		    bridge text,	\
		    boundary text,	\
		    building text,	\
		    construction text,	\
		    covered text,	\
		    foot text,	\
		    highway text,	\
		    historic text,	\
		    horse text,	\
		    junction text,	\
		    landuse text,	\
		    layer integer,	\
		    leisure text,	\
		    lock text,	\
		    man_made text,	\
		    military text,	\
		    name text,	\
		    \"natural\" text,	\
		    oneway text,	\
		    place text,	\
		    power text,	\
		    railway text,	\
		    ref text,	\
		    religion text,	\
		    route text,	\
		    service text,	\
		    shop text,	\
		    surface text,	\
		    tourism text,	\
		    tracktype text,	\
		    tunnel text,	\
		    water text,	\
		    waterway text,	\
		    way_area real,	\
		    z_order integer,	\
		    tags public.hstore,	\
		    way public.geometry(Geometry,3857)	\
		);		\
	"
	sqlexec(cur, sql, False, '    ')
	sql = "CREATE INDEX IF NOT EXISTS planet_otm_polygon_otm_id_master_idx ON public.planet_otm_polygon USING btree (otm_id_master);"
	sqlexec(cur, sql, False, '    ')
	sql = "CREATE INDEX IF NOT EXISTS planet_otm_polygon_osm_id_idx ON public.planet_otm_polygon USING btree (osm_id);"
	sqlexec(cur, sql, False, '    ')
	sql = "CREATE INDEX IF NOT EXISTS planet_otm_polygon_way_idx ON public.planet_otm_polygon USING gist (way);"
	sqlexec(cur, sql, False, '    ')

	sql = "CREATE INDEX IF NOT EXISTS planet_otm_polygon_admin ON planet_otm_polygon USING GIST (ST_PointOnSurface(way)) WHERE name IS NOT NULL AND boundary = 'administrative' AND admin_level IN ('0', '1', '2', '3', '4');"
	sqlexec(cur, sql, False, '    ')
	sql = "CREATE INDEX IF NOT EXISTS planet_otm_polygon_military ON planet_otm_polygon USING GIST (way) WHERE (landuse = 'military' OR military = 'danger_area') AND building IS NULL;"
	sqlexec(cur, sql, False, '    ')
	sql = "CREATE INDEX IF NOT EXISTS planet_otm_polygon_name ON planet_otm_polygon USING GIST (ST_PointOnSurface(way)) WHERE name IS NOT NULL;"
	sqlexec(cur, sql, False, '    ')
	sql = "CREATE INDEX IF NOT EXISTS planet_otm_polygon_nobuilding ON planet_otm_polygon USING GIST (way) WHERE building IS NULL;"
	sqlexec(cur, sql, False, '    ')
	sql = "CREATE INDEX IF NOT EXISTS planet_otm_polygon_water ON planet_otm_polygon USING GIST (way) WHERE waterway IN ('dock', 'riverbank', 'canal') OR landuse IN ('reservoir', 'basin') OR \"natural\" IN ('water', 'glacier');"
	sqlexec(cur, sql, False, '    ')
	sql = "CREATE INDEX IF NOT EXISTS planet_otm_polygon_way_area_z10 ON planet_otm_polygon USING GIST (way) WHERE way_area > 23300;"
	sqlexec(cur, sql, False, '    ')
	sql = "CREATE INDEX IF NOT EXISTS planet_otm_polygon_way_area_z6 ON planet_otm_polygon USING GIST (way) WHERE way_area > 5980000;"
	sqlexec(cur, sql, False, '    ')

	log("log.txt",'')
	sql = "		\
		CREATE TABLE IF NOT EXISTS public.planet_otm_roads (	\
			otm_id_master int,		\
		    osm_id bigint,	\
		    access text,	\
		    \"addr:housename\" text,	\
		    \"addr:housenumber\" text,	\
		    \"addr:interpolation\" text,	\
		    admin_level text,	\
		    aerialway text,	\
		    aeroway text,	\
		    amenity text,	\
		    barrier text,	\
		    bicycle text,	\
		    bridge text,	\
		    boundary text,	\
		    building text,	\
		    construction text,	\
		    covered text,	\
		    foot text,	\
		    highway text,	\
		    historic text,	\
		    horse text,	\
		    junction text,	\
		    landuse text,	\
		    layer integer,	\
		    leisure text,	\
		    lock text,	\
		    man_made text,	\
		    military text,	\
		    name text,	\
		    \"natural\" text,	\
		    oneway text,	\
		    place text,	\
		    power text,	\
		    railway text,	\
		    ref text,	\
		    religion text,	\
		    route text,	\
		    service text,	\
		    shop text,	\
		    surface text,	\
		    tourism text,	\
		    tracktype text,	\
		    tunnel text,	\
		    water text,	\
		    waterway text,	\
		    way_area real,	\
		    z_order integer,	\
		    tags public.hstore,	\
		    way public.geometry(LineString,3857)	\
		);		\
	"
	sqlexec(cur, sql, False, '    ')
	sql = "CREATE INDEX IF NOT EXISTS planet_otm_roads_otm_id_master_idx ON public.planet_otm_roads USING btree (otm_id_master);"
	sqlexec(cur, sql, False, '    ')
	sql = "CREATE INDEX IF NOT EXISTS planet_otm_roads_osm_id_idx ON public.planet_otm_roads USING btree (osm_id);"
	sqlexec(cur, sql, False, '    ')
	sql = "CREATE INDEX IF NOT EXISTS planet_otm_roads_way_idx ON public.planet_otm_roads USING gist (way);"
	sqlexec(cur, sql, False, '    ')

	sql = "CREATE INDEX IF NOT EXISTS planet_otm_roads_admin ON planet_otm_roads USING GIST (way) WHERE boundary = 'administrative';"
	sqlexec(cur, sql, False, '    ')
	sql = "CREATE INDEX IF NOT EXISTS planet_otm_roads_admin_low ON planet_otm_roads USING GIST (way) WHERE boundary = 'administrative' AND admin_level IN ('0', '1', '2', '3', '4');"
	sqlexec(cur, sql, False, '    ')
	sql = "CREATE INDEX IF NOT EXISTS planet_otm_roads_roads_ref ON planet_otm_roads USING GIST (way) WHERE highway IS NOT NULL AND ref IS NOT NULL;"
  	sqlexec(cur, sql, False, '    ')

  	sql = "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO osm;"
  	sqlexec(cur, sql, False, '    ')

	conn.commit()
	cur.close()
	conn.close()

def clean_db(param):
	conn = psycopg2.connect("dbname="+param['PGSQL_DB']+" user="+param['PGSQL_USER']+" password="+param['PGSQL_PASSWORD']+" host="+param['PGSQL_HOST']+" port="+param['PGSQL_PORT'])
	cur = conn.cursor()

	log("log.txt","    DROP TABLE IF EXISTS planet_osm_*")
	sqlexec(cur, "DROP TABLE IF EXISTS planet_osm_line", False, '    ')
	sqlexec(cur, "DROP TABLE IF EXISTS planet_osm_nodes", False, '    ')
	sqlexec(cur, "DROP TABLE IF EXISTS planet_osm_point", False, '    ')
	sqlexec(cur, "DROP TABLE IF EXISTS planet_osm_polygon", False, '    ')
	sqlexec(cur, "DROP TABLE IF EXISTS planet_osm_rels", False, '    ')
	sqlexec(cur, "DROP TABLE IF EXISTS planet_osm_roads", False, '    ')
	sqlexec(cur, "DROP TABLE IF EXISTS planet_osm_ways", False, '    ')

	sqlexec(cur, "END TRANSACTION;", False, '    ')
	sqlexec(cur, "VACUUM ANALYZE planet_otm_line;", True, '    ')
	sqlexec(cur, "VACUUM ANALYZE planet_otm_roads;", True, '    ')
	sqlexec(cur, "VACUUM ANALYZE planet_otm_point;", True, '    ')
	sqlexec(cur, "VACUUM ANALYZE planet_otm_polygon;", True, '    ')

	conn.commit()
	cur.close()
	conn.close()

def update_master_start(param, pbffilename):
	conn = psycopg2.connect("dbname="+param['PGSQL_DB']+" user="+param['PGSQL_USER']+" password="+param['PGSQL_PASSWORD']+" host="+param['PGSQL_HOST']+" port="+param['PGSQL_PORT'])
	cur = conn.cursor()
	cur2 = conn.cursor()

	#check if previous line and update status and clean otrouting_ways table
	sql = "SELECT id FROM gis_master WHERE pbffilename='"+pbffilename+"' and status != 'deleted' ORDER BY timestamp DESC"
	cur = sqlexec(cur, sql, False, '    ')
	data = cur.fetchone()
	while data is not None:
		sql = "UPDATE gis_master SET status='toBeDeleted' WHERE id="+str(data[0])
		sqlexec(cur2, sql, False, '    ')
		data = cur.fetchone()
	#clean otm table if otm_id_master IS NULL
	# set otm_id_master in planet_otm_line
	sql = 	"DELETE FROM planet_otm_line WHERE otm_id_master IS NULL"
	cur = sqlexec(cur, sql, False, '    ')
	sql = 	"DELETE FROM planet_otm_roads WHERE otm_id_master IS NULL"
	cur = sqlexec(cur, sql, False, '    ')
	sql = 	"DELETE FROM planet_otm_point WHERE otm_id_master IS NULL"
	cur = sqlexec(cur, sql, False, '    ')
	sql = 	"DELETE FROM planet_otm_polygon WHERE otm_id_master IS NULL"
	cur = sqlexec(cur, sql, False, '    ')

	#add new row
	sql = "INSERT INTO gis_master (pbffilename, status, timestamp) VALUES ('"+pbffilename+"','importStarted', "+str(time.time())+")"
	sqlexec(cur, sql, False, '    ')

	sql = "SELECT id FROM gis_master WHERE pbffilename='"+pbffilename+"' ORDER BY id DESC LIMIT 1"
	cur = sqlexec(cur, sql, False, '    ')
	data = cur.fetchone()
	while data is not None:
		otm_id_master = data[0]
		data = cur.fetchone()


	conn.commit()
	cur.close()
	cur2.close()
	conn.close()

	return otm_id_master

def update_master_end(param, otm_id_master):
	conn = psycopg2.connect("dbname="+param['PGSQL_DB']+" user="+param['PGSQL_USER']+" password="+param['PGSQL_PASSWORD']+" host="+param['PGSQL_HOST']+" port="+param['PGSQL_PORT'])
	cur = conn.cursor()
	cur2 = conn.cursor()

	#loop on contours_master to delete obsolete data
	sql = "SELECT id FROM gis_master WHERE status = 'toBeDeleted' ORDER BY timestamp DESC"
	cur = sqlexec(cur, sql, False, '    ')
	data = cur.fetchone()
	while data is not None:
		sql = "DELETE FROM planet_otm_line WHERE otm_id_master = "+str(data[0])
		sqlexec(cur2, sql, False, '    ')
		sql = "DELETE FROM planet_otm_roads WHERE otm_id_master = "+str(data[0])
		sqlexec(cur2, sql, False, '    ')
		sql = "DELETE FROM planet_otm_point WHERE otm_id_master = "+str(data[0])
		sqlexec(cur2, sql, False, '    ')
		sql = "DELETE FROM planet_otm_polygon WHERE otm_id_master = "+str(data[0])
		sqlexec(cur2, sql, False, '    ')
		sql = "UPDATE gis_master SET status='deleted' WHERE id="+str(data[0])
		sqlexec(cur2, sql, False, '    ')
		data = cur.fetchone()

	#set status completed in contours_master
	log_ = ''
	log_ = log_ + str(time.strftime("%Y-%m-%d %H:%M:%S")) + '\n'

	sql = "SELECT COUNT(*) FROM planet_otm_line WHERE otm_id_master="+str(otm_id_master)
	cur = sqlexec(cur, sql, False, '    ')
	data = cur.fetchone()
	n = data[0]
	log_ = log_ + str(n) + ' row imported into planet_otm_line' + '\n'

	sql = "SELECT COUNT(*) FROM planet_otm_roads WHERE otm_id_master="+str(otm_id_master)
	cur = sqlexec(cur, sql, False, '    ')
	data = cur.fetchone()
	n = data[0]
	log_ = log_ + str(n) + ' row imported into planet_otm_roads' + '\n'

	sql = "SELECT COUNT(*) FROM planet_otm_point WHERE otm_id_master="+str(otm_id_master)
	cur = sqlexec(cur, sql, False, '    ')
	data = cur.fetchone()
	n = data[0]
	log_ = log_ + str(n) + ' row imported into planet_otm_point' + '\n'

	sql = "SELECT COUNT(*) FROM planet_otm_polygon WHERE otm_id_master="+str(otm_id_master)
	cur = sqlexec(cur, sql, False, '    ')
	data = cur.fetchone()
	n = data[0]
	log_ = log_ + str(n) + ' row imported into planet_otm_polygon' + '\n'

	sql = "UPDATE gis_master SET status='importCompleted', log='"+log_+"' WHERE id="+str(otm_id_master)
	cur = sqlexec(cur, sql, False, '    ')
	log("log.txt",'    Log:')
	log("log.txt", log_)
	conn.commit()
	cur.close()
	cur2.close()
	conn.close()

def insert_into_planetotm_tables(param, otm_id_master):
	conn = psycopg2.connect("dbname="+param['PGSQL_DB']+" user="+param['PGSQL_USER']+" password="+param['PGSQL_PASSWORD']+" host="+param['PGSQL_HOST']+" port="+param['PGSQL_PORT'])
	cur = conn.cursor()
	cur2 = conn.cursor()

	# copy/paste from planet_osm_line to planet_otm_line
	sql = 	"INSERT INTO planet_otm_line (\
				osm_id,		\
			    access,		\
			    \"addr:housename\",		\
			    \"addr:housenumber\",		\
			    \"addr:interpolation\",		\
			    admin_level,		\
			    aerialway,		\
			    aeroway,		\
			    amenity,		\
			    barrier,		\
			    bicycle,		\
			    bridge,		\
			    boundary,		\
			    building,		\
			    construction,		\
			    covered,		\
			    foot,		\
			    highway,		\
			    historic,		\
			    horse,		\
			    junction,		\
			    landuse,		\
			    layer,		\
			    leisure,		\
			    lock,		\
			    man_made,		\
			    military,		\
			    name,		\
			    \"natural\",		\
			    oneway,		\
			    place,		\
			    power,		\
			    railway,		\
			    ref,		\
			    religion,		\
			    route,		\
			    service,		\
			    shop,		\
			    surface,		\
			    tourism,		\
			    tracktype,		\
			    tunnel,		\
			    water,		\
			    waterway,		\
			    way_area,		\
			    z_order,		\
			    tags,		\
			    way		\
			) \
			SELECT \
				osm_id,		\
			    access,		\
			    \"addr:housename\",		\
			    \"addr:housenumber\",		\
			    \"addr:interpolation\",		\
			    admin_level,		\
			    aerialway,		\
			    aeroway,		\
			    amenity,		\
			    barrier,		\
			    bicycle,		\
			    bridge,		\
			    boundary,		\
			    building,		\
			    construction,		\
			    covered,		\
			    foot,		\
			    highway,		\
			    historic,		\
			    horse,		\
			    junction,		\
			    landuse,		\
			    layer,		\
			    leisure,		\
			    lock,		\
			    man_made,		\
			    military,		\
			    name,		\
			    \"natural\",		\
			    oneway,		\
			    place,		\
			    power,		\
			    railway,		\
			    ref,		\
			    religion,		\
			    route,		\
			    service,		\
			    shop,		\
			    surface,		\
			    tourism,		\
			    tracktype,		\
			    tunnel,		\
			    water,		\
			    waterway,		\
			    way_area,		\
			    z_order,		\
			    tags,		\
			    way		\
			FROM planet_osm_line"
	cur = sqlexec(cur, sql, True, '    ')
	# set otm_id_master in planet_otm_line
	sql = 	"UPDATE planet_otm_line SET otm_id_master=" + str(otm_id_master) + " WHERE otm_id_master IS NULL"
	cur = sqlexec(cur, sql, False, '    ')

	# copy/paste from planet_osm_roads to planet_otm_roads
	sql = 	"INSERT INTO planet_otm_roads (\
			    osm_id,	\
			    access,	\
			    \"addr:housename\",	\
			    \"addr:housenumber\",	\
			    \"addr:interpolation\",	\
			    admin_level,	\
			    aerialway,	\
			    aeroway,	\
			    amenity,	\
			    barrier,	\
			    bicycle,	\
			    bridge,	\
			    boundary,	\
			    building,	\
			    construction,	\
			    covered,	\
			    foot,	\
			    highway,	\
			    historic,	\
			    horse,	\
			    junction,	\
			    landuse,	\
			    layer,	\
			    leisure,	\
			    lock,	\
			    man_made,	\
			    military,	\
			    name,	\
			    \"natural\",	\
			    oneway,	\
			    place,	\
			    power,	\
			    railway,	\
			    ref,	\
			    religion,	\
			    route,	\
			    service,	\
			    shop,	\
			    surface,	\
			    tourism,	\
			    tracktype,	\
			    tunnel,	\
			    water,	\
			    waterway,	\
			    way_area,	\
			    z_order,	\
			    tags,	\
			    way	\
			) \
			SELECT \
				osm_id,	\
			    access,	\
			    \"addr:housename\",	\
			    \"addr:housenumber\",	\
			    \"addr:interpolation\",	\
			    admin_level,	\
			    aerialway,	\
			    aeroway,	\
			    amenity,	\
			    barrier,	\
			    bicycle,	\
			    bridge,	\
			    boundary,	\
			    building,	\
			    construction,	\
			    covered,	\
			    foot,	\
			    highway,	\
			    historic,	\
			    horse,	\
			    junction,	\
			    landuse,	\
			    layer,	\
			    leisure,	\
			    lock,	\
			    man_made,	\
			    military,	\
			    name,	\
			    \"natural\",	\
			    oneway,	\
			    place,	\
			    power,	\
			    railway,	\
			    ref,	\
			    religion,	\
			    route,	\
			    service,	\
			    shop,	\
			    surface,	\
			    tourism,	\
			    tracktype,	\
			    tunnel,	\
			    water,	\
			    waterway,	\
			    way_area,	\
			    z_order,	\
			    tags,	\
			    way	\
			FROM planet_osm_roads"
	cur = sqlexec(cur, sql, True, '    ')
	# set otm_id_master in planet_otm_roads
	sql = 	"UPDATE planet_otm_roads SET otm_id_master=" + str(otm_id_master) + " WHERE otm_id_master IS NULL"
	cur = sqlexec(cur, sql, False, '    ')

	# copy/paste from planet_osm_point to planet_otm_point
	sql = 	"INSERT INTO planet_otm_point (\
			    osm_id,	\
			    access,	\
			    \"addr:housename\",	\
			    \"addr:housenumber\",	\
			    admin_level,	\
			    aerialway,	\
			    aeroway,	\
			    amenity,	\
			    barrier,	\
			    boundary,	\
			    building,	\
			    highway,	\
			    historic,	\
			    junction,	\
			    landuse,	\
			    layer,	\
			    leisure,	\
			    lock,	\
			    man_made,	\
			    military,	\
			    name,	\
			    \"natural\",	\
			    oneway,	\
			    place,	\
			    power,	\
			    railway,	\
			    ref,	\
			    religion,	\
			    shop,	\
			    tourism,	\
			    water,	\
			    waterway,	\
			    tags,	\
			    way	\
			) \
			SELECT \
				osm_id,	\
			    access,	\
			    \"addr:housename\",	\
			    \"addr:housenumber\",	\
			    admin_level,	\
			    aerialway,	\
			    aeroway,	\
			    amenity,	\
			    barrier,	\
			    boundary,	\
			    building,	\
			    highway,	\
			    historic,	\
			    junction,	\
			    landuse,	\
			    layer,	\
			    leisure,	\
			    lock,	\
			    man_made,	\
			    military,	\
			    name,	\
			    \"natural\",	\
			    oneway,	\
			    place,	\
			    power,	\
			    railway,	\
			    ref,	\
			    religion,	\
			    shop,	\
			    tourism,	\
			    water,	\
			    waterway,	\
			    tags,	\
			    way	\
			FROM planet_osm_point"
	cur = sqlexec(cur, sql, True, '    ')
	# set otm_id_master in planet_otm_point
	sql = 	"UPDATE planet_otm_point SET otm_id_master=" + str(otm_id_master) + " WHERE otm_id_master IS NULL"
	cur = sqlexec(cur, sql, False, '    ')

	# copy/paste from planet_osm_polygon to planet_otm_polygon
	sql = 	"INSERT INTO planet_otm_polygon (\
			    osm_id,	\
			    access,	\
			    \"addr:housename\",	\
			    \"addr:housenumber\",	\
			    \"addr:interpolation\",	\
			    admin_level,	\
			    aerialway,	\
			    aeroway,	\
			    amenity,	\
			    barrier,	\
			    bicycle,	\
			    bridge,	\
			    boundary,	\
			    building,	\
			    construction,	\
			    covered,	\
			    foot,	\
			    highway,	\
			    historic,	\
			    horse,	\
			    junction,	\
			    landuse,	\
			    layer,	\
			    leisure,	\
			    lock,	\
			    man_made,	\
			    military,	\
			    name,	\
			    \"natural\",	\
			    oneway,	\
			    place,	\
			    power,	\
			    railway,	\
			    ref,	\
			    religion,	\
			    route,	\
			    service,	\
			    shop,	\
			    surface,	\
			    tourism,	\
			    tracktype,	\
			    tunnel,	\
			    water,	\
			    waterway,	\
			    way_area,	\
			    z_order,	\
			    tags,	\
			    way 	\
			) \
			SELECT \
				osm_id,	\
			    access,	\
			    \"addr:housename\",	\
			    \"addr:housenumber\",	\
			    \"addr:interpolation\",	\
			    admin_level,	\
			    aerialway,	\
			    aeroway,	\
			    amenity,	\
			    barrier,	\
			    bicycle,	\
			    bridge,	\
			    boundary,	\
			    building,	\
			    construction,	\
			    covered,	\
			    foot,	\
			    highway,	\
			    historic,	\
			    horse,	\
			    junction,	\
			    landuse,	\
			    layer,	\
			    leisure,	\
			    lock,	\
			    man_made,	\
			    military,	\
			    name,	\
			    \"natural\",	\
			    oneway,	\
			    place,	\
			    power,	\
			    railway,	\
			    ref,	\
			    religion,	\
			    route,	\
			    service,	\
			    shop,	\
			    surface,	\
			    tourism,	\
			    tracktype,	\
			    tunnel,	\
			    water,	\
			    waterway,	\
			    way_area,	\
			    z_order,	\
			    tags,	\
			    way 	\
			FROM planet_osm_polygon"
	cur = sqlexec(cur, sql, True, '    ')
	# set otm_id_master in planet_otm_polygon
	sql = 	"UPDATE planet_otm_polygon SET otm_id_master=" + str(otm_id_master) + " WHERE otm_id_master IS NULL"
	cur = sqlexec(cur, sql, False, '    ')

	conn.commit()
	cur.close()
	cur2.close()
	conn.close()

def update_planetotmline_from_planetosmrels(param, otm_id_master):
	conn = psycopg2.connect("dbname="+param['PGSQL_DB']+" user="+param['PGSQL_USER']+" password="+param['PGSQL_PASSWORD']+" host="+param['PGSQL_HOST']+" port="+param['PGSQL_PORT'])
	cur = conn.cursor()
	cur2 = conn.cursor()

	log("log.txt", '    start: '+str(time.strftime("%Y-%m-%d %H:%M:%S")))
	log("log.txt", '        get all relations from planet_osm_rels')
	sql = "SELECT members, tags FROM planet_osm_rels"
	cur = sqlexec(cur, sql, False, '            ')
	data = cur.fetchone()
	w = {}
	while data is not None:
		#log("log.txt", str(data))
		#sys.exit()
		members = data[0]
		tags = data[1]
		otag = read_osm_tag(tags, [])
		if 'network' in otag.keys():
			o = {}
			if otag['network'] in ['lcn', 'rcn', 'ncn', 'icn', 'lwn', 'rwn', 'nwn', 'iwn']:
				if 'name' in otag.keys():
					o[ otag['network'] + '_name'] = otag['name']
				if 'ref' in otag.keys():
					o[ otag['network'] + '_ref'] = otag['ref']
				o[ 'is_' + otag['network'] ] = 'true'
			if 'osmc:symbol' in otag.keys():
				o['has_symbol'] = 'true'
				o['symbol'] = otag['osmc:symbol']
				if otag['network'] not in ['lcn', 'rcn', 'ncn', 'icn', 'lwn', 'rwn', 'nwn', 'iwn']:
					if 'name' in otag.keys():
						o['symbol_name'] = otag['name']
					if 'ref' in otag.keys():
						o['symbol_ref'] = otag['ref']

			if len(o.keys()) > 0:
				for i in range(0, len(members)):
					member = members[i]
					if len(member) > 1:
						if member[0] == 'w' and member[1:].isdigit():
							member = member[1:]
							if member not in w.keys():
								w[member] = {}
							for j in range(0, len(o.keys())):
								w[member][o.keys()[j]] = o[o.keys()[j]]
													
		data = cur.fetchone()

	# loop on w list to update planet_otm_line table
	log("log.txt", '        update planet_otm_line')
	log("log.txt","            sql*> UPDATE planet_otm_line SET otm_ref='XXXX', otm_name='XXXX', otm_tags='XXXX' WHERE  osm_id=XXXX AND otm_id_master="+str(otm_id_master))
	for i in range(0, len(w.keys())):
		wloc = w[w.keys()[i]]

		otm_name = ''
		if 'iwn_name' in wloc.keys():
			otm_name = wloc['iwn_name']
		elif 'nwn_name' in wloc.keys():
			otm_name = wloc['nwn_name']
		elif 'icn_name' in wloc.keys():
			otm_name = wloc['icn_name']
		elif 'ncn_name' in wloc.keys():
			otm_name = wloc['ncn_name']
		elif 'rwn_name' in wloc.keys():
			otm_name = wloc['rwn_name']
		elif 'lwn_name' in wloc.keys():
			otm_name = wloc['lwn_name']
		elif 'rcn_name' in wloc.keys():
			otm_name = wloc['rcn_name']
		elif 'lcn_name' in wloc.keys():
			otm_name = wloc['lcn_name']
		elif 'symbol_name' in wloc.keys():
			otm_name = wloc['symbol_name']
		if otm_name == '':
			otm_name = 'NULL'
		else:
			otm_name = '\''+otm_name.strip()+'\''

		otm_ref = ''
		if 'iwn_ref' in wloc.keys():
			otm_ref = wloc['iwn_ref']
		elif 'nwn_ref' in wloc.keys():
			otm_ref = wloc['nwn_ref']
		elif 'icn_ref' in wloc.keys():
			otm_ref = wloc['icn_ref']
		elif 'ncn_ref' in wloc.keys():
			otm_ref = wloc['ncn_ref']
		elif 'rwn_ref' in wloc.keys():
			otm_ref = wloc['rwn_ref']
		elif 'lwn_ref' in wloc.keys():
			otm_ref = wloc['lwn_ref']
		elif 'rcn_ref' in wloc.keys():
			otm_ref = wloc['rcn_ref']
		elif 'lcn_ref' in wloc.keys():
			otm_ref = wloc['lcn_ref']
		elif 'symbol_ref' in wloc.keys():
			otm_ref = wloc['symbol_ref']
		if otm_ref == '':
			otm_ref = 'NULL'
		else:
			otm_ref = '\''+otm_ref.strip()+'\''

		otm_tags = ''
		for j in range(0, len(wloc.keys())):
			key = wloc.keys()[j]	
			if otm_tags != '':
				otm_tags = otm_tags + ', '
			otm_tags = otm_tags + '"' + re.sub("\"", "\\\"", key) + '"=>"'+re.sub("\"", "\\\"", wloc[key])+'"'
		if otm_tags == '':
			otm_tags = 'NULL'
		else:
			otm_tags = '\''+otm_tags+'\''

		sql = "UPDATE planet_otm_line SET otm_ref=" + otm_ref + ", otm_name=" + otm_name + ", otm_tags=" + otm_tags + " WHERE osm_id="+w.keys()[i]+" AND otm_id_master="+str(otm_id_master)
		#cur = sqlexec(cur, sql, False, '    ')
		#sys.exit()
		#log("log.txt", '            '+sql)
		cur.execute(sql)

	log("log.txt", '    end: '+str(time.strftime("%Y-%m-%d %H:%M:%S")))
	conn.commit()
	cur.close()
	cur2.close()
	conn.close()

def read_osm_tag(tags, tValues):
	oReturn = {}
	oReturn["index"] = []
	if len(tValues) == 0:
		if tags != None:
			for i in range(0, len(tags), 2):
				oReturn[tags[i]] = tags[i+1].replace("'"," ")
				oReturn["index"].append(tags[i])
	else:	
		for i in range(0, len(tValues)):
				oReturn[tValues[i]] = None
				if tags != None:
					index = -1
					for ii in range(0, len(tags)):
						if tags[ii] == tValues[i]:
								index = ii
								break;
					if index >= 0 and index+1 < len(tags):
							oReturn[tValues[i]] = tags[index+1].replace("'"," ")
							oReturn["index"].append(tValues[i])
	return oReturn

def _____main_____():

	log("log.txt", "",True)
	
	log("log.txt", "------------------------------------------------------------------------------")
	log("log.txt", 'opentraveller import osm data')
	log("log.txt", "------------------------------------------------------------------------------")

	log("log.txt",'')
	log("log.txt", str(time.strftime("%Y-%m-%d %H:%M:%S")))

	log("log.txt",'')
	log("log.txt",'')
	log("log.txt", 'READ PARAM')
	param = readparam()
	
	log("log.txt",'')
	log("log.txt",'')
	log("log.txt", 'READ GEOFABRIK DATA LIST')
	geofabriklist = readgeofabriklist(param)

	log("log.txt",'')
	log("log.txt",'')
	log("log.txt",'PREPARE DB')
	prepare_db(param)

	#loop on geofabrik pbf files
	i_pbf = 0
	for pbffile in geofabriklist:
		i_pbf = i_pbf + 1

		tpbffile = pbffile.split('/')
		pbffilename = tpbffile[len(tpbffile)-1]

		log("log.txt",'')
		log("log.txt",'')
		log("log.txt", "------------------------------------------------------------------------------")
		log("log.txt", "import pbf file " + str(i_pbf) + " / " + str(len(geofabriklist)))
		log("log.txt", pbffilename)

		log("log.txt",'')
		log("log.txt", str(time.strftime("%Y-%m-%d %H:%M:%S")))

		log("log.txt",'')
		shellexec('rm -rf workdir', False, '')
		shellexec('mkdir workdir', False, '')

		
		log("log.txt",'')
		shellexec('rm -rf ' + pbffilename, False, '')
		shellexec('wget ' + pbffile, True, '')
		shellexec('mv ' + pbffilename + ' workdir/' + pbffilename, False, '')

		log("log.txt",'')
		log("log.txt",'update_master_start')
		otm_id_master = update_master_start(param, pbffile)
		#otm_id_master = 4
		log("log.txt",'    otm_id_master: '+str(otm_id_master))
		

		log("log.txt",'')
		cmd = "osm2pgsql -U " + param['PGSQL_USER'] + " -H " + param['PGSQL_HOST'] + " -P " + param['PGSQL_PORT'] + " -d " + param['PGSQL_DB'] + " --create --slim  -G --hstore --tag-transform-script openstreetmap-carto.lua -C 2500 --number-processes " + param['OSM2PGSQL_number-processes'] + " -S openstreetmap-carto.style workdir/" + pbffilename
		shellexec(cmd, True, '')

		log("log.txt",'')
		insert_into_planetotm_tables(param, otm_id_master)
		
		log("log.txt",'')
		log("log.txt",'update_planetotmline_from_planetosmrels')
		update_planetotmline_from_planetosmrels(param, otm_id_master)

		log("log.txt",'')
		log("log.txt",'update_master_end')
		update_master_end(param, otm_id_master)

		log("log.txt",'')
		log("log.txt", str(time.strftime("%Y-%m-%d %H:%M:%S")))

		#in debug mode do not delete workingdirectory (following line to be commented/uncommented)
		shellexec('rm -rf workdir', False, '')

	log("log.txt",'')
	log("log.txt",'')
	log("log.txt", "------------------------------------------------------------------------------")
	log("log.txt",'')
	log("log.txt",'')
	log("log.txt",'CLEAN DB')
	clean_db(param)
	log("log.txt",'')
	log("log.txt",'')
	log("log.txt", str(time.strftime("%Y-%m-%d %H:%M:%S")))	
	log("log.txt",'')	
	log("log.txt",'')	
	log("log.txt",'')	
	
_____main_____()
