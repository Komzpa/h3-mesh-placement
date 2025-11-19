.PHONY: all clean

all: db/procedure/mesh_run_greedy ## Build entire pipeline end-to-end

clean: ## Remove intermediate data and build markers
	rm -rf data/mid data/out db

data:
	mkdir -p data

data/in: | data
	mkdir -p data/in

data/in/osm: | data/in
	mkdir -p data/in/osm

data/in/population: | data/in
	mkdir -p data/in/population

data/in/gebco: | data/in
	mkdir -p data/in/gebco

data/mid: | data
	mkdir -p data/mid

data/mid/osm: | data/mid
	mkdir -p data/mid/osm

data/mid/population: | data/mid
	mkdir -p data/mid/population

data/mid/gebco: | data/mid
	mkdir -p data/mid/gebco

data/out: | data
	mkdir -p data/out

db:
	mkdir -p db

db/raw: | db
	mkdir -p db/raw

db/table: | db
	mkdir -p db/table

db/function: | db
	mkdir -p db/function

db/procedure: | db
	mkdir -p db/procedure

data/in/osm/georgia-latest.osm.pbf: | data/in/osm ## Download Georgia OSM extract
	curl -L --retry 3 --continue-at - -o data/in/osm/georgia-latest.osm.pbf https://download.geofabrik.de/europe/georgia-latest.osm.pbf

data/in/population/kontur_population_20231101.gpkg.gz: | data/in/population ## Download Kontur population archive
	rm -f data/in/population/kontur_population_20231101.gpkg.gz
	curl -L --retry 3 --continue-at - -o data/in/population/kontur_population_20231101.gpkg.gz https://geodata-eu-central-1-kontur-public.s3.eu-central-1.amazonaws.com/kontur_datasets/kontur_population_20231101.gpkg.gz

data/mid/population/kontur_population_20231101.gpkg: data/in/population/kontur_population_20231101.gpkg.gz | data/mid/population ## Decompress Kontur population geopackage
	rm -f data/mid/population/kontur_population_20231101.gpkg
	gunzip -c data/in/population/kontur_population_20231101.gpkg.gz > data/mid/population/kontur_population_20231101.gpkg

data/in/gebco/gebco_2024_geotiff.zip: | data/in/gebco ## Download GEBCO 2024 GeoTIFF archive
	curl -L --retry 3 --continue-at - -o data/in/gebco/gebco_2024_geotiff.zip https://www.bodc.ac.uk/data/open_download/gebco/gebco_2024/geotiff/

data/mid/gebco/gebco_2024_geotiffs_unzip: data/in/gebco/gebco_2024_geotiff.zip | data/mid/gebco ## Unpack GEBCO rasters
	rm -f data/mid/gebco/*.tif
	unzip -o data/in/gebco/gebco_2024_geotiff.zip -d data/mid/gebco
	rm -f data/mid/gebco/*.pdf
	touch data/mid/gebco/gebco_2024_geotiffs_unzip

db/table/postgis_extension: | db/table ## Ensure required Postgres extensions exist
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "create extension if not exists postgis;"
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "create extension if not exists h3;"
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "create extension if not exists hstore;"
	touch db/table/postgis_extension

db/table/mesh_pipeline_settings: tables/mesh_pipeline_settings.sql db/table/postgis_extension | db/table ## Store pipeline constants
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/mesh_pipeline_settings.sql
	touch db/table/mesh_pipeline_settings

db/table/mesh_towers: tables/mesh_towers.sql db/table/mesh_pipeline_settings | db/table ## Create mesh_towers registry
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/mesh_towers.sql
	touch db/table/mesh_towers

db/table/mesh_greedy_iterations: tables/mesh_greedy_iterations.sql db/table/postgis_extension | db/table ## Create greedy iterations log table
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/mesh_greedy_iterations.sql
	touch db/table/mesh_greedy_iterations

db/raw/initial_nodes: data/in/existing_mesh_nodes.geojson db/table/mesh_towers | db/raw ## Import seed tower locations
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "drop table if exists mesh_initial_nodes;"
	ogr2ogr -f PostgreSQL PG:"" data/in/existing_mesh_nodes.geojson -nln mesh_initial_nodes -nlt POINT -lco GEOMETRY_NAME=geom -overwrite -a_srs EPSG:4326
	touch db/raw/initial_nodes

db/table/osm_georgia: data/in/osm/georgia-latest.osm.pbf db/table/postgis_extension | db/table ## Import full Georgia OSM extract
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "drop table if exists osm_georgia;"
	osmium export -i sparse_mem_array -c osmium.config.json -f pg data/in/osm/georgia-latest.osm.pbf -v --progress | psql --no-psqlrc --set=ON_ERROR_STOP=1 -1 -c "create table osm_georgia(geog geography, osm_type text, osm_id bigint, version int, osm_user text, ts timestamptz, way_nodes bigint[], tags jsonb); alter table osm_georgia alter geog set storage external, alter osm_type set storage main, alter osm_user set storage main, alter way_nodes set storage external, alter tags set storage external, set (fillfactor=100); copy osm_georgia from stdin freeze;"
	touch db/table/osm_georgia

data/mid/osm/georgia_boundary.geojson: db/table/osm_georgia | data/mid/osm ## Export Georgia boundary for debugging
	ogr2ogr -overwrite -f GeoJSON data/mid/osm/georgia_boundary.geojson PG:"" -sql "select st_multi(st_union(geog::geometry)) as geom from osm_georgia where tags ? 'boundary' and tags ->> 'boundary' = 'administrative' and tags ->> 'admin_level' = '2'"

db/raw/kontur_population: data/mid/population/kontur_population_20231101.gpkg db/table/postgis_extension | db/raw ## Import Kontur population
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "drop table if exists kontur_population;"
	ogr2ogr -f PostgreSQL PG:"" data/mid/population/kontur_population_20231101.gpkg -nln kontur_population -nlt MULTIPOLYGON -lco GEOMETRY_NAME=geom -overwrite -a_srs EPSG:4326
	touch db/raw/kontur_population

db/raw/gebco_elevation: data/mid/gebco/gebco_2024_geotiffs_unzip db/table/georgia_convex_hull db/table/postgis_extension | db/raw ## Import GEBCO rasters for Georgia extent
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "drop table if exists gebco_elevation cascade;"
	raster2pgsql -I -C -M -s 4326 -t auto data/mid/gebco/*.tif gebco_elevation_all | psql --no-psqlrc --set=ON_ERROR_STOP=1 -q
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "create table gebco_elevation as select * from gebco_elevation_all where ST_Intersects(rast, (select geom from georgia_convex_hull));"
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "drop table gebco_elevation_all;"
	touch db/raw/gebco_elevation

db/table/georgia_boundary: tables/georgia_boundary.sql db/table/osm_georgia | db/table ## Build Georgia boundary table
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/georgia_boundary.sql
	touch db/table/georgia_boundary

db/table/georgia_convex_hull: tables/georgia_convex_hull.sql db/table/georgia_boundary | db/table ## Build convex hull for Georgia
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/georgia_convex_hull.sql
	touch db/table/georgia_convex_hull

db/table/mesh_initial_nodes_h3_r8: tables/mesh_initial_nodes_h3_r8.sql db/raw/initial_nodes db/table/mesh_towers | db/table ## Convert seed nodes to H3
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/mesh_initial_nodes_h3_r8.sql
	touch db/table/mesh_initial_nodes_h3_r8

db/table/mesh_surface_domain_h3_r8: tables/mesh_surface_domain_h3_r8.sql db/table/georgia_convex_hull | db/table ## Build convex hull H3 domain
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/mesh_surface_domain_h3_r8.sql
	touch db/table/mesh_surface_domain_h3_r8

db/table/georgia_roads_geom: tables/georgia_roads_geom.sql db/table/osm_georgia | db/table ## Extract Georgia roads geometries
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/georgia_roads_geom.sql
	touch db/table/georgia_roads_geom

db/table/roads_h3_r8: tables/roads_h3_r8.sql db/table/georgia_roads_geom | db/table ## Convert roads into H3 coverage
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/roads_h3_r8.sql
	touch db/table/roads_h3_r8

db/table/population_h3_r8: tables/population_h3_r8.sql db/raw/kontur_population db/table/georgia_boundary | db/table ## Aggregate population into H3
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/population_h3_r8.sql
	touch db/table/population_h3_r8

db/table/gebco_elevation_h3_r8: scripts/raster_values_into_h3.sql db/raw/gebco_elevation db/table/georgia_convex_hull | db/table ## Sample GEBCO values per H3
	psql --no-psqlrc --set=ON_ERROR_STOP=1 \
		-v table_name=gebco_elevation \
		-v table_name_h3=gebco_elevation_h3_r8 \
		-v item_name=ele \
		-v aggr_func=avg \
		-v resolution=8 \
		-v clip_table=georgia_convex_hull \
		-f scripts/raster_values_into_h3.sql
	touch db/table/gebco_elevation_h3_r8

db/function/h3_los_between_cells: functions/h3_los_between_cells.sql db/table/gebco_elevation_h3_r8 | db/function ## Install LOS helper
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f functions/h3_los_between_cells.sql
	touch db/function/h3_los_between_cells

db/table/mesh_surface_h3_r8: tables/mesh_surface_h3_r8.sql db/table/mesh_surface_domain_h3_r8 db/table/roads_h3_r8 db/table/population_h3_r8 db/table/mesh_towers db/table/gebco_elevation_h3_r8 | db/table ## Populate mesh_surface_h3_r8 table
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/mesh_surface_h3_r8.sql
	touch db/table/mesh_surface_h3_r8

db/table/mesh_visibility_edges_seed: tables/mesh_visibility_edges_seed.sql db/table/mesh_initial_nodes_h3_r8 db/function/h3_los_between_cells | db/table ## Materialize seed visibility diagnostics
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/mesh_visibility_edges_seed.sql
	touch db/table/mesh_visibility_edges_seed

db/table/mesh_visibility_edges_active: tables/mesh_visibility_edges_active.sql db/function/h3_los_between_cells db/table/mesh_towers | db/table ## Materialize active visibility diagnostics
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/mesh_visibility_edges_active.sql
	touch db/table/mesh_visibility_edges_active

db/procedure/mesh_run_greedy: procedures/mesh_run_greedy.sql db/table/mesh_visibility_edges_active db/table/mesh_surface_h3_r8 | db/procedure ## Execute greedy placement loop
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_run_greedy.sql
	touch db/procedure/mesh_run_greedy
