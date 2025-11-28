all: db/procedure/mesh_run_greedy ## [FINAL] Build entire pipeline end-to-end

test: db/test/georgia_roads_geom db/test/population_h3_r8 db/test/h3_los_between_cells db/test/mesh_surface_visible_towers db/test/mesh_surface_refresh_reception_metrics db/test/mesh_surface_refresh_visible_tower_counts db/test/mesh_visibility_edges db/test/mesh_visibility_invisible_route_geom db/test/mesh_run_greedy_prepare db/test/mesh_route_cache_graph_priority db/test/mesh_route_corridor_between_towers db/test/mesh_route ## [FINAL] Run verification suite

clean: ## [FINAL] Remove intermediate data and build markers
	@if [ -n "$(filter clean,$(MAKECMDGOALS))" ]; then rm -rf data/mid data/out db; fi

data: ## Ensure data staging directory exists
	mkdir -p data

data/in: | data ## Ensure raw data input directory exists
	mkdir -p data/in

data/in/osm: | data/in ## Ensure raw OSM input directory exists
	mkdir -p data/in/osm

data/in/population: | data/in ## Ensure raw population input directory exists
	mkdir -p data/in/population

data/in/gebco: | data/in ## Ensure raw GEBCO input directory exists
	mkdir -p data/in/gebco

data/mid: | data ## Ensure intermediate data directory exists
	mkdir -p data/mid

data/mid/population: | data/mid ## Ensure intermediate population directory exists
	mkdir -p data/mid/population

data/mid/gebco: | data/mid ## Ensure intermediate GEBCO directory exists
	mkdir -p data/mid/gebco

db: ## Ensure database artifacts directory exists
	mkdir -p db

db/raw: | db ## Ensure raw database marker directory exists
	mkdir -p db/raw

db/table: | db ## Ensure table marker directory exists
	mkdir -p db/table

db/function: | db ## Ensure function marker directory exists
	mkdir -p db/function

db/procedure: | db ## Ensure procedure marker directory exists
	mkdir -p db/procedure

db/test: | db ## Ensure test marker directory exists
	mkdir -p db/test

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
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "create extension if not exists pgrouting;"
	touch db/table/postgis_extension

db/table/mesh_pipeline_settings: tables/mesh_pipeline_settings.sql db/table/postgis_extension | db/table ## Store pipeline constants
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/mesh_pipeline_settings.sql
	touch db/table/mesh_pipeline_settings

db/table/mesh_towers: tables/mesh_towers.sql db/table/mesh_pipeline_settings | db/table ## Create mesh_towers registry
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/mesh_towers.sql
	touch db/table/mesh_towers

db/table/mesh_los_cache: tables/mesh_los_cache.sql db/table/postgis_extension | db/table ## Store cached LOS evaluations
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/mesh_los_cache.sql
	touch db/table/mesh_los_cache

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

db/raw/kontur_population: data/mid/population/kontur_population_20231101.gpkg db/table/postgis_extension | db/raw ## Import Kontur population
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "drop table if exists kontur_population;"
	ogr2ogr -f PostgreSQL PG:"" data/mid/population/kontur_population_20231101.gpkg -nln kontur_population -nlt MULTIPOLYGON -lco GEOMETRY_NAME=geom -overwrite -t_srs EPSG:4326
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
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "create index if not exists gebco_elevation_h3_r8_h3_idx on gebco_elevation_h3_r8 using btree (h3) include (ele);"
	touch db/table/gebco_elevation_h3_r8

db/function/h3_los_between_cells: functions/h3_los_between_cells.sql db/table/gebco_elevation_h3_r8 db/table/mesh_los_cache | db/function ## Install LOS helper
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f functions/h3_los_between_cells.sql
	touch db/function/h3_los_between_cells

db/function/mesh_surface_fill_visible_population: functions/mesh_surface_fill_visible_population.sql db/function/h3_los_between_cells | db/function ## Fill visible population for a single candidate cell
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f functions/mesh_surface_fill_visible_population.sql
	touch db/function/mesh_surface_fill_visible_population

db/function/mesh_surface_refresh_reception_metrics: functions/mesh_surface_refresh_reception_metrics.sql db/function/h3_los_between_cells db/table/mesh_surface_h3_r8 db/table/mesh_towers | db/function ## Refresh reception metrics near a new tower
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f functions/mesh_surface_refresh_reception_metrics.sql
	touch db/function/mesh_surface_refresh_reception_metrics

db/function/mesh_surface_refresh_visible_tower_counts: functions/mesh_surface_refresh_visible_tower_counts.sql db/function/h3_los_between_cells db/table/mesh_surface_h3_r8 db/table/mesh_towers | db/function ## Refresh visible tower counts near a new tower
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f functions/mesh_surface_refresh_visible_tower_counts.sql
	touch db/function/mesh_surface_refresh_visible_tower_counts

db/function/mesh_visibility_invisible_route_geom: functions/mesh_visibility_invisible_route_geom.sql db/table/mesh_surface_h3_r8 db/table/mesh_route_graph db/table/mesh_route_graph_cache | db/function ## Build routing helper for invisible visibility edges
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f functions/mesh_visibility_invisible_route_geom.sql
	touch db/function/mesh_visibility_invisible_route_geom

db/function/mesh_route_corridor_between_towers: functions/mesh_route_corridor_between_towers.sql db/procedure/mesh_route_cache_graph | db/function ## Produce pgRouting corridors between specific tower pairs
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f functions/mesh_route_corridor_between_towers.sql
	touch db/function/mesh_route_corridor_between_towers

db/test/georgia_roads_geom: tests/georgia_roads_geom.sql db/table/georgia_roads_geom | db/test ## Verify only car-capable highways stay in roads layer
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tests/georgia_roads_geom.sql
	touch db/test/georgia_roads_geom

db/test/population_h3_r8: tests/population_h3_r8.sql db/table/population_h3_r8 db/raw/kontur_population | db/test ## Check population table stays a 1:1 Kontur H3 cast
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tests/population_h3_r8.sql
	touch db/test/population_h3_r8

db/test/h3_los_between_cells: tests/h3_los_between_cells.sql db/function/h3_los_between_cells db/table/mesh_initial_nodes_h3_r8 | db/test ## Validate LOS results for seed nodes
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tests/h3_los_between_cells.sql
	touch db/test/h3_los_between_cells

db/test/mesh_surface_visible_towers: tests/mesh_surface_visible_towers.sql db/table/mesh_surface_h3_r8 db/function/h3_los_between_cells db/table/mesh_towers | db/test ## Confirm visible_tower_count matches sampled LOS tower counts
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tests/mesh_surface_visible_towers.sql
	touch db/test/mesh_surface_visible_towers

db/test/mesh_surface_refresh_reception_metrics: tests/mesh_surface_refresh_reception_metrics.sql db/function/mesh_surface_refresh_reception_metrics db/table/mesh_surface_h3_r8 db/table/mesh_towers | db/test ## Ensure localized reception refresh restores clearance and path loss
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tests/mesh_surface_refresh_reception_metrics.sql
	touch db/test/mesh_surface_refresh_reception_metrics

db/test/mesh_surface_refresh_visible_tower_counts: tests/mesh_surface_refresh_visible_tower_counts.sql db/function/mesh_surface_refresh_visible_tower_counts db/table/mesh_surface_h3_r8 db/table/mesh_towers | db/test ## Ensure localized visible tower refresh repopulates counts
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tests/mesh_surface_refresh_visible_tower_counts.sql
	touch db/test/mesh_surface_refresh_visible_tower_counts

db/table/mesh_surface_h3_r8: tables/mesh_surface_h3_r8.sql db/table/mesh_surface_domain_h3_r8 db/table/roads_h3_r8 db/table/population_h3_r8 db/table/mesh_towers db/table/gebco_elevation_h3_r8 db/function/h3_los_between_cells | db/table ## Populate mesh_surface_h3_r8 table
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/mesh_surface_h3_r8.sql
	touch db/table/mesh_surface_h3_r8

db/table/mesh_route_graph_cache: tables/mesh_route_graph_cache.sql | db/table ## Cache precomputed routing geometries per tower pair
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/mesh_route_graph_cache.sql
	touch db/table/mesh_route_graph_cache

db/table/mesh_route_graph: tables/mesh_route_graph.sql db/table/mesh_surface_h3_r8 db/table/mesh_route_graph_cache | db/table ## Precompute routing graph for invisible edge fallback
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/mesh_route_graph.sql
	touch db/table/mesh_route_graph

db/table/mesh_visibility_edges: tables/mesh_visibility_edges.sql scripts/mesh_visibility_edges_refresh.sql db/table/mesh_towers db/table/mesh_surface_h3_r8 db/table/mesh_route_graph db/table/mesh_route_graph_cache db/function/h3_los_between_cells db/function/mesh_visibility_invisible_route_geom db/procedure/mesh_visibility_edges_refresh | db/table ## Materialize visibility diagnostics for seed and active towers
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/mesh_visibility_edges.sql
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/mesh_visibility_edges_refresh.sql
	touch db/table/mesh_visibility_edges

db/test/mesh_visibility_edges: tests/mesh_visibility_edges.sql db/table/mesh_visibility_edges db/table/mesh_initial_nodes_h3_r8 db/function/h3_los_between_cells | db/test ## Validate seed visibility diagnostics stay accurate
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tests/mesh_visibility_edges.sql
	touch db/test/mesh_visibility_edges

db/test/mesh_visibility_invisible_route_geom: tests/mesh_visibility_invisible_route_geom.sql db/function/mesh_visibility_invisible_route_geom db/table/mesh_surface_h3_r8 db/table/mesh_route_graph db/table/mesh_route_graph_cache | db/test ## Ensure invisible visibility edges gain routed geometries
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tests/mesh_visibility_invisible_route_geom.sql
	touch db/test/mesh_visibility_invisible_route_geom

db/test/mesh_run_greedy_prepare: tests/mesh_run_greedy_prepare.sql procedures/mesh_run_greedy_prepare.sql db/table/mesh_surface_h3_r8 db/table/mesh_towers db/table/mesh_greedy_iterations | db/test ## Ensure greedy preparation preserves routed towers
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_run_greedy_prepare.sql
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tests/mesh_run_greedy_prepare.sql
	touch db/test/mesh_run_greedy_prepare

db/test/mesh_route_cache_graph_priority: tests/mesh_route_cache_graph_priority.sql db/table/mesh_surface_h3_r8 db/table/mesh_towers db/table/mesh_los_cache db/table/mesh_visibility_edges | db/test ## Validate LOS cache prioritizes nearest invisible edges
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tests/mesh_route_cache_graph_priority.sql
	touch db/test/mesh_route_cache_graph_priority

db/test/mesh_route_corridor_between_towers: tests/mesh_route_corridor_between_towers.sql db/function/mesh_route_corridor_between_towers | db/test ## Validate corridor extraction between tower pairs
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tests/mesh_route_corridor_between_towers.sql
	touch db/test/mesh_route_corridor_between_towers


db/test/mesh_route: tests/mesh_route.sql procedures/mesh_route_cache_graph.sql procedures/mesh_route_bridge.sql db/table/mesh_surface_h3_r8 db/table/mesh_towers db/table/mesh_los_cache | db/test ## Validate mesh_route cache/bridge stages
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_route_cache_graph.sql
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_route_bridge.sql
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tests/mesh_route.sql
	touch db/test/mesh_route

db/procedure/mesh_route_cache_graph: procedures/mesh_route_cache_graph.sql db/table/mesh_surface_h3_r8 db/table/mesh_towers db/table/mesh_los_cache db/table/mesh_visibility_edges | db/procedure ## Cache LOS metrics and build routing graph
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_route_cache_graph.sql
	touch db/procedure/mesh_route_cache_graph

db/procedure/mesh_route_bridge: procedures/mesh_route_bridge.sql db/procedure/mesh_route_cache_graph db/function/mesh_surface_refresh_reception_metrics db/function/mesh_surface_refresh_visible_tower_counts | db/procedure ## Bridge farthest tower clusters via pgRouting
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_route_bridge.sql
	touch db/procedure/mesh_route_bridge

db/procedure/mesh_visibility_edges_refresh: procedures/mesh_visibility_edges_refresh.sql db/table/mesh_towers db/table/mesh_surface_h3_r8 db/table/gebco_elevation_h3_r8 db/function/h3_los_between_cells db/function/mesh_visibility_invisible_route_geom | db/procedure ## Install visibility refresh procedure
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_visibility_edges_refresh.sql
	touch db/procedure/mesh_visibility_edges_refresh

db/procedure/mesh_route_cluster_slim: procedures/mesh_route_cluster_slim.sql scripts/mesh_route_cluster_slim.sql db/procedure/mesh_route_bridge db/procedure/mesh_route_cache_graph db/function/mesh_route_corridor_between_towers db/function/mesh_surface_refresh_reception_metrics db/function/mesh_surface_refresh_visible_tower_counts db/procedure/mesh_visibility_edges_refresh | db/procedure ## Shorten long intra-cluster hops with routing towers
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_route_cluster_slim.sql
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/mesh_route_cluster_slim.sql
	touch db/procedure/mesh_route_cluster_slim

db/procedure/mesh_route_refresh_visibility: scripts/mesh_visibility_edges_refresh.sql db/table/mesh_visibility_edges db/procedure/mesh_route_cluster_slim db/procedure/mesh_visibility_edges_refresh | db/procedure ## Rebuild visibility diagnostics after routing stages
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/mesh_visibility_edges_refresh.sql
	touch db/procedure/mesh_route_refresh_visibility

db/procedure/mesh_route: db/procedure/mesh_route_refresh_visibility | db/procedure ## Build PG routing bridges between tower clusters
	touch db/procedure/mesh_route

db/procedure/mesh_run_greedy: procedures/mesh_run_greedy_prepare.sql procedures/mesh_run_greedy.sql procedures/mesh_run_greedy_finalize.sql scripts/mesh_visibility_edges_refresh.sql db/procedure/mesh_route db/table/mesh_visibility_edges db/table/mesh_surface_h3_r8 db/table/mesh_greedy_iterations db/function/mesh_surface_refresh_reception_metrics db/function/mesh_surface_refresh_visible_tower_counts db/function/mesh_surface_fill_visible_population db/table/mesh_initial_nodes_h3_r8 | db/procedure ## Execute greedy placement loop
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_run_greedy_prepare.sql
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "call mesh_run_greedy_prepare();"
	bash -lc 'set -euo pipefail; for iter in $$(seq 1 100); do echo ">> Greedy iteration $$iter"; psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_run_greedy.sql; psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/mesh_visibility_edges_refresh.sql; done'
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_run_greedy_finalize.sql
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/mesh_visibility_edges_refresh.sql
	touch db/procedure/mesh_run_greedy
