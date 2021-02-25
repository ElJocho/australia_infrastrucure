from python.code.utils.ohsome import query
from python.code.utils.definitions import logger, DATA_PATH
from python.code.utils.postgres import PostgresDB, geojson_to_table, get_bpolys_from_db
import os
import geojson
"""landuse in (commercial, construction, industrial, residential, retail, cemetery,
        garages, depot) 
        or """
human_settlements = {
    "endpoint": "elements/geometry",
    "filter": """
         (leisure = park or landuse in (residential, 
        commercial, industrial, retail)) and geometry:polygon
    """
}

admin_4 = {
    "endpoint": "elements/geometry",
    "filter": """
        admin_level=4 and geometry:polygon
    """
}

all_weather_roads = {
    "endpoint": "elements/geometry",
    "filter": """
        geometry:line 
        and highway!=footway and highway!=bridleway and highway!=steps 
        and highway!=path and highway!=sidewalk and highway!=cycleway
        and seasonal!=no 
        and surface in 
            (paved, asphalt, concrete, paving_stones,
            sett, cobblestone, unhewn_cobblestone, metal, wood) 
        and smoothness!=bad and smoothness!="very bad" and smoothness!=horrible 
        and smoothness!="very horrible" and smoothness!=impassable
    """
}


def get_ohsome_data(geom_infile, ohsome_outfiles):
    with open(geom_infile) as infile:
        bpolys = geojson.load(infile)
        if bpolys.is_valid is not True:
            raise ValueError("Invalid bpolys: {}".format(bpolys.errors()))
        bpolys = geojson.dumps(bpolys)
    admin_areas = query(request=admin_4, bpolys=bpolys)
    logger.info("recieved {} admin_areas".format(len(admin_areas["features"])))

    counter = 0
    for feature in admin_areas["features"]:
        feature = geojson.dumps({"type": "FeatureCollection",
                                 "features": [feature]})
        #settlements = query(request=human_settlements, bpolys=feature)
        #logger.info("recieved {} settlement_areas".format(len(settlements["features"])))
        #with open(ohsome_outfiles[0] + "_{}".format(counter), 'w') as outfile:
        #    geojson.dump(settlements, outfile)
        roads = query(request=all_weather_roads, bpolys=feature)
        logger.info("recieved {} roads".format(len(roads["features"])))
        with open(ohsome_outfiles[1] + "_{}".format(counter), 'w') as outfile:
            geojson.dump(roads, outfile)

        counter += 1

    return counter


def upload_data(ohsome_outfiles, geom_infile, counter):
    settlement_sql = """
           drop table if exists settlements_aust;
           create table settlements_aust as (
           SELECT * FROM {}
       """.format("settlements_aust_0")

    road_sql = """
           drop table if exists all_weather_roads;
           create table all_weather_roads as (
           SELECT * FROM {}
       """.format("all_weather_roads_0")

    for x in range(0, counter):
        geojson_to_table("settlements_aust_{}".format(x), ohsome_outfiles[0] + "_{}".format(x))
        geojson_to_table("all_weather_roads_{}".format(x), ohsome_outfiles[1] + "_{}".format(x))
        if x!=counter-1:
            settlement_sql += """
                   UNION ALL
                   SELECT * FROM settlements_aust_{}
               """.format(x + 1)
            road_sql += """
                   UNION ALL
                   SELECT * FROM all_weather_roads_{}
               """.format(x + 1)

    settlement_sql += ")"
    road_sql += ")"

    db = PostgresDB()
    db.query(settlement_sql)
    db.query(road_sql)

    geojson_to_table("australia", geom_infile)

    logger.info("uploaded ohsome data")


def buffer_and_union_polygons():
    db = PostgresDB()
    sql = """
        drop table if exists combined_polys;
        create table combined_polys as (            
            SELECT (ST_Dump(geom)).geom as geom, ROW_NUMBER() over (order by (Select Null)) as fid
            FROM (SELECT ST_UNION(ST_Transform(public.ST_BUFFER(ST_TRANSFORM(geom, 900913), 2000), 954009)) AS geom
            FROM settlements_aust) as unioned
        )

    """
    db.query(sql)
    logger.info("unioned overlapping polys")


def population_per_city():
    db = PostgresDB()
    sql = """
        drop table if exists cities_with_pop;
        create table cities_with_pop as (
            SELECT (ST_SummaryStats(ST_CLIP(rast, geom))).sum as pop, geom
            FROM combined_polys, pop.ghspop
            where st_intersects(rast, geom)
        )
    """
    db.query(sql)
    logger.info("calculated approx pop per human settlement")


def remove_urban_from_raster():
    db = PostgresDB()
    sql = """
        drop table if exists rural_pop;
        create table rural_pop as (
            with clip as (
            select ST_Difference(ST_Transform(a.geom, 954009), c.geom) as clipper
            from (select ST_UNION(geom) as geom from cities_with_pop where pop>=10000) as c, australia as a
            )
            select rid, ST_Clip(rast, clipper) as rast
                    from clip, pop.ghspop
                    where ST_Intersects(rast, clipper)
        )
    """

    db.query(sql)
    logger.info("got rural australia raster")


def buffer_roads():
    sql = """
    drop table if exists buffered_streets;
    create table buffered_streets as (
        select ST_Union(ST_Transform(public.ST_BUFFER(ST_TRANSFORM(geom, 900913), 2000), 954009)) as buffer
        from all_weather_roads
    )
    """
    db=PostgresDB()
    db.query(sql)
    logger.info("buffered streets")


def get_all_reachable_pop():
    sql = """
    drop table if exists reachable_pop;
    create table reachable_pop as (
            select rid, ST_Clip(rast, buffer) as reachable_pop
            from rural_pop, buffered_streets
            where ST_Intersects(rast, buffer)

    )
    """
    db=PostgresDB()
    db.query(sql)
    logger.info("got reachable pop raster")

def get_share_rural_population_within_2km_of_all_weather_road():
    sql = """
        with all_rural_pop as (
        select Sum((ST_SummaryStats(rast)).sum) as all_pop
        from rural_pop
        ),
        reachable_pop_t as (
            select Sum((ST_SummaryStats(reachable_pop)).sum) as reachable
            from reachable_pop
        )
        select all_pop, reachable
        from all_rural_pop, reachable_pop_t
    """
    db = PostgresDB()
    result = db.retr_query(sql)
    print(result)
def settlement_workflow():
    geom_infile = os.path.join(DATA_PATH, "area_of_interest/australia.geojson")
    ohsome_outfiles = [os.path.join(DATA_PATH, "query_answers/settlements_aust.geojson"),
                       os.path.join(DATA_PATH, "query_answers/all_weather_roads.geojson")]
    counter=10
    counter = get_ohsome_data(geom_infile, ohsome_outfiles)
    upload_data(ohsome_outfiles, geom_infile, counter)
    buffer_and_union_polygons()
    population_per_city()
    remove_urban_from_raster()
    buffer_roads()
    get_all_reachable_pop()
    get_share_rural_population_within_2km_of_all_weather_road()
settlement_workflow()

def get_table_as_geojson(tablename):
    result =get_bpolys_from_db(tablename)
    drop_temp_path = os.path.join(DATA_PATH, "query_answers/temp.geojson")
    with open(drop_temp_path, 'w') as outfile:
        geojson.dump(result, outfile)

get_table_as_geojson("rural_pop_2")