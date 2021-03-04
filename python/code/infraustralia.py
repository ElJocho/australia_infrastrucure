from python.code.utils.ohsome import query
from python.code.utils.definitions import logger, DATA_PATH
from python.code.utils.postgres import PostgresDB, geojson_to_table, get_bpolys_from_db
from python.code.utils.utils import to_json
import os
import geojson
import json
import psycopg2

continent_infile = os.path.join(DATA_PATH, "area_of_interest/africa.geojson")
continent_outfile = os.path.join(DATA_PATH, "query_answers/african_countries.geojson")
geom_infile = os.path.join(DATA_PATH, "area_of_interest/australia.geojson")
ohsome_outfiles = [os.path.join(DATA_PATH, "query_answers/settlements.geojson"),
                   os.path.join(DATA_PATH, "query_answers/all_weather_roads.geojson")]
result_outfile = os.path.join(DATA_PATH, "result.json")


human_settlements = {
    "description":"Areas which indicate human cities",
    "endpoint": "elements/geometry",
    "filter": """
         (leisure = park or landuse in (residential, 
        commercial, industrial, retail)) and geometry:polygon
    """
}
admin_2 = {
    "description": "admin_2 are usually independent countries",
    "endpoint": "elements/geometry",
    "filter":"""
        admin_level=2 and geometry:polygon and name=* and flag=*
    """
}


admin_4 = {
    "description":"Usually administrative regions e.g. german Bundesländer",
    "endpoint": "elements/geometry",
    "filter": """
        admin_level=4 and geometry:polygon
    """
}

all_weather_roads = {
    "description":"roads which are usable during every weather condition",
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


def get_countries(continent_infile, continent_outfile):

    with open(continent_infile) as infile:
        bpolys = geojson.load(infile)
        if bpolys.is_valid is not True:
            raise ValueError("Invalid bpolys: {}".format(bpolys.errors()))

        bpolys = geojson.dumps(bpolys)

    countries = query(request=admin_2, bpolys=bpolys, properties="tags")
    with open(continent_outfile, 'w') as outfile:
        geojson.dump(countries, outfile)

    return countries
get_countries(continent_infile, continent_outfile)

def get_ohsome_data(geom_infile=None, ohsome_outfiles=None, feature=None):
    if geom_infile is not None:
        with open(geom_infile) as infile:
            bpolys = geojson.load(infile)
            if bpolys.is_valid is not True:
                raise ValueError("Invalid bpolys: {}".format(bpolys.errors()))
            bpolys = geojson.dumps(bpolys)
    else:
        bpolys = feature
        with open(os.path.join(DATA_PATH, "area_of_interest/feature.geojson"), "w") as infile:

            geojson.dump(geojson.loads(bpolys), infile)
    admin_areas = query(request=admin_4, bpolys=bpolys)
    logger.info("recieved {} admin_areas".format(len(admin_areas["features"])))
    if len(admin_areas["features"])==0:
        return False
    counter = 0
    for feature in admin_areas["features"]:
        try:
            feature = geojson.dumps({"type": "FeatureCollection",
                                     "features": [feature]})
            settlements = query(request=human_settlements, bpolys=feature)
            logger.info("recieved {} settlement_areas".format(len(settlements["features"])))
            if len(settlements["features"]) == 0:
                continue
            with open(ohsome_outfiles[0] + "_{}".format(counter), 'w') as outfile:
                geojson.dump(settlements, outfile)
            roads = query(request=all_weather_roads, bpolys=feature)
            logger.info("recieved {} roads".format(len(roads["features"])))

            if len(roads["features"]) == 0:
                continue
            with open(ohsome_outfiles[1] + "_{}".format(counter), 'w') as outfile:
                geojson.dump(roads, outfile)

            counter += 1
        except:
            logger.info("skipped a feature")
            continue

    return counter


def upload_data(ohsome_outfiles, geom_infile, counter, drop_tables: bool):
    settlement_sql = """
           drop table if exists settlements;
           create table settlements as (
       """

    road_sql = """
           drop table if exists all_weather_roads;
           create table all_weather_roads as (
       """
    valid_areas = []  # necessairy to catch empty or faulty areas
    for x in range(0, counter):
        try:
            geojson_to_table("settlements_{}".format(x), ohsome_outfiles[0] + "_{}".format(x))
            geojson_to_table("all_weather_roads_{}".format(x), ohsome_outfiles[1] + "_{}".format(x))
            valid_areas.append(x)
        except:
            continue
    test = False
    if len(valid_areas) != 0:
        for x in range(0, len(valid_areas)):
            if x == 0:
                settlement_sql += "SELECT * FROM settlements_{}".format(valid_areas[x])
                road_sql += "SELECT * FROM all_weather_roads_{}".format(valid_areas[x])
                test=True
            else:
                settlement_sql += """
                       UNION ALL
                       SELECT * FROM settlements_{}
                   """.format(valid_areas[x])
                road_sql += """
                       UNION ALL
                       SELECT * FROM all_weather_roads_{}
                   """.format(valid_areas[x])
                test = True
        settlement_sql += ")"
        road_sql += ")"
        if test:
            db = PostgresDB()
            db.query(settlement_sql)
            db.query(road_sql)
            geojson_to_table("feature", geom_infile)
        else:
            return False
    else:
        return False

    if drop_tables:
        sql = ""
        for x in range(0, counter):
            sql += """
                drop table if exists settlements_{counter};
                drop table if exists all_weather_roads_{counter};
            """.format(counter=counter)
            if os.path.exists(ohsome_outfiles[0]+ "_{}".format(x)):
                os.remove(ohsome_outfiles[0]+ "_{}".format(x))
            if os.path.exists(ohsome_outfiles[1]+ "_{}".format(x)):
                os.remove(ohsome_outfiles[1]+ "_{}".format(x))
        db.query(sql)

    logger.info("uploaded ohsome data")


def buffer_and_union_polygons(drop_tables: bool):
    db = PostgresDB()
    sql = """
        drop table if exists combined_polys;
        create table combined_polys as (            
            SELECT (ST_Dump(geom)).geom as geom, ROW_NUMBER() over (order by (Select Null)) as fid
            FROM (SELECT ST_UNION(ST_Transform(public.ST_BUFFER(ST_TRANSFORM(geom, 900913), 2000), 954009)) AS geom
            FROM settlements) as unioned
        )

    """
    db.query(sql)
    logger.info("unioned overlapping polys")

    if drop_tables:
        sql = "drop table if exists settlements;"
        db.query(sql)


def population_per_city(drop_tables: bool):
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

    if drop_tables:
        sql = "drop table if exists combined_polys;"
        db.query(sql)


def remove_urban_from_raster(drop_tables: bool):
    db = PostgresDB()
    sql = """
        drop table if exists rural_pop;
        create table rural_pop as (
            with clip as (
            select ST_Difference(ST_Transform(a.geom, 954009), c.geom) as clipper
            from (select ST_UNION(geom) as geom from cities_with_pop where pop>=10000) as c, feature as a
            )
            select rid, ST_Clip(rast, clipper) as rast
                    from clip, pop.ghspop
                    where ST_Intersects(rast, clipper)
        )
    """

    db.query(sql)
    logger.info("got rural feature raster")

    if drop_tables:
        sql = "drop table if exists cities_with_pop;"
        db.query(sql)


def buffer_roads(drop_tables: bool):
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

    if drop_tables:
        sql = "drop table if exists all_weather_roads;"
        db.query(sql)


def get_all_reachable_pop(drop_tables: bool):
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

    if drop_tables:
        sql = "drop table if exists buffered_streets;"
        db.query(sql)


def get_share_rural_population_within_2km_of_all_weather_road(drop_tables: bool):
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

    if drop_tables:
        sql = """
            drop table if exists reachable_pop;
            drop table if exists rural_pop;
        """
        db.query(sql)

    return result[0]


def fill_result_into_countries(continent_outfile, result_file):
    with open(continent_outfile, "r") as file:
        countries = geojson.load(file)
    with open(result_file, "r") as file:
        result = json.load(file)
    for feature in countries["features"]:
        name = feature["properties"]["name"]
        if name not in result.keys():
            feature["properties"]["share_reachable_pop"] = -1
            feature["properties"]["rural_pop"] = -1
            feature["properties"]["reachable_pop"] = -1
        else:
            if result[name] is None:
                feature["properties"]["share_reachable_pop"] = -1
                feature["properties"]["rural_pop"] = -1
                feature["properties"]["reachable_pop"] = -1
            else:
                feature["properties"]["share_reachable_pop"] = result[name]["share_reachable_pop"]
                feature["properties"]["rural_pop"] = result[name]["rural_pop"]
                feature["properties"]["reachable_pop"] = result[name]["reachable_pop "]
    with open(continent_outfile, "w") as file:
        json.dump(countries, file)

fill_result_into_countries(os.path.join(DATA_PATH, "query_answers/african_countries.geojson"), os.path.join(DATA_PATH, "result.json"))
def settlement_workflow(continent_workflow: bool = False, drop_tables: bool = False):

    if continent_workflow:
        geom_infile=os.path.join(DATA_PATH, "area_of_interest/feature.geojson")

        countries = get_countries(continent_infile, continent_outfile)
        with open(result_outfile) as file:
            already_done_countries = json.load(file).keys()
        for feature in countries["features"]:
            country_name = feature["properties"]["name"]

            if country_name in already_done_countries:  # ohsome API is unstable atm
                logger.info("Already processed {}".format(country_name))
                continue
            feature = geojson.dumps({"type": "FeatureCollection",
                                     "features": [feature]})

            counter = get_ohsome_data(feature=feature, ohsome_outfiles=ohsome_outfiles)
            if counter is False:
                continue
            not_empty = upload_data(ohsome_outfiles, geom_infile, counter, drop_tables=drop_tables)
            if not_empty is False:
                continue
            try:
                buffer_and_union_polygons(drop_tables)
            except psycopg2.OperationalError:
                logger.info("server issue with {}".format(country_name))
                continue
            population_per_city(drop_tables)
            remove_urban_from_raster(drop_tables)
            buffer_roads(drop_tables)
            get_all_reachable_pop(drop_tables)
            rural_pop, reachable_pop = get_share_rural_population_within_2km_of_all_weather_road(drop_tables)
            if rural_pop is not None and reachable_pop is not None and rural_pop !=0:
                result = {
                    "share_reachable_pop": round((reachable_pop/rural_pop)*100,1),
                    "rural_pop": rural_pop,
                    "reachable_pop": reachable_pop
                }
            else:
                result = None
            to_json("result.json", key=country_name, value=result)

        fill_result_into_countries(continent_outfile, result_outfile)
    else:
        counter= 10
        counter = get_ohsome_data(geom_infile=geom_infile, ohsome_outfiles=ohsome_outfiles)
        upload_data(ohsome_outfiles=ohsome_outfiles, geom_infile=geom_infile, counter=counter, drop_tables=drop_tables)
        buffer_and_union_polygons(drop_tables)
        population_per_city(drop_tables)
        remove_urban_from_raster(drop_tables)
        buffer_roads(drop_tables)
        get_all_reachable_pop(drop_tables)
        result=get_share_rural_population_within_2km_of_all_weather_road(drop_tables)
        print(result)
#settlement_workflow(continent_workflow=True, drop_tables=True)

def get_table_as_geojson(tablename):
    result =get_bpolys_from_db(tablename)
    drop_temp_path = os.path.join(DATA_PATH, "query_answers/temp.geojson")
    with open(drop_temp_path, 'w') as outfile:
        geojson.dump(result, outfile)

#get_table_as_geojson("rural_pop_2")