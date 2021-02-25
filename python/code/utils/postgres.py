import os
import json
import psycopg2
from psycopg2 import sql

class PostgresDB(object):
    """Helper class for Postgres interactions"""

    _db_connection = None
    _db_cur = None

    def __init__(self):
        self._db_connection = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", default="localhost"),
            port=os.getenv("POSTGRES_PORT", default=5437),
            database=os.getenv("POSTGRES_DB", default="infraustralia"),
            user=os.getenv("POSTGRES_USER", default="infraustralia"),
            password=os.environ["POSTGRES_PASSWORD"],
        )

    def query(self, query, data=None):
        self._db_cur = self._db_connection.cursor()
        self._db_cur.execute(query, data)
        self._db_connection.commit()
        self._db_cur.close()

    def copy_from(self, f, table, columns=None):
        self._db_cur = self._db_connection.cursor()
        self._db_cur.copy_from(f, table, columns=columns)
        self._db_connection.commit()
        self._db_cur.close()

    def copy_expert(self, sql, file):
        self._db_cur = self._db_connection.cursor()
        self._db_cur.copy_expert(sql, file)
        self._db_connection.commit()
        self._db_cur.close()

    def retr_query(self, query, data=None):
        self._db_cur = self._db_connection.cursor()
        self._db_cur.execute(query, data)
        content = self._db_cur.fetchall()
        self._db_connection.commit()
        self._db_cur.close()
        return content

    def __del__(self):
        self._db_connection.close()


def create_dataset_table(dataset: str):
    """Creates dataset table with columns fid and geom"""
    db = PostgresDB()
    exe = sql.SQL(
        """DROP TABLE IF EXISTS {};
        CREATE TABLE {} (
            fid integer NOT Null,
            geom geometry,
            PRIMARY KEY(fid)
        );"""
    ).format(*[sql.Identifier(dataset)] * 2)
    db.query(exe)


def geojson_to_table(dataset: str, infile: str, fid_key="@osmId"):
    """creates a table and loads the content of a geojson file to it"""

    create_dataset_table(dataset)

    with open(infile) as inf:
        data = json.load(inf)

    db = PostgresDB()
    for feature in data["features"]:
        polygon = json.dumps(feature["geometry"])
        fid = int(feature["properties"][fid_key][9:])
        exe = sql.SQL(
            """INSERT INTO {table} (fid, geom)
                          VALUES (%(fid)s , st_setsrid(public.ST_GeomFromGeoJSON(%(polygon)s), 4236))
                          ON CONFLICT (fid) DO UPDATE
                          SET geom = excluded.geom;;"""
        ).format(table=sql.Identifier(dataset))
        db.query(exe, {"fid": fid, "polygon": polygon})



def get_bpolys_from_db(dataset: str):
    """Get geometry and properties from geo database as a geojson feature collection."""

    db = PostgresDB()

    # TODO: adjust this for other input tables
    query = sql.SQL(
        """
        SELECT json_build_object(
            'type', 'FeatureCollection',
            'crs',  json_build_object(
                'type',      'name',
                'properties', json_build_object(
                    'name', 'EPSG:4326'
                )
            ),
            'features', json_agg(
                json_build_object(
                    'type',       'Feature',
                    'id',         fid::varchar(255),
                    'geometry',   public.ST_AsGeoJSON(ST_Transform(geom, 4326))::json,
                    'properties', json_build_object(
                        -- list of fields
                        'fid', fid::varchar(255)
                    )
                )
            )
        )
        FROM {}
    """
    ).format(sql.Identifier(dataset))
    query_results = db.retr_query(query=query)
    bpolys = query_results[0][0]
    return bpolys
