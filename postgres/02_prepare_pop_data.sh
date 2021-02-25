#!/usr/bin/env bash

echo "donwload ghsl pop data"
wget http://cidportal.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_POP_MT_GLOBE_R2019A/GHS_POP_E2015_GLOBE_R2019A_54009_250/V1-0/GHS_POP_E2015_GLOBE_R2019A_54009_250_V1_0.zip --output-document=/data/ghspop.zip

echo "unzip ghsl pop data"
unzip /data/ghspop.zip -d /data/

echo "upload ghsl pop data"
raster2pgsql -Y -c -I -M -t 100x100 -s 954009 /data/GHS_POP_E2015_GLOBE_R2019A_54009_250_V1_0.tif pop.ghspop | psql -U $POSTGRES_USER

psql -U $POSTGRES_USER -c "SELECT AddRasterConstraints('ghspop'::name, 'rast'::name);"
echo "finished"
