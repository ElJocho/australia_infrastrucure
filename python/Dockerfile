FROM python:3.7

# Set C.UTF-8 locale as default (Needed by the Click library)
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

RUN apt-get update
RUN apt-get install -y \
    postgresql \
    postgresql-contrib \

WORKDIR /root


#trying to get ogr to work

RUN apt-get install -y libgdal-dev g++ --no-install-recommends && \
    apt-get clean -y
RUN apt-get update &&\
    apt-get install -y binutils libproj-dev gdal-bin

# Update C env vars so compiler can find gdal
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal


# Create directories for config, data and logs
RUN mkdir --parents .local/share/infraustralia

# Copy infraustralia workers repo from local repo
COPY infraustralia/ infraustralia/
COPY config/ config/
COPY requirements.txt .
COPY setup.py .

# Update with dependencies (requirements.txt)
RUN pip3 install --upgrade setuptools
RUN pip3 install .