[![Build Status](https://travis-ci.com/dcs4cop/xcube-server.svg?branch=master)](https://travis-ci.com/dcs4cop/xcube-server)
[![codecov](https://codecov.io/gh/dcs4cop/xcube-server/branch/master/graph/badge.svg)](https://codecov.io/gh/dcs4cop/xcube-server)


# xcube Server

## Objective

`xcube-server` is a light-weight web server that provides various services based on 
xcube datasets:

* Catalogue services to query for datasets and their variables and dimensions, and feature collections. 
* Tile map service, with some OGC WMTS 1.0 compatibility (REST and KVP APIs)
* Dataset services to extract subsets like time-series and profiles for e.g. JS clients 

Find its API description [here](https://app.swaggerhub.com/apis-docs/bcdev/xcube-server). 

xcube datasets are any datasets that 

* that comply to [Unidata's CDM](https://www.unidata.ucar.edu/software/thredds/v4.3/netcdf-java/CDM/) and to the [CF Conventions](http://cfconventions.org/); 
* that can be opened with the [xarray](https://xarray.pydata.org/en/stable/) Python library;
* that have variables that have at least the dimensions and shape (`time`, `lat`, `lon`), in exactly this order; 
* that have 1D-coordinate variables corresponding to the dimensions;
* that have their spatial grid defined in the WGS84 (`EPSG:4326`) coordinate reference system.

`xcube-server` supports local NetCDF files or local or remote [Zarr](https://zarr.readthedocs.io/en/stable/) directories.
Remote Zarr directories must be stored in publicly accessible, AWS S3 compatible 
object storage (OBS).

As an example, here is the [configuration of the demo server](https://github.com/bcdev/xcube-server/blob/master/xcube_server/res/demo/config.yml).

## OGC WMTS compatibility

`xcube-server` implements the RESTful and KVP architectural styles
of the [OGC WMTS 1.0.0 specification](http://www.opengeospatial.org/standards/wmts).

The following operations are supported:

* **GetCapabilities**: `/xcube/wmts/1.0.0/WMTSCapabilities.xml`
* **GetTile**: `/xcube/wmts/1.0.0/tile/{DatasetName}/{VarName}/{TileMatrix}/{TileCol}/{TileRow}.png`
* **GetFeatureInfo**: *in progress*

## Explore API of existing xcube-servers

To explore the API of existing xcube-servers go to the [SwaggerHub of bcdev](https://app.swaggerhub.com/apis/bcdev/xcube-server/0.1.0.dev6).
The SwaggerHub allows to choose the xcube-server project and therefore the datasets which are used for the exploration. 

## Run the demo

### Server

Initially, checkout code and create conda environment `xcube-server-dev`:

    $ git clone https://github.com/bcdev/xcube-server.git
    $ cd xcube-server
    $ conda env create

If the last command fails because `xcube-server-dev` environment already exists, then just update it

    $ conda env update

Once in a while

    $ cd xcube-server
    $ git pull

Install

    $ source activate xcube-server-dev
    $ python setup.py develop
    $ pytest --cov=xcube_server

To run the server on default port 8080:

    $ xcube-server -v -c xcube_server/res/demo/config.yml

or shorter

    $ xcs -v -c xcube_server/res/demo/config.yml

Test it:

* Datasets (Data Cubes):
    * [Get datasets](http://localhost:8080/xcube/api/0.1.0.dev6/datasets)
    * [Get dataset details](http://localhost:8080/xcube/api/0.1.0.dev6/datasets/local)
    * [Get dataset coordinates](http://localhost:8080/xcube/api/0.1.0.dev6/datasets/local/coords/time)
* Color bars:
    * [Get color bars](http://localhost:8080/xcube/api/0.1.0.dev6/colorbars)
    * [Get color bars (HTML)](http://localhost:8080/xcube/api/0.1.0.dev6/colorbars.html)
* WMTS:
    * [Get WMTS KVP Capabilities (XML)](http://localhost:8080/xcube/api/0.1.0.dev6/wmts/kvp?Service=WMTS&Request=GetCapabilities)
    * [Get WMTS KVP local tile (PNG)](http://localhost:8080/xcube/api/0.1.0.dev6/wmts/kvp?Service=WMTS&Request=GetTile&Version=1.0.0&Layer=local.conc_chl&TileMatrix=0&TileRow=0&TileCol=0&Format=image/png)
    * [Get WMTS KVP remote tile (PNG)](http://localhost:8080/xcube/api/0.1.0.dev6/wmts/kvp?Service=WMTS&Request=GetTile&Version=1.0.0&Layer=remote.conc_chl&TileMatrix=0&TileRow=0&TileCol=0&Format=image/png)
    * [Get WMTS REST Capabilities (XML)](http://localhost:8080/xcube/api/0.1.0.dev6/wmts/1.0.0/WMTSCapabilities.xml)
    * [Get WMTS REST local tile (PNG)](http://localhost:8080/xcube/api/0.1.0.dev6/wmts/1.0.0/tile/local/conc_chl/0/0/1.png)
    * [Get WMTS REST remote tile (PNG)](http://localhost:8080/xcube/api/0.1.0.dev6/wmts/1.0.0/tile/remote/conc_chl/0/0/1.png)
* Tiles
    * [Get tile (PNG)](http://localhost:8080/xcube/api/0.1.0.dev6/datasets/local/vars/conc_chl/tiles/0/1/0.png)
    * [Get tile grid for OpenLayers 4.x](http://localhost:8080/xcube/api/0.1.0.dev6/datasets/local/vars/conc_chl/tilegrid?tiles=ol4)
    * [Get tile grid for Cesium 1.x](http://localhost:8080/xcube/api/0.1.0.dev6/datasets/local/vars/conc_chl/tilegrid?tiles=cesium)
    * [Get legend for layer (PNG)](http://localhost:8080/xcube/api/0.1.0.dev6/datasets/local/vars/conc_chl/legend.png)
* Time series service (preliminary & unstable, will likely change soon)
    * [Get time stamps per dataset](http://localhost:8080/xcube/api/0.1.0.dev6/ts)
    * [Get time series for single point](http://localhost:8080/xcube/api/0.1.0.dev6/ts/local/conc_chl/point?lat=51.4&lon=2.1&startDate=2017-01-15&endDate=2017-01-29)
* Places service (preliminary & unstable, will likely change soon)
    * [Get all features](http://localhost:8080/xcube/api/0.1.0.dev6/places/all)
    * [Get all features of collection "inside-cube"](http://localhost:8080/xcube/api/0.1.0.dev6/features/inside-cube)
    * [Get all features for dataset "local"](http://localhost:8080/xcube/api/0.1.0.dev6/places/all/local)
    * [Get all features of collection "inside-cube" for dataset "local"](http://localhost:8080/xcube/api/0.1.0.dev6/places/inside-cube/local)


### Clients

There are example HTML pages for some tile server clients. They need to be run in 
a web server. If you don't have one, you can use the Node `httpserver`:

    $ npm install -g httpserver
    
After starting both the xcube-server and web server, e.g. on port 9090

    $ httpserver -d -p 9090

you can run the client demos by following their links given below.
    
    
### Docker

To start a demo using docker use the following commands

    $ docker build -t [your name] .
    $ docker run -p [host port]:8000 [your name] 

**TODO:** 

The idea is to have the container automatically build on quay.io 
and then used in a xcube-services ```docker-compose.yml``` configuration.


#### OpenLayers

[OpenLayers 4 Demo](http://localhost:9090/xcube_server/res/demo/index-ol4.html)
[OpenLayers 4 Demo with WMTS](http://localhost:9090/xcube_server/res/demo/index-ol4-wmts.html)

#### Cesium

To run the [Cesium Demo](http://localhost:9090/xcube_server/res/demo/index-cesium.html) first
[download Cesium](https://cesiumjs.org/downloads/) and unpack the zip
into the `xcube-server` source directory so that there exists an 
`./Cesium-<version>` sub-directory. You may have to adapt the Cesium version number 
in the [demo's HTML file](https://github.com/bcdev/xcube-server/blob/master/xcube_server/res/demo/index-cesium.html).

### TODO:

* Bug/Performance: ServiceContext.dataset_cache uses dataset names as ID, but actually, caching of *open* datasets 
  should be based on *same* dataset sources, namely given the local file path or the remote URL
* Bug/Performance: /xcube/wmts/1.0.0/WMTSCapabilities.xml slow for ZARR data cubes in OTC's object storage.
  15 seconds for first call - investigate and e.g. cache.
* Bug/Performance: open datasets must be cached based on their paths, not the config identifier names.
  There may be different identifiers that have the same path!
* Performance: use multi-resolution levels embedded in future cube datasets
* Performance: consider external chunking when computing TileGrid
* Performance: After some period check if datasets haven't been used for a long time - close them and remove from cache.
* Performance: Internally cache TileGrid instances, so we don't need to recompute them.
  TileGrid cache keys must be based on dataset path, array shape, and chunk shape.
* Feature: WMTS GetFeatureInfo
* Feature: Let users specify TileGrid in configuration
* Feature: Add a service that allows retrieving the actual cubes indexers and coordinates given a
  variable and dimension KVP.
  This is because we use `var.sel(method='nearest, **indexers)`, users cannot know the actual,
  effectively selected coordinates.
* Feature: collect Path entry of any Dataset and observe if the file are modified, if so remove dataset from cache.

