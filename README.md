# xcube Tile Server (xcts)

## Objective

`xcts` is a tile server used to publish imagery of xcube datasets. 

xcube datasets are any datasets that 

* that comply to [Unidata's CDM](https://www.unidata.ucar.edu/software/thredds/v4.3/netcdf-java/CDM/) and to the [CF Conventions](http://cfconventions.org/); 
* that can be opened with the [xarray](https://xarray.pydata.org/en/stable/) Python library;
* that have variables that have at least the dimensions and shape (`time`, `lat`, `lon`), in exactly this order; 
* that have 1D-coordinate variables corresponding to the dimensions;
* that have their spatial grid defined in the WGS84 (`EPSG:4326`) coordinate reference system.

`xcts` supports local NetCDF files or local or remote [Zarr](https://zarr.readthedocs.io/en/stable/) directories.
Remote Zarr directories must be stored in publicly accessible, AWS S3 compatible 
object storage (OBS).

As an example, here is the configuration of the [demo server](https://github.com/bcdev/xcube-wmts/blob/master/xcts/res/config.yml).

## OGC WMTS compatibility

`xcts` currently only conforms to the REST API of version 1.0
of the [OGC WMTS specification](http://www.opengeospatial.org/standards/wmts). 

The following operations are supported:

* **GetCapabilities**: `/xcts-wmts/1.0.0/WMTSCapabilities.xml`
* **GetTile**: `/xcts-wmts/1.0.0/tile/{DatasetName}/{VarName}/{TileMatrix}/{TileCol}/{TileRow}.png`
* **GetFeatureInfo**: *in progress*
 

## Run the demo

### Server

Initially

    $ git clone https://github.com/bcdev/xcube-tileserver.git
    $ cd xcube-tileserver
    $ conda env create

Once in a while

    $ cd xcube-tileserver
    $ git pull

Install

    $ source activate xcts-dev
    $ python setup.py develop
    $ pytest -v --cov=xcts test

To run the server on default port 8080:

    $ xcts -v -c xcts/res/demo.yml


### Clients

After starting the server, try the following clients:

* [OpenLayers 4 Demo](http://localhost:8080/res/demo/ol4.html) 
* [Cesium Demo](http://localhost:8080/res/demo/cesium.html)

Here is how to use configure an OpenLayers tile layer from WMTS capabilities: 

* https://openlayers.org/en/latest/examples/wmts-layer-from-capabilities.html
