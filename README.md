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

As an example, here is the [configuration of the demo server](https://github.com/bcdev/xcube-tileserver/blob/master/xcts/res/local/config.yml).

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

If the last command fails because `xcube-dev` environment already exists, then just update it

    $ conda env update

Once in a while

    $ cd xcube-tileserver
    $ git pull

Install

    $ source activate xcube-dev
    $ python setup.py develop
    $ pytest --cov=xcts

To run the server on default port 8080:

    $ xcts -v -c xcts/res/local/config.yml

Test it:

* WMTS:
  * [/xcts-wmts/1.0.0/WMTSCapabilities.xml](http://localhost:8080/xcts-wmts/1.0.0/WMTSCapabilities.xml)
  * [/xcts-wmts/1.0.0/tile/local/conc_chl/0/0/1.png](http://localhost:8080/xcts-wmts/1.0.0/tile/local/conc_chl/0/0/1.png)
  * [/xcts-wmts/1.0.0/tile/remote/conc_chl/0/0/1.png](http://localhost:8080/xcts-wmts/1.0.0/tile/remote/conc_chl/0/0/1.png)
* Tiles
  * [/xcts/tile/local/conc_chl/0/1/0.png](http://localhost:8080/xcts/tile/local/conc_chl/0/1/0.png)
  * [/xcts/tile/remote/conc_chl/0/1/0.png](http://localhost:8080/xcts/tile/remote/conc_chl/0/1/0.png)
* Tile grids
  * [/xcts/tilegrid/local/conc_chl/ol4.json](http://localhost:8080/xcts/tilegrid/local/conc_chl/ol4.json)
  * [/xcts/tilegrid/local/conc_chl/cesium.json](http://localhost:8080/xcts/tilegrid/local/conc_chl/cesium.json)
  * [/xcts/tilegrid/remote/conc_chl/ol4.json](http://localhost:8080/xcts/tilegrid/remote/conc_chl/ol4.json)
  * [/xcts/tilegrid/remote/conc_chl/cesium.json](http://localhost:8080/xcts/tilegrid/remote/conc_chl/cesium.json)
* Color bars service:
  * [/xcts/colorbars.json](http://localhost:8080/xcts/colorbars.json)
  * [/xcts/colorbars.html](http://localhost:8080/xcts/colorbars.html)


### Clients


#### OpenLayers

After starting the server, the [OpenLayers 4 Demo](http://localhost:8080/res/demo/index-ol4.html)
should run without further actions.

Here is how to use configure an OpenLayers tile layer from WMTS capabilities: 

* https://openlayers.org/en/latest/examples/wmts-layer-from-capabilities.html

#### Cesium

To run the [Cesium Demo](http://localhost:8080/res/demo/index-cesium.html) first
[download Cesium](https://cesiumjs.org/downloads/) and unpack the zip
into the `xcube-tileserver` source directory so that there exists an 
`./Cesium-<version>` sub-directory. You may have to adapt the Cesium version number 
in the [demo's HTML file](https://github.com/bcdev/xcube-tileserver/blob/master/xcts/res/demo/index-cesium.html).

## TODO

* Bug/Performance: /xcts-wmts/1.0.0/WMTSCapabilities.xml is veeerry slow,
  169466.91ms - investigate and e.g. cache.
* Bug/Performance: open datasets must be cached based on their paths, not the config identifier names.
  There may be different identifiers that have the same path!
* Performance: After some period check if datasets haven't been used for a long time - close them and remove from cache.
* Performance: Internally cache TileGrid instances, so we don't need to recompute them.
  TileGrid cache keys must be based on dataset path, array shape, and chunk shape.
* Need: Add a service that allows retrieving the actual cubes indexers and coordinates given a
  variable and dimension KVP.
  This is because we use `var.sel(method='nearest, **indexers)`, users cannot know the actual,
  effectively selected coordinates.

* Build on Travis & AppVeyor
* Configure Flake8
* Configure Coverage

