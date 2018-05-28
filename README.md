# xcube Tile Server


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


### Client

Open [demo.html](http://localhost:8080/res/demo.html) in a browser. 

## Related

* https://openlayers.org/en/latest/examples/wmts-layer-from-capabilities.html