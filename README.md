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

To run the server on default port 9090:

    $ xcts -v -c demo.yml


### Client

Run a local web server to serve `./demo.html`. Current working directory must be `.`.
