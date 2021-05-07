# openeo-odc


Map an openEO process graph to a job based on OpenDataCube and Xarray functions. 


## Description

The `openeo-odc` package converts a job from the openEO syntax to an executable Python file, where each openEO process is mapped to its related Xarray function defined in the [openeo-processes-python](https://github.com/Open-EO/openeo-processes-python) repository. Two processes (`load_collection` and `load_results`) map to functions depending on [Open Data Cube](https://github.com/opendatacube).

Package dependencies:
1. [openeo-pg-parser-python](https://github.com/Open-EO/openeo-pg-parser-python.git)

Note: `opendatacube`, `xarray` and `openeo-processes-python` are not dependencies because this package simply creates a python file that can be executed in the correct environment where these dependencies are resolved.

## Installation

1.  At the moment, this package is only installable from source.
    So start with cloning the repository:

        git clone https://github.com/Open-EO/openeo-odc.git
        cd openeo-odc

2.  It is recommended to install this package in a virtual environment,
    e.g. by using [`venv` (from the Python standard library)](https://docs.python.org/3/library/venv.html),
    `virtualenv`, a conda environment, ... For example, to create a new virtual
    environment using `venv` (in a folder called `.venv`) and to activate it:

        python3 -m venv .venv
        source .venv/bin/activate

    (You might want to use a different bootstrap python executable
    instead of `python3` in this example.)

3. Install the package in the virtual environment using one of the following ways, as you prefer:

    - traditional way: `python setup.py install`
    - with pip: `pip install .`
    - if you plan to do development on the `openeo-pg-parser-python` package itself,
        install it in "development" mode with `python setup.py develop` or `pip install -e .`

    (Note that in this step we are using `python` and `pip` from the virtual environment.)


## Run tests

```
source .venv/bin/activate
python setup.py test
```


## Note

This project has been set up using PyScaffold 3.1. For details and usage
information on PyScaffold see https://pyscaffold.org/.
