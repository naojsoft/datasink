This is the datasink package, part of the Gen2 Observation Control
System.

It provides the datasink program for moving observation data between
various nodes.  This package is necessary for a site to participate as
a data destination.

## Dependencies

* Requires `g2cam` package from naojsoft.

* Requires `pika` and `pyyaml` packages (installed by installer)

## Installation

It is recommended that you install a virtual (miniconda, virtualenv,
etc) environment to run the software in with related dependencies.

Activate this environment and then:

```bash
$ python setup.py install
```


