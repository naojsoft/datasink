This is the datasink package, which can be used to reliably send jobs
in a distributed environment based on the RabbitMQ open source queueing
system.

It provides the datasink program for moving observation data between
various nodes.  This package is necessary for a site to participate as
a data destination.

## Dependencies

* Requires `pika` and `pyyaml` packages (installed by installer)
* Requires a RabbitMQ server in the locations you want to run a hub

## Installation

It is recommended that you install a virtual (miniconda, virtualenv,
etc) environment to run the software in with related dependencies.

Activate this environment and then:

```bash
$ pip install .
```


