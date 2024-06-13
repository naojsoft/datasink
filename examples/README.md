# Datasink Tutorial

## About this example

In this directory we have an example of using the datasink machinery
to send jobs from a producer/publisher/source to some number of
consumer/subscriber/sink(s) all operating in the same realm (hub).

The example shows how to define your own job with an associated *action*
keyword.  Everything takes place on a single host (localhost), but it is
easily expandable to multiple hosts.  We will use a number of terminal
windows to run these command line examples.

## Installation

We will use "conda" to demonstrate how to install and run the software.
This makes it easy to install in a separate environment which you can
easily delete later.  We will need two environments.

First we will create an environment to run the server:

```bash
$ conda create -n rabbitmq -c conda-forge
$ conda activate rabbitmq
(rabbitmq) $ conda install rabbitmq-server
...
```

Next we will create an environment to run the sources, sinks, etc. from
these examples:

```bash
$ conda create -n datasink python=3.11
$ conda activate datasink
(datasink) $ git clone http://github.com/naojsoft/datasink
(datasink) $ cd datasink
(datasink) $ pip install .
...
```

## Running the example

In our example, everything is running on the same host (localhost), so
you will just need a few terminal windows to run the different entities.
All the terminals will be numbered in the command listings below.

### Setting up the realm

The *realm* is an area you are trying to connect job sources with sinks
(and workers on those sinks).  A realm must be network reachable by any
sources or sinks that wish to connect to it.  In this way you can also
think of a realm as a "hub" which connects these things.

Practically speaking, a realm is implemented by a RabbitMQ server running
on a node and a hub program (here "ds_hub.py") usually running on the same
node and configured with a realm configuration file that will create
RabbitMQ exchanges and queues for the data sinks, with properties defined
in the configuration file.

Let's start by starting up the RabbitMQ server in terminal (1):

```bash
$ conda activate rabbitmq
(rabbitmq) $ rabbitmq-server
...
```

From another terminal (2), check what queues are created (there should be none
at first):

```bash
$ conda activate rabbitmq
(rabbitmq) $ rabbitmqctl list_queues
Timeout: 60.0 seconds ...
Listing queues for vhost / ...
(rabbitmq) $
```

Then in terminal number (3), let's start up the realm hub, which defines
some queues for sinks (look at "hub.yml" to see the queue definitions):

```bash
$ conda activate datasink
(datasink) $ cd examples
(datasink) $ ./ds_hub.py -f hub.yml --dlx
```

Going back to terminal (2), listing queues should show four new ones:

```bash
(rabbitmq) $ rabbitmqctl list_queues
Timeout: 60.0 seconds ...
Listing queues for vhost / ...
name	messages
backlog	0
ins2	0
ins1	0
```

### Setting up the sinks (subscribers)

We will run two sinks, one in terminal (4) and one in terminal (5). You
can then experiment with having them service the same queue (with same or
different topic filters), or different queues.

In terminal (4) we will run a job sink pulling jobs off of queue "ins1"
if the topic of the job matches "foo":

```bash
$ conda activate datasink
(datasink) $ ./ds_sub.py -f sub.yml -n ins1 -t foo --loglevel=20 --stderr
...
```

In terminal (5)  we will run a job sink pulling jobs off of queue "ins2"
if the topic of the job matches "foo":

```bash
$ conda activate datasink
(datasink) $ ./ds_sub.py -f sub.yml -n ins2 -t foo --loglevel=20 --stderr
...
```

### Running the job source (publisher)

The job source example submits one job and terminates.  You can run it over
and over with the same job or different jobs.

In terminal (6) we will submit a job with topic "foo"; you should see it be
handled by both job sinks in terminals (4) and (5):

```bash
$ conda activate datasink
(datasink) $ /ds_pub.py -n BLUE -f pub.yml -t foo -j job.json --loglevel=20 --stderr
```

You can run this command over and over, or with different job files.

You can increase the thread parallelism (number of thread workers per job
sink), by increasing the value of "num_workers" in sub.yml

#### Variation 1: running job sinks on the same queue

You can also increase process parallelism by increasing the number of sinks
running on the same queue.

In the previous example we ran two sinks on two separate queues.  This sent
the same job to different locations.  In this example lets switch to running
the two sinks on the *same* queue.

To do this we need to disable the "ins2" queue so messages don't continue
to pile up there.

In terminal (6), disable the "ins2" queue using the "ds_queue.py" program:

```bash
(datasink) $ ./ds_queue.py -f hub.yml -n ins2 -a disable -t foo
...
```

In terminal (5), interrupt the sink with CTRL+C and restart it, changing 
the name of the sink from "ins2" to "ins1"

```bash
^C
2024-06-12 11:49:07,597 | I | worker.py:193 (serve) | detected keyboard interrupt!
2024-06-12 11:49:07,599 | I | worker.py:216 (serve) | Shutting down...
...

(datasink) $ ./ds_sub.py -f sub.yml -n ins1 -t foo --loglevel=20 --stderr
...
```

Now *both* sinks are reading jobs from the same queue.  If you run the job
source over and over you should see jobs being distributed alternately between
the two running sinks.

#### Variation 2: running job sinks on different queues, with different topics

In terminal (6), disable the "ins1" and "ins2" queues using the "ds_queue.py"
program:

```bash
(datasink) $ ./ds_queue.py -f hub.yml -n ins1 -a disable -t foo
(datasink) $ ./ds_queue.py -f hub.yml -n ins2 -a disable -t foo
...
```

In terminal (4), interrupt the sink with CTRL+C and restart it, changing 
the topic of the sink from "foo" to "foo.bar":

```bash
^C
2024-06-12 11:49:07,597 | I | worker.py:193 (serve) | detected keyboard interrupt!
2024-06-12 11:49:07,599 | I | worker.py:216 (serve) | Shutting down...
...

(datasink) $ ./ds_sub.py -f sub.yml -n ins1 -t foo.bar --loglevel=20 --stderr
...
```

In terminal (5), interrupt the sink with CTRL+C and restart it, changing 
the name of the sink back to "ins2" and the topic of the sink from "foo"
to "foo.baz":

```bash
^C
2024-06-12 11:49:07,597 | I | worker.py:193 (serve) | detected keyboard interrupt!
2024-06-12 11:49:07,599 | I | worker.py:216 (serve) | Shutting down...
...

(datasink) $ ./ds_sub.py -f sub.yml -n ins2 -t foo.baz --loglevel=20 --stderr
...
```

In terminal (6) submit jobs with topic "foo.bar" or "foo.baz"; jobs with
topic "foo.bar" should be handled in terminal (4) and jobs with "foo.baz"
in terminal (5).

#### Variation 3: with wildcard topics

In terminal (6), disable the "ins1" queue using the "ds_queue.py"
program, and then re-enable it with the wildcard topic as shown:

```bash
(datasink) $ ./ds_queue.py -f hub.yml -n ins1 -a disable -t foo.bar
(datasink) $ ./ds_queue.py -f hub.yml -n ins1 -a enable -t 'foo.#'
...
```

No need to restart the sinks, in terminal (6) send a few jobs with
topic "foo.bar" and a few with topic "foo.baz"; you should see that *all*
jobs are handled by the sink in terminal (4) and the ones with "foo.baz"
are also handled in terminal (5).



