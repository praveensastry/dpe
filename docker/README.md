# dpe Docker

This directory contains a sample Dockerfile for dpe container, based on [Alpine](https://hub.docker.com/_/alpine/) container. 

It also contains a sample Compose file to deploy a minimal stack on a single host or a docker swarm.

This docker configuration is not meant for production, ok for evaluation or experimentation.

## Installing

As a prerequisite, [Docker](https://docker.com) must be installed, in version v1.12.0 or higher.

To download this docker image from the public docker hub:

	$ docker pull dpe/dpe

To re-build this image from the dockerfile:

	$ docker build -t dpe/dpe .

## Deploying on a single host

This can be done simply with `docker-compose` and the provided `docker-compose.yml` file:

	$ docker-compose up

## Deploying on a cluster

The provided image and compose files are compatible to run distributed dpe using the docker engine in [swarm mode](https://docs.docker.com/engine/swarm/).

First create a cluster of docker machines in a swarm (see docker [documentation](https://docs.docker.com/engine/swarm/swarm-tutorial/create-swarm/))

Once a docker swarm is ready, one can deploy a dpe stack using the `stack` command and the same `docker-compose.yml` file:

	$ docker stack deploy -c docker-compose.yml dpe

Then you can adjust the size of the dpe cluster by setting the number of worker controllers:

	$ docker service scale dpe_dpe-worker=3

There should be one instance of dpe-worker per host. During jobs, each worker controller will spawn as many worker processes as CPUs on each host.

## Running programs

To execute dpe programs onto the previously deployed dpe stack, the `dpe_HOST` environment variable must point to the cluster public address, i.e the one given by `docker info` on the docker host (or the swarm master): 

	$ docker info | grep 'Node Address'
	  Node Address: 192.168.99.101

For example, to run a sample program from the examples directory:

	$ dpe_DEBUG=2 dpe_HOST=192.168.99.101 ../examples/parallelize.js
	[master 0.050s] workers: 3
	[master 0.054s] start result stage, partitions: 3
	[master 0.067s] part 0 from worker-w17 (1/3)
	[master 0.075s] part 1 from worker-w18 (2/3)
	[master 0.080s] part 2 from worker-w19 (3/3)
	[ 1, 2, 3, 4, 5 ]
