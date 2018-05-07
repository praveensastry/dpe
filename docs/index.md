
[![npm badge](https://img.shields.io/npm/v/dpe.svg)](https://www.npmjs.com/package/dpe)


High performance distributed data processing and machine learning.

dpe provides a high-level API in Javascript and an optimized
parallel execution engine on top of NodeJS.

## Features
* Pure javascript implementation of a Spark like engine
* Multiple data sources: filesystems, databases, cloud (S3, azure)
* Multiple data formats: CSV, JSON, Columnar (Parquet)...
* 50 high level operators to build parallel apps
* Machine learning: scalable classification, regression, clusterization
* Run interactively in a nodeJS REPL shell
* Docker [ready](https://github.com/dpe-me/dpe/blob/master/docker/), simple local mode or full distributed mode
* Very fast, see [benchmark](https://github.com/dpe-me/dpe/blob/master/benchmark/)

## Quickstart
```sh
npm install dpe
```

Word count example: 

```javascript
var sc = require('@praveensastry/dpe').context();

sc.textFile('/my/path/*.txt')
  .flatMap(line => line.split(' '))
  .map(word => [word, 1])
  .reduceByKey((a, b) => a + b, 0)
  .count(function (err, result) {
    console.log(result);
    sc.end();
  });
```

### Local mode
In local mode, worker processes are automatically forked and
communicate with app through child process IPC channel. This is
the simplest way to operate, and it allows to use all machine
available cores.

To run in local mode, just execute your app script:
```sh
node my_app.js
```

or with debug traces:
```sh
dpe_DEBUG=2 node my_app.js
```

### Distributed mode
In distributed mode, a cluster server process and worker processes
must be started prior to start app. Processes communicate with each
other via raw TCP or via websockets.

To run in distributed cluster mode, first start a cluster server
on `server_host`:
```sh
./bin/server.js
```

On each worker host, start a worker controller process which connects
to server:
```sh
./bin/worker.js -H server_host
```

Then run your app, setting the cluster server host in environment:
```sh
dpe_HOST=server_host node my_app.js
```

The same with debug traces:
```sh
dpe_HOST=server_host dpe_DEBUG=2 node my_app.js
```


## License

[Apache-2.0](https://github.com/dpe-me/dpe/blob/master/LICENSE)

## Credits

<div>Logo Icon made by <a href="https://www.flaticon.com/authors/smashicons" title="Smashicons">Smashicons</a> from <a href="https://www.flaticon.com/" title="Flaticon">www.flaticon.com</a> is licensed by <a href="http://creativecommons.org/licenses/by/3.0/" title="Creative Commons BY 3.0" target="_blank">CC 3.0 BY</a></div>
