// Copyright 2016 Luca-SAS, licensed under the Apache License 2.0

'use strict';

const fs = require('fs');
const stream = require('stream');
const inherits = require('util').inherits;
const zlib = require('zlib');

const thenify = require('thenify').withCallback;
const uuid = require('uuid');
const merge2 = require('merge2');
const glob = require('glob');
const micromatch = require('micromatch');
const seedrandom = require('seedrandom');
const S3 = require('aws-sdk/clients/s3');
const azure = require('azure-storage');

// Disable File split for now
//var splitLocalFile = require('./readsplit.js').splitLocalFile;
//var splitHDFSFile = require('./readsplit.js').splitHDFSFile;

const Lines = require('./lines');
const parquet = require('./stub-parquet.js');

/* global log */
/* global mkdirp */
/* global url */

function Dataset(sc, dependencies) {
  this.id = sc.datasetIdCounter++;
  this.dependencies = dependencies || [];
  this.persistent = false;
  this.sc = sc;
}

Dataset.prototype.persist = function () {this.persistent = true; return this;};

Dataset.prototype.map = function (mapper, args) {return new _Map(this, mapper, args);};

Dataset.prototype.flatMap = function (mapper, args) {return new FlatMap(this, mapper, args);};

Dataset.prototype.mapValues = function (mapper, args) {return new MapValues(this, mapper, args);};

Dataset.prototype.flatMapValues = function (mapper, args) {return new FlatMapValues(this, mapper, args);};

Dataset.prototype.filter = function (filter, args) {return new Filter(this, filter, args);};

Dataset.prototype.sample = function (withReplacement, frac, seed) {return new Sample(this, withReplacement, frac, seed || 1);};

Dataset.prototype.union = function (other) {return (other.id === this.id) ? this : new Union(this.sc, [this, other]);};

Dataset.prototype.aggregateByKey = function (reducer, combiner, init, args) {
  if (arguments.length < 3) throw new Error('Missing argument for function aggregateByKey().');
  return new AggregateByKey(this.sc, [this], reducer, combiner, init, args);
};

Dataset.prototype.reduceByKey = function (reducer, init, args) {
  if (arguments.length < 2) throw new Error('Missing argument for function reduceByKey().');
  return new AggregateByKey(this.sc, [this], reducer, reducer, init, args);
};

Dataset.prototype.groupByKey = function () {
  const reducer = (a, b) => {a.push(b); return a;};
  const combiner = (a, b) => a.concat(b);
  return new AggregateByKey(this.sc, [this], reducer, combiner, [], {});
};

Dataset.prototype.coGroup = function (other) {
  const reducer = (a, b) => {a.push(b); return a;};
  const combiner = (a, b) => {
    for (let i = 0; i < b.length; i++) a[i] = a[i].concat(b[i]);
    return a;
  };
  return new AggregateByKey(this.sc, [this, other], reducer, combiner, [], {});
};

Dataset.prototype.cartesian = function (other) {return new Cartesian(this.sc, [this, other]);};

Dataset.prototype.sortBy = function (sorter, ascending, numPartitions) {
  return new SortBy(this.sc, this, sorter, ascending, numPartitions);
};

Dataset.prototype.partitionBy = function (partitioner) {
  return new PartitionBy(this.sc, this, partitioner);
};

Dataset.prototype.sortByKey = function (ascending, numPartitions) {
  return new SortBy(this.sc, this, function (data) {return data[0];}, ascending, numPartitions);
};

Dataset.prototype.join = function (other) {
  return this.coGroup(other).flatMapValues(v => {
    const res = [];

    for (let i in v[0])
      for (let j in v[1])
        res.push([v[0][i], v[1][j]]);
    return res;
  });
};

Dataset.prototype.leftOuterJoin = function (other) {
  return this.coGroup(other).flatMapValues(v => {
    const res = [];

    if (v[1].length === 0) {
      for (let i in v[0]) res.push([v[0][i], null]);
    } else {
      for (let i in v[0])
        for (let j in v[1]) res.push([v[0][i], v[1][j]]);
    }
    return res;
  });
};

Dataset.prototype.rightOuterJoin = function (other) {
  return this.coGroup(other).flatMapValues(v => {
    const res = [];

    if (v[0].length === 0) {
      for (let i in v[1]) res.push([null, v[1][i]]);
    } else {
      for (let i in v[0])
        for (let j in v[1]) res.push([v[0][i], v[1][j]]);
    }
    return res;
  });
};

Dataset.prototype.distinct = function () {
  return this.map(e => [e, null])
    .reduceByKey(a => a, null)
    .map(a => a[0]);
};

Dataset.prototype.intersection = function (other) {
  const mapper = e => [e, 0];
  const reducer = a => a + 1;
  const a = this.map(mapper).reduceByKey(reducer, 0);
  const b = other.map(mapper).reduceByKey(reducer, 0);
  return a.coGroup(b).flatMap(a => {
    return (a[1][0].length && a[1][1].length) ? [a[0]] : [];
  });
};

Dataset.prototype.subtract = function (other) {
  const mapper = e => [e, 0];
  const reducer = a => a + 1;
  const a = this.map(mapper).reduceByKey(reducer, 0);
  const b = other.map(mapper).reduceByKey(reducer, 0);
  return a.coGroup(b).flatMap(a => {
    const res = [];
    if (a[1][0].length && (a[1][1].length === 0))
      for (let i = 0; i < a[1][0][0]; i++) res.push(a[0]);
    return res;
  });
};

Dataset.prototype.keys = function () {return this.map(a => a[0]);};

Dataset.prototype.values = function () {return this.map(a => a[1]);};

Dataset.prototype.lookup = thenify(function (key, done) {
  return this.filter((kv, args) => kv[0] === args.key, {key})
    .map(kv => kv[1])
    .collect(done);
});

Dataset.prototype.countByValue = thenify(function (done) {
  return this.map(e => [e, 1])
    .reduceByKey((a, b) => a + b, 0)
    .collect(done);
});

Dataset.prototype.countByKey = thenify(function (done) {
  return this.mapValues(function () {return 1;})
    .reduceByKey((a, b) => a + b, 0)
    .collect(done);
});

Dataset.prototype.collect = thenify(function (done) {
  return this.aggregate((a, b) => {a.push(b); return a;}, (a, b) => a.concat(b), [], done);
});

// The stream action allows the master to return a dataset as a stream
// Each worker spills its partitions to disk
// then master pipes each remote partition into output stream
Dataset.prototype.stream = function (options = {}) {
  const self = this;
  const outStream = merge2();
  const opt = {
    gzip: options.gzip,
    _preIterate: function (opt, wc, p) {
      const suffix = opt.gzip ? '.gz' : '';
      wc.exportFile = wc.basedir + 'export/' + p + suffix;
      try {fs.unlinkSync(wc.exportFile);} catch(err) {null;}
    },
    _postIterate: function (acc, opt, wc, p, done) {
      if (opt.gzip) {
        fs.appendFileSync(wc.exportFile, zlib.gzipSync(acc, {
          chunckSize: 65536,
          level: zlib.Z_BEST_SPEED
        }));
      } else {
        fs.appendFileSync(wc.exportFile, acc);
      }
      done(wc.exportFile);
    }
  };
  const pstreams = [];

  const reducer = (acc, val, opt, wc) => {
    acc += JSON.stringify(val) + '\n';
    if (acc.length >= 65536) {
      if (opt.gzip) {
        fs.appendFileSync(wc.exportFile, zlib.gzipSync(acc, {
          chunckSize: 65536,
          level: zlib.Z_BEST_SPEED
        }));
      } else {
        fs.appendFileSync(wc.exportFile, acc);
      }
      acc = '';
    }
    return acc;
  };

  const combiner = (acc1, acc2) => {
    const p = acc2.path.match(/.+\/([0-9]+)/)[1];
    pstreams[p] = self.sc.getReadStreamSync(acc2);
  };

  this.aggregate(reducer, combiner, '', opt, () => {
    for (let i = 0; i < pstreams.length; i++)
      outStream.add(pstreams[i]);
  });

  if (options.end) outStream.once('end', self.sc.end);
  return outStream;
};

// In save action, each worker exports its dataset partitions to
// a destination: a directory on the master, a remote S3, a database, etc.
// The format is JSON, one per dataset entry (dataset = stream of JSON)
//
// Step 1: partition is spilled to disk (during pipelining)
// Step 2: partition file is streamed from disk to destination (at end of pipeline)
// This is necessary because all pipeline functions are synchronous
// and to avoid back pressure during streaming out.
//
Dataset.prototype.save = thenify(function (path, options = {}, done) {
  if (arguments.length < 3) done = options;
  path = path.replace(/\/+$/, '');  // Trim trailing slashes (confusing for S3)
  const opt = {
    gzip: options.gzip,
    parquet: options.parquet,
    stream: options.stream,
    csv: options.csv,
    path: path,
    _preIterate: function (opt, wc, p) {
      let suffix = opt.gzip ? '.gz' : opt.parquet ? '.parquet' : '';

      if (opt.csv) suffix = '.csv' + suffix;

      wc.exportFile = wc.basedir + 'export/' + p + suffix;
      log('opt;', opt, 'suffix:', suffix, 'wc.exportFile:', wc.exportFile);

      try {fs.unlinkSync(wc.exportFile);} catch(err) {null;}

      if (opt.parquet) {
        wc.parquetFile = new parquet.ParquetWriter(wc.exportFile, opt.parquet.schema, opt.parquet.compression);
      }
      if (!opt.stream) return;

      const u = url.parse(opt.path);

      switch (u.protocol) {
      case 's3:': {
        const s3 = new S3({httpOptions: {timeout: 3600000}, signatureVersion: 'v4'});
        if (opt.gzip) {
          wc.outputStream = zlib.createGzip({chunkSize: 65536, level: zlib.Z_BEST_SPEED});
        } else {
          wc.outputStream = new stream.PassThrough();
        }
        wc.uploadPromise = s3.upload({
          Bucket: u.host,
          Key: u.path.slice(1) + '/' + p + suffix,
          Body: wc.outputStream
        }, err => {
          if (err) log('S3 upload error', err);
          done();
        }).promise();
        break;
      }
      case 'wasb:': {
        const retry = new azure.ExponentialRetryPolicyFilter();
        const az = azure.createBlobService().withFilter(retry);
        wc.outputSystemStream = az.createWriteStreamToBlockBlob(u.auth, u.path.slice(1) + '/' + p + suffix);

        if (opt.gzip) {
          wc.outputStream = zlib.createGzip({chunkSize: 65536, level: zlib.Z_BEST_SPEED});
          wc.outputStream.pipe(wc.outputSystemStream);
        } else
          wc.outputStream = wc.outputSystemStream;
        log('save stream:', u.auth, u.path.slice(1) + '/' + p + suffix);

        break;
      }
      case 'file:':
      case null: {
        log('save_preiterate, stream saving to', u.path + '/' + p + suffix);
        mkdirp.sync(opt.path);
        wc.outputSystemStream = fs.createWriteStream(u.path + '/' + p + suffix);

        if (opt.gzip) {
          wc.outputStream = zlib.createGzip({chunkSize: 65536, level: zlib.Z_BEST_SPEED});
          wc.outputStream.pipe(wc.outputSystemStream);
        } else
          wc.outputStream = wc.outputSystemStream;

        break;
      }
      default:
        log('Error: unsupported protocol', u.protocol);
      }

      if (opt.csv && opt.csv.header) {
        if (wc.outputStream)
          wc.outputStream.write(opt.csv.header + '\n');
      }
    },
    _postIterate: function (acc, opt, wc, p, done) {
      const suffix = opt.gzip ? '.gz' : opt.parquet ? '.parquet' : '';

      if (opt.stream) {
        wc.outputStream.end(acc);
        if (wc.outputSystemStream) {
          wc.outputSystemStream.once('close', done);
        } else if (wc.uploadPromise) {
          wc.uploadPromise.then(done);
        }
        return;
      }
      if (opt.parquet) {
        wc.parquetFile.write(acc);
        wc.parquetFile.close();
      } else if (opt.gzip) {
        fs.appendFileSync(wc.exportFile, zlib.gzipSync(acc, {
          chunckSize: 65536,
          level: zlib.Z_BEST_SPEED
        }));
      } else {
        fs.appendFileSync(wc.exportFile, acc);
      }

      const readStream = fs.createReadStream(wc.exportFile);
      const u = url.parse(opt.path);

      switch (u.protocol) {
      case 'wasb:': {
        const retry = new azure.ExponentialRetryPolicyFilter();
        const az = azure.createBlobService().withFilter(retry);

        log('upload', wc.exportFile, 'to', u.auth, u.path.slice(1) + '/' + p + suffix);
        az.createBlockBlobFromLocalFile(
          u.path.slice(1),
          p + suffix,
          wc.exportFile,
          null,
          err => {
            if (err) log('Azure upload error', err);
            done();
          }
        );
        break;
      }
      case 's3:': {
        const s3 = new S3({
          httpOptions: {timeout: 3600000},
          signatureVersion: 'v4'
        });
        s3.upload(
          {
            Bucket: u.host,
            Key: u.path.slice(1) + '/' + p + suffix,
            Body: readStream
          },
          err => {
            if (err) log('S3 upload error', err);
            done();
          }
        );
        break;
      }
      case 'file:':
      case null: {
        mkdirp.sync(opt.path);
        const writeStream = fs.createWriteStream(u.path + '/' + p + suffix);
        readStream.pipe(writeStream);
        writeStream.once('close', done);
        break;
      }
      default:
        log('Error: unsupported protocol', u.protocol);
        done();
      }
    }
  };

  const jsonStreamReducer = (acc, val, opt, wc) => {
    acc += JSON.stringify(val) + '\n';
    if (acc.length >= 65536) {
      wc.outputStreamOk = wc.outputStream.write(acc);
      acc = '';
    }
    return acc;
  };

  const csvStreamReducer = (acc, val, opt, wc) => {
    const { csv: { sep = ';' } } = opt;

    if (val instanceof Object) {
      for (let i in val) {
        if (val[i] !== undefined && val[i] !== null) {
          acc += val[i].toString();
        }
        acc += sep;
      }
      acc = acc.substr(0, acc.length - 1);
    } else acc += val;
    acc += '\n';
    if (acc.length >= 65536) {
      wc.outputStreamOk = wc.outputStream.write(acc);
      acc = '';
    }
    return acc;
  };

  const jsonFileReducer = (acc, val, opt, wc) => {
    acc += JSON.stringify(val) + '\n';
    if (acc.length >= 65536) {
      if (opt.gzip) {
        fs.appendFileSync(wc.exportFile, zlib.gzipSync(acc, {
          chunckSize: 65536,
          level: zlib.Z_BEST_SPEED
        }));
      } else {
        fs.appendFileSync(wc.exportFile, acc);
      }
      acc = '';
    }
    return acc;
  };

  const parquetReducer = (acc, val, opt, wc) => {
    if (Array.isArray(val)) acc.push(val);
    else acc.push([val]);
    if (acc.length >= 10000) {
      wc.parquetFile.write(acc);
      acc = [];
    }
    return acc;
  };

  if (opt.parquet)
    return this.aggregate(parquetReducer, () => {}, [], opt, done);
  if (opt.stream) {
    if (opt.csv) {
      return this.aggregate(csvStreamReducer, () => {}, '', opt, done);
    }
    return this.aggregate(jsonStreamReducer, () => {}, '', opt, done);
  }
  return this.aggregate(jsonFileReducer, () => {}, '', opt, done);
});

Dataset.prototype.take = thenify(function (N, done) {
  const reducer = (a, b, opt) => {if (a.length < opt._max) a.push(b); return a;};
  const combiner = (a, b, opt) => {return ((a.length < opt._max) ? a.concat(b) : a).slice(0, opt._max);};
  return this.aggregate(reducer, combiner, [], {_max: N, _maxBusy: 1}, done);
});

Dataset.prototype.top = thenify(function (N, done) {
  const reducer = (a, b, opt) => {a.push(b); return (a.length > opt._max) ? a.slice(1) : a;};
  const combiner = (a, b, opt) => {return ((a.length < opt._max) ? b.concat(a) : a).slice(-opt._max);};
  return this.aggregate(reducer, combiner, [], {_max: N, _maxBusy: 1, _lifo: true}, done);
});

Dataset.prototype.first = thenify(function (done) {
  return this.take(1, (err, res) => {
    if (res) done(err, res[0]);
    else done(err);
  });
});

// Aggregate is the main action. All others are implemented on top of it.
// The following internal option flags drive its behaviour:
// * _max: maximum number of dataset entries to combine. Set by take and top.
//    this allows to skip useless processing once result is obtained.
// * _maxBusy: maximum number of parallel aggregate tasks. Set to 1 by take and top.
// * _lifo: enable partition processing from last to first. Set by top.
//
Dataset.prototype.aggregate = thenify(function (reducer, combiner, init, opt = {}, done) {
  const action = {args: [], src: reducer, init: init, opt: opt};
  const self = this;

  if (arguments.length < 5) done = opt;

  return this.sc.runJob(opt, this, action, (job, tasks) => {
    const tmp = [];                                         // Pending tasks results waiting for combine
    const lastIndex = opt._lifo ? 0 : tasks.length;         // 0 for top action
    const maxBusy = opt._maxBusy || self.sc.worker.length;  // set to 1 for take/top
    const incr = opt._lifo ? -1 : 1;
    let index = opt._lifo ? tasks.length - 1 : 0;           // start from 0, or last if top action
    let result = deepCopy(init);                            // reducer/combiner result init
    let busy = 0;                                           // Number of busy tasks
    let complete = 0;
    let error;

    function runNext() {
      while (busy < maxBusy && index !== lastIndex) {
        self.sc.runTask(tasks[index], (err, res, task) => {
          if (err) {
            // FIXME: should handle task re-submit here for fault tolerance
            console.error('ERROR: aggregate partition', task.pid);
          }

          const stop = opt._max && res.data.length >= opt._max;
          const tmpIndex = opt._lifo ? tasks.length - 1 - task.pid : task.pid;

          tmp[tmpIndex] = res.data;
          complete++;
          busy--;
          self.sc.dlog(task._start, 'part', task.pid, 'from worker-' + res.workerId, '(' + complete + '/' + tasks.length + ')');

          if (!stop && complete < tasks.length) return runNext();

          for (let i = 0; i < tmp.length; i++)
            result = combiner(result, tmp[i], opt);

          self.sc.dlog(self.resultStart, 'result stage', self.totalStages + '/' + self.totalStages, 'done');
          done(error, result);
        });
        index += incr;
        busy++;
      }
    }

    runNext();
  });
});

Dataset.prototype.reduce = thenify(function (reducer, init, opt = {}, done) {
  if (arguments.length < 4) done = opt;
  return this.aggregate(reducer, reducer, init, opt, done);
});

Dataset.prototype.count = thenify(function (done) {
  return this.aggregate(a => a + 1, (a, b) => a + b, 0, done);
});

Dataset.prototype.forEach = thenify(function (eacher, opt, done) {
  const arg = {opt, _foreach: true};
  if (arguments.length < 3) done = opt;
  return this.aggregate(eacher, () => null, null, arg, done);
});

Dataset.prototype.getPartitions = function (done) {
  if (this.partitions === undefined) {
    this.partitions = {};
    let cnt = 0;
    for (let i = 0; i < this.dependencies.length; i++) {
      for (let j = 0; j < this.dependencies[i].nPartitions; j++) {
        this.partitions[cnt] = new Partition(this.id, cnt, this.dependencies[i].id, this.dependencies[i].partitions[j].partitionIndex);
        cnt++;
      }
    }
    this.nPartitions = cnt;
  }
  done();
};

// Randomize order of an array using Fisher-Yates shuffe
function randomizeArray(array) {
  let i = array.length;
  while (i) {
    let j = Math.floor(Math.random() * i--);
    [array[i], array[j]] = [array[j], array[i]];
  }
  return array;
}

// Returns a sampling rate that ensures a size >= lowerBound most of the time.
// Inspired from spark
function sampleSizeFraction(num, total, withReplacement) {
  const minSamplingRate = 1e-10;        // Limited by RNG's resolution
  const delta = 1e-4;                   // To have 0.9999 success rate
  const fraction = num / total;
  let upperBound;

  if (withReplacement) {
    // Poisson upper bound for Pr(num successful poisson trials) > (1-delta)
    let numStd = num < 6 ? 12 : num < 16 ? 9 : 6;
    upperBound = Math.max(num + numStd * Math.sqrt(num), minSamplingRate) / total;
  } else {
    // Binomial upper bound for Pr(num successful bernoulli trials) > (1-delta)
    let gamma = - Math.log(delta) / total;
    upperBound = Math.min(1, Math.max(minSamplingRate, fraction + gamma + Math.sqrt(gamma * gamma + 2 * gamma * fraction)));
  }
  return upperBound;
}

Dataset.prototype.takeSample = thenify(function (withReplacement, num, done) {
  const self = this;
  if (!num) return done();
  this.count(function (err, total) {
    if (err || !total) return done(err);
    if (total <= num) return this.collect((err, res) => {done(err, randomizeArray(res));});
    const fraction = sampleSizeFraction(num, total, withReplacement);
    iterate();

    function iterate() {
      self.sample(withReplacement, fraction).collect((err, result) => {
        if (result.length < num) return iterate();
        done(err, randomizeArray(result).slice(0, num));
      });
    }
  });
});

Dataset.prototype.getPreferedLocation = function () {return [];};

function Partition(datasetId, partitionIndex, parentDatasetId, parentPartitionIndex) {
  this.data = [];
  this.datasetId = datasetId;
  this.partitionIndex = partitionIndex;
  this.parentDatasetId = parentDatasetId;
  this.parentPartitionIndex = parentPartitionIndex;
  this.type = 'Partition';
  //this.count = 0;
  //this.bsize = 0;   // TODO: mv in worker only. estimated size of memory increment per period
  //this.tsize = 0;   // TODO: mv in worker only. estimated total partition size
  //this.skip = false;  // TODO: mv in worker only. true when partition is evicted due to memory shortage
}

Partition.prototype.transform = function (data) {
  if (this.skip) return data; // Passthrough if partition is evicted

  // Periodically check/update available memory, and evict partition
  // if necessary. In this case it will be recomputed if required by
  // a future action.
  if (this.count++ === 9999) {
    this.count = 0;
    if (this.bsize === 0) this.bsize = this.mm.sizeOf(this.data);
    this.tsize += this.bsize;
    this.mm.storageMemory += this.bsize;
    if (this.mm.storageFull()) {
      console.log('# Out of memory, evict partition', this.partitionIndex);
      this.skip = true;
      this.mm.storageMemory -= this.tsize;
      this.data = [];
      this.mm.unregister(this);
      return data;
    }
  }

  // Perform persistence of partition in memory here
  for (let i = 0; i < data.length; i++) this.data.push(data[i]);
  return data;
};

Partition.prototype.iterate = function (task, p, pipeline, done) {
  let buffer = this.data;
  for (let t = 0; t < pipeline.length; t++)
    buffer = pipeline[t].transform(buffer, task);
  done();
};

function Source(sc, N, getItem, args, npart) {
  Dataset.call(this, sc);
  this.getItem = getItem;
  this.npart = npart;
  this.N = N;
  this.args = args;
  this.type = 'Source';
}
inherits(Source, Dataset);

Source.prototype.iterate = function (task, p, pipeline, done) {
  const n = this.sizes[p];
  let buffer = [];
  let index = this.bases[p];

  for (let i = 0; i < n; i++, index++) {
    buffer.push(this.getItem(index, this.args, task));
    if (buffer.length === task.blockSize) {
      for (let t = 0; t < pipeline.length; t++)
        buffer = pipeline[t].transform(buffer, task);
      buffer = [];
    }
  }
  if (buffer.length) {
    for (let t = 0; t < pipeline.length; t++)
      buffer = pipeline[t].transform(buffer, task);
  }
  done();
};

Source.prototype.getPartitions = function (done) {
  const P = this.npart || this.sc.worker.length;
  const N = this.N;
  const plen = Math.ceil(N / P);

  this.partitions = {};
  this.sizes = {};
  this.bases = {};
  this.nPartitions = P;
  for (let i = 0, max = plen; i < P; i++, max += plen) {
    this.partitions[i] = new Partition(this.id, i);
    this.sizes[i] = (max <= N) ? plen : (max - N < plen) ? N - (plen * i) : 0;
    this.bases[i] = i ? this.bases[i - 1] + this.sizes[i - 1] : 0;
  }
  done();
};

function parallelize(sc, localArray, P) {
  return new Source(sc, localArray.length, function (i, a) {return a[i];}, localArray, P);
}

function range(sc, start, end, step, P) {
  if (end === undefined) { end = start; start = 0; }
  if (step === undefined) step = 1;

  return new Source(sc, Math.ceil((end - start) / step), function (i, a) {
    return i * a.step + a.start;
  }, {step: step, start: start}, P);
}

function Obj2line() {
  stream.Transform.call(this, {objectMode: true});
}
inherits(Obj2line, stream.Transform);

Obj2line.prototype._transform = function (chunk, encoding, done) {
  done(null, JSON.stringify(chunk) + '\n');
};

function Stream(sc, stream, type) { // type = 'line' ou 'object'
  const id = uuid.v4();
  const tmpFile = sc.basedir + 'tmp/' + id;
  const targetFile = sc.basedir + 'stream/' + id;
  const out = fs.createWriteStream(tmpFile);
  const dataset = sc.textFile(targetFile);

  dataset.watched = true;         // notify dpe to wait for file before launching
  dataset.parse = type === 'object';
  out.once('close', function () {
    fs.renameSync(tmpFile, targetFile);
    dataset.watched = false;
  });
  if (type === 'object')
    stream.pipe(new Obj2line()).pipe(out);
  else
    stream.pipe(out);
  return dataset;
}

function parquetIterate(path, task, pipeline, done) {
  const reader = new parquet.ParquetReader(path);
  const info = reader.info();
  const numRows = info.rows;
  const rows = reader.rows(numRows);
  let buffer = [];

  //log('rows:', rows);
  for (let i = 0; i < numRows; i++) {
    buffer.push(rows[i]);
    if (buffer.length === task.blockSize) {
      for (let t = 0; t < pipeline.length; t++)
        buffer = pipeline[t].transform(buffer, task);
      buffer = [];
    }
  }
  if (buffer.length) {
    for (let t = 0; t < pipeline.length; t++)
      buffer = pipeline[t].transform(buffer, task);
  }
  done();
  reader.close();
}

function TextAzure(sc, dir, options) {
  Dataset.call(this, sc);
  const _a = dir.split('/');
  sc.log('_a:', _a);
  this.container = _a[1].replace(/@.*/, '');
  this.filematch = _a.slice(2).join('/');
  this.prefix = this.filematch.replace(/[*[].*/, ''); // Cut prefix before any globbing exp.
  if (this.prefix.slice(-1) === '/' && this.prefix === this.filematch)
    this.filematch += '*';
  if (this.filematch === '' && this.prefix === '')
    this.filematch += '*';
  sc.log('container:', this.container, 'filematch:', this.filematch, 'prefix:', this.prefix);
  this.type = 'TextAzure';
  this.options = options || {};
  this.options.azure = this.options.azure || {};
}

inherits(TextAzure, Dataset);

TextAzure.prototype.getPartitions = function (done) {
  const self = this;
  const retry = new azure.ExponentialRetryPolicyFilter();
  const az = azure.createBlobService().withFilter(retry);

  function getList(list, token, done) {
    az.listBlobsSegmentedWithPrefix(self.container, self.prefix, token, function (err, data) {
      if (err) throw new Error('az.listBlobsSegmented failed');
      list = list.concat(data.entries);
      if (data.continuationToken)
        return getList(list, data.continuationToken, done);
      done(err, list);
    });
  }

  getList([], null, function (err, res) {
    if (err) return done(err);
    self.partitions = {};
    self.nPartitions = 0;
    const isMatch = micromatch.matcher(self.filematch);
    let size = 0;
    let pindex = 0;
    for (let i = 0; i < res.length; i++) {
      if (!isMatch(res[i].name)) continue;
      //self.sc.log('name:', res[i].name);
      size += Number(res[i].contentLength);
      self.partitions[pindex] = new Partition(self.id, pindex);
      self.partitions[pindex].path = res[i].name;
      pindex++;
      if (self.options.maxFiles && self.options.maxFiles === pindex) break;
    }
    self.nPartitions = pindex;
    self.sc.log('source:', self.nPartitions, 'partitions from Azure files, total size:', (size / (1 << 20)).toFixed(3), 'MB');
    done();
  });
};

TextAzure.prototype.iterate = function (task, p, pipeline, done) {
  const path = this.partitions[p].path;
  const retry = new azure.ExponentialRetryPolicyFilter();
  const az = azure.createBlobService().withFilter(retry);
  //return azureDownload(az, this.container, path, task, pipeline, done);
  let rs = az.createReadStream(this.container, path, null);

  log('stream azure', this.container, path);
  if (this.options.parquet || path.slice(-8) === '.parquet')
    return parquetStream(rs, path, task, pipeline, done);
  if (path.slice(-3) === '.gz')
    rs = rs.pipe(zlib.createGunzip({chunkSize: 65536}));

  iterateStream(rs, task, pipeline, done);
};

// Complete download from azure to local, then process.
/*
function azureDownload(az, container, name, task, pipeline, done) {
  var filename = task.basedir + 'import/' + name.replace(/\//g, '-');
  var gz = filename.slice(-3) === '.gz';
  var delay = 1000, retry = 5;
  task.log('getBlob', name);

  function getBlob() {
    az.getBlobToLocalFile(container, name, filename, function (err) {
      task.log('getBlob', name, 'error:', err);
      if (err) {
        delay *= 2;
        if (!retry) throw new Error(err);
        task.log('retry getBlob', name, 'in', delay, 'ms');
        return setTimeout(getBlob, delay);
      }
      var stream = task.lib.fs.createReadStream(filename);
      if (gz) stream = stream.pipe(task.lib.zlib.createGunzip({chunkSize: 65536}));
      iterateStream(stream, task, pipeline, done);
    });
  }
  getBlob();
}
*/

function iterateStream(readStream, task, pipeline, done) {
  let tail = '';

  readStream.on('data', function (chunk) {
    const str = tail + chunk;
    let lines = str.split(/\r\n|\r|\n/);
    tail = lines.pop();
    for (let t = 0; t < pipeline.length; t++)
      lines = pipeline[t].transform(lines, task);
  });

  readStream.once('end', function () {
    if (tail) {
      let buffer = [tail];
      for (let t = 0; t < pipeline.length; t++)
        buffer = pipeline[t].transform(buffer, task);
    }
    done();
  });

  readStream.on('error', function (err) {
    log('iterateStream stream error:', err);
  });
}

function TextS3(sc, dir, options) {
  Dataset.call(this, sc);
  const _a = dir.split('/');
  this.bucket = _a[0];
  this.filematch = _a.slice(1).join('/');
  this.prefix = this.filematch.replace(/[*[].*/, ''); // Cut prefix before any globbing exp.
  if (this.prefix.slice(-1) === '/' && this.prefix === this.filematch)
    this.filematch += '*';
  this.type = 'TextS3';
  this.options = options || {};
  this.options.s3 = this.options.s3 || {};
  this.options.s3.signatureVersion = this.options.s3.signatureVersion || 'v4';
}

inherits(TextS3, Dataset);

TextS3.prototype.getPartitions = function (done) {
  const self = this;
  const s3 = new S3(this.options.s3);

  function getList(list, token, done) {
    s3.listObjectsV2({
      Bucket: self.bucket,
      Prefix: self.prefix,
      ContinuationToken: token
    }, function (err, data) {
      if (err) throw new Error('s3.listObjectsV2 failed');
      list = list.concat(data.Contents);
      if (data.IsTruncated)
        return getList(list, data.NextContinuationToken, done);
      done(err, list);
    });
  }

  getList([], null, function (err, res) {
    //self.sc.log('TextS3 list:', res.length, res[0]);
    if (err) return done(err);
    self.partitions = {};
    self.nPartitions = 0;
    const isMatch = micromatch.matcher(self.filematch);
    let size = 0;
    let pindex = 0;
    for (let i = 0; i < res.length; i++) {
      if (!isMatch(res[i].Key)) continue;
      //self.sc.log('name:', res[i].Key);
      size += res[i].Size;
      self.partitions[pindex] = new Partition(self.id, pindex);
      self.partitions[pindex].path = res[i].Key;
      pindex++;
      if (self.options.maxFiles && self.options.maxFiles === pindex) break;
    }
    self.nPartitions = pindex;
    self.sc.log('source:', self.nPartitions, 'partitions from S3 files, total size:', (size / (1 << 20)).toFixed(3), 'MB');
    done();
  });
};

TextS3.prototype.iterate = function (task, p, pipeline, done) {
  const path = this.partitions[p].path;
  const s3 = new S3(this.options.s3);
  let rs = s3.getObject({Bucket: this.bucket, Key: path}).createReadStream();

  log('stream s3 #', this.bucket, path);
  if (this.options.parquet || path.slice(-8) === '.parquet') {
    return parquetStream(rs, path, task, pipeline, done);
  }
  if (path.slice(-3) === '.gz') {
    rs = rs.pipe(zlib.createGunzip({chunkSize: 65536}));
  }
  iterateStream(rs, task, pipeline, done);
};

function parquetStream(rs, name, task, pipeline, done) {
  const filename = task.basedir + 'import/' + name.replace(/\//g, '-');
  const ws = fs.createWriteStream(filename, {highWaterMark: 1 << 16});
  log('Download ', filename);
  rs.pipe(ws);

  ws.once('close', function () {
    parquetIterate(filename, task, pipeline, done);
  });
}

function TextLocal(sc, dir, options) {
  Dataset.call(this, sc);
  this.type = 'TextLocal';
  if (dir.slice(-1) === '/') dir += '*';
  this.dir = dir;
  this.options = options || {};
}

inherits(TextLocal, Dataset);

TextLocal.prototype.getPartitions = function (done) {
  const self = this;

  glob(this.dir, function (err, res) {
    let size = 0;
    if (err) return done(err);
    self.partitions = {};
    if (self.options.maxFiles && self.options.maxFiles < res.length)
      self.nPartitions = self.options.maxFiles;
    else
      self.nPartitions = res.length;
    for (let i = 0; i < self.nPartitions; i++) {
      self.partitions[i] = new Partition(self.id, i);
      self.partitions[i].path = res[i];
      const stat = fs.statSync(res[i]);
      size += stat.size;
    }
    self.sc.log('source:', self.nPartitions, 'partitions from local files, total size:', (size / (1 << 20)).toFixed(3), 'MB');
    done();
  });
};

TextLocal.prototype.iterate = function (task, p, pipeline, done) {
  const path = this.partitions[p].path;
  log('stream local', path);
  if (this.options.parquet || path.slice(-8) === '.parquet')
    return parquetIterate(path, task, pipeline, done);
  let rs = fs.createReadStream(path);
  if (path.slice(-3) === '.gz')
    rs = rs.pipe(zlib.createGunzip({chunkSize: 65536}));

  iterateStream(rs, task, pipeline, done);
};

//FIXME: File splitting should be impletemented as a helper i.o a class,
//
//function TextFile(sc, file, nPartitions) {
//  Dataset.call(this, sc);
//  this.file = file;
//  this.type = 'TextFile';
//  this.nSplit = nPartitions || sc.worker.length;
//  this.basedir = sc.basedir;
//}
//
//inherits(TextFile, Dataset);
//
//TextFile.prototype.getPartitions = function (done) {
//  var self = this;
//
//  function getSplits() {
//    var u = url.parse(self.file);
//
//    if ((u.protocol === 'hdfs:') && u.slashes && u.hostname && u.port)
//      splitHDFSFile(u.path, self.nSplit, mapLogicalSplit);
//    else
//      splitLocalFile(u.path, self.nSplit, mapLogicalSplit);
//
//    function mapLogicalSplit(split) {
//      self.splits = split;
//      self.partitions = {};
//      self.nPartitions = self.splits.length;
//      for (var i = 0; i < self.splits.length; i++)
//        self.partitions[i] = new Partition(self.id, i);
//      done();
//    }
//  }
//
//  if (this.watched) {
//    var watcher = fs.watch(self.basedir + 'stream', function (event, filename) {
//      if ((event === 'rename') && (filename === basename(self.file))) {
//        watcher.close();  // stop watching directory
//        getSplits();
//      }
//    });
//  } else getSplits();
//};
//
//TextFile.prototype.iterate = function (task, p, pipeline, done) {
//  var buffer;
//
//  function processLine(line) {
//    if (!line) return;  // skip empty lines
//    buffer = [line];
//    for (var t = 0; t < pipeline.length; t++)
//      buffer = pipeline[t].transform(buffer);
//  }
//
//  function processLineParse(line) {
//    if (!line) return;  // skip empty lines
//    buffer = [JSON.parse(line)];
//    for (var t = 0; t < pipeline.length; t++)
//      buffer = pipeline[t].transform(buffer);
//  }
//
//  task.lib.readSplit(this.splits, this.splits[p].index, this.parse ? processLineParse : processLine, done, function (part, opt) {
//    return task.getReadStreamSync(part, opt);
//  });
//};
//
//TextFile.prototype.getPreferedLocation = function (pid) {return this.splits[pid].ip;};

function _Map(parent, mapper, args) {
  Dataset.call(this, parent.sc, [parent]);
  this.mapper = mapper;
  this.args = args;
  this.type = 'Map';
}

inherits(_Map, Dataset);

_Map.prototype.transform = function map(data, task) {
  const tmp = [];
  for (let i = 0; i < data.length; i++)
    tmp[i] = this.mapper(data[i], this.args, task);
  return tmp;
};

function FlatMap(parent, mapper, args) {
  Dataset.call(this, parent.sc, [parent]);
  this.mapper = mapper;
  this.args = args;
  this.type = 'FlatMap';
}

inherits(FlatMap, Dataset);

FlatMap.prototype.transform = function flatmap(data, task) {
  let tmp = [];
  for (let i = 0; i < data.length; i++)
    tmp = tmp.concat(this.mapper(data[i], this.args, task));
  return tmp;
};

function MapValues(parent, mapper, args) {
  Dataset.call(this, parent.sc, [parent]);
  this.mapper = mapper;
  this.args = args;
  this.type = 'MapValues';
}

inherits(MapValues, Dataset);

MapValues.prototype.transform = function (data, task) {
  const tmp = [];
  for (let i = 0; i < data.length; i++)
    tmp[i] = [data[i][0], this.mapper(data[i][1], this.args, task)];
  return tmp;
};

function FlatMapValues(parent, mapper, args) {
  Dataset.call(this, parent.sc, [parent]);
  this.mapper = mapper;
  this.args = args;
  this.type = 'FlatMapValues';
}

inherits(FlatMapValues, Dataset);

FlatMapValues.prototype.transform = function (data, task) {
  let tmp = [];
  for (let i = 0; i < data.length; i++) {
    const t0 = this.mapper(data[i][1], this.args, task);
    tmp = tmp.concat(t0.map(function (e) {return [data[i][0], e];}));
  }
  return tmp;
};

function Filter(parent, filter, args) {
  Dataset.call(this, parent.sc, [parent]);
  this._filter = filter;
  this.args = args;
  this.type = 'Filter';
}

inherits(Filter, Dataset);

Filter.prototype.transform = function (data, task) {
  const tmp = [];
  for (let i = 0; i < data.length; i++)
    if (this._filter(data[i], this.args, task)) tmp.push(data[i]);
  return tmp;
};

function Poisson(lambda) {
  this.L = Math.exp(-lambda);

  this.sample = function () {
    let k = 0;
    let p = 1;
    do {
      k++;
      p *= Math.random();
    } while (p > this.L);
    return k - 1;
  };
}

function Sample(parent, withReplacement, frac) {
  Dataset.call(this, parent.sc, [parent]);
  this.withReplacement = withReplacement;
  this.frac = frac;
  this.rng = new Poisson(frac);
  this.type = 'Sample';
}

inherits(Sample, Dataset);

Sample.prototype.transform = function (data) {
  const tmp = [];
  if (this.withReplacement) {
    for (let i = 0; i < data.length; i++)
      for (let j = 0; j < this.rng.sample(); j++) tmp.push(data[i]);
  } else {
    for (let i = 0; i < data.length; i++)
      if (Math.random() < this.frac) tmp.push(data[i]);
  }
  return tmp;
};

function Union(sc, parents) {
  Dataset.call(this, sc, parents);
  this.type = 'Union';
}

inherits(Union, Dataset);

Union.prototype.transform = function (data) {return data;};

// AggregateByKey is the main [k,v] transform on top of which all others
// [k,v] transforms are implemented.
//
function AggregateByKey(sc, dependencies, reducer, combiner, init, args) {
  Dataset.call(this, sc, dependencies);
  this.combiner = combiner;
  this.reducer = reducer;
  this.init = init;
  this.args = args;
  this.shuffling = true;
  this.executed = false;
  this.buffer = {};
  this.type = 'AggregateByKey';
}

inherits(AggregateByKey, Dataset);

// On master: allocate output partitions. Called both at pre-shuffle and post-shuffle
AggregateByKey.prototype.getPartitions = function (done) {
  if (this.partitions === undefined) {
    // output partitions are undefined only a pre-shuffle stage
    let P = 0;
    this.partitions = {};
    for (let i = 0; i < this.dependencies.length; i++)
      P = Math.max(P, this.dependencies[i].nPartitions);
    if (this.sc.maxShufflePartitions)
      P = Math.min(P, this.sc.maxShufflePartitions);
    for (let i = 0; i < P; i++) this.partitions[i] = new Partition(this.id, i);
    this.nPartitions = P;
    this.partitioner = new HashPartitioner(P);
  }
  done();
};

AggregateByKey.prototype.transform = function (data, task) {
  const buf = this.buffer;
  for (let i = 0; i < data.length; i++) {
    const d = data[i];
    if (d[0] === undefined) continue;
    const key = JSON.stringify(d[0]);
    const acc = (key in buf) ? buf[key] : deepCopy(this.init);
    buf[key] = this.reducer(acc, d[1], this.args, task);
  }
};

AggregateByKey.prototype.spillToDisk = function (task, done) {
  const buf = this.buffer;
  const plen = this.nPartitions;
  const str = Array(plen).fill('');
  const size = Array(plen).fill(0);
  const path = task.basedir + 'shuffle/' + task.workerId + '-' + task.datasetId + '-';

  if (this.dependencies.length > 1) {   // COGROUP
    const isLeft = (this.shufflePartitions[task.pid].parentDatasetId === this.dependencies[0].id);
    if (isLeft) {
      for (let key in buf) {
        const pid = hash(key) % plen;
        str[pid] += key + '\n[' + JSON.stringify(buf[key]) + ',[]]\n';
        if (str[pid].length >= 65536) {
          fs.appendFileSync(path + pid, str[pid]);
          size[pid] += str[pid].length;
          str[pid] = '';
        }
      }
    } else {
      for (let key in buf) {
        const pid = hash(key) % plen;
        str[pid] += key + '\n[[],' + JSON.stringify(buf[key]) + ']\n';
        if (str[pid].length >= 65536) {
          fs.appendFileSync(path + pid, str[pid]);
          size[pid] += str[pid].length;
          str[pid] = '';
        }
      }
    }
  } else {                              // AGGREGATE BY KEY
    for (let key in buf) {
      const pid = hash(key) % plen;
      str[pid] += key + '\n' + JSON.stringify(buf[key]) + '\n';
      if (str[pid].length >= 65536) {
        fs.appendFileSync(path + pid, str[pid]);
        size[pid] += str[pid].length;
        str[pid] = '';
      }
    }
  }
  for (let i = 0; i < plen; i++) {
    //log('pre-shuffle path:', path + i);
    fs.appendFileSync(path + i, str[i]);
    size[i] += str[i].length;
    task.files[i] = {host: task.grid.hostname, path: path + i, size: size[i]};
  }
  done();
};

AggregateByKey.prototype.iterate = function (task, p, pipeline, done) {
  //var self = this, cnt = 0, file, files = [], shuffleFiles = {}, map = new Map(), i;
  const self = this;
  const files = [];
  const shuffleFiles = {};
  const map = new Map();
  let cnt = 0;

  for (let i = 0; i < self.nShufflePartitions; i++) {
    const file = self.shufflePartitions[i].files[p];
    if (!(file.path in shuffleFiles)) {
      files.push(file);
      shuffleFiles[file.path] = true;
    }
  }

  processShuffleFile(files[cnt], processDone);

  function processShuffleFile(file, done) {
    task.getReadStream(file, undefined, function (err, stream) {
      let tail = '';
      let lastk;
      //var start = Date.now();

      // Input format: interleaving of key lines and value lines
      stream.on('data', function (buf) {
        const data = tail + buf;
        const lines = data.split('\n');
        tail = lines.pop();
        const len = lastk ? lines.unshift(lastk) : lines.length;
        lastk = (len & 1) ? lines.pop() : undefined;
        let i = 0;
        while (i < lines.length) {
          const k = lines[i++];
          const v = JSON.parse(lines[i++]);
          const m = map.get(k);
          if (m !== undefined) map.set(k, self.combiner(map.get(k), v, self.args, self.global));
          else map.set(k, v);
        }
      });
      stream.once('end', function () {
        if (lastk && tail.length) {
          const v = JSON.parse(tail);
          const m = map.get(lastk);
          if (m !== undefined) map.set(lastk, self.combiner(m, v, self.args, self.global));
          else map.set(lastk, v);
        }
        //dlog(start, 'processed', file.path, file.size);
        done();
      });
    });
  }

  function processDone() {
    if (++cnt < files.length)
      return processShuffleFile(files[cnt], processDone);

    const it = map.entries();
    let buffer = [];
    iterate();

    function iterate() {
      while (task.outputStreamOk) {
        const kv = it.next().value;
        if (!kv) {
          if (buffer.length) {
            for (let t = 0; t < pipeline.length; t++)
              buffer = pipeline[t].transform(buffer, task);
          }
          return done();
        }
        buffer.push([JSON.parse(kv[0]), kv[1]]);
        if (buffer.length === task.blockSize) {
          for (let t = 0; t < pipeline.length; t++)
            buffer = pipeline[t].transform(buffer, task);
          buffer = [];
        }
      }
      task.outputStreamOk = true;
      task.outputStream.once('drain', iterate);
    }
  }
};

function Cartesian(sc, dependencies) {
  Dataset.call(this, sc, dependencies);
  this.shuffling = true;
  this.executed = false;
  this.buffer = [];
  this.type = 'Cartesian';
}

inherits(Cartesian, Dataset);

Cartesian.prototype.getPartitions = function (done) {
  if (this.partitions === undefined) {
    this.pleft = this.dependencies[0].nPartitions;
    this.pright =  this.dependencies[1].nPartitions;
    const P = this.pleft * this.pright;
    this.partitions = {};
    this.nPartitions = P;
    for (let i = 0; i < P; i++)
      this.partitions[i] = new Partition(this.id, i);
  }
  done();
};

Cartesian.prototype.transform = function (data) {
  for (let i = 0; i < data.length; i++) this.buffer.push(data[i]);
};

Cartesian.prototype.spillToDisk = function (task, done) {
  const path = task.basedir + 'shuffle/' + uuid.v4();
  let str = '';
  for (let  i = 0; i < this.buffer.length; i++) {
    str += JSON.stringify(this.buffer[i]) + '\n';
    if (str.length >= 65536) {
      fs.appendFileSync(path, str);
      str = '';
    }
  }
  fs.appendFileSync(path, str);
  const size = fs.statSync(path).size;
  task.files = {host: task.grid.hostname, path: path, size: size};
  log(task.files);
  done();
};

Cartesian.prototype.iterate = function (task, p, pipeline, done) {
  const p1 = Math.floor(p / this.pright);
  const p2 = p % this.pright + this.pleft;
  const self = this;
  let s1 = '';

  task.getReadStream(this.shufflePartitions[p1].files, undefined, function (err, stream1) {
    stream1.on('data', function (s) {s1 += s;});
    stream1.once('end', function () {
      const a1 = s1.split('\n');
      let s2 = '';
      task.getReadStream(self.shufflePartitions[p2].files, undefined, function (err, stream2) {
        stream2.on('data', function (s) {s2 += s;});
        stream2.once('end', function () {
          const a2 = s2.split('\n');
          for (let i = 0; i < a1.length; i++) {
            if (a1[i] === '') continue;
            for (let j = 0; j < a2.length; j++) {
              if (a2[j] === '') continue;
              let buffer = [[JSON.parse(a1[i]), JSON.parse(a2[j])]];
              for (let t = 0; t < pipeline.length; t++)
                buffer = pipeline[t].transform(buffer, task);
            }
          }
          done();
        });
      });
    });
  });
};

function SortBy(sc, dependencies, keyFunc, ascending, numPartitions) {
  Dataset.call(this, sc, [dependencies]);
  this.shuffling = true;
  this.executed = false;
  this.keyFunc = keyFunc;
  this.ascending = (ascending === undefined) ? true : ascending;
  this.buffer = [];
  this.numPartitions = numPartitions;
  this.type = 'SortBy';
}

inherits(SortBy, Dataset);

SortBy.prototype.getPartitions = function (done) {
  if (this.partitions === undefined) {
    const P = Math.max(this.numPartitions || 1, this.dependencies[0].nPartitions);

    this.partitions = {};
    this.nPartitions = P;
    for (let p = 0; p < P; p++) this.partitions[p] = new Partition(this.id, p);
    this.partitioner = new RangePartitioner(P, this.keyFunc, this.dependencies[0]);
    this.partitioner.init(done);
  } else done();
};

SortBy.prototype.transform = function (data, task) {
  for (let i = 0; i < data.length; i++) {
    const pid = this.partitioner.getPartitionIndex(this.keyFunc(data[i], task));
    if (this.buffer[pid] === undefined) this.buffer[pid] = [];
    this.buffer[pid].push(data[i]);
  }
};

SortBy.prototype.spillToDisk = function (task, done) {
  for (let i = 0; i < this.nPartitions; i++) {
    const path = task.basedir + 'shuffle/' + uuid.v4();
    let str = '';
    if (this.buffer[i] !== undefined) {
      for (let j = 0; j < this.buffer[i].length; j++) {
        str += JSON.stringify(this.buffer[i][j]) + '\n';
        if (str.length >= 65536) {
          fs.appendFileSync(path, str);
          str = '';
        }
      }
    }
    fs.appendFileSync(path, str);
    const size = fs.statSync(path).size;
    task.files[i] = {host: task.grid.hostname, path: path, size: size};
  }
  done();
};

SortBy.prototype.iterate = function (task, p, pipeline, done) {
  const self = this;
  const cbuffer = [];
  const files = [];
  let cnt = 0;

  for (let i = 0; i < self.nShufflePartitions; i++)
    files.push(self.shufflePartitions[i].files[p]);

  processShuffleFile(files[cnt], processDone);

  function processShuffleFile(file, done) {
    const lines = new Lines();
    task.getReadStream(file, undefined, function (err, stream) {
      stream.pipe(lines);
    });
    lines.on('data', function (linev) {
      for (let i = 0; i < linev.length; i++)
        cbuffer.push(JSON.parse(linev[i]));
    });
    lines.once('end', done);
  }

  function processDone() {
    if (++cnt === files.length) {
      cbuffer.sort(compare);
      for (let i = 0; i < cbuffer.length; i++) {
        let buffer = [cbuffer[i]];
        for (let t = 0; t < pipeline.length; t++)
          buffer = pipeline[t].transform(buffer, task);
      }
      done();
    } else processShuffleFile(files[cnt], processDone);

    function compare(a, b) {
      if (self.keyFunc(a, task) < self.keyFunc(b, task)) return self.ascending ? -1 : 1;
      if (self.keyFunc(a, task) > self.keyFunc(b, task)) return self.ascending ? 1 : -1;
      return 0;
    }
  }
};

function PartitionBy(sc, dependencies, partitioner) {
  Dataset.call(this, sc, [dependencies]);
  this.shuffling = true;
  this.executed = false;
  this.buffer = [];
  this.partitioner = partitioner;
  this.type = 'PartitionBy';
}

inherits(PartitionBy, Dataset);

PartitionBy.prototype.getPartitions = function (done) {
  if (this.partitions === undefined) {
    const P = this.partitioner.numPartitions;
    this.partitions = {};
    this.nPartitions = P;
    for (let p = 0; p < P; p++) this.partitions[p] = new Partition(this.id, p);
    if (this.partitioner.init) this.partitioner.init(done);
    else done();
  } else done();
};

PartitionBy.prototype.transform = function (data, task) {
  for (let i = 0; i < data.length; i++) {
    const pid = this.partitioner.getPartitionIndex(data[i][0], task);
    if (this.buffer[pid] === undefined) this.buffer[pid] = [];
    this.buffer[pid].push(data[i]);
  }
};

PartitionBy.prototype.spillToDisk = function (task, done) {
  for (let i = 0; i < this.nPartitions; i++) {
    const path = task.basedir + 'shuffle/' + uuid.v4();
    let str = '';
    if (this.buffer[i] !== undefined) {
      for (let j = 0; j < this.buffer[i].length; j++) {
        str += JSON.stringify(this.buffer[i][j]) + '\n';
        if (str.length >= 65536) {
          fs.appendFileSync(path, str);
          str = '';
        }
      }
    }
    fs.appendFileSync(path, str);
    const size = fs.statSync(path);
    task.files[i] = {host: task.grid.hostname, path: path, size: size};
  }
  done();
};

PartitionBy.prototype.iterate = function (task, p, pipeline, done) {
  const self = this;
  const cbuffer = [];
  const files = [];
  let cnt = 0;

  for (let i = 0; i < self.nShufflePartitions; i++)
    files.push(self.shufflePartitions[i].files[p]);

  processShuffleFile(files[cnt], processDone);

  function processShuffleFile(file, done) {
    const lines = new Lines();
    task.getReadStream(file, undefined, function (err, stream) {
      stream.pipe(lines);
    });
    lines.on('data', function (linev) {
      for (let i = 0; i < linev.length; i++)
        cbuffer.push(JSON.parse(linev[i]));
    });
    lines.once('end', done);
  }

  function processDone() {
    if (++cnt === files.length) {
      for (let i = 0; i < cbuffer.length; i++) {
        let buffer = [cbuffer[i]];
        for (let t = 0; t < pipeline.length; t++)
          buffer = pipeline[t].transform(buffer, task);
      }
      done();
    } else processShuffleFile(files[cnt], processDone);
  }
};

function RangePartitioner(numPartitions, keyFunc, dataset) {
  this.numPartitions = numPartitions;

  this.init = function (done) {
    const self = this;
    dataset.sample(false, 0.5).collect(function (err, result) {
      function compare(a, b) {
        if (keyFunc(a) < keyFunc(b)) return -1;
        if (keyFunc(a) > keyFunc(b)) return 1;
        return 0;
      }
      result.sort(compare);
      self.upperbounds = [];
      if (result.length <= numPartitions - 1) {
        self.upperbounds = result;  // supprimer les doublons peut-etre ici
      } else {
        const s = Math.floor(result.length / numPartitions);
        for (let i = 0; i < numPartitions - 1; i++) self.upperbounds.push(result[s * (i + 1)]);
      }
      done();
    });
  };

  this.getPartitionIndex = function (data) {
    let i;
    for (i = 0; i < this.upperbounds.length; i++)
      if (data < this.upperbounds[i]) break;
    return i;
  };
}

function HashPartitioner(numPartitions) {
  this.numPartitions = numPartitions;
  this.type = 'HashPartitioner';
}

HashPartitioner.prototype.hash = hash;

function hash(s) {
  let h = 0;
  for (let i = 0; i < s.length; i++)
    h = (h << 5) - h + s.charCodeAt(i);
  return h >>> 0;   // Convert to unsigned
}

HashPartitioner.prototype.getPartitionIndex = function (data) {
  return this.hash(data) % this.numPartitions;
};

function deepCopy(o) {
  if (typeof o !== 'object' || !o) return o;
  if (o.constructor === Array) {
    const n = new Array(o.length);
    for (let i = 0; i < o.length; i++) n[i] = deepCopy(o[i]);
    return n;
  }
  const n = {};
  for (let i in o) n[i] = deepCopy(o[i]);
  return n;
}

function setRandomSeed(seed) {
  seedrandom(seed, {global: true});
}

module.exports = {
  Dataset: Dataset,
  Partition: Partition,
  parallelize: parallelize,
  range: range,
  setRandomSeed: setRandomSeed,
  TextLocal: TextLocal,
  TextS3: TextS3,
  TextAzure: TextAzure,
  Source: Source,
  Stream: Stream,
  Map: _Map,
  FlatMap: FlatMap,
  MapValues: MapValues,
  FlatMapValues: FlatMapValues,
  Filter: Filter,
  Sample: Sample,
  Union: Union,
  AggregateByKey: AggregateByKey,
  Cartesian: Cartesian,
  SortBy: SortBy,
  PartitionBy: PartitionBy,
  RangePartitioner: RangePartitioner,
  HashPartitioner: HashPartitioner
};
