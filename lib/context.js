// Copyright 2016 Luca-SAS, licensed under the Apache License 2.0

'use strict';

const fs = require('fs');
const path = require('path');
const util = require('util');
const url = require('url');
const zlib = require('zlib');

const browserify = require('browserify');
const callsite = require('callsite');
const mkdirp = require('mkdirp');
const resolve = require('resolve');
const seedrandom = require('seedrandom');
const uuid = require('uuid');

const dpeClient = require('./client.js');
const dataset = require('./dataset.js');
const Task = require('./task.js');

module.exports = dpeContext;

const envKeys = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_KEY_ID' ];
const start = Date.now();

util.inherits(dpeContext, dpeClient);

function dpeContext(arg = {}) {
  if (!(this instanceof dpeContext))
    return new dpeContext(arg);
  const self = this;

  arg.data = arg.data || {};
  arg.data.type = 'master';
  dpeClient.call(this, arg);
  let nworker = process.env.dpe_WORKERS;
  const tmp = arg.tmp || process.env.dpe_TMP || '/tmp';

  this.started = this.ended = false;
  this.jobId = 0;
  this.bundleDone = true;
  this.env = arg.env || {};
  this.maxShufflePartitions = arg.maxShufflePartitions || process.env.dpe_MAX_SHUFFLE_PARTITIONS;
  this.blockSize = arg.blockSize || process.env.dpe_BLOCK_SIZE || 128;
  this.worker = [];
  this.log = log;
  this.dlog = dlog;
  this.contextId = uuid.v4(); // context id which will be used as scratch directory name

  this.basedir = tmp + '/dpe/' + this.contextId + '/';
  mkdirp.sync(this.basedir + 'tmp');
  mkdirp.sync(this.basedir + 'stream');

  if (process.env.dpe_RANDOM_SEED)
    seedrandom(process.env.dpe_RANDOM_SEED, {global: true});

  // Forward useful env in context for workers
  for (let i = 0; i < envKeys.length; i++)
    this.env[envKeys[i]] = process.env[envKeys[i]];

  this.once('connect', function(data) {
    process.title = 'dpe-master_' + data.devices[0].wsid + ' ' + __filename;
    if (!nworker || nworker > data.devices.length)
      nworker = data.devices.length;
    log('workers:', nworker);
    for (let i = 0; i < nworker; i++) {
      self.worker.push(new Worker(data.devices[i]));
    }
    self.started = true;
  });

  this.on('workerError', function workerError(msg) {
    console.error('Error from worker id', msg.from, msg.args);
  });

  this.on('remoteClose', function getWorkerClose() {
    throw 'Fatal error: unexpected worker exit';
  });

  this.getWorkers = function (callback) {
    if (self.started) return callback();
    this.once('connect', callback);
  };

  function Worker(w) {
    this.uuid = w.uuid;
    this.id = w.id;
    this.ip = w.ip;
    this.ntask = 0;
  }

  Worker.prototype.rpc = function (cmd, args, done) {
    self.request({uuid: this.uuid, id: this.id}, {cmd: cmd, args: args}, done);
  };

  Worker.prototype.send = function (cmd, args) {
    self.send(this.uuid, {cmd: cmd, args: args});
  };

  this.on('request', function (msg) {
    // Protocol to handle stream flow control: reply when data is consumed
    if (msg.data.cmd === 'stream') {
      self.emit(msg.data.stream, msg.data.data, function() {
        try {self.reply(msg);} catch(err) { console.log(err); }
      });
    }
  });

  this.on('sendFile', function (msg) {
    fs.createReadStream(msg.path, msg.opt).pipe(self.createStreamTo(msg));
  });

  this.require = function (obj) {
    const stack = callsite();
    const requester = stack[1].getFileName();
    if (!this.browserify) {
      this.browserify = browserify();
      this.postBundle = '';
    }
    for (let name in obj) {
      let file = obj[name];
      let pathname = resolve.sync(file, {basedir: path.dirname(requester)});
      this.browserify.require(pathname, {expose: file});
      this.postBundle += 'var ' + name + '=require("' + file + '");';
    }
    this.bundleDone = false;
    return this;
  };

  this.end = function () {
    if (global._scn) {
      global._scn--;
      return;
    }
    if (self.ended) return;
    self.ended = true;
    if (this.started) self.set({complete: 1});
    self._end();
  };

  this.datasetIdCounter = 0;  // global dataset id counter

  // Dataset source methods (functions creating a new dataset object with no parent)
  this.parallelize = function (localArray, nPartitions) { return dataset.parallelize(this, localArray, nPartitions);};
  this.range = function (start, end, step, nPartitions) { return dataset.range(this, start, end, step, nPartitions);};
  this.lineStream = function (stream, config) {return new dataset.Stream(this, stream, 'line', config);};
  this.objectStream = function (stream, config) {return new dataset.Stream(this, stream, 'object', config);};
  this.source = function (size, callback, args, nPartitions) {return new dataset.Source(this, size, callback, args, nPartitions);};

  this.textFile = function (file, opt = {} /*, nPartitions*/) {
    const u = url.parse(file);

    if (u.protocol === 's3:')
      return new dataset.TextS3(this, file.slice(5), opt);
    if (u.protocol === 'wasb:')
      return new dataset.TextAzure(this, file.slice(7), opt);
    return new dataset.TextLocal(this, file, opt);
  };

  this.getReadStreamSync = function (fileObj, opt) {
    if (fs.existsSync(fileObj.path))
      return fs.createReadStream(fileObj.path, opt);
    return this.createStreamFrom(fileObj.host, {cmd: 'sendFile', path: fileObj.path, opt: opt});
  };

  this.runTask = function(task, callback) {
    task._start = Date.now();
    task.blockSize = this.blockSize;

    function getLeastBusyWorkerId(/* preferredLocation */) {
      let wid;
      let ntask;
      for (let i = 0; i < self.worker.length; i++) {
        if ((ntask === undefined) || (ntask > self.worker[i].ntask)) {
          ntask = self.worker[i].ntask;
          wid = i;
        }
      }
      return wid;
    }

    function serialize(task) {
      const pindex = {};
      let pleft;
      let pright;
      let nodeId;
      let p = task.pid;
      let node = task.nodes[task.datasetId];
      let part = node.shufflePartitions ? node.shufflePartitions[p] : node.partitions[p];

      // Walk through dataset ancestors to track partition dependencies
      while (part) {
        pindex[part.datasetId] = p;
        node = task.nodes[part.parentDatasetId];
        if (!node) break;
        p = part.parentPartitionIndex;
        part = node.shufflePartitions ? node.shufflePartitions[p] : node.partitions[p];
      }

      // Stringification of dataset: skip any data not relevant to the task
      return JSON.stringify(task, function(key, value) {
        if (key === 'sc') return undefined;
        if (key === '_start') return undefined;
        if (key === 'dependencies') {
          const dep = [];
          for (let i = 0; i < value.length; i++) dep[i] = value[i].id;
          return dep;
        }
        if (key === 'pleft') pleft = value;
        else if (key === 'pright') pright = value;
        else if (key === 'id') nodeId = value;

        // For shufflePartitions (not cartesian), return only the ones used by the task.
        if (key === 'files' && ! value.path) {
          const v = {};
          v[pindex[nodeId]] = value[pindex[nodeId]];
          return v;
        }

        // For cartesian shufflePartitions, return only the ones used by the task
        if (key === 'shufflePartitions' && value[0] && value[0].files && value[0].files.path) {
          const p1 = Math.floor(task.pid / pright);
          const p2 = task.pid % pright + pleft;
          const v = {};
          v[task.pid] = value[task.pid];
          v[p1] = value[p1];
          v[p2] = value[p2];
          return v;
        }
        return (typeof value === 'function') ? value.toString() : value;
      });
    }

    const wid = getLeastBusyWorkerId(task.nodes[task.datasetId].getPreferedLocation(task.pid));

    // Init some environment on the worker the first time we send it a task
    if (!this.worker[wid].init) {
      this.worker[wid].init = true;
      task.env = this.env;
    }

    this.worker[wid].ntask++;
    const str = serialize(task);
    //dlog(task._start, 'task size for worker ' + wid + ':', str.length);
    //log('task', str);
    if (str.length > 1000000) {
      zlib.gzip(str, {chunkSize: 65536}, function (err, res) {
        const filename = task.basedir + 'task-' + uuid.v4() + '.gz';
        fs.writeFile(filename, res, function (err) {
          if (err) throw new Error(err);
          rpc('runztask', wid, filename, function (err, res) {
            self.worker[wid].ntask--;
            callback(err, res, task);
          });
        });
      });
    } else {
      rpc('runTask', wid, str, function(err, res) {
        self.worker[wid].ntask--;
        callback(err, res, task);
      });
    }
  };

  this.onBundle = function (callback) {
    if (this.bundleDone) {
      this.bundle = undefined;
      return callback();
    }
    this.browserify.bundle(function (err, res) {
      self.bundle = res.toString() + self.postBundle;
      self.bundleDone = true;
      callback();
    });
  };

  this.runJob = function(opt, root, action, callback) {
    const jobId = this.jobId++;
    let totalStages;

    this.onBundle(function () {
      self.getWorkers(function () {
        findShuffleStages(function(shuffleStages) {
          let cnt = 0;
          totalStages = shuffleStages.length + 1;
          if (shuffleStages.length === 0) {
            runResultStage();
          } else {
            runShuffleStage(shuffleStages[cnt], cnt, shuffleDone);
          }
          function shuffleDone() {
            if (++cnt < shuffleStages.length) runShuffleStage(shuffleStages[cnt], cnt, shuffleDone);
            else runResultStage();
          }
        });
      });
    });

    function runShuffleStage(stage, stageNum, done) {
      const stageStart = Date.now();

      findNodes(stage, function(nodes) {
        const tasks = [];
        let pid = 0;
        let index = 0;
        let busy = 0;
        let complete = 0;
        let totalFiles = 0;
        let totalSize = 0;
        stage.shufflePartitions = {};

        for (let i = 0; i < stage.dependencies.length; i++) {
          const node = stage.dependencies[i];
          for (let j = 0; j < node.nPartitions; j++)
            stage.shufflePartitions[pid++] = new dataset.Partition(stage.id, pid, node.id, node.partitions[j].partitionIndex);
        }
        stage.nShufflePartitions = pid;

        for (let i = 0; i < stage.nShufflePartitions; i++) {
          tasks.push(new Task({
            basedir: self.basedir,
            bundle: self.bundle,
            jobId: jobId,
            nodes: nodes,
            datasetId: stage.id,
            pid: i
          }));
        }

        function runNext() {
          while (busy <  nworker && index < tasks.length) {
            busy++;
            self.runTask(tasks[index++], function (err, res, task) {
              stage.shufflePartitions[res.pid].files = res.files;
              busy--;
              complete++;
              let n = 0;
              let size = 0;
              for (let f in res.files) {
                n++;
                size += res.files[f].size;
              }
              totalFiles += n;
              totalSize += size;
              dlog(task._start, 'part', task.pid, 'from worker-' + res.workerId, '(' + complete + '/' + tasks.length + '), shuffle out:', n, 'files,', (size/(1<<20)).toFixed(3), 'MB,');
              if (complete === tasks.length) {
                dlog(stageStart, 'pre-shuffle stage', stageNum + 1 + '/' + totalStages, 'done, output:', totalFiles, 'files,', (totalSize / (1 << 20)).toFixed(3), 'MB');
                stage.executed = true;
                return done();
              }
              runNext();
            });
          }
        }

        log('start shuffle stage', stageNum + 1 + '/' + totalStages + ', partitions:', stage.nShufflePartitions);
        runNext();
      });
    }

    function runResultStage() {
      root.resultStart = Date.now();
      root.totalStages = totalStages;

      findNodes(root, function(nodes) {
        const tasks = [];
        log('start result stage', root.totalStages + '/' + root.totalStages + ', partitions:', root.nPartitions);
        for (let i = 0; i < root.nPartitions; i++) {
          tasks.push(new Task({
            basedir: self.basedir,
            bundle: self.bundle,
            jobId: jobId,
            nodes: nodes,
            datasetId: root.id,
            pid: i,
            action: action
          }));
        }
        callback({id: jobId}, tasks);
      });
    }

    function findNodes(node, done) {
      const nodes = {};
      interruptibleTreewalk(node, function cin(n, done) {
        done(n.shuffling && n.executed);
      }, function cout(n, done) {
        n.getPartitions(function() {
          if (nodes[n.id] === undefined) nodes[n.id] = n;
          done();
        });
      }, function() {done(nodes);});
    }

    function findShuffleStages(callback) {
      const stages = [];
      interruptibleTreewalk(root, function cin(n, done) {
        if (n.shuffling && !n.executed) stages.unshift(n);
        done(n.shuffling && n.executed);  // stage boundary are shuffle nodes
      }, function cout(n, done) {done();}, function() {callback(stages);});
    }

    function interruptibleTreewalk(n, cin, cout, done) {
      cin(n, function(uturn) { // if uturn equals true the subtree under node won't be treewalked
        if (!uturn) {
          let nDependencies = 0;
          for (let i = 0; i < n.dependencies.length; i++)
            interruptibleTreewalk(n.dependencies[i], cin, cout, function() {
              if (++nDependencies === n.dependencies.length) cout(n, done);
            });
          if (n.dependencies.length === 0) cout(n, done);
        } else cout(n, done);
      });
    }
  };

  function rpc(cmd, workerNum, args, callback) {
    self.request(self.worker[workerNum], {cmd: cmd, args: args, master_uuid: self.uuid, worker: self.worker}, callback);
  }
}

let log;
let dlog;

if (process.env.dpe_DEBUG) {
  log =  function log() {
    const args = Array.prototype.slice.call(arguments);
    args.unshift('[master ' + (Date.now() - start) / 1000 + 's]');
    console.error.apply(null, args);
  };
  dlog = function dlog() {
    const args = Array.prototype.slice.call(arguments);
    const now = Date.now();
    const lstart = args.shift();
    args.unshift('[master ' + (now - start) / 1000 + 's]');
    args.push('in ' + (now - lstart) / 1000 + 's');
    console.error.apply(null, args);
  };
} else {
  dlog = log = function nop() {};
}
