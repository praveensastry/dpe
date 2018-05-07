#!/usr/bin/env node

const sc = require('dpe').context();

sc.parallelize([ 1, 2, 3, 1, 4, 3, 5 ]).
  distinct().
  collect(function(err, res) {
    console.log(res);
    res.sort();
    console.assert(JSON.stringify(res) === JSON.stringify([1, 2, 3, 4, 5]));
    sc.end();
  });
