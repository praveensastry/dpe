#!/usr/bin/env node

const sc = require('dpe').context();

sc.parallelize([[1, 2], [3, 4], [3, 6]])
  .countByKey()
  .then(function(res) {
    console.log(res);
    res.sort();
    console.assert(JSON.stringify(res) === JSON.stringify([[1, 1], [3, 2]]));
    sc.end();
  });
