#!/usr/bin/env node

const sc = require('dpe').context();

function sum(a, b) {return a + b;}

sc.parallelize([1, 2, 3, 4], 2)
  .reduce(sum, 0).then(function(res) {
    console.log(res);
    console.assert(res === 10);
    sc.end();
  });
