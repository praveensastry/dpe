#!/usr/bin/env node

const sc = require('@praveensastry/dpe').context();

const da1 = sc.parallelize([[10, 1], [20, 2]]);
const da2 = sc.parallelize([[10, 'world'], [30, 3]]);

da1.rightOuterJoin(da2)
  .collect(function(err, res) {
    console.log(res);
    res.sort();
    console.assert(JSON.stringify(res) === JSON.stringify([[10, [1, 'world']], [30, [null, 3]]])); 
    sc.end();
  });
