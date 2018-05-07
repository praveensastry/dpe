#!/usr/bin/env node

const sc = require('@praveensastry/dpe').context();

const data = [['hello', 1], ['hello', 1], ['world', 1]];
const nPartitions = 2;

const init = 0;

function reducer(a, b) {return a + b;}
function combiner(a, b) {return a + b;}

sc.parallelize(data, nPartitions)
  .aggregateByKey(reducer, combiner, init)
  .collect(function(err, res) {
    console.log(res);
    console.assert(JSON.stringify(res) === JSON.stringify([['hello', 2], ['world', 1]]));
    sc.end();
  });
