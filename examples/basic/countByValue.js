#!/usr/bin/env node

const sc = require('dpe').context();

const data = [[1, 1], [1, 1], [2, 3], [2, 4], [3, 5]];

const nPartitions = 1;

function valueFlatMapper(e) {
  const out = [];
  for (let i = e; i <= 5; i++) out.push(i);
  return out;
}

sc.parallelize(data, nPartitions)
  .flatMapValues(valueFlatMapper)
  .countByValue()
  .then(function(res) {
    console.log(res);
    console.assert(JSON.stringify(res) === JSON.stringify([[[1, 1], 2], [[1, 2], 2], [[1, 3], 2], [[1, 4], 2], [[1, 5], 2], [[2, 3], 1], [[2, 4], 2], [[2, 5], 2], [[3, 5], 1]]));
    sc.end();
  });
