#!/usr/bin/env node

const dpe = require('@praveensastry/dpe');
const sc = dpe.context();

const data = [['hello', 1], ['world', 1], ['hello', 2], ['world', 2], ['cedric', 3]];

sc.parallelize(data)
  .partitionBy(new dpe.HashPartitioner(3))
  .collect(function(err, res) {
    console.log(res);
    console.assert(JSON.stringify(res) === JSON.stringify([['world', 1], ['world', 2],['hello', 1],['hello', 2],['cedric', 3]])); 
    sc.end();
  });
