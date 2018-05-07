#!/usr/bin/env node

const sc = require('dpe').context();
const input = sc.textFile('s3://dpe-demo/datasets/*-ny.json.gz');
//const input = sc.textFile('s3://dpe-demo/datasets/restaurants-ny.json.gz');
//const input = sc.textFile('s3://dpe-demo/datasets/restaurants-ny.json');
//const s = input.stream();
//s.pipe(process.stdout);
//s.on('end', sc.end);

input.count(function (err, res) {
  console.log(res);
  sc.end();
});
