#!/usr/bin/env node

const sc = require('dpe').context();

const file = process.argv[2] || '/etc/hosts';

sc.textFile(file)
  .flatMap(line => line.split(' '))
  .map(word => [word, 1])
  .reduceByKey((a, b) => a + b, 0)
  .count()
  .then(function (res) {
    console.log(res);
    sc.end();
  });
