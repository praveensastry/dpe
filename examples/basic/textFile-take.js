#!/usr/bin/env node

const sc = require('@praveensastry/dpe').context();

const file = __dirname + '/kv.data';

sc.textFile(file)
  .take(1)
  .then(function (res) {
    console.log(res);
    sc.end();
  });
