#!/usr/bin/env node

const sc = require('@praveensastry/dpe').context();

sc.env.MY_VAR = 'Hello';

sc.range(5).
  map(function (i) {return process.env.MY_VAR + i;}).
  collect(function (err, res) {
    console.log(res);
    sc.end();
  });
