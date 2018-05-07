#!/usr/bin/env node

const sc = require('@praveensastry/dpe').context();

sc.range(6).map((a) => a*a).reduce((a,b) => a+b, 0)
  .then(function (res) {
    console.log(res);
    console.assert(res === 55);
    sc.end();
  });
