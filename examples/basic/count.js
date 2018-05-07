#!/usr/bin/env node

const sc = require('dpe').context();

sc.parallelize([1, 2, 3, 4]).count()
  .then(function(data) {
    console.log(data);
    console.assert(data === 4);
    sc.end();
  });
