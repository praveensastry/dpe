#!/usr/bin/env node

process.env.dpe_RANDOM_SEED = 'dpe';

const sc = require('dpe').context();

sc.range(100)
  .sample(false, 0.1)
  .collect(function(err, res) {
    console.log(res);
    sc.end();
  });
