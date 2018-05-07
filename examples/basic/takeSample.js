#!/usr/bin/env node

process.env.dpe_RANDOM_SEED = 'dpe';

const sc = require('@praveensastry/dpe').context();

sc.range(100)
  .takeSample(false, 4, function(err, res) {
    console.log(res);
    sc.end();
  });
