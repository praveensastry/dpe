#!/usr/bin/env node

const sc = require('dpe').context();

sc.textFile(process.argv[2]).stream({end: true}).pipe(process.stdout);
