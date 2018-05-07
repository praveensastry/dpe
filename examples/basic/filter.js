#!/usr/bin/env node

const sc = require('@praveensastry/dpe').context();

function reducer(a, b) {a.push(b); return a;}
function combiner(a, b) {return a.concat(b);}

function filter(a) {return a % 2;}

sc.parallelize([1, 2, 3, 4])
  .filter(filter)
  .aggregate(reducer, combiner, [], function(err, res) {
    console.log(res);
    console.assert(JSON.stringify(res) === JSON.stringify([1, 3])); 
    sc.end();
  });
