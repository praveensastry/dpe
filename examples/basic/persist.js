#!/usr/bin/env node

const sc = require('@praveensastry/dpe').context();

function reducer(a, b) {a.push(b); return a;}
function combiner(a, b) {return a.concat(b);}

const a = sc.parallelize([1, 2, 3, 4], 2).persist();

a.aggregate(reducer, combiner, [], function(err, res) {
  console.log('First Time !');
  console.log(res);

  a.aggregate(reducer, combiner, [], function(err, res) {
    console.log('\nSecond Time !');
    console.log(res);
    sc.end();
  });
});
