#!/usr/bin/env node

const sc = require('dpe').context();

//sc.range(900).save('/tmp/truc', {gzip: true}, (err, res) => {
//sc.range(900).save('/tmp/truc', {stream: true}, (err, res) => {
//sc.range(900).save('s3://dpe-demo/test/s1', {gzip: false, stream: true}, (err, res) => {
sc.range(900).save('/tmp/truc', {gzip: true, stream: true}, (err, res) => {
  console.log(res);
  sc.end();
});
