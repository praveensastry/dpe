#!/usr/bin/env node

const sc = require('@praveensastry/dpe').context();

sc.textFile(__dirname + '/xxx.gz').count().then(function (res) {console.log(res); sc.end();});
