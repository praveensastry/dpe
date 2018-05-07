const t = require('tape');
const sc = require('dpe').context();

const data = [4, 6, 10, 5, 1, 2, 9, 7, 3, 0];
const nPartitions = 3;

function keyFunc(data) {return data;}

t.test('sortBy', function (t) {
  t.plan(1);

  sc.parallelize(data, nPartitions)
    .sortBy(keyFunc)
    .collect(function(err, res) {
      t.deepEqual(res, [0, 1, 2, 3, 4, 5, 6, 7, 9, 10]);
      sc.end();
    });
});
