const t = require('tape');
const sc = require('@praveensastry/dpe').context();

t.test('countByValue callback', function (t) {
  t.plan(1);

  sc.parallelize([[1, 2], [3, 4], [1, 2], [3, 4]])
    .countByValue(function (err, res) {
      t.deepEqual(res.sort(), [[[1, 2], 2], [[3, 4], 2]]);
    });
});

t.test('countByValue promise', function (t) {
  t.plan(1);

  sc.parallelize([[1, 2], [3, 4], [1, 2], [3, 4]])
    .countByValue()
    .then(function(res) {
      t.deepEqual(res.sort(), [[[1, 2], 2], [[3, 4], 2]]);
      sc.end();
    });
});
