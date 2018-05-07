const t = require('tape');
const sc = require('@praveensastry/dpe').context();

t.test('count callback', function (t) {
  t.plan(1);

  sc.range(6)
    .count(function (err, res) {
      t.equal(res, 6);
    });
});

t.test('count promise', function (t) {
  t.plan(1);

  sc.range(6)
    .count()
    .then(function (res) {
      t.equal(res, 6);
      sc.end();
    });
});
