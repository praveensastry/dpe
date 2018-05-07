process.env.dpe_RANDOM_SEED = 'dpe';

const t = require('tape');
const sc = require('dpe').context();

t.test('sample', function (t) {
  t.plan(1);

  sc.env.dpe_RANDOM_SEED = process.env.dpe_RANDOM_SEED;

  sc.range(100)
    .sample(false, 0.1)
    .collect(function(err, res) {
      console.log(res);
      t.ok(res.length > 0 && res.length < 20);
      sc.end();
    });
});
