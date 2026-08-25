// Runs the pure helpers of the admin UI (ui/app.js) under node, without a DOM.
// Usage: node uilogic.mjs <path to app.js>. Prints nothing and exits 0 when
// every check passes.
import fs from 'node:fs';
import vm from 'node:vm';

const source = fs.readFileSync(process.argv[2], 'utf8');
if (!/\nstart\(\);\s*$/.test(source)) throw new Error('app.js no longer ends with start();');
const exported = ['num', 'parseDuration', 'protoDuration', 'durationNs', 'formatDuration', 'parseStages', 'splitList', 'fmtPercent'];
const context = vm.createContext({});
vm.runInContext(source.replace(/\nstart\(\);\s*$/, '\n') + `\nglobalThis.t = { ${exported.join(', ')} };`, context);
const t = context.t;

const failures = [];
const same = (got, want, what) => {
  if (JSON.stringify(got) !== JSON.stringify(want)) failures.push(`${what}: got ${JSON.stringify(got)}, want ${JSON.stringify(want)}`);
};
const throws = (fn, what) => {
  try {
    fn();
    failures.push(`${what}: did not throw`);
  } catch {
    // expected
  }
};

// int64s arrive as strings.
same(t.num('7'), 7, 'num of a revision string');
same(t.num('12') > t.num('9'), true, 'revisions compare numerically, not as text');
same(t.num(undefined), 0, 'num of an unset revision');
same(t.num('0'), 0, 'num of "0"');

same(t.parseDuration('30s'), 30e9, '30s');
same(t.parseDuration('10m'), 600e9, '10m');
same(t.parseDuration('1h30m'), 5400e9, '1h30m');
same(t.parseDuration('250ms'), 250e6, '250ms');
same(t.parseDuration('1.5s'), 1.5e9, '1.5s');
same(t.parseDuration('0'), 0, '0');
same(t.parseDuration(' 5s '), 5e9, 'padded 5s');
for (const bad of ['', '10', 'm', '-5s', '5x', '1e3s', '5s garbage', '5 s']) throws(() => t.parseDuration(bad), `parseDuration(${JSON.stringify(bad)})`);

// The wire format of a duration is seconds with up to nine decimals.
same(t.protoDuration(30e9), '30s', 'protoDuration 30s');
same(t.protoDuration(1.5e9), '1.5s', 'protoDuration 1.5s');
same(t.protoDuration(250e6), '0.25s', 'protoDuration 250ms');
same(t.protoDuration(1), '0.000000001s', 'protoDuration 1ns');
same(t.durationNs('600s'), 600e9, 'durationNs 600s');
same(t.durationNs('1.500s'), 1.5e9, 'durationNs 1.500s');
same(t.durationNs('0.000000001s'), 1, 'durationNs 1ns');
same(t.durationNs(null), 0, 'durationNs of an unset duration');
same(t.durationNs(undefined), 0, 'durationNs of a missing duration');
for (const ns of [1, 999, 250e6, 1e9, 1.5e9, 90.5e9, 3600e9, 5400e9 + 1e6]) {
  same(t.durationNs(t.protoDuration(ns)), ns, `durationNs(protoDuration(${ns}))`);
  same(t.parseDuration(t.formatDuration(ns)), ns, `parseDuration(formatDuration(${ns}))`);
}

same(
  t.parseStages('1:10m,5:10m, 25:30m ,100'),
  [
    { percent: 1, duration: 600e9 },
    { percent: 5, duration: 600e9 },
    { percent: 25, duration: 1800e9 },
    { percent: 100, duration: 0 },
  ],
  'the documented stage syntax',
);
same(t.parseStages('0.5%:1h'), [{ percent: 0.5, duration: 3600e9 }], 'percent sign and fraction');
same(t.parseStages('.5'), [{ percent: 0.5, duration: 0 }], 'leading dot');
for (const bad of ['', ',', 'x', '5:', '5:10', '5:10m:1', ':10m', '0x10', '1e1', 'Infinity', '-5', '5%%']) {
  throws(() => t.parseStages(bad), `parseStages(${JSON.stringify(bad)})`);
}

// An allowlist entry is never split, so saving a flag cannot change it.
same(t.splitList('a\nb\n\n a \nb\n'), ['a', 'b'], 'splitList trims and drops blanks and repeats');
same(t.splitList('user,with,commas\nx'), ['user,with,commas', 'x'], 'splitList keeps commas');
same(t.splitList(''), [], 'splitList of nothing');

same(t.fmtPercent(33.333), '33.33%', 'fmtPercent');
same(t.fmtPercent(0), '0%', 'fmtPercent 0');
same(t.fmtPercent(undefined), '0%', 'fmtPercent unset');

if (failures.length > 0) {
  console.error(failures.join('\n'));
  process.exit(1);
}
