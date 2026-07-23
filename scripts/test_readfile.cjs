const { DuckDBInstance } = require('@duckdb/node-api');
const fs = require('fs');
const path = require('path');
const os = require('os');

(async () => {
  const inst = await DuckDBInstance.create(':memory:');
  const conn = await inst.connect();
  
  const tmpFile = path.join(os.tmpdir(), 'test_export.parquet');
  
  await conn.run('CREATE TABLE t AS SELECT range AS id FROM range(10000);');
  await conn.run(`COPY (SELECT * FROM t) TO '${tmpFile}' (FORMAT PARQUET);`);
  
  // Test 1: sync
  console.time('readFileSync');
  const bufSync = fs.readFileSync(tmpFile);
  console.timeEnd('readFileSync');
  console.log('sync size:', bufSync.length);
  
  // Test 2: async
  console.time('readFileAsync');
  const bufAsync = await fs.promises.readFile(tmpFile);
  console.timeEnd('readFileAsync');
  console.log('async size:', bufAsync.length);
  console.log('Same:', Buffer.compare(bufSync, bufAsync) === 0);
  
  fs.unlinkSync(tmpFile);
  conn.closeSync();
})();
