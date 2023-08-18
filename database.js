const sqlite3 = require('sqlite3');

const db = new sqlite3.Database(':memory:');

/**
 * Changes: Added a parameter to ensure that the SQL statements are 
 * prepared using placeholders to prevent SQL injection.
 */
const run = (query, parameters = []) => {
  return new Promise((resolve, reject) => {
    db.run(query, parameters, (err, results) => {
      if (err) {
        reject(err)
      } else {
        resolve(results);
      }
    });
  });
}
module.exports.run = run;

const all = (query) => {
  return new Promise((resolve, reject) => {
    db.all(query, (err, results) => {
      if (err) {
        reject(err)
      } else {
        resolve(results);
      }
    });
  });
}
module.exports.all = all;