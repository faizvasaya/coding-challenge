const sqlite3 = require('sqlite3');

const db = new sqlite3.Database(':memory:');

/**
 * Changes: Added a parameter to ensure that the SQL statements are 
 * prepared using placeholders to prevent SQL injection.
 * Also corrected the callback to get the lastId of the inserted/updated row
 */
const run = (query, parameters = []) => {
  return new Promise((resolve, reject) => {
    db.run(query, parameters, function (err) {
      if (err) {
        reject(err)
      } else {
        resolve({
          lastId: this.lastID,
          changes: this.changes,
        });
      }
    });
  });
}
module.exports.run = run;

/**
 * Changes: Added a parameter to ensure that the SQL statements are 
 * prepared using placeholders to prevent SQL injection.
 */
const all = (query, parameters = []) => {
  return new Promise((resolve, reject) => {
    db.all(query,parameters, (err, results) => {
      if (err) {
        reject(err)
      } else {
        resolve(results);
      }
    });
  });
}
module.exports.all = all;