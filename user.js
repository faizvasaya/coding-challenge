const db = require('./database');

const init = async () => {
  await db.run('CREATE TABLE Users (id INTEGER PRIMARY KEY AUTOINCREMENT, name varchar(32));');
  await db.run('CREATE TABLE Friends (id INTEGER PRIMARY KEY AUTOINCREMENT, userId int, friendId int);');
  // Created B-tree Index for efficient 1st and 2nd connection search
  await db.run('CREATE INDEX idx_friends_userId_friendId ON Friends(userId, friendId);');
  // Created B-tree Index for efficient name based search
  await db.run('CREATE INDEX idx_users_name ON Users(name);');

  const users = [];
  const names = ['foo', 'bar', 'baz'];
  for (i = 0; i < 27000; ++i) {
    let n = i;
    let name = '';
    for (j = 0; j < 3; ++j) {
      name += names[n % 3];
      n = Math.floor(n / 3);
      name += n % 10;
      n = Math.floor(n / 10);
    }
    users.push(name);
  }
  const friends = users.map(() => []);
  for (i = 0; i < friends.length; ++i) {
    const n = 10 + Math.floor(90 * Math.random());
    const list = [...Array(n)].map(() => Math.floor(friends.length * Math.random()));
    list.forEach((j) => {
      if (i === j) {
        return;
      }
      if (friends[i].indexOf(j) >= 0 || friends[j].indexOf(i) >= 0) {
        return;
      }
      friends[i].push(j);
      friends[j].push(i);
    });
  }
  console.log("Init Users Table...");
  await Promise.all(users.map((un) => db.run(`INSERT INTO Users (name) VALUES ('${un}');`)));
  console.log("Init Friends Table...");
  await Promise.all(friends.map((list, i) => {
    return Promise.all(list.map((j) => db.run(`INSERT INTO Friends (userId, friendId) VALUES (${i + 1}, ${j + 1});`)));
  }));
  console.log("Ready.");
}
module.exports.init = init;

const search = async (req, res) => {
  const query = req.params.query;
  const userId = parseInt(req.params.userId);

  try {
    const results = await db.all(
      `
      SELECT u.id, u.name,
      MIN(
          CASE
              WHEN firstLevel.friendId IS NOT NULL THEN 1
              WHEN secondLevel.friendId IS NOT NULL THEN 2
              ELSE 0
          END
      ) AS connection
      FROM Users u
      LEFT JOIN Friends firstLevel ON u.id = firstLevel.friendId AND firstLevel.userId = ?
      LEFT JOIN Friends secondLevel ON u.id = secondLevel.friendId AND secondLevel.userId IN (
        SELECT friendId FROM Friends WHERE userId = ?
      )
      WHERE u.name LIKE ?
      GROUP BY u.id, u.name
      LIMIT 20;
      `
    , [userId, userId, `${query}%`]);
    res.statusCode = 200;
    res.json({
      success: true,
      users: results
    });
  } catch(err) {
    res.statusCode = 500;
    res.json({ success: false, error: err });
  }
}
module.exports.search = search;

const friend = async (req, res) =>{
  const userId = parseInt(req.params.userId);
  const friendId = parseInt(req.params.friendId);

  try{
    await db.run('BEGIN TRANSACTION;');
      await db.run(`INSERT INTO Friends (userId, friendId) VALUES (?, ?);`, [userId, friendId]);
      await db.run(`INSERT INTO Friends (userId, friendId) VALUES (?, ?);`, [friendId, userId]);
    await db.run('COMMIT;');
    res.statusCode = 200;
    res.json({
      success: true,
    });
  }catch(error) {
    await db.run('ROLLBACK;');
    console.error(error);
    res.statusCode = 500;
    res.json({
      success: false,
      error: err
    });
  }
}
module.exports.friend = friend;

const unFriend = async (req, res) =>{
  const userId = parseInt(req.params.userId);
  const friendId = parseInt(req.params.friendId);

  try{
    await db.run('BEGIN TRANSACTION;');
      await db.run(`DELETE FROM Friends WHERE userId = ? AND friendId = ?;`, [userId, friendId]);
      await db.run(`DELETE FROM Friends WHERE userId = ? AND friendId = ?;`, [friendId, userId]);
    await db.run('COMMIT;');
    res.statusCode = 200;
    res.json({
      success: true,
    });
  }catch(error) {
    await db.run('ROLLBACK;');
    console.error(error);
    res.statusCode = 500;
    res.json({
      success: false,
      error: err
    });
  }
}
module.exports.unFriend = unFriend;