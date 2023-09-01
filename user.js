const db = require('./database');

const init = async () => {
  // @change: Added a NOT NULL constraint for the name column of Users table
  await db.run('CREATE TABLE Users (id INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR(32) NOT NULL);');
  // @change: Modified the Friends table to add constraints
  await db.run(`
    CREATE TABLE Friends (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      userId INT,
      friendId INT,
      UNIQUE (userId, friendId),
      FOREIGN KEY (userId) REFERENCES Users(id),
      FOREIGN KEY (friendId) REFERENCES Users(id)
    );`);
  // @change: Created B-tree Index for efficient name based search
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

/**
 * 
 * @change: Modified the search query for 1st and 2nd level connections
 */
const search = async (req, res) => {
  const query = req.params.query;
  const userId = parseInt(req.params.userId);

  /**
   * Explanation of search query
   * The connectionLevels is a recursive Common Table Expression which has a base condition
   * of all the friends who are a friend of user who is searching. Friends found in the base
   * condition are assigned a level of 1.
   * The recursive query in the CTE fetches the friends of each friend found in base condition
   * and assigns it a level of 2. The recursion stops at level 2 and it serves as an exit condition.
   * 
   * Now, there could be a possibility where the same friend is a 1st level and a 2nd level connection.
   * In order to tackle that scenario, the minimumConnectionLevels CTE further groups the users by id
   * and selects on the minimum level for each of the friends of the user.
   * 
   * Finally, in the main query, we are searching the users by name and assigning 0 for the connections
   * which are not a 1st and 2nd level connections as they don't exist in minimumConnectionLevels.
   * 
   * NOTE: In order to get it for more levels of connections, replace `cl.level < 2` with `cl.level < 3` or 
   * `cl.level < 4`. However, increasing the levels will degrade the performance due to increase in recursion levels.
   * I would prefer using the closure table approach for fetching 3rd and 4th. In closure table approach we would 
   * readily store each users connection and their minimum levels of connections with each user so that it could 
   * be fetched directly without recursions.
   */
  try {
    const results = await db.all(
      `
      WITH RECURSIVE connectionLevels AS (
        SELECT friendId AS id, 1 AS level
        FROM Friends
        WHERE userId = ?
    
        UNION ALL

        SELECT f.friendId AS id, cl.level + 1 AS level
        FROM connectionLevels cl, Friends f
        WHERE f.userId = cl.id AND cl.level < 2
    ),
    minimumConnectionLevels AS (
        SELECT id, MIN(level) AS level
        FROM connectionLevels
        GROUP BY id
    )
    SELECT id, name,
        CASE
            WHEN (SELECT level FROM minimumConnectionLevels WHERE id = Users.id) IS NULL THEN 0
            ELSE (SELECT level FROM minimumConnectionLevels WHERE id = Users.id)
        END AS connection
    FROM Users
    WHERE name LIKE ?;
    `
    , [userId, `${query}%`]);
    res.statusCode = 200;
    res.json({
      success: true,
      users: results
    });
  } catch(err) {
    console.log(err);
    res.statusCode = 500;
    res.json({ success: false, error: err });
  }
}
module.exports.search = search;

// @change: Added route for Add Friend
const friend = async (req, res) =>{
  const userId = parseInt(req.params.userId);
  const friendId = parseInt(req.params.friendId);

  try{
    await db.run('BEGIN TRANSACTION;');
      await db.run(`INSERT INTO Friends (userId, friendId) VALUES (?, ?), (?, ?);`, [userId, friendId, friendId, userId]);
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
      error: error
    });
  }
}
module.exports.friend = friend;

// @change: Added route to Remove Friend
const unFriend = async (req, res) =>{
  const userId = parseInt(req.params.userId);
  const friendId = parseInt(req.params.friendId);

  try{
    await db.run('BEGIN TRANSACTION;');
      await db.run(`
        DELETE FROM Friends 
        WHERE (userId = ? AND friendId = ?) OR (userId = ? AND friendId = ?);
    `, [userId, friendId, friendId, userId]);
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
      error: error
    });
  }
}
module.exports.unFriend = unFriend;