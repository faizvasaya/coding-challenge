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
  // @change: Create a table to store user relationships along with its connection levels. This is the trick to optimize search queries
  await db.run(`
    CREATE TABLE UserConnections (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      userId INT,
      friendId INT,
      connectionLevel INT,
      FOREIGN KEY (userId) REFERENCES Users(id),
      FOREIGN KEY (friendId) REFERENCES Users(id)
  );`);
  // @change: Created B-tree Index for efficient 1st and 2nd level connection search
  await db.run('CREATE INDEX idx_friends_userId_friendId ON Friends(userId, friendId);');
  // @change: Created B-tree Index for efficient name based search
  await db.run('CREATE INDEX idx_users_name ON Users(name);');
  // @change: Created B-tree Index for efficient search
  await db.run('CREATE INDEX idx_userconnections_userId_friendId ON UserConnections(userId, friendId);');
  // @change: Created B-tree Index for efficient search
  await db.run('CREATE INDEX idx_userconnections_userId_connectionLevel ON UserConnections(userId, connectionLevel);');
  // @change: Created B-tree Index for efficient search
  await db.run('CREATE INDEX idx_userconnections_userId_friendId_connectionLevel ON UserConnections(userId, friendId, connectionLevel);');

  const users = [];
  const names = ['foo', 'bar', 'baz'];
  // Loading for 27000 users would take around 2 hours. May also crash your system
  // hence try with 500 or 1K users first to check the implementation.
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
    return Promise.all(
      list.map((j) =>{
          return db.run(`INSERT INTO Friends (userId, friendId) VALUES (${i + 1}, ${j + 1});`);
        }));
  }));

  // @change: Load 1st level connections in UserConnections table
  await Promise.all(friends.map((list, i) => {
    return Promise.all(
      list.map((j) =>{
          return  db.run(`INSERT INTO UserConnections (userId, friendId, connectionLevel) VALUES (${i + 1}, ${j + 1}, 1);`);
        }));
  }));

  // @change: Load 2nd level connections in UserConnections table. This consumes a lot of time
  await Promise.all(friends.map((list, i) => {
    return Promise.all(
      list.map((j) =>{
        return db.run(`
        INSERT INTO UserConnections (userId, friendId, connectionLevel)
        SELECT ${i + 1}, friendId, 2
        FROM UserConnections
        WHERE userId = ${j + 1} AND connectionLevel = 1 AND friendId NOT IN (
          SELECT friendId from UserConnections WHERE userId = ${i + 1}
        ) AND friendId != ${i + 1};
    `);
    }));
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

  try {
    const results = await db.all(
      `
      SELECT u.id, u.name,
      COALESCE(
        (SELECT connectionLevel FROM UserConnections WHERE userId = ? AND friendId = u.id),
        0
      ) AS connection
      FROM Users u
      WHERE u.name LIKE ?;
      `
    , [userId, `${query}%`]);
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

// @change: Added route for Add Friend
const friend = async (req, res) =>{
  const userId = parseInt(req.params.userId);
  const friendId = parseInt(req.params.friendId);

  try{
    await db.run('BEGIN TRANSACTION;');
      // Add the 1st level connections
      await db.run(`INSERT INTO Friends (userId, friendId) VALUES (?, ?), (?, ?);`, [userId, friendId, friendId, userId]);
      // Delete all 2nd level connection of A to B
      await db.run(`DELETE FROM UserConnections 
          WHERE (userId = ? AND friendId = ? AND connectionLevel != 1) 
          OR (userId = ? AND friendId = ? AND connectionLevel != 1);
          `, [userId, friendId, friendId, userId]);
      // Insert 1st level connection between A -> B and B -> A
      await db.run(`INSERT INTO UserConnections (userId, friendId, connectionLevel) 
                      VALUES (?, ?, ?), (?, ?, ?);`, [userId, friendId, 1, friendId, userId, 1]);

      // Add the 2nd level connections between A and B
      await db.run(`
          INSERT INTO UserConnections (userId, friendId, connectionLevel)
          SELECT ?, friendId, 2
          FROM UserConnections
          WHERE userId = ? AND connectionLevel = 1 AND friendId NOT IN (
            SELECT friendId from UserConnections WHERE userId = ?
          ) AND friendId != ?
          UNION ALL
          SELECT ?, friendId, 2
          FROM UserConnections
          WHERE userId = ? AND connectionLevel = 1 AND friendId NOT IN (
            SELECT friendId from UserConnections WHERE userId = ?
          ) AND friendId != ?;
      `, [userId, friendId, userId, userId,
          friendId, userId, friendId, friendId]);

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
    // Remove the 1st level connection
    await db.run(`
        DELETE FROM Friends 
        WHERE (userId = ? AND friendId = ?) OR (userId = ? AND friendId = ?);
    `, [userId, friendId, friendId, userId]);

    // Remove the 1st level connection
    await db.run(`DELETE FROM UserConnections
        WHERE (userId = ? AND friendId = ? AND connectionLevel = 1) 
        OR (userId = ? AND friendId = ? AND connectionLevel = 1);
    `,[userId, friendId, friendId, userId]);

    // Find such B's 1st level connections which are a second level connections with A and delete them
    // Find such A's 1st level connections which are a second level connections with B and delete them
    // This is time consuming. In real world we can make it async
    await db.run(`
        DELETE FROM UserConnections
        WHERE (userId = ? 
          AND friendId IN (SELECT friendId FROM UserConnections WHERE userId = ? AND connectionLevel = 1) 
          AND connectionLevel = 2
        )
        OR (userId = ? 
          AND friendId IN (SELECT friendId FROM UserConnections WHERE userId = ? AND connectionLevel = 1)
          AND connectionLevel = 2);
    `, [userId, friendId, friendId, userId]);  

    // Reestablish 2nd level connections of A and B
    await db.run(`
        INSERT INTO UserConnections (userId, friendId, connectionLevel) 
        SELECT DISTINCT(friendId), ?, 2 FROM UserConnections
            WHERE friendId NOT IN (
                SELECT friendId FROM UserConnections WHERE userId = ?
            ) AND friendId != ?;
    `, [friendId, friendId, friendId]);
    
    // Reestablish 2nd level connections of B and A
    await db.run(`
        INSERT INTO UserConnections (userId, friendId, connectionLevel)
        SELECT DISTINCT(friendId), ?, 2 FROM UserConnections
            WHERE friendId NOT IN (
                SELECT friendId FROM UserConnections WHERE userId = ?
            ) AND friendId != ?;
    `, [userId, userId, userId]); 
    
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