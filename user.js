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
  /**
   * @change: Created a closure table to store all the levels of relationships for each user to efficiently fetch different levels
   * of relationship based on user's search query
   */
  await db.run(`
    CREATE TABLE FriendsClosure (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      parent int,
      child int,
      depth int,
      FOREIGN KEY (parent) REFERENCES Users(id),
      FOREIGN KEY (child) REFERENCES Users(id)
    );`);
  // @change: Created B-tree Index for efficient name based search on Users table
  await db.run('CREATE INDEX idx_users_name ON Users(name);');
  // @change: Created B-tree Index for efficient 1st and 2nd level connection search on Friends table
  await db.run('CREATE INDEX idx_friends_userId_friendId ON Friends(userId, friendId);');
  // @change: Created B-tree Index for efficient search for search while adding new friend
  await db.run('CREATE INDEX idx_friends_userId ON Friends(userId);');
  // @change: Created B-tree Index for efficient search of parent child while unfriending a user
  await db.run('CREATE INDEX idx_friendsClosure_parent_child ON FriendsClosure(parent, child);');
  // @change: Created B-tree Index for efficient search of parent on FriendsClosure table while searching 
  await db.run('CREATE INDEX idx_friendsClosure_parent ON FriendsClosure(parent);');
  // @change: Created B-tree Index for efficient search of depth, parent and child on FriendsClosure table while searching 
  await db.run('CREATE INDEX idx_friendsClosure_depth_parent_child ON FriendsClosure(depth, parent, child);');
  // @change: Created B-tree Index for efficient search of depth and child on FriendsClosure table while searching 
  await db.run('CREATE INDEX idx_friendsClosure_depth_child ON FriendsClosure(depth, child);');
  

  const users = [];
  const names = ['foo', 'bar', 'baz'];
  // @change: Changed it to 1000 for testing 3rd and 4th level connection, worked okay. The queries did not perform well when increased
  // to 27000 for 3rd and 4th connection
  for (i = 0; i < 1000; ++i) {
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

  await db.run(`
    INSERT INTO FriendsClosure (parent, child, depth)
    SELECT userId, friendId, 1
    FROM Friends;
  `);

  await db.run(`
    WITH RECURSIVE PopulateFriendsClosure AS (
      SELECT userId, friendId, 1 AS depth
      FROM Friends
      UNION ALL
      SELECT pc.userId, ft.friendId, pc.depth + 1
      FROM PopulateFriendsClosure pc
      JOIN Friends ft ON pc.userId = ft.friendId
      WHERE pc.depth < 4 AND pc.userId != pc.friendId 
    )
    INSERT INTO FriendsClosure (parent, child, depth)
    SELECT userId, friendId, depth
    FROM PopulateFriendsClosure WHERE userId != friendId;
  `)

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
    WITH RECURSIVE MatchedUsers AS (
        SELECT id
        FROM Users
        WHERE name LIKE ?
    ),
    Connections AS (
            SELECT parent, child, depth
            FROM FriendsClosure
            WHERE depth <= 4 AND parent = ? AND child IN (
                SELECT id FROM MatchedUsers
            )
            UNION
            SELECT c.parent, fc.child, c.depth + 1
            FROM Connections c
            JOIN FriendsClosure fc ON c.child = fc.parent
            WHERE c.depth <=  4 AND fc.child IN (
                SELECT id FROM MatchedUsers
            )
        )
          SELECT u.id, u.name, MIN(c.depth) as connection FROM Connections c
          JOIN Users u ON c.child = u.id
          WHERE c.depth <= 4
          GROUP BY c.child
          LIMIT 20;
      `
    , [`${query}%`, userId]);
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
      
    await db.run(`INSERT INTO Friends (userId, friendId) VALUES (?, ?), (?, ?);`, [userId, friendId, friendId, userId]);
      
      await db.run(`
        WITH RECURSIVE Friendship AS (
          SELECT ? AS parent, ? AS child, 1 AS depth
          UNION ALL
          SELECT fp.parent, f.friendId AS child, fp.depth + 1
          FROM Friendship fp
          JOIN Friends f ON fp.child = f.userId
          WHERE fp.depth < 4
      )
      INSERT INTO FriendsClosure (parent, child, depth)
      SELECT parent, child, depth
      FROM Friendship;
      `, [userId, friendId]);

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
      await db.run(`
        WITH DeletedRows AS (
            SELECT parent, child
            FROM FriendsClosure
            WHERE (parent = ? AND child = ?)
              OR (parent = ? AND child = ?)
        )
        DELETE FROM FriendsClosure
        WHERE (parent, child) IN (SELECT parent, child FROM DeletedRows);
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
      error
    });
  }
}
module.exports.unFriend = unFriend;