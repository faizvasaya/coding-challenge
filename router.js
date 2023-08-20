const express = require('express');

const user = require('./user');

const router = express.Router();

router.get('/search/:userId/:query', user.search);

// @change: Added route to Add Friend
router.get('/friend/:userId/:friendId', user.friend);

// @change: Added route to Remove Friend
router.get('/unfriend/:userId/:friendId', user.unFriend);

module.exports = router;