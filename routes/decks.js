const express = require("express");
const { getDecks, getTotalDecks, getDecksDetails, getDeckStats, getDecksTest, getDeckStatsTest } = require("../controllers/deckController");

const router = express.Router();

router.post("/decks", getDecks);
router.post("/decks/stats", getDeckStats);
router.get("/decks/total", getTotalDecks);
router.get("/decks/:id", getDecksDetails);
router.post("/decks_test", getDecksTest);
router.post("/decks/stats_test", getDeckStatsTest);


module.exports = router;
