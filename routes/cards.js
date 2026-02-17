const express = require("express");
const { getCards, getCardCategories, getAll, searchCards, getCardsTest } = require("../controllers/cardsController");

const router = express.Router();

router.post("/cards", getCards);
router.get("/card-category", getCardCategories);
router.get("/cards", getAll);
router.get("/card-search", searchCards);
router.post("/cards_test", getCardsTest);

module.exports = router;
