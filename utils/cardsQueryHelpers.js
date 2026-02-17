/**
 * Helper utilities for cards controller - parameterized queries and validation
 */

const VALID_OPERATORS = ['=', '>=', '<=', '!='];
const CARD_NAME_REGEX = /^[a-zA-Z0-9\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FAF\s\-_().']+$/;

/**
 * Build rank integers array from ranks object
 * @param {Object} ranks - { winner, runnerUp, top4, top8, top16, all }
 * @returns {number[]} Array of rank integers for IN clause
 */
function getRankIntsFromObject(ranks) {
  if (!ranks || typeof ranks !== 'object') return [];
  const ints = new Set();
  if (ranks.winner) ints.add(1);
  if (ranks.runnerUp) ints.add(2);
  if (ranks.top4) {
    [1, 2, 3, 4].forEach((r) => ints.add(r));
  }
  if (ranks.top8) {
    [1, 2, 3, 4, 5, 6, 7, 8].forEach((r) => ints.add(r));
  }
  if (ranks.top16) {
    for (let r = 1; r <= 16; r++) ints.add(r);
  }
  if (ranks.all) {
    return null; // Special case: use "rank_int > 0" instead of IN
  }
  return Array.from(ints);
}

/**
 * Build rankConditions SQL string from ranks object (for FilteredDecks WHERE)
 */
function buildRankConditions(ranks) {
  if (!ranks || typeof ranks !== 'object') return '';
  const rankFilters = [];
  if (ranks.winner) rankFilters.push('d.rank_int = 1');
  if (ranks.runnerUp) rankFilters.push('d.rank_int = 2');
  if (ranks.top4) rankFilters.push('d.rank_int <= 4');
  if (ranks.top8) rankFilters.push('d.rank_int <= 8');
  if (ranks.top16) rankFilters.push('d.rank_int <= 16');
  if (ranks.all) rankFilters.push('d.rank_int > 0');
  if (rankFilters.length === 0) return '';
  return `AND (${rankFilters.join(' OR ')})`;
}

/**
 * Build rank filter for decks count query (uses rank_int IN or rank_int > 0)
 */
function buildDecksCountRankFilter(ranks) {
  const rankInts = getRankIntsFromObject(ranks);
  if (rankInts === null) {
    return { sql: 'AND decks.rank_int > 0', params: [] };
  }
  if (rankInts.length === 0) return { sql: '', params: [] };
  const placeholders = rankInts.map(() => '?').join(',');
  return { sql: `AND decks.rank_int IN (${placeholders})`, params: rankInts };
}

/**
 * Sanitize and validate category condition item from deck_categories1
 */
function sanitizeCategoryCondition(item) {
  const cardName = String(item?.cardName ?? '').trim();
  const cardNumber = Math.max(0, parseInt(item?.cardNumber, 10) || 0);
  const op = String(item?.cardCondition ?? 'eql').toLowerCase();
  const operatorMap = { eql: '=', gte: '>=', lte: '<=', ueq: '!=' };
  const operator = VALID_OPERATORS.includes(operatorMap[op]) ? operatorMap[op] : '=';

  if (!CARD_NAME_REGEX.test(cardName)) return null;
  return { cardName, cardNumber, operator };
}

/**
 * Parse prefectures into array of strings (for parameterized IN)
 */
function parsePrefectures(prefectures) {
  if (prefectures === undefined || prefectures === null) return { list: null, isEmpty: false };
  if (Array.isArray(prefectures)) {
    if (prefectures.length === 0) return { list: null, isEmpty: true };
    const list = prefectures.map((p) => String(p).trim()).filter(Boolean);
    return { list: list.length ? list : null, isEmpty: list.length === 0 && prefectures.length > 0 };
  }
  if (typeof prefectures === 'string') {
    const trimmed = prefectures.trim();
    if (trimmed.length === 0) return { list: null, isEmpty: true };
    const list = trimmed.split(',').map((p) => p.trim()).filter(Boolean);
    return { list: list.length ? list : null, isEmpty: false };
  }
  return { list: null, isEmpty: false };
}

/**
 * Validate filter input
 */
function validateFilter(filter) {
  if (!filter || typeof filter !== 'object') {
    return { valid: false, message: 'Missing filter object' };
  }
  const { startDate, endDate, league } = filter;
  if (!startDate || !endDate || !league) {
    return { valid: false, message: 'Missing required parameters: startDate, endDate, league' };
  }
  const leagueInt = parseInt(league, 10);
  if (isNaN(leagueInt) || leagueInt < 0) {
    return { valid: false, message: 'Invalid league: must be a non-negative integer' };
  }
  return { valid: true, leagueInt };
}

module.exports = {
  getRankIntsFromObject,
  buildRankConditions,
  buildDecksCountRankFilter,
  sanitizeCategoryCondition,
  parsePrefectures,
  validateFilter,
};
