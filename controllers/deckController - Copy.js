const db = require("../config/db");

exports.getDecks = async (req, res) => {
  console.log("�� API FROM FRONTEND IS ARRIVED!!!!!!!!!!!!!!! ��");
  try {
    const { page, pageSize, filter } = req.body;
    const offset = (page - 1) * pageSize;
    console.log("filter==", filter);

    // Convert dates to JST (UTC+9) by treating input as JST date
    const startDate = filter.startDate; // Keep as-is since MySQL DATE type doesn't store timezone
    const endDate = filter.endDate;

    // If category is empty string, skip category filtering
    let conds = [];
    if (filter.category && filter.category.trim() !== '') {
      let cd_query = "";
      if (filter.category.includes("【")) {
        cd_query = `SELECT conds from deck_categories1 WHERE category1_var = ?`;
      }else{
        cd_query = `SELECT conds from deck_categories1 WHERE category1_var = ? OR category1_var LIKE '${filter.category}%'`;
      }
      const [conditions] = await db.query(cd_query,[filter.category])
      conds = conditions[0] && conditions[0].conds && conditions[0].conds.length > 0 ? JSON.parse(conditions[0].conds) : [];
    }

    let having_cond = "";
    let select_cond = "";
    let requiredPairsSQL = "";
    let whereConditions = "";
    if (conds.length > 0) {
        conds.forEach((item,index) => {
            let operator;
            switch(item.cardCondition) {
            case "eql":
                operator = "=";
                break;
            case "gte":
                operator = ">=";
                break;
            case "lte":
                operator = "<=";
                break;
            case "ueq":
                operator = "!=";
                break;
            default:
                operator = "=";
                break;
            }
            having_cond += ` AND count_val_${index+1} ${operator} ${item.cardNumber}`;
            select_cond += `SUM(CASE WHEN name_var = '${item.cardName}' THEN c.count_int ELSE 0 END) AS count_val_${index+1}`;
            // Append SQL for RequiredPairs table
            requiredPairsSQL += `    SELECT '${item.cardName}' AS name_var, ${item.cardNumber} AS required_count, '${operator}' AS operator`;
            whereConditions += `    (rp.operator = '${operator}' AND dcc.count_int ${operator} rp.required_count)`;

            // Add UNION ALL for all but the last entry
            if (index < conds.length - 1) {
                requiredPairsSQL += " UNION ALL";
                whereConditions += " OR";
            }
        })
    }

    let whereCardConditions = "";
    if(filter.cardName){
      whereCardConditions += ` AND c.name_var LIKE '%${filter.cardName}%'`;
    }
    if(filter.cardNumMin){
      whereCardConditions += ` AND c.count_int >= '${filter.cardNumMin}'`;
    }
    if(filter.cardNumMax){
      whereCardConditions += ` AND c.count_int <= '${filter.cardNumMax}'`;
    }    

    // Format prefectures with quotes for SQL IN clause
    // If prefectures is an empty array [], we should return no results
    let prefectureList = null;
    let isPrefectureFilterEmpty = false;

    if (filter.prefectures !== undefined && filter.prefectures !== null) {
      if (Array.isArray(filter.prefectures)) {
        if (filter.prefectures.length === 0) {
          isPrefectureFilterEmpty = true; // Empty array means no results
        } else {
          prefectureList = filter.prefectures.map(p => `'${p}'`).join(',');
        }
      } else if (typeof filter.prefectures === 'string') {
        const trimmed = filter.prefectures.trim();
        if (trimmed.length === 0) {
          isPrefectureFilterEmpty = true; // Empty string means no results
        } else {
          prefectureList = trimmed.split(',').map(p => `'${p.trim()}'`).join(',');
        }
      }
    }

    // If prefecture filter is explicitly empty, return empty results immediately
    if (isPrefectureFilterEmpty) {
      res.status(200).json([]);
      return;
    }

    let query;
    if (conds.length > 0) {
      // Query with card filtering conditions
      query = `
        WITH FilteredEvents AS (
            SELECT id, event_holding_id
            FROM events
            WHERE event_date_date BETWEEN ? AND ?
            AND event_league_int = ?${prefectureList ? ` AND event_prefecture IN (${prefectureList})` : ''}
        ),
        FilteredDecks AS (
            SELECT d.*
            FROM decks d
            JOIN FilteredEvents fe ON d.event_holding_id = fe.event_holding_id
        ),
        RequiredPairs AS (
            ${requiredPairsSQL}
        ),
        DeckCardCounts AS (
            SELECT
                c.deck_ID_var,
                REPLACE(c.name_var, ' ', '') AS name_var,
                c.count_int,
                COUNT(*) AS pair_count
            FROM cards c
            WHERE EXISTS (
                SELECT 1
                FROM FilteredDecks fd
                WHERE c.deck_ID_var = fd.deck_ID_var
            ) ${whereCardConditions}
            GROUP BY c.deck_ID_var, REPLACE(c.name_var, ' ', ''), c.count_int
        ),
        FilteredValidDecks AS (
            SELECT dcc.deck_ID_var as deck_id
            FROM DeckCardCounts dcc
            JOIN RequiredPairs rp ON REPLACE(dcc.name_var, ' ', '') = rp.name_var
            WHERE
                ${whereConditions}
            GROUP BY dcc.deck_ID_var
            HAVING COUNT(DISTINCT dcc.name_var) >= (SELECT COUNT(*) FROM RequiredPairs)
        )
        SELECT d.*, e.event_prefecture
        FROM FilteredValidDecks fvd
        LEFT JOIN decks d ON fvd.deck_id = d.deck_ID_var
        LEFT JOIN events e ON d.event_holding_id = e.event_holding_id
        WHERE d.rank_int IN (${filter.ranks})
        ORDER BY d.rank_int DESC
      `;
    } else {
      // Simplified query without card filtering (no category conditions)
      query = `
        WITH FilteredEvents AS (
            SELECT id, event_holding_id
            FROM events
            WHERE event_date_date BETWEEN ? AND ?
            AND event_league_int = ?${prefectureList ? ` AND event_prefecture IN (${prefectureList})` : ''}
        ),
        FilteredDecks AS (
            SELECT d.*
            FROM decks d
            JOIN FilteredEvents fe ON d.event_holding_id = fe.event_holding_id
            WHERE d.rank_int IN (${filter.ranks})
            GROUP BY d.id
        ),
        DeckCardCounts AS (
            SELECT
                c.deck_ID_var,
                REPLACE(c.name_var, ' ', '') AS name_var,
                c.count_int,
                COUNT(*) AS pair_count
            FROM cards c
            WHERE EXISTS (
                SELECT 1
                FROM FilteredDecks fd
                WHERE c.deck_ID_var = fd.deck_ID_var
            ) ${whereCardConditions}
            GROUP BY c.deck_ID_var, REPLACE(c.name_var, ' ', ''), c.count_int
        ),
        FilteredValidDecks AS (
            SELECT dcc.deck_ID_var as deck_id
            FROM DeckCardCounts dcc
            GROUP BY dcc.deck_ID_var
        )
        SELECT d.*, e.event_prefecture
        FROM FilteredValidDecks fvd
        JOIN decks d ON fvd.deck_id = d.deck_ID_var
        JOIN events e ON d.event_holding_id = e.event_holding_id
        ORDER BY d.rank_int DESC
      `;
    }
    const [decks_result] = await db.query(query, [startDate, endDate, filter.league])
    console.log("query==>", query);
    
    res.status(200).json(decks_result);
  } catch (err) {
    console.error("Error fetching data:", err);
    res.status(500).json({ message: "Internal Server Error" });
  }
};

exports.getTotalDecks = async (req, res) => {
  console.log("������‍�� API FROM FRONTEND IS ARRIVED! ����‍��");
  try {
    const query = `SELECT COUNT(*) AS total_events_count FROM events`;
    const [events_count] = await db.query(query);
    res.status(200).json(events_count[0]?.total_events_count || 0);
  } catch (err) {
    console.error("Error fetching data:", err);
    res.status(500).json({ message: "Internal Server Error" });
  }
};

exports.getDecksDetails = async (req, res) => {
  console.log("������‍�� API FROM FRONTEND IS ARRIVED! ����‍��");
  try {
    const { id } = req.params;
    const query = `SELECT d.event_holding_id, d.deck_ID_var, d.rank_int, d.point_int FROM decks as d WHERE event_holding_id = ${id} ORDER BY point_int DESC, id ASC`;
    const [events_result] = await db.query(query);
    console.log('events_result==', events_result);
    res.status(200).json(events_result);
  } catch (err) {
    console.error("Error fetching data:", err);
    res.status(500).json({ message: "Internal Server Error" });
  }
};

exports.getDeckStats = async (req, res) => {
  console.log("�� API FROM FRONTEND FOR DECK STATS IS ARRIVED!!!!!!!!!!!!!!! ��");
  try {
    const { filter } = req.body;
    console.log("filter==", filter);

    // Convert dates to JST (UTC+9) by treating input as JST date
    const startDate = filter.startDate; // Keep as-is since MySQL DATE type doesn't store timezone
    const endDate = filter.endDate;

    // If category is empty string, skip category filtering
    let conds = [];
    if (filter.category && filter.category.trim() !== '') {
      let cd_query = "";
      if (filter.category.includes("【")) {
        cd_query = `SELECT conds from deck_categories1 WHERE category1_var = ?`;
      }else{
        cd_query = `SELECT conds from deck_categories1 WHERE category1_var = ? OR category1_var LIKE '${filter.category}%'`;
      }
      const [conditions] = await db.query(cd_query,[filter.category])
      conds = conditions[0] && conditions[0].conds && conditions[0].conds.length > 0 ? JSON.parse(conditions[0].conds) : [];
    }

    let having_cond = "";
    let select_cond = "";
    let requiredPairsSQL = "";
    let whereConditions = "";
    if (conds.length > 0) {
        conds.forEach((item,index) => {
            let operator;
            switch(item.cardCondition) {
            case "eql":
                operator = "=";
                break;
            case "gte":
                operator = ">=";
                break;
            case "lte":
                operator = "<=";
                break;
            case "ueq":
                operator = "!=";
                break;
            default:
                operator = "=";
                break;
            }
            having_cond += ` AND count_val_${index+1} ${operator} ${item.cardNumber}`;
            select_cond += `SUM(CASE WHEN name_var = '${item.cardName}' THEN c.count_int ELSE 0 END) AS count_val_${index+1}`;
            // Append SQL for RequiredPairs table
            requiredPairsSQL += `    SELECT '${item.cardName}' AS name_var, ${item.cardNumber} AS required_count, '${operator}' AS operator`;
            whereConditions += `    (rp.operator = '${operator}' AND dcc.count_int ${operator} rp.required_count)`;

            // Add UNION ALL for all but the last entry
            if (index < conds.length - 1) {
                requiredPairsSQL += " UNION ALL";
                whereConditions += " OR";
            }
        })
    }

    let whereCardConditions = "";
    if(filter.cardName){
      whereCardConditions += ` AND c.name_var LIKE '%${filter.cardName}%'`;
    }
    if(filter.cardNumMin){
      whereCardConditions += ` AND c.count_int >= '${filter.cardNumMin}'`;
    }
    if(filter.cardNumMax){
      whereCardConditions += ` AND c.count_int <= '${filter.cardNumMax}'`;
    }

    // Format prefectures with quotes for SQL IN clause
    // If prefectures is an empty array [], we should return no results
    let prefectureList = null;
    let isPrefectureFilterEmpty = false;

    if (filter.prefectures !== undefined && filter.prefectures !== null) {
      if (Array.isArray(filter.prefectures)) {
        if (filter.prefectures.length === 0) {
          isPrefectureFilterEmpty = true; // Empty array means no results
        } else {
          prefectureList = filter.prefectures.map(p => `'${p}'`).join(',');
        }
      } else if (typeof filter.prefectures === 'string') {
        const trimmed = filter.prefectures.trim();
        if (trimmed.length === 0) {
          isPrefectureFilterEmpty = true; // Empty string means no results
        } else {
          prefectureList = trimmed.split(',').map(p => `'${p.trim()}'`).join(',');
        }
      }
    }

    // If prefecture filter is explicitly empty, return zero stats immediately
    if (isPrefectureFilterEmpty) {
      res.status(200).json({
        eventCount: 0,
        totalDeckCount: 0,
        filteredDeckCount: 0
      });
      return;
    }

    // Query to get event count
    const eventQuery = `
      SELECT COUNT(DISTINCT e.event_holding_id) AS event_count
      FROM events e
      WHERE e.event_date_date BETWEEN ? AND ?
      AND e.event_league_int = ?${prefectureList ? ` AND e.event_prefecture IN (${prefectureList})` : ''}
    `;

    // Query to get total deck count in events
    const totalDeckQuery = `
      WITH FilteredEvents AS (
          SELECT id, event_holding_id
          FROM events
          WHERE event_date_date BETWEEN ? AND ?
          AND event_league_int = ?${prefectureList ? ` AND event_prefecture IN (${prefectureList})` : ''}
      )
      SELECT COUNT(*) AS total_deck_count
      FROM decks d
      JOIN FilteredEvents fe ON d.event_holding_id = fe.event_holding_id
      WHERE d.rank_int IN (${filter.ranks})
      GROUP BY d.id
    `;

    // Execute queries in parallel
    const [eventResult] = await db.query(eventQuery, [startDate, endDate, filter.league]);
    const [totalDeckResult] = await db.query(totalDeckQuery, [startDate, endDate, filter.league]);

    let filteredDeckCount = 0;
    let extra_msg = '';

    if (conds.length > 0) {
      // Query to get filtered deck count (matching the category conditions)
      extra_msg = 'cond exist';
      const filteredDeckQuery = `
        WITH FilteredEvents AS (
            SELECT id, event_holding_id
            FROM events
            WHERE event_date_date BETWEEN ? AND ?
            AND event_league_int = ?${prefectureList ? ` AND event_prefecture IN (${prefectureList})` : ''}
        ),
        FilteredDecks AS (
            SELECT d.*
            FROM decks d
            JOIN FilteredEvents fe ON d.event_holding_id = fe.event_holding_id
        ),
        RequiredPairs AS (
            ${requiredPairsSQL}
        ),
        DeckCardCounts AS (
            SELECT
                c.deck_ID_var,
                REPLACE(c.name_var, ' ', '') AS name_var,
                c.count_int,
                COUNT(*) AS pair_count
            FROM cards c
            WHERE EXISTS (
                SELECT 1
                FROM FilteredDecks fd
                WHERE c.deck_ID_var = fd.deck_ID_var
            ) ${whereCardConditions}
            GROUP BY c.deck_ID_var, REPLACE(c.name_var, ' ', ''), c.count_int
        ),
        FilteredValidDecks AS (
            SELECT dcc.deck_ID_var as deck_id
            FROM DeckCardCounts dcc
            JOIN RequiredPairs rp ON REPLACE(dcc.name_var, ' ', '') = rp.name_var
            WHERE
                ${whereConditions}
            GROUP BY dcc.deck_ID_var
            HAVING COUNT(DISTINCT dcc.name_var) >= (SELECT COUNT(*) FROM RequiredPairs)
        )
        SELECT COUNT(*) AS filtered_deck_count
        FROM FilteredValidDecks fvd
        LEFT JOIN decks d on fvd.deck_id = d.deck_ID_var
        WHERE d.rank_int IN (${filter.ranks})
      `;
      const [filteredDeckResult] = await db.query(filteredDeckQuery, [startDate, endDate, filter.league]);
      filteredDeckCount = filteredDeckResult[0]?.filtered_deck_count || 0;
    } else {
      // If no category filter, filtered count equals total count
      extra_msg = 'cond no';
      const filteredDeckQuery = `
        WITH FilteredEvents AS (
            SELECT id, event_holding_id
            FROM events
            WHERE event_date_date BETWEEN ? AND ?
            AND event_league_int = ?${prefectureList ? ` AND event_prefecture IN (${prefectureList})` : ''}
        ),
        FilteredDecks AS (
            SELECT d.*
            FROM decks d
            JOIN FilteredEvents fe ON d.event_holding_id = fe.event_holding_id
            WHERE d.rank_int IN (${filter.ranks})
            GROUP BY d.id
        ),
        SELECT COUNT(*) AS filtered_deck_count
        FROM FilteredDecks fd
      `;
      const [filteredDeckResult] = await db.query(filteredDeckQuery, [startDate, endDate, filter.league]);
      filteredDeckCount = filteredDeckResult[0]?.filtered_deck_count || 0;
    }

    const stats = {
      eventCount: eventResult[0]?.event_count || 0,
      totalDeckCount: totalDeckResult[0]?.total_deck_count || 0,
      filteredDeckCount: filteredDeckCount,
      extra_msg: extra_msg
    };

    console.log("Deck stats==>", stats);
    res.status(200).json(stats);
  } catch (err) {
    console.error("Error fetching deck stats:", err);
    res.status(500).json({ message: "Internal Server Error" });
  }
};
