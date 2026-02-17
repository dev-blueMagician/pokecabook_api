const db = require("../config/db");

exports.getDecks = async (req, res) => {
  console.log("🏢 API FROM FRONTEND IS ARRIVED!!!!!!!!!!!!!!! 🏢");
  try {
    const { page, pageSize, filter } = req.body;
    const offset = (page - 1) * pageSize;
    console.log("filter==", filter);

    // Convert dates to JST (UTC+9) by treating input as JST date
    const startDate = filter.startDate; // Keep as-is since MySQL DATE type doesn't store timezone
    const endDate = filter.endDate;

    // If category is empty string, skip category filtering
    let conds = [];
    let having_cond = "";
    let select_cond = "";
    let requiredPairsSQL = "";
    let whereConditions = "";
    let conditions_length = 0;

    if (filter.category && filter.category.trim() !== '') {
      let cd_query = "";
      if (filter.category.includes("【")) {
        cd_query = `SELECT conds from deck_categories1 WHERE category1_var = ?`;
      }else{
        cd_query = `SELECT conds from deck_categories1 WHERE category1_var = ? OR category1_var LIKE '${filter.category}%'`;
      }
      const [conditions] = await db.query(cd_query,[filter.category])
      if(conditions && conditions.length > 0){
        conditions_length = conditions.length;
        for(let i = 0; i < conditions.length; i++){
          conds = conditions[i] && conditions[i].conds && conditions[i].conds.length > 0 ? JSON.parse(conditions[i].conds) : [];
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

                if(requiredPairsSQL !== '')
                  requiredPairsSQL += " UNION ALL ";

                if(whereConditions !== '')
                  whereConditions += " OR ";

                // Append SQL for RequiredPairs table
                requiredPairsSQL += `    SELECT '${item.cardName}' AS name_var, ${item.cardNumber} AS required_count, '${operator}' AS operator`;
                whereConditions += `    (rp.operator = '${operator}' AND dcc.count_int ${operator} rp.required_count)`;
            });

          }
        }
      }
    }

    if(whereConditions === ""){
      whereConditions = "1=1";
    }else{
      whereConditions = "(" + whereConditions + ")";
    }

    if(filter.cardName){
      whereConditions += ` AND dcc.name_var LIKE '%${filter.cardName}%'`;
    }
    if(filter.cardNumMin){
      whereConditions += ` AND dcc.count_int >= '${filter.cardNumMin}'`;
    }
    if(filter.cardNumMax){
      whereConditions += ` AND dcc.count_int <= '${filter.cardNumMax}'`;
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
    let having_sql = 'HAVING COUNT(DISTINCT dcc.name_var) >= (SELECT COUNT(*) FROM RequiredPairs)';
    if(conditions_length > 1)
      having_sql = '';
      // Query with card filtering conditions
    if(requiredPairsSQL !== ''){
      query = `
        WITH FilteredEvents AS (
            SELECT event_holding_id
            FROM events
            WHERE event_date_date BETWEEN ? AND ?
            AND event_league_int = ?${prefectureList ? ` AND event_prefecture IN (${prefectureList})` : ''}
        ),
        FilteredDecks AS (
            SELECT DISTINCT d.deck_ID_var
            FROM decks d
            JOIN FilteredEvents fe ON d.event_holding_id = fe.event_holding_id
            WHERE d.rank_int IN (${filter.ranks})
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
            )
            GROUP BY c.deck_ID_var, REPLACE(c.name_var, ' ', ''), c.count_int
        ),
        FilteredValidDecks AS (
            SELECT dcc.deck_ID_var as deck_id
            FROM DeckCardCounts dcc
            JOIN RequiredPairs rp ON REPLACE(dcc.name_var, ' ', '') = rp.name_var
            WHERE
                ${whereConditions}
            GROUP BY dcc.deck_ID_var
            ${having_sql}
        )
        SELECT d2.*, e.event_prefecture
        FROM FilteredValidDecks fvd
        INNER JOIN decks d2 ON fvd.deck_id = d2.deck_ID_var
        LEFT JOIN events e ON d2.event_holding_id = e.event_holding_id
        GROUP BY d2.deck_ID_var
        ORDER BY d2.rank_int DESC
      `;
    }else{
      query = `
        WITH FilteredEvents AS (
            SELECT event_holding_id
            FROM events
            WHERE event_date_date BETWEEN ? AND ?
            AND event_league_int = ?${prefectureList ? ` AND event_prefecture IN (${prefectureList})` : ''}
        ),
        FilteredDecks AS (
            SELECT DISTINCT d.deck_ID_var
            FROM decks d
            JOIN FilteredEvents fe ON d.event_holding_id = fe.event_holding_id
            WHERE d.rank_int IN (${filter.ranks})
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
            )
            GROUP BY c.deck_ID_var, REPLACE(c.name_var, ' ', ''), c.count_int
        ),
        FilteredValidDecks AS (
            SELECT dcc.deck_ID_var as deck_id
            FROM DeckCardCounts dcc
            WHERE
                ${whereConditions}
            GROUP BY dcc.deck_ID_var
        )
        SELECT d2.*, e.event_prefecture
        FROM FilteredValidDecks fvd
        INNER JOIN decks d2 ON fvd.deck_id = d2.deck_ID_var
        LEFT JOIN events e ON d2.event_holding_id = e.event_holding_id
        GROUP BY d2.deck_ID_var
        ORDER BY d2.rank_int DESC
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
  console.log("🏢🏢👨‍💼 API FROM FRONTEND IS ARRIVED! 🏢👨‍💼");
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
  console.log("🏢🏢👨‍💼 API FROM FRONTEND IS ARRIVED! 🏢👨‍💼");
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
  console.log("🏢 API FROM FRONTEND FOR DECK STATS IS ARRIVED!!!!!!!!!!!!!!! 🏢");
  try {
    const { filter } = req.body;
    console.log("filter==", filter);

    // Convert dates to JST (UTC+9) by treating input as JST date
    const startDate = filter.startDate; // Keep as-is since MySQL DATE type doesn't store timezone
    const endDate = filter.endDate;

    // If category is empty string, skip category filtering
    let conds = [];
    let having_cond = "";
    let select_cond = "";
    let requiredPairsSQL = "";
    let whereConditions = "";
    let conditions_length = 0;

    if (filter.category && filter.category.trim() !== '') {
      let cd_query = "";
      if (filter.category.includes("【")) {
        cd_query = `SELECT conds from deck_categories1 WHERE category1_var = ?`;
      }else{
        cd_query = `SELECT conds from deck_categories1 WHERE category1_var = ? OR category1_var LIKE '${filter.category}%'`;
      }
      const [conditions] = await db.query(cd_query,[filter.category])
      if(conditions && conditions.length > 0){
        conditions_length = conditions.length;
        for(let i = 0; i < conditions.length; i++){
          conds = conditions[i] && conditions[i].conds && conditions[i].conds.length > 0 ? JSON.parse(conditions[i].conds) : [];
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

                if(requiredPairsSQL !== '')
                  requiredPairsSQL += " UNION ALL ";

                if(whereConditions !== '')
                  whereConditions += " OR ";

                // Append SQL for RequiredPairs table
                requiredPairsSQL += `    SELECT '${item.cardName}' AS name_var, ${item.cardNumber} AS required_count, '${operator}' AS operator`;
                whereConditions += `    (rp.operator = '${operator}' AND dcc.count_int ${operator} rp.required_count)`;
            });

          }
        }
      }
    }

    if(whereConditions === ""){
      whereConditions = "1=1";
    }else{
      whereConditions = "(" + whereConditions + ")";
    }

    if(filter.cardName){
      whereConditions += ` AND dcc.name_var LIKE '%${filter.cardName}%'`;
    }
    if(filter.cardNumMin){
      whereConditions += ` AND dcc.count_int >= '${filter.cardNumMin}'`;
    }
    if(filter.cardNumMax){
      whereConditions += ` AND dcc.count_int <= '${filter.cardNumMax}'`;
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
          SELECT event_holding_id
          FROM events
          WHERE event_date_date BETWEEN ? AND ?
          AND event_league_int = ?${prefectureList ? ` AND event_prefecture IN (${prefectureList})` : ''}
      )
      SELECT COUNT(DISTINCT d.deck_ID_var) AS total_deck_count
      FROM decks d
      JOIN FilteredEvents fe ON d.event_holding_id = fe.event_holding_id
      WHERE d.rank_int IN (${filter.ranks})
    `;

    // Execute queries in parallel
    const [eventResult] = await db.query(eventQuery, [startDate, endDate, filter.league]);
    const [totalDeckResult] = await db.query(totalDeckQuery, [startDate, endDate, filter.league]);

    let filteredDeckCount = 0;
    let extra_msg = '';

  let filteredDeckQuery = '';  
  let having_sql = 'HAVING COUNT(DISTINCT dcc.name_var) >= (SELECT COUNT(*) FROM RequiredPairs)';
  if(conditions_length > 1)
    having_sql = '';

  if(requiredPairsSQL !== ''){
    filteredDeckQuery = `
      WITH FilteredEvents AS (
          SELECT event_holding_id
          FROM events
          WHERE event_date_date BETWEEN ? AND ?
          AND event_league_int = ?${prefectureList ? ` AND event_prefecture IN (${prefectureList})` : ''}
      ),
      FilteredDecks AS (
          SELECT DISTINCT d.deck_ID_var
          FROM decks d
          JOIN FilteredEvents fe ON d.event_holding_id = fe.event_holding_id
          WHERE d.rank_int IN (${filter.ranks})
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
          )
          GROUP BY c.deck_ID_var, REPLACE(c.name_var, ' ', ''), c.count_int
      ),
      FilteredValidDecks AS (
          SELECT dcc.deck_ID_var as deck_id
          FROM DeckCardCounts dcc
          JOIN RequiredPairs rp ON REPLACE(dcc.name_var, ' ', '') = rp.name_var
          WHERE
              ${whereConditions}
          GROUP BY dcc.deck_ID_var
          ${having_sql}
      )
      SELECT COUNT(*) as filtered_deck_count
      FROM FilteredValidDecks
    `;    
  }else{
    filteredDeckQuery = `
      WITH FilteredEvents AS (
          SELECT event_holding_id
          FROM events
          WHERE event_date_date BETWEEN ? AND ?
          AND event_league_int = ?${prefectureList ? ` AND event_prefecture IN (${prefectureList})` : ''}
      ),
      FilteredDecks AS (
          SELECT DISTINCT d.deck_ID_var
          FROM decks d
          JOIN FilteredEvents fe ON d.event_holding_id = fe.event_holding_id
          WHERE d.rank_int IN (${filter.ranks})
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
          )
          GROUP BY c.deck_ID_var, REPLACE(c.name_var, ' ', ''), c.count_int
      ),
      FilteredValidDecks AS (
          SELECT dcc.deck_ID_var as deck_id
          FROM DeckCardCounts dcc
          WHERE
              ${whereConditions}
          GROUP BY dcc.deck_ID_var
      )
      SELECT COUNT(*) as filtered_deck_count
      FROM FilteredValidDecks
    `;
  }
  
    const [filteredDeckResult] = await db.query(filteredDeckQuery, [startDate, endDate, filter.league]);
    filteredDeckCount = filteredDeckResult[0]?.filtered_deck_count || 0;
    extra_msg = filteredDeckQuery;

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

exports.getDecksTest = async (req, res) => {
  console.log("🏢 API FROM FRONTEND IS ARRIVED!!!!!!!!!!!!!!! 🏢");
  try {
    const { page, pageSize, filter } = req.body;
    const offset = (page - 1) * pageSize;
    console.log("filter==", filter);

    // Convert dates to JST (UTC+9) by treating input as JST date
    const startDate = filter.startDate; // Keep as-is since MySQL DATE type doesn't store timezone
    const endDate = filter.endDate;

    // If category is empty string, skip category filtering
    let conds = [];
    let having_cond = "";
    let select_cond = "";
    let requiredPairsSQL = "";
    let whereConditions = "";
    let conditions_length = 0;

    if (filter.category && filter.category.trim() !== '') {
      let cd_query = "";
      if (filter.category.includes("【")) {
        cd_query = `SELECT conds from deck_categories1 WHERE category1_var = ?`;
      }else{
        cd_query = `SELECT conds from deck_categories1 WHERE category1_var = ? OR category1_var LIKE '${filter.category}%'`;
      }
      const [conditions] = await db.query(cd_query,[filter.category])
      if(conditions && conditions.length > 0){
        conditions_length = conditions.length;
        for(let i = 0; i < conditions.length; i++){
          conds = conditions[i] && conditions[i].conds && conditions[i].conds.length > 0 ? JSON.parse(conditions[i].conds) : [];
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

                if(requiredPairsSQL !== '')
                  requiredPairsSQL += " UNION ALL ";

                if(whereConditions !== '')
                  whereConditions += " OR ";

                // Append SQL for RequiredPairs table
                requiredPairsSQL += `    SELECT '${item.cardName}' AS name_var, ${item.cardNumber} AS required_count, '${operator}' AS operator`;
                whereConditions += `    (rp.operator = '${operator}' AND dcc.count_int ${operator} rp.required_count)`;
            });

          }
        }
      }
    }

    if(whereConditions === ""){
      whereConditions = "1=1";
    }else{
      whereConditions = "(" + whereConditions + ")";
    }

    if(filter.cardName){
      whereConditions += ` AND dcc.name_var LIKE '%${filter.cardName}%'`;
    }
    if(filter.cardNumMin){
      whereConditions += ` AND dcc.count_int >= '${filter.cardNumMin}'`;
    }
    if(filter.cardNumMax){
      whereConditions += ` AND dcc.count_int <= '${filter.cardNumMax}'`;
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
    let having_sql = 'HAVING COUNT(DISTINCT dcc.name_var) >= (SELECT COUNT(*) FROM RequiredPairs)';
    if(conditions_length > 1)
      having_sql = '';
      // Query with card filtering conditions
    if(requiredPairsSQL !== ''){
      query = `
        WITH FilteredEvents AS (
            SELECT event_holding_id
            FROM events
            WHERE event_date_date BETWEEN ? AND ?
            AND event_league_int = ?${prefectureList ? ` AND event_prefecture IN (${prefectureList})` : ''}
        ),
        FilteredDecks AS (
            SELECT DISTINCT d.deck_ID_var
            FROM decks d
            JOIN FilteredEvents fe ON d.event_holding_id = fe.event_holding_id
            WHERE d.rank_int IN (${filter.ranks})
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
            )
            GROUP BY c.deck_ID_var, REPLACE(c.name_var, ' ', ''), c.count_int
        ),
        FilteredValidDecks AS (
            SELECT dcc.deck_ID_var as deck_id
            FROM DeckCardCounts dcc
            JOIN RequiredPairs rp ON REPLACE(dcc.name_var, ' ', '') = rp.name_var
            WHERE
                ${whereConditions}
            GROUP BY dcc.deck_ID_var
            ${having_sql}
        )
        SELECT d2.*, e.event_prefecture
        FROM FilteredValidDecks fvd
        INNER JOIN decks d2 ON fvd.deck_id = d2.deck_ID_var
        LEFT JOIN events e ON d2.event_holding_id = e.event_holding_id
        GROUP BY d2.deck_ID_var
        ORDER BY d2.rank_int DESC
      `;
    }else{
      query = `
        WITH FilteredEvents AS (
            SELECT event_holding_id
            FROM events
            WHERE event_date_date BETWEEN ? AND ?
            AND event_league_int = ?${prefectureList ? ` AND event_prefecture IN (${prefectureList})` : ''}
        ),
        FilteredDecks AS (
            SELECT DISTINCT d.deck_ID_var
            FROM decks d
            JOIN FilteredEvents fe ON d.event_holding_id = fe.event_holding_id
            WHERE d.rank_int IN (${filter.ranks})
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
            )
            GROUP BY c.deck_ID_var, REPLACE(c.name_var, ' ', ''), c.count_int
        ),
        FilteredValidDecks AS (
            SELECT dcc.deck_ID_var as deck_id
            FROM DeckCardCounts dcc
            WHERE
                ${whereConditions}
            GROUP BY dcc.deck_ID_var
        )
        SELECT d2.*, e.event_prefecture
        FROM FilteredValidDecks fvd
        INNER JOIN decks d2 ON fvd.deck_id = d2.deck_ID_var
        LEFT JOIN events e ON d2.event_holding_id = e.event_holding_id
        GROUP BY d2.deck_ID_var
        ORDER BY d2.rank_int DESC
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

exports.getDeckStatsTest = async (req, res) => {
  console.log("🏢 API FROM FRONTEND FOR DECK STATS IS ARRIVED!!!!!!!!!!!!!!! 🏢");
  try {
    const { filter } = req.body;
    console.log("filter==", filter);

    // Convert dates to JST (UTC+9) by treating input as JST date
    const startDate = filter.startDate; // Keep as-is since MySQL DATE type doesn't store timezone
    const endDate = filter.endDate;

    // If category is empty string, skip category filtering
    let conds = [];
    let having_cond = "";
    let select_cond = "";
    let requiredPairsSQL = "";
    let whereConditions = "";
    let conditions_length = 0;

    if (filter.category && filter.category.trim() !== '') {
      let cd_query = "";
      if (filter.category.includes("【")) {
        cd_query = `SELECT conds from deck_categories1 WHERE category1_var = ?`;
      }else{
        cd_query = `SELECT conds from deck_categories1 WHERE category1_var = ? OR category1_var LIKE '${filter.category}%'`;
      }
      const [conditions] = await db.query(cd_query,[filter.category])
      if(conditions && conditions.length > 0){
        conditions_length = conditions.length;
        for(let i = 0; i < conditions.length; i++){
          conds = conditions[i] && conditions[i].conds && conditions[i].conds.length > 0 ? JSON.parse(conditions[i].conds) : [];
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

                if(requiredPairsSQL !== '')
                  requiredPairsSQL += " UNION ALL ";

                if(whereConditions !== '')
                  whereConditions += " OR ";

                // Append SQL for RequiredPairs table
                requiredPairsSQL += `    SELECT '${item.cardName}' AS name_var, ${item.cardNumber} AS required_count, '${operator}' AS operator`;
                whereConditions += `    (rp.operator = '${operator}' AND dcc.count_int ${operator} rp.required_count)`;
            });

          }
        }
      }
    }

    if(whereConditions === ""){
      whereConditions = "1=1";
    }else{
      whereConditions = "(" + whereConditions + ")";
    }

    if(filter.cardName){
      whereConditions += ` AND dcc.name_var LIKE '%${filter.cardName}%'`;
    }
    if(filter.cardNumMin){
      whereConditions += ` AND dcc.count_int >= '${filter.cardNumMin}'`;
    }
    if(filter.cardNumMax){
      whereConditions += ` AND dcc.count_int <= '${filter.cardNumMax}'`;
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
          SELECT event_holding_id
          FROM events
          WHERE event_date_date BETWEEN ? AND ?
          AND event_league_int = ?${prefectureList ? ` AND event_prefecture IN (${prefectureList})` : ''}
      )
      SELECT COUNT(DISTINCT d.deck_ID_var) AS total_deck_count
      FROM decks d
      JOIN FilteredEvents fe ON d.event_holding_id = fe.event_holding_id
      WHERE d.rank_int IN (${filter.ranks})
    `;

    // Execute queries in parallel
    const [eventResult] = await db.query(eventQuery, [startDate, endDate, filter.league]);
    const [totalDeckResult] = await db.query(totalDeckQuery, [startDate, endDate, filter.league]);

    let filteredDeckCount = 0;
    let extra_msg = '';

  let filteredDeckQuery = '';  
  let having_sql = 'HAVING COUNT(DISTINCT dcc.name_var) >= (SELECT COUNT(*) FROM RequiredPairs)';
  if(conditions_length > 1)
    having_sql = '';

  if(requiredPairsSQL !== ''){
    filteredDeckQuery = `
      WITH FilteredEvents AS (
          SELECT event_holding_id
          FROM events
          WHERE event_date_date BETWEEN ? AND ?
          AND event_league_int = ?${prefectureList ? ` AND event_prefecture IN (${prefectureList})` : ''}
      ),
      FilteredDecks AS (
          SELECT DISTINCT d.deck_ID_var
          FROM decks d
          JOIN FilteredEvents fe ON d.event_holding_id = fe.event_holding_id
          WHERE d.rank_int IN (${filter.ranks})
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
          )
          GROUP BY c.deck_ID_var, REPLACE(c.name_var, ' ', ''), c.count_int
      ),
      FilteredValidDecks AS (
          SELECT dcc.deck_ID_var as deck_id
          FROM DeckCardCounts dcc
          JOIN RequiredPairs rp ON REPLACE(dcc.name_var, ' ', '') = rp.name_var
          WHERE
              ${whereConditions}
          GROUP BY dcc.deck_ID_var
          ${having_sql}
      )
      SELECT COUNT(*) as filtered_deck_count
      FROM FilteredValidDecks
    `;    
  }else{
    filteredDeckQuery = `
      WITH FilteredEvents AS (
          SELECT event_holding_id
          FROM events
          WHERE event_date_date BETWEEN ? AND ?
          AND event_league_int = ?${prefectureList ? ` AND event_prefecture IN (${prefectureList})` : ''}
      ),
      FilteredDecks AS (
          SELECT DISTINCT d.deck_ID_var
          FROM decks d
          JOIN FilteredEvents fe ON d.event_holding_id = fe.event_holding_id
          WHERE d.rank_int IN (${filter.ranks})
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
          )
          GROUP BY c.deck_ID_var, REPLACE(c.name_var, ' ', ''), c.count_int
      ),
      FilteredValidDecks AS (
          SELECT dcc.deck_ID_var as deck_id
          FROM DeckCardCounts dcc
          WHERE
              ${whereConditions}
          GROUP BY dcc.deck_ID_var
      )
      SELECT COUNT(*) as filtered_deck_count
      FROM FilteredValidDecks
    `;
  }

    const [filteredDeckResult] = await db.query(filteredDeckQuery, [startDate, endDate, filter.league]);
    filteredDeckCount = filteredDeckResult[0]?.filtered_deck_count || 0;
    extra_msg = filteredDeckQuery;

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