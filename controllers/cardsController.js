const db = require("../config/db");
const db_test = require("../config/db_test");

exports.getCards = async (req, res) => {
  console.log("⛳ API FROM FRONTEND IS ARRIVED!@@@@@@@@@@@@@@@@@ ⛳");
  try {
    console.log("req.body==>", req.body);

    const {
      startDate: filterStartDate,
      endDate: filterEndDate,
      league,
      category,
      ranks,
      prefectures,
    } = req.body.filter;

    if (!filterStartDate || !filterEndDate || !league) {
      return res.status(400).json({ message: "Missing required parameters" });
    }

    // Convert dates to JST (UTC+9) by treating input as JST date
    const startDate = filterStartDate; // Keep as-is since MySQL DATE type doesn't store timezone
    const endDate = filterEndDate;

    console.log("ranks", ranks);

    // Process ranking filters
    let rankConditions = "";
    if (ranks && typeof ranks === "object") {
      const rankFilters = [];

      if (ranks.winner) rankFilters.push("d.rank_int = 1");
      if (ranks.runnerUp) rankFilters.push("d.rank_int = 2");
      if (ranks.top4) rankFilters.push("d.rank_int <= 4");
      if (ranks.top8) rankFilters.push("d.rank_int <= 8");
      if (ranks.top16) rankFilters.push("d.rank_int <= 16");
      if (ranks.all) rankFilters.push("d.rank_int > 0"); // All ranked decks

      if (rankFilters.length > 0) {
        rankConditions = `AND (${rankFilters.join(" OR ")})`;
      }
    }

    // Format prefectures with quotes for SQL IN clause
    // If prefectures is an empty array [], we should return no results
    let prefectureList = null;
    let isPrefectureFilterEmpty = false;

    if (prefectures !== undefined && prefectures !== null) {
      if (Array.isArray(prefectures)) {
        if (prefectures.length === 0) {
          isPrefectureFilterEmpty = true; // Empty array means no results
        } else {
          prefectureList = prefectures.map((p) => `'${p}'`).join(",");
        }
      } else if (typeof prefectures === "string") {
        const trimmed = prefectures.trim();
        if (trimmed.length === 0) {
          isPrefectureFilterEmpty = true; // Empty string means no results
        } else {
          prefectureList = trimmed
            .split(",")
            .map((p) => `'${p.trim()}'`)
            .join(",");
        }
      }
    }

    // If prefecture filter is explicitly empty, return empty results immediately
    if (isPrefectureFilterEmpty) {
      res.status(200).json({
        rows: [],
        filtered_events_count: 0,
        filtered_decks_count: 0,
        filtered_specific_decks_count: 0,
      });
      return;
    }

    // If category is empty string, skip category filtering
    let conds = [];
    if (category && category.trim() !== "") {
      const cd_query = `
          SELECT conds from deck_categories1 WHERE category1_var = ?
      `;
      const [conditions] = await db.query(cd_query, [category]);
      console.log("****conditions", conditions[0]);
      conds =
        conditions[0] && conditions[0].conds && conditions[0].conds.length > 0
          ? JSON.parse(conditions[0].conds)
          : [];
    }

    // let where_cond = "";
    // if(conds.length > 0) {
    //     conds.forEach(item => {
    //         switch(item.cardCondition) {
    //             case "eql":
    //                 where_cond += `AND c.name_var = '${item.cardName}' AND c.count_int = ${item.cardNumber} `
    //                 break;
    //             case "gte":
    //                 where_cond += `AND c.name_var = '${item.cardName}' AND c.count_int >= ${item.cardNumber} `
    //                 break;
    //             case "lte":
    //                 where_cond += `AND c.name_var = '${item.cardName}' AND c.count_int <= ${item.cardNumber} `
    //                 break;
    //             case "ueq":
    //                 where_cond += `AND c.name_var = '${item.cardName}' AND c.count_int != ${item.cardNumber} `
    //                 break;
    //             default:
    //                 break;
    //         }
    //     })
    // }

    let having_cond = "";
    let select_cond = "";
    let requiredPairsSQL = "";
    let whereConditions = "";
    if (conds.length > 0) {
      conds.forEach((item, index) => {
        let operator;
        switch (item.cardCondition) {
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
        having_cond += ` AND count_val_${index + 1} ${operator} ${item.cardNumber}`;
        select_cond += `SUM(CASE WHEN name_var = '${item.cardName}' THEN c.count_int ELSE 0 END) AS count_val_${index + 1}`;
        // Append SQL for RequiredPairs table
        requiredPairsSQL += `    SELECT '${item.cardName}' AS name_var, ${item.cardNumber} AS required_count, '${operator}' AS operator`;
        whereConditions += `    (rp.operator = '${operator}' AND dcc.count_int ${operator} rp.required_count)`;

        // Add UNION ALL for all but the last entry
        if (index < conds.length - 1) {
          requiredPairsSQL += " UNION ALL";
          whereConditions += " OR";
        }
        console.log(
          "having_cond==>",
          having_cond,
          "select_cond==>",
          select_cond,
          "requiredPairsSQL==>",
          requiredPairsSQL,
          "whereConditions==>",
          whereConditions,
        );
      });
    }
    // FilteredCardsByCategory AS (
    //     SELECT c.*
    //     FROM cards c
    //     WHERE EXISTS (
    //         SELECT 1
    //         FROM FilteredDecks fd
    //         WHERE c.deck_ID_var = fd.deck_ID_var
    //     )
    //     ${where_cond}
    // ),
    // where_cond = where_cond == "" ? "true" : where_cond;
    // console.log("****where_cond", where_cond)

    let query;
    if (conds.length > 0) {
      // Query with category filtering
      query = `
        WITH FilteredEvents AS (
            SELECT id, event_holding_id
            FROM events
            WHERE event_date_date BETWEEN ? AND ?
            AND event_league_int = ?${prefectureList ? ` AND event_prefecture IN (${prefectureList})` : ""}
        ),
        FilteredDecks AS (
            SELECT d.*
            FROM decks d
            JOIN FilteredEvents fe ON d.event_holding_id = fe.event_holding_id
            WHERE 1=1 ${rankConditions}
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
            SELECT dcc.deck_ID_var
            FROM DeckCardCounts dcc
            JOIN RequiredPairs rp ON dcc.name_var = rp.name_var
            WHERE
                ${whereConditions}
            GROUP BY dcc.deck_ID_var
            HAVING COUNT(DISTINCT dcc.name_var) >= (SELECT COUNT(*) FROM RequiredPairs)
        ),
        RelatedDecks AS (
            SELECT DISTINCT fvd.deck_ID_var FROM FilteredValidDecks  fvd LEFT JOIN decks d on fvd.deck_ID_var = d.deck_ID_var WHERE d.rank_int IN (${ranks})
        ),
        ExcludedDecks AS (
            SELECT DISTINCT c.deck_ID_var
            FROM cards c
            JOIN RequiredPairs rp ON REPLACE(c.name_var, ' ', '') = rp.name_var
            WHERE c.deck_ID_var IN (SELECT deck_ID_var FROM RelatedDecks)
            AND rp.operator = '>='
            AND c.count_int < rp.required_count
        ),
        FinalRelatedDecks AS (
            SELECT deck_ID_var FROM RelatedDecks
            WHERE deck_ID_var NOT IN (SELECT deck_ID_var FROM ExcludedDecks)
        ),
        AllRelatedCards AS (
            SELECT
                c.deck_ID_var,
                c.category_int,
                MIN(c.image_var) as image_var,
                CASE
                    WHEN TRIM(COALESCE(c.first_attack, '')) != '' THEN
                        REPLACE(CONCAT(c.name_var, '(', c.first_attack, ')'), ' ', '')
                    ELSE
                        REPLACE(c.name_var, ' ', '')
                END AS name_var,
                c.count_int
            FROM cards c
            INNER JOIN FinalRelatedDecks rd ON c.deck_ID_var = rd.deck_ID_var
            GROUP BY c.deck_ID_var, c.category_int, name_var
        ),
        CardCountsByDeck AS (
            SELECT
                deck_ID_var,
                category_int,
                image_var,
                name_var,
                count_int
            FROM AllRelatedCards
        ),
        PairAppearanceInDecks AS (
            SELECT
                category_int,
                image_var,
                name_var,
                count_int,
                COUNT(deck_ID_var) AS appearance_count
            FROM CardCountsByDeck
            GROUP BY category_int, name_var, count_int
            ORDER BY category_int, count_int ASC
        )
        SELECT
            category_int,
            image_var,
            name_var,
            GROUP_CONCAT(count_int ORDER BY count_int) AS COUNT,
            GROUP_CONCAT(appearance_count ORDER BY count_int) AS counts_array,
            MAX(appearance_count) as max_appearance
        FROM PairAppearanceInDecks
        GROUP BY category_int, name_var
        ORDER BY max_appearance DESC
      `;
    } else {
      // Simplified query without category filtering
      query = `
        WITH FilteredEvents AS (
            SELECT id, event_holding_id
            FROM events
            WHERE event_date_date BETWEEN ? AND ?
            AND event_league_int = ?${prefectureList ? ` AND event_prefecture IN (${prefectureList})` : ""}
        ),
        FilteredDecks AS (
            SELECT d.deck_ID_var
            FROM decks d
            JOIN FilteredEvents fe ON d.event_holding_id = fe.event_holding_id
            WHERE 1=1 ${rankConditions}
        ),
        AllRelatedCards AS (
            SELECT
                c.deck_ID_var,
                c.category_int,
                MIN(c.image_var) as image_var,
                CASE
                    WHEN TRIM(COALESCE(c.first_attack, '')) != '' THEN
                        REPLACE(CONCAT(c.name_var, '(', c.first_attack, ')'), ' ', '')
                    ELSE
                        REPLACE(c.name_var, ' ', '')
                END AS name_var,
                c.count_int
            FROM cards c
            INNER JOIN FilteredDecks fd ON c.deck_ID_var = fd.deck_ID_var
            GROUP BY c.deck_ID_var, c.category_int, name_var
        ),
        CardCountsByDeck AS (
            SELECT
                deck_ID_var,
                category_int,
                image_var,
                name_var,
                count_int
            FROM AllRelatedCards
        ),
        PairAppearanceInDecks AS (
            SELECT
                category_int,
                image_var,
                name_var,
                count_int,
                COUNT(deck_ID_var) AS appearance_count
            FROM CardCountsByDeck
            GROUP BY category_int, name_var, count_int
            ORDER BY category_int, count_int ASC
        )
        SELECT
            category_int,
            image_var,
            name_var,
            GROUP_CONCAT(count_int ORDER BY count_int) AS COUNT,
            GROUP_CONCAT(appearance_count ORDER BY count_int) AS counts_array,
            MAX(appearance_count) as max_appearance
        FROM PairAppearanceInDecks
        GROUP BY category_int, name_var
        ORDER BY max_appearance DESC
      `;
    }

    const [rows] = await db.query(query, [startDate, endDate, league]);

    const events_count_query = `
                SELECT COUNT(*) AS total_events_count
                FROM events
                WHERE event_date_date BETWEEN ? AND ? AND event_league_int = ?${prefectureList ? ` AND event_prefecture IN (${prefectureList})` : ""};
            `;

    const events_count = await db.query(events_count_query, [
      startDate,
      endDate,
      league,
    ]);
    const filtered_events_count = events_count[0][0]?.total_events_count || 0;

    console.log("😀😀😀😀filtered_events_count==>", filtered_events_count);

    const decks_count_query = `
                SELECT COUNT(*) AS total_decks_count
                FROM decks LEFT JOIN events ON decks.event_holding_id = events.event_holding_id
                WHERE events.event_date_date BETWEEN ? AND ? AND decks.rank_int IN (${ranks}) AND events.event_league_int = ${league}${prefectureList ? ` AND events.event_prefecture IN (${prefectureList})` : ""};
            `;

    const [decks_count] = await db.query(decks_count_query, [
      startDate,
      endDate,
    ]);

    const filtered_decks_count = decks_count[0]?.total_decks_count || 0;

    console.log("decks_count==>", filtered_decks_count);

    let filtered_specific_decks_count = 0;

    if (conds.length > 0) {
      const specific_decks_count_query = `
                  WITH FilteredEvents AS (
                      SELECT id, event_holding_id
                      FROM events
                      WHERE event_date_date BETWEEN ? AND ?
                      AND event_league_int = ?${prefectureList ? ` AND event_prefecture IN (${prefectureList})` : ""}
                  ),
                  FilteredDecks AS (
                      SELECT d.deck_ID_var
                      FROM decks d
                      JOIN FilteredEvents fe ON d.event_holding_id = fe.event_holding_id
                      WHERE rank_int IN (${ranks})
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
                      SELECT dcc.deck_ID_var
                      FROM DeckCardCounts dcc
                      JOIN RequiredPairs rp ON REPLACE(dcc.name_var, ' ', '') = rp.name_var
                      WHERE
                          ${whereConditions}
                      GROUP BY dcc.deck_ID_var
                      HAVING COUNT(DISTINCT dcc.name_var) >= (SELECT COUNT(*) FROM RequiredPairs)
                  )
                  SELECT COUNT(*) AS specific_count FROM FilteredValidDecks
              `;

      const [specific_decks_count] = await db.query(
        specific_decks_count_query,
        [startDate, endDate, league],
      );
      console.log(specific_decks_count);
      filtered_specific_decks_count =
        specific_decks_count[0]?.specific_count || 0;
    } else {
      // If no category filter, specific count equals total deck count
      filtered_specific_decks_count = filtered_decks_count;
    }

    console.log("specific_decks_count==>", filtered_specific_decks_count);

    res.status(200).json({
      rows,
      filtered_events_count,
      filtered_decks_count,
      filtered_specific_decks_count,
    });
  } catch (err) {
    console.error("Error fetching data:", err);
    res.status(500).json({ message: "Internal Server Error" });
  }
};

exports.getCardsTest = async (req, res) => {
  console.log("⛳ API FROM FRONTEND IS ARRIVED!@@@@@@@@@@@@@@@@@ TEST ⛳");
  try {
    console.log("req.body==>", req.body);

    const {
      startDate: filterStartDate,
      endDate: filterEndDate,
      league,
      category,
      ranks,
      prefectures,
    } = req.body.filter;

    if (!filterStartDate || !filterEndDate || !league) {
      return res.status(400).json({ message: "Missing required parameters" });
    }

    // Convert dates to JST (UTC+9) by treating input as JST date
    const startDate = filterStartDate; // Keep as-is since MySQL DATE type doesn't store timezone
    const endDate = filterEndDate;

    console.log("ranks", ranks);

    // Process ranking filters
    let rankConditions = "";
    if (ranks && typeof ranks === "object") {
      const rankFilters = [];

      if (ranks.winner) rankFilters.push("d.rank_int = 1");
      if (ranks.runnerUp) rankFilters.push("d.rank_int = 2");
      if (ranks.top4) rankFilters.push("d.rank_int <= 4");
      if (ranks.top8) rankFilters.push("d.rank_int <= 8");
      if (ranks.top16) rankFilters.push("d.rank_int <= 16");
      if (ranks.all) rankFilters.push("d.rank_int > 0"); // All ranked decks

      if (rankFilters.length > 0) {
        rankConditions = `AND (${rankFilters.join(" OR ")})`;
      }
    }

    // Format prefectures with quotes for SQL IN clause
    // If prefectures is an empty array [], we should return no results
    let prefectureList = null;
    let isPrefectureFilterEmpty = false;

    if (prefectures !== undefined && prefectures !== null) {
      if (Array.isArray(prefectures)) {
        if (prefectures.length === 0) {
          isPrefectureFilterEmpty = true; // Empty array means no results
        } else {
          prefectureList = prefectures.map((p) => `'${p}'`).join(",");
        }
      } else if (typeof prefectures === "string") {
        const trimmed = prefectures.trim();
        if (trimmed.length === 0) {
          isPrefectureFilterEmpty = true; // Empty string means no results
        } else {
          prefectureList = trimmed
            .split(",")
            .map((p) => `'${p.trim()}'`)
            .join(",");
        }
      }
    }

    // If prefecture filter is explicitly empty, return empty results immediately
    if (isPrefectureFilterEmpty) {
      res.status(200).json({
        rows: [],
        filtered_events_count: 0,
        filtered_decks_count: 0,
        filtered_specific_decks_count: 0,
      });
      return;
    }

    // If category is empty string, skip category filtering
    let conds = [];
    if (category && category.trim() !== "") {
      const cd_query = `
          SELECT conds from deck_categories1 WHERE category1_var = ?
      `;
      const [conditions] = await db_test.query(cd_query, [category]);
      console.log("****conditions", conditions[0]);
      conds =
        conditions[0] && conditions[0].conds && conditions[0].conds.length > 0
          ? JSON.parse(conditions[0].conds)
          : [];
    }

    // let where_cond = "";
    // if(conds.length > 0) {
    //     conds.forEach(item => {
    //         switch(item.cardCondition) {
    //             case "eql":
    //                 where_cond += `AND c.name_var = '${item.cardName}' AND c.count_int = ${item.cardNumber} `
    //                 break;
    //             case "gte":
    //                 where_cond += `AND c.name_var = '${item.cardName}' AND c.count_int >= ${item.cardNumber} `
    //                 break;
    //             case "lte":
    //                 where_cond += `AND c.name_var = '${item.cardName}' AND c.count_int <= ${item.cardNumber} `
    //                 break;
    //             case "ueq":
    //                 where_cond += `AND c.name_var = '${item.cardName}' AND c.count_int != ${item.cardNumber} `
    //                 break;
    //             default:
    //                 break;
    //         }
    //     })
    // }

    let having_cond = "";
    let select_cond = "";
    let requiredPairsSQL = "";
    let whereConditions = "";
    if (conds.length > 0) {
      conds.forEach((item, index) => {
        let operator;
        switch (item.cardCondition) {
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
        having_cond += ` AND count_val_${index + 1} ${operator} ${item.cardNumber}`;
        select_cond += `SUM(CASE WHEN name_var = '${item.cardName}' THEN c.count_int ELSE 0 END) AS count_val_${index + 1}`;
        // Append SQL for RequiredPairs table
        requiredPairsSQL += `    SELECT '${item.cardName}' AS name_var, ${item.cardNumber} AS required_count, '${operator}' AS operator`;
        whereConditions += `    (rp.operator = '${operator}' AND dcc.count_int ${operator} rp.required_count)`;

        // Add UNION ALL for all but the last entry
        if (index < conds.length - 1) {
          requiredPairsSQL += " UNION ALL";
          whereConditions += " OR";
        }
        console.log(
          "having_cond==>",
          having_cond,
          "select_cond==>",
          select_cond,
          "requiredPairsSQL==>",
          requiredPairsSQL,
          "whereConditions==>",
          whereConditions,
        );
      });
    }
    // FilteredCardsByCategory AS (
    //     SELECT c.*
    //     FROM cards c
    //     WHERE EXISTS (
    //         SELECT 1
    //         FROM FilteredDecks fd
    //         WHERE c.deck_ID_var = fd.deck_ID_var
    //     )
    //     ${where_cond}
    // ),
    // where_cond = where_cond == "" ? "true" : where_cond;
    // console.log("****where_cond", where_cond)

    let query;
    if (conds.length > 0) {
      // Query with category filtering
      query = `
        WITH FilteredEvents AS (
            SELECT id, event_holding_id
            FROM events
            WHERE event_date_date BETWEEN ? AND ?
            AND event_league_int = ?${prefectureList ? ` AND event_prefecture IN (${prefectureList})` : ""}
        ),
        FilteredDecks AS (
            SELECT d.*
            FROM decks d
            JOIN FilteredEvents fe ON d.event_holding_id = fe.event_holding_id
            WHERE 1=1 ${rankConditions}
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
            SELECT dcc.deck_ID_var
            FROM DeckCardCounts dcc
            JOIN RequiredPairs rp ON dcc.name_var = rp.name_var
            WHERE
                ${whereConditions}
            GROUP BY dcc.deck_ID_var
            HAVING COUNT(DISTINCT dcc.name_var) >= (SELECT COUNT(*) FROM RequiredPairs)
        ),
        RelatedDecks AS (
            SELECT DISTINCT fvd.deck_ID_var FROM FilteredValidDecks  fvd LEFT JOIN decks d on fvd.deck_ID_var = d.deck_ID_var WHERE d.rank_int IN (${ranks})
        ),
        ExcludedDecks AS (
            SELECT DISTINCT c.deck_ID_var
            FROM cards c
            JOIN RequiredPairs rp ON REPLACE(c.name_var, ' ', '') = rp.name_var
            WHERE c.deck_ID_var IN (SELECT deck_ID_var FROM RelatedDecks)
            AND rp.operator = '>='
            AND c.count_int < rp.required_count
        ),
        FinalRelatedDecks AS (
            SELECT deck_ID_var FROM RelatedDecks
            WHERE deck_ID_var NOT IN (SELECT deck_ID_var FROM ExcludedDecks)
        ),
        AllRelatedCards AS (
            SELECT
                c.deck_ID_var,
                c.category_int,
                MIN(c.image_var) as image_var,
                CASE
                    WHEN TRIM(COALESCE(c.first_attack, '')) != '' THEN
                        REPLACE(CONCAT(c.name_var, '(', c.first_attack, ')'), ' ', '')
                    ELSE
                        REPLACE(c.name_var, ' ', '')
                END AS name_var,
                c.count_int
            FROM cards c
            INNER JOIN FinalRelatedDecks rd ON c.deck_ID_var = rd.deck_ID_var
            GROUP BY c.deck_ID_var, c.category_int, name_var
        ),
        CardCountsByDeck AS (
            SELECT
                deck_ID_var,
                category_int,
                image_var,
                name_var,
                count_int
            FROM AllRelatedCards
        ),
        PairAppearanceInDecks AS (
            SELECT
                category_int,
                image_var,
                name_var,
                count_int,
                COUNT(deck_ID_var) AS appearance_count
            FROM CardCountsByDeck
            GROUP BY category_int, name_var, count_int
            ORDER BY category_int, count_int ASC
        )
        SELECT
            category_int,
            image_var,
            name_var,
            GROUP_CONCAT(count_int ORDER BY count_int) AS COUNT,
            GROUP_CONCAT(appearance_count ORDER BY count_int) AS counts_array,
            MAX(appearance_count) as max_appearance
        FROM PairAppearanceInDecks
        GROUP BY category_int, name_var
        ORDER BY max_appearance DESC
      `;
    } else {
      // Simplified query without category filtering
      query = `
        WITH FilteredEvents AS (
            SELECT id, event_holding_id
            FROM events
            WHERE event_date_date BETWEEN ? AND ?
            AND event_league_int = ?${prefectureList ? ` AND event_prefecture IN (${prefectureList})` : ""}
        ),
        FilteredDecks AS (
            SELECT d.deck_ID_var
            FROM decks d
            JOIN FilteredEvents fe ON d.event_holding_id = fe.event_holding_id
            WHERE 1=1 ${rankConditions}
        ),
        AllRelatedCards AS (
            SELECT
                c.deck_ID_var,
                c.category_int,
                MIN(c.image_var) as image_var,
                CASE
                    WHEN TRIM(COALESCE(c.first_attack, '')) != '' THEN
                        REPLACE(CONCAT(c.name_var, '(', c.first_attack, ')'), ' ', '')
                    ELSE
                        REPLACE(c.name_var, ' ', '')
                END AS name_var,
                c.count_int
            FROM cards c
            INNER JOIN FilteredDecks fd ON c.deck_ID_var = fd.deck_ID_var
            GROUP BY c.deck_ID_var, c.category_int, name_var
        ),
        CardCountsByDeck AS (
            SELECT
                deck_ID_var,
                category_int,
                image_var,
                name_var,
                count_int
            FROM AllRelatedCards
        ),
        PairAppearanceInDecks AS (
            SELECT
                category_int,
                image_var,
                name_var,
                count_int,
                COUNT(deck_ID_var) AS appearance_count
            FROM CardCountsByDeck
            GROUP BY category_int, name_var, count_int
            ORDER BY category_int, count_int ASC
        )
        SELECT
            category_int,
            image_var,
            name_var,
            GROUP_CONCAT(count_int ORDER BY count_int) AS COUNT,
            GROUP_CONCAT(appearance_count ORDER BY count_int) AS counts_array,
            MAX(appearance_count) as max_appearance
        FROM PairAppearanceInDecks
        GROUP BY category_int, name_var
        ORDER BY max_appearance DESC
      `;
    }

    console.log("search query=>", query);
    const [rows] = await db_test.query(query, [startDate, endDate, league]);

    const events_count_query = `
                SELECT COUNT(*) AS total_events_count
                FROM events
                WHERE event_date_date BETWEEN ? AND ? AND event_league_int = ?${prefectureList ? ` AND event_prefecture IN (${prefectureList})` : ""};
            `;

    const events_count = await db_test.query(events_count_query, [
      startDate,
      endDate,
      league,
    ]);
    const filtered_events_count = events_count[0][0]?.total_events_count || 0;

    console.log("😀😀😀😀filtered_events_count==>", filtered_events_count);

    const decks_count_query = `
                SELECT COUNT(*) AS total_decks_count
                FROM decks LEFT JOIN events ON decks.event_holding_id = events.event_holding_id
                WHERE events.event_date_date BETWEEN ? AND ? AND decks.rank_int IN (${ranks}) AND events.event_league_int = ${league}${prefectureList ? ` AND events.event_prefecture IN (${prefectureList})` : ""};
            `;

    const [decks_count] = await db_test.query(decks_count_query, [
      startDate,
      endDate,
    ]);

    const filtered_decks_count = decks_count[0]?.total_decks_count || 0;

    console.log("decks_count==>", filtered_decks_count);

    let filtered_specific_decks_count = 0;

    if (conds.length > 0) {
      const specific_decks_count_query = `
                  WITH FilteredEvents AS (
                      SELECT id, event_holding_id
                      FROM events
                      WHERE event_date_date BETWEEN ? AND ?
                      AND event_league_int = ?${prefectureList ? ` AND event_prefecture IN (${prefectureList})` : ""}
                  ),
                  FilteredDecks AS (
                      SELECT d.deck_ID_var
                      FROM decks d
                      JOIN FilteredEvents fe ON d.event_holding_id = fe.event_holding_id
                      WHERE rank_int IN (${ranks})
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
                      SELECT dcc.deck_ID_var
                      FROM DeckCardCounts dcc
                      JOIN RequiredPairs rp ON REPLACE(dcc.name_var, ' ', '') = rp.name_var
                      WHERE
                          ${whereConditions}
                      GROUP BY dcc.deck_ID_var
                      HAVING COUNT(DISTINCT dcc.name_var) >= (SELECT COUNT(*) FROM RequiredPairs)
                  )
                  SELECT COUNT(*) AS specific_count FROM FilteredValidDecks
              `;

      const [specific_decks_count] = await db_test.query(
        specific_decks_count_query,
        [startDate, endDate, league],
      );
      console.log(specific_decks_count);
      filtered_specific_decks_count =
        specific_decks_count[0]?.specific_count || 0;
    } else {
      // If no category filter, specific count equals total deck count
      filtered_specific_decks_count = filtered_decks_count;
    }

    console.log("specific_decks_count==>", filtered_specific_decks_count);

    res.status(200).json({
      rows,
      filtered_events_count,
      filtered_decks_count,
      filtered_specific_decks_count,
    });
  } catch (err) {
    console.error("Error fetching data:", err);
    res.status(500).json({ message: "Internal Server Error" });
  }
};

exports.getCardCategories = async (req, res) => {
  try {
    const query = `SELECT * FROM deck_categories1`;
    const [deck_categories1] = await db.query(query);
    res.status(200).json(deck_categories1);
  } catch (err) {
    console.error("Error fetching data:", err);
    res
      .status(500)
      .json({ message: "Internal Server Error", error: err.message });
  }
};

exports.getAll = async (req, res) => {
  try {
    const query = `SELECT * FROM cards`;
    const [cards] = await db.query(query);
    res.status(200).json(cards);
  } catch (err) {
    console.error("error cards:", err);
    res
      .status(500)
      .json({ message: "Internal Server error", error: err.message });
  }
};

exports.searchCards = async (req, res) => {
  try {
    const keyword = req.query.keyword;
    const query = `SELECT name_var FROM cards WHERE name_var LIKE '%${keyword}%' GROUP BY name_var`;
    const [rows] = await db.query(query);

    const cards = rows.map((row) => row.name_var);
    // cards = ['test01', 'test02'];
    res.status(200).json(cards);
  } catch (err) {
    console.error("error cards:", err);
    res
      .status(500)
      .json({ message: "Internal Server error", error: err.message });
  }
};
