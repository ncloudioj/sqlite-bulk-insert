use itertools::Itertools;
use rusqlite::{named_params, params, Connection, ToSql};

pub const SQL: &str = "
    CREATE TABLE IF NOT EXISTS keywords(
        keyword TEXT NOT NULL,
        suggestion_id INTEGER NOT NULL,
        rank INTEGER NOT NULL,
        PRIMARY KEY (keyword, suggestion_id)
    ) WITHOUT ROWID;
";

fn set_up_db(conn: &mut Connection) {
    let initial_pragmas = "
        -- Use in-memory storage for TEMP tables.
        PRAGMA temp_store = 2;

        PRAGMA journal_mode = WAL;
        PRAGMA foreign_keys = ON;
    ";
    conn.execute_batch(initial_pragmas).unwrap();
    conn.execute_batch(SQL).unwrap();
}

fn main() {
    // 149 is a multiplier of the total keywords so that it's easier to handle here.
    const BATCH_SIZE: usize = 149;
    let keywords = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/", "keywords.txt"));

    let mut conn = Connection::open("keywords.db").unwrap();
    set_up_db(&mut conn);

    let mut values_clause = " (?, ?, ?),".repeat(BATCH_SIZE);
    values_clause.pop();
    let insert_stmt = format!("INSERT INTO keywords VALUES {values_clause}");
    let mut stmt = conn.prepare_cached(&insert_stmt).unwrap();

    for chunk in keywords.split(',').chunks(BATCH_SIZE).into_iter() {
        let mut triplets = Vec::<(String, i32, i32)>::new();
        for keyword in chunk {
            triplets.push((keyword.to_owned(), 100, 1000));
        }

        let mut param_values: Vec<_> = Vec::new();
        for triplet in triplets.iter() {
            param_values.push(&triplet.0 as &dyn ToSql);
            param_values.push(&triplet.1 as &dyn ToSql);
            param_values.push(&triplet.2 as &dyn ToSql);
        }
        stmt.execute(&*param_values).unwrap();
    }

    /* Prepared & unbatched
    for keyword in keywords.split(',') {
        conn.execute(
            "INSERT INTO keywords(
                keyword,
                suggestion_id,
                rank
            )
            VALUES(
                :keyword,
                :suggestion_id,
                :rank
            )",
            named_params! {
                ":keyword": keyword,
                ":rank": 100,
                ":suggestion_id": 1000,
            },
        )
        .unwrap();
    }
    */
}
