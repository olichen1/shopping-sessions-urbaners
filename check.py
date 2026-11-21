"""Compare /output parquet against the dev-answer CSVs.
Usage: python check.py OUTPUT_DIR DEV_ANSWER_DIR   (needs: pip install duckdb)"""
import sys
import duckdb

out, exp = sys.argv[1], sys.argv[2]
con = duckdb.connect()
spec = {
    "user_daily": (
        "user_id VARCHAR, day DATE, sessions BIGINT, events BIGINT, revenue_cents BIGINT, top_category VARCHAR",
        "user_id, day, sessions, events, revenue_cents, top_category",
    ),
    "campaign_daily": (
        "campaign_id VARCHAR, channel VARCHAR, day DATE, sessions BIGINT, purchasing_sessions BIGINT, revenue_cents BIGINT, assisted_sessions BIGINT",
        "campaign_id, channel, day, sessions, purchasing_sessions, revenue_cents, assisted_sessions",
    ),
}
ok = True
for name, (types, cols) in spec.items():
    con.execute(f"CREATE TABLE a_{name} AS SELECT {cols} FROM read_parquet('{out}/{name}/*.parquet')")
    tdict = "{" + ", ".join("'%s': '%s'" % tuple(t.split()) for t in types.split(", ")) + "}"
    con.execute(f"CREATE TABLE b_{name} AS SELECT {cols} FROM read_csv('{exp}/{name}.csv', header=true, types={tdict})")
    missing = con.execute(f"SELECT count(*) FROM (SELECT * FROM b_{name} EXCEPT SELECT * FROM a_{name})").fetchone()[0]
    extra = con.execute(f"SELECT count(*) FROM (SELECT * FROM a_{name} EXCEPT SELECT * FROM b_{name})").fetchone()[0]
    na = con.execute(f"SELECT count(*) FROM a_{name}").fetchone()[0]
    nb = con.execute(f"SELECT count(*) FROM b_{name}").fetchone()[0]
    print(f"{name}: yours={na} expected={nb} missing={missing} extra={extra}")
    ok &= (missing == 0 and extra == 0 and na == nb)
print("PASS" if ok else "FAIL")
sys.exit(0 if ok else 1)
