import calendar
import datetime
import json
import os
import sys
import time

import duckdb
import numpy as np
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components

DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", "/scratch")
NCPU = int(os.environ.get("NCPU", "8"))
MEM = int(os.environ.get("MEM_GB", "24"))

WEEK = 604800000
DAY = 86400000
GAP = 1800000

SEP = chr(31)  # never appears in the data: each CSV line becomes one VARCHAR
QUOTE = chr(30)  # same, used so DuckDB never interprets quotes itself

# One CSV field: either wrapped in double quotes, or with no quote at all.
# No value ever holds a comma or a quote, so this is exact.
FIELD = '(?:"[^",]*"|[^",]*)'


def line_re(n):
    return FIELD + ("," + FIELD) * (n - 1)


EV_HDR = "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents"
CK_HDR = "click_id,user_id,campaign_id,ts"
LK_HDR = "user_id_a,user_id_b"

TS_MACRO = r"""
CREATE OR REPLACE MACRO parse_ts(s) AS (
  CASE
    WHEN s IS NULL THEN NULL
    WHEN regexp_full_match(s, '[0-9]+') THEN try_cast(s AS BIGINT)
    WHEN regexp_full_match(s, '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]Z')
      THEN epoch_ms(try_strptime(substr(s, 1, 19), '%Y-%m-%dT%H:%M:%S'))
    WHEN regexp_full_match(s, '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9][+-]([01][0-9]|2[0-3]):[0-5][0-9]')
      THEN epoch_ms(try_strptime(substr(s, 1, 19), '%Y-%m-%dT%H:%M:%S'))
           - (CASE WHEN substr(s, 20, 1) = '-' THEN -1 ELSE 1 END)
             * (CAST(substr(s, 21, 2) AS BIGINT) * 3600000
                + CAST(substr(s, 24, 2) AS BIGINT) * 60000)
    ELSE NULL
  END)
"""


def iso_ms(s):
    t = datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
    return calendar.timegm(t.timetuple()) * 1000


def log(msg, t0):
    print(f"[{time.time() - t0:7.2f}s] {msg}", file=sys.stderr, flush=True)


def lines_src(pattern):
    return (
        f"read_csv('{pattern}', columns={{'line': 'VARCHAR'}}, header=false, "
        f"auto_detect=false, delim='{SEP}', quote='{QUOTE}', escape='{QUOTE}', "
        f"ignore_errors=true)"
    )


def main():
    t0 = time.time()
    with open(os.path.join(DATA, "manifest.json")) as fh:
        man = json.load(fh)
    WS = iso_ms(man["window_start"])
    WE = iso_ms(man["window_end"])

    os.makedirs(os.path.join(OUT, "user_daily"), exist_ok=True)
    os.makedirs(os.path.join(OUT, "campaign_daily"), exist_ok=True)
    tmpdir = os.path.join(SCRATCH, "duck_tmp")
    os.makedirs(tmpdir, exist_ok=True)

    con = duckdb.connect()
    con.execute(f"SET threads={NCPU}")
    con.execute(f"SET memory_limit='{max(MEM - 4, 4)}GB'")
    con.execute(f"SET temp_directory='{tmpdir}'")
    con.execute("SET preserve_insertion_order=false")
    con.execute(TS_MACRO)

    # ---------------- reference files ----------------
    con.execute(
        f"CREATE TEMP TABLE products AS SELECT product_id, category FROM "
        f"read_csv('{DATA}/products.csv', header=true, all_varchar=true)"
    )
    con.execute(
        f"CREATE TEMP TABLE campaigns AS SELECT campaign_id, channel FROM "
        f"read_csv('{DATA}/campaigns.csv', header=true, all_varchar=true)"
    )

    # ---------------- links -> persons ----------------
    con.execute(
        f"""
        CREATE TEMP TABLE lk AS
        SELECT DISTINCT f[1] AS a, f[2] AS b FROM (
          SELECT string_split(replace(line, '"', ''), ',') AS f
          FROM {lines_src(DATA + '/links/*.csv')}
          WHERE line IS NOT NULL AND line <> '{LK_HDR}'
            AND regexp_full_match(line, '{line_re(2)}')
        ) WHERE f[1] <> '' AND f[2] <> ''
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE lid AS
        SELECT id, CAST(row_number() OVER () - 1 AS BIGINT) AS idx
        FROM (SELECT a AS id FROM lk UNION SELECT b FROM lk)
        """
    )
    n = con.execute("SELECT count(*) FROM lid").fetchone()[0]
    if n == 0:
        con.execute("CREATE TEMP TABLE pmap (id VARCHAR, person VARCHAR)")
    else:
        e = con.execute(
            """
            SELECT x.idx AS a, y.idx AS b
            FROM lk JOIN lid x ON lk.a = x.id JOIN lid y ON lk.b = y.id
            """
        ).fetchnumpy()
        ea = np.asarray(e["a"], dtype=np.int64)
        eb = np.asarray(e["b"], dtype=np.int64)
        g = coo_matrix((np.ones(len(ea), dtype=np.int8), (ea, eb)), shape=(n, n))
        _, labels = connected_components(g, directed=False)
        lab = {
            "idx": np.arange(n, dtype=np.int64),
            "comp": labels.astype(np.int64),
        }
        try:
            con.execute("CREATE TEMP TABLE labt AS SELECT idx, comp FROM lab")
        except Exception:
            con.execute(
                "CREATE TEMP TABLE labt AS SELECT unnest(?::BIGINT[]) AS idx, "
                "unnest(?::BIGINT[]) AS comp",
                [lab["idx"].tolist(), lab["comp"].tolist()],
            )
        con.execute(
            """
            CREATE TEMP TABLE pmap AS
            SELECT id, min(id) OVER (PARTITION BY comp) AS person
            FROM (SELECT l.id AS id, c.comp AS comp
                  FROM lid l JOIN labt c ON l.idx = c.idx)
            """
        )
    log(f"links done ({n} linked ids)", t0)

    # ---------------- events ----------------
    con.execute(
        f"""
        CREATE TEMP TABLE ev AS
        SELECT f[1] AS event_id,
               arg_min(f[2], f[4]) AS user_id,
               arg_min(f[3], f[4]) AS ts,
               min(f[4]) AS ingest,
               arg_min(f[5], f[4]) AS etype,
               arg_min(f[6], f[4]) AS product,
               arg_min(f[7], f[4]) AS price
        FROM (
          SELECT string_split(replace(line, '"', ''), ',') AS f
          FROM {lines_src(DATA + '/events/*.csv')}
          WHERE line IS NOT NULL AND line <> '{EV_HDR}'
            AND regexp_full_match(line, '{line_re(7)}')
        )
        WHERE f[1] <> ''
        GROUP BY f[1]
        """
    )
    log("events deduped", t0)
    con.execute(
        f"""
        CREATE TEMP TABLE evv AS
        SELECT user_id, ts_ms, etype, product, price_c FROM (
          SELECT user_id, parse_ts(ts) AS ts_ms, parse_ts(ingest) AS ing_ms,
                 etype, product,
                 CASE WHEN etype = 'purchase' THEN
                   CASE WHEN regexp_full_match(price, '[0-9]+')
                        THEN try_cast(price AS BIGINT) END
                 END AS price_c
          FROM ev
          WHERE user_id <> '' AND product <> ''
            AND etype IN ('view', 'cart', 'purchase')
        )
        WHERE ts_ms >= {WS} AND ts_ms < {WE}
          AND ing_ms - ts_ms <= {DAY}
          AND (etype <> 'purchase' OR price_c BETWEEN 1 AND 10000000)
        """
    )
    con.execute("DROP TABLE ev")
    con.execute(
        """
        CREATE TEMP TABLE evp AS
        SELECT coalesce(m.person, e.user_id) AS person, e.ts_ms, e.etype,
               e.product, e.price_c
        FROM evv e LEFT JOIN pmap m ON e.user_id = m.id
        """
    )
    con.execute("DROP TABLE evv")
    log("events validated", t0)

    # ---------------- sessions ----------------
    # Default RANGE frame: events with equal ts are peers and get the same sid.
    con.execute(
        f"""
        CREATE TEMP TABLE evs AS
        SELECT person, ts_ms, etype, product, price_c,
               sum(CASE WHEN prev IS NULL OR ts_ms - prev > {GAP} THEN 1 ELSE 0 END)
                 OVER (PARTITION BY person ORDER BY ts_ms) AS sid
        FROM (SELECT *, lag(ts_ms) OVER (PARTITION BY person ORDER BY ts_ms) AS prev
              FROM evp)
        """
    )
    con.execute("DROP TABLE evp")
    con.execute(
        """
        CREATE TEMP TABLE sess AS
        SELECT person, sid, min(ts_ms) AS start_ms,
               CAST(epoch_ms(min(ts_ms)) AS DATE) AS day,
               count(*) AS ev,
               CAST(coalesce(sum(price_c), 0) AS BIGINT) AS rev,
               count(*) FILTER (WHERE etype = 'purchase') AS np
        FROM evs GROUP BY person, sid
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE pc AS
        SELECT s.person, s.day, coalesce(p.category, 'unknown') AS cat,
               sum(e.price_c) AS amt
        FROM (SELECT * FROM evs WHERE etype = 'purchase') e
        JOIN sess s ON s.person = e.person AND s.sid = e.sid
        LEFT JOIN products p ON p.product_id = e.product
        GROUP BY s.person, s.day, coalesce(p.category, 'unknown')
        """
    )
    con.execute("DROP TABLE evs")
    con.execute(
        """
        CREATE TEMP TABLE top AS
        SELECT person, day, cat FROM (
          SELECT person, day, cat,
                 row_number() OVER (PARTITION BY person, day
                                    ORDER BY amt DESC, cat ASC) AS rn
          FROM pc) WHERE rn = 1
        """
    )
    log("sessions built", t0)

    # ---------------- clicks ----------------
    con.execute(
        f"""
        CREATE TEMP TABLE ck AS
        SELECT coalesce(m.person, c.user_id) AS person, c.click_id,
               c.campaign_id, c.ts_ms
        FROM (
          SELECT click_id, user_id, campaign_id, parse_ts(ts) AS ts_ms FROM (
            SELECT f[1] AS click_id, any_value(f[2]) AS user_id,
                   any_value(f[3]) AS campaign_id, any_value(f[4]) AS ts
            FROM (
              SELECT string_split(replace(line, '"', ''), ',') AS f
              FROM {lines_src(DATA + '/clicks/*.csv')}
              WHERE line IS NOT NULL AND line <> '{CK_HDR}'
                AND regexp_full_match(line, '{line_re(4)}')
            )
            WHERE f[1] <> ''
            GROUP BY f[1]
          ) WHERE user_id <> ''
        ) c
        JOIN campaigns cp ON cp.campaign_id = c.campaign_id
        LEFT JOIN pmap m ON c.user_id = m.id
        WHERE c.ts_ms >= {WS - WEEK} AND c.ts_ms < {WE}
        """
    )
    # latest click per (person, ts), ties -> smallest click_id
    con.execute(
        """
        CREATE TEMP TABLE ck1 AS
        SELECT person, ts_ms, arg_min(campaign_id, click_id) AS campaign_id
        FROM ck GROUP BY person, ts_ms
        """
    )
    # distinct (person, campaign, ts) for assists
    con.execute(
        """
        CREATE TEMP TABLE ck2 AS
        SELECT DISTINCT person, campaign_id, ts_ms FROM ck
        """
    )
    log("clicks ready", t0)

    # ---------------- attribution ----------------
    con.execute(
        f"""
        CREATE TEMP TABLE cred AS
        SELECT s.person, s.sid, s.day, s.ev, s.rev, s.np,
               CASE WHEN c.ts_ms IS NOT NULL AND s.start_ms - c.ts_ms <= {WEEK}
                    THEN c.campaign_id ELSE 'organic' END AS camp
        FROM sess s
        ASOF LEFT JOIN ck1 c ON s.person = c.person AND s.start_ms >= c.ts_ms
        """
    )
    con.execute(
        f"""
        CREATE TEMP TABLE asst AS
        SELECT campaign_id, day, count(*) AS assisted_sessions FROM (
          SELECT DISTINCT s.person, s.sid, s.day, k.campaign_id
          FROM sess s JOIN ck2 k
            ON k.person = s.person
           AND k.ts_ms <= s.start_ms
           AND k.ts_ms >= s.start_ms - {WEEK}
        ) GROUP BY campaign_id, day
        """
    )
    log("attribution done", t0)

    # ---------------- outputs ----------------
    con.execute(
        f"""
        COPY (
          SELECT u.person AS user_id, u.day AS day, u.sessions, u.events,
                 u.revenue_cents, t.cat AS top_category
          FROM (
            SELECT person, day, count(*) AS sessions,
                   CAST(sum(ev) AS BIGINT) AS events,
                   CAST(sum(rev) AS BIGINT) AS revenue_cents
            FROM cred GROUP BY person, day
          ) u
          LEFT JOIN top t ON t.person = u.person AND t.day = u.day
        ) TO '{OUT}/user_daily/part-0.parquet'
        (FORMAT PARQUET, COMPRESSION 'uncompressed')
        """
    )
    con.execute(
        f"""
        COPY (
          SELECT k.campaign_id,
                 CASE WHEN k.campaign_id = 'organic' THEN 'organic'
                      ELSE ch.channel END AS channel,
                 k.day AS day,
                 CAST(coalesce(c.sessions, 0) AS BIGINT) AS sessions,
                 CAST(coalesce(c.purchasing_sessions, 0) AS BIGINT) AS purchasing_sessions,
                 CAST(coalesce(c.revenue_cents, 0) AS BIGINT) AS revenue_cents,
                 CAST(coalesce(a.assisted_sessions, 0) AS BIGINT) AS assisted_sessions
          FROM (SELECT camp AS campaign_id, day FROM cred
                UNION SELECT campaign_id, day FROM asst) k
          LEFT JOIN (
            SELECT camp AS campaign_id, day, count(*) AS sessions,
                   count(*) FILTER (WHERE np > 0) AS purchasing_sessions,
                   CAST(sum(rev) AS BIGINT) AS revenue_cents
            FROM cred GROUP BY camp, day
          ) c ON c.campaign_id = k.campaign_id AND c.day = k.day
          LEFT JOIN asst a ON a.campaign_id = k.campaign_id AND a.day = k.day
          LEFT JOIN campaigns ch ON ch.campaign_id = k.campaign_id
        ) TO '{OUT}/campaign_daily/part-0.parquet'
        (FORMAT PARQUET, COMPRESSION 'uncompressed')
        """
    )
    log("done", t0)


if __name__ == "__main__":
    main()
