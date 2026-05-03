import json
from contextlib import contextmanager
from datetime import date, datetime
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from klunkar import config
from klunkar.models import Source, Subscriber, Wine
from klunkar.sources.base import EnrichmentResult


@contextmanager
def get_conn():
    with psycopg.connect(config.DATABASE_URL) as conn:
        yield conn


def migrate(conn: psycopg.Connection) -> None:
    with conn.transaction(), conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS subscribers (
                chat_id          BIGINT PRIMARY KEY,
                created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
                max_price        FLOAT,
                last_preview_date DATE
            )
        """)
        cur.execute("ALTER TABLE subscribers ADD COLUMN IF NOT EXISTS max_price FLOAT")
        cur.execute("ALTER TABLE subscribers ADD COLUMN IF NOT EXISTS last_preview_date DATE")
        cur.execute(
            "ALTER TABLE subscribers ADD COLUMN IF NOT EXISTS rank_source TEXT NOT NULL DEFAULT 'munskankarna'"
        )
        cur.execute("ALTER TABLE subscribers ALTER COLUMN rank_source SET DEFAULT 'munskankarna'")
        cur.execute("ALTER TABLE subscribers ADD COLUMN IF NOT EXISTS value_filter TEXT[]")
        cur.execute("ALTER TABLE subscribers ADD COLUMN IF NOT EXISTS wine_type_filter TEXT[]")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS seen_releases (
                release_date DATE PRIMARY KEY,
                notified_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                wine_count   INT,
                message      TEXT
            )
        """)
        cur.execute("ALTER TABLE seen_releases ADD COLUMN IF NOT EXISTS message TEXT")

        cur.execute("DROP TABLE IF EXISTS release_wines")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS wines (
                release_date         DATE NOT NULL,
                sb_product_number    TEXT NOT NULL,
                sb_product_id        TEXT NOT NULL,
                name                 TEXT NOT NULL,
                producer             TEXT NOT NULL,
                sb_url               TEXT NOT NULL,
                price                FLOAT,
                wine_type            TEXT,
                fetched_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (release_date, sb_product_number)
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS wines_release_idx ON wines (release_date)")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS wine_enrichments (
                release_date         DATE NOT NULL,
                sb_product_number    TEXT NOT NULL,
                source               TEXT NOT NULL,
                confidence           FLOAT NOT NULL,
                payload              JSONB NOT NULL,
                fetched_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (release_date, sb_product_number, source),
                FOREIGN KEY (release_date, sb_product_number)
                    REFERENCES wines (release_date, sb_product_number) ON DELETE CASCADE
            )
        """)
        cur.execute(
            "CREATE INDEX IF NOT EXISTS wine_enrichments_release_source_idx "
            "ON wine_enrichments (release_date, source)"
        )

        cur.execute("""
            CREATE TABLE IF NOT EXISTS enrichment_runs (
                release_date  DATE NOT NULL,
                source        TEXT NOT NULL,
                run_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
                matched_count INT NOT NULL,
                total_count   INT NOT NULL,
                PRIMARY KEY (release_date, source, run_at)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS notified_subscribers (
                release_date DATE NOT NULL,
                chat_id      BIGINT NOT NULL,
                notified_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (release_date, chat_id)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS apim_key (
                id         INT PRIMARY KEY DEFAULT 1,
                key        TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                CHECK (id = 1)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS upcoming_release_dates (
                release_date DATE PRIMARY KEY,
                fetched_at   TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS applied_migrations (
                name       TEXT PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """)
        # One-shot data migrations. Each guarded by applied_migrations so a
        # later explicit user choice (e.g. /source vivino) is not undone on
        # subsequent migrate() runs.
        _apply_once(
            cur,
            "2026_05_02_default_source_to_munskankarna",
            "UPDATE subscribers SET rank_source = 'munskankarna' WHERE rank_source = 'vivino'",
        )


def _apply_once(cur: psycopg.Cursor, name: str, sql: str) -> None:
    cur.execute(
        "INSERT INTO applied_migrations (name) VALUES (%s) ON CONFLICT DO NOTHING RETURNING name",
        (name,),
    )
    if cur.fetchone():
        cur.execute(sql)


# ---- APIM key (unchanged) -----------------------------------------------


def get_apim_key(conn: psycopg.Connection) -> str | None:
    with conn.cursor() as cur:
        cur.execute("SELECT key FROM apim_key WHERE id = 1")
        row = cur.fetchone()
        return row[0] if row else None


def set_apim_key(conn: psycopg.Connection, key: str) -> None:
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO apim_key (id, key, updated_at)
            VALUES (1, %s, now())
            ON CONFLICT (id) DO UPDATE SET key = EXCLUDED.key, updated_at = now()
            """,
            (key,),
        )


# ---- Releases (seen / upcoming) -----------------------------------------


def is_release_seen(conn: psycopg.Connection, release_date: date) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM seen_releases WHERE release_date = %s", (release_date,))
        return cur.fetchone() is not None


def mark_release_seen(conn: psycopg.Connection, release_date: date, wine_count: int) -> None:
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO seen_releases (release_date, wine_count)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
            """,
            (release_date, wine_count),
        )


def has_notified_subscriber(conn: psycopg.Connection, release_date: date, chat_id: int) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM notified_subscribers WHERE release_date = %s AND chat_id = %s",
            (release_date, chat_id),
        )
        return cur.fetchone() is not None


def mark_notified_subscriber(conn: psycopg.Connection, release_date: date, chat_id: int) -> None:
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO notified_subscribers (release_date, chat_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
            """,
            (release_date, chat_id),
        )


def save_release_dates(conn: psycopg.Connection, dates: list[date]) -> None:
    with conn.transaction(), conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO upcoming_release_dates (release_date) VALUES (%s) ON CONFLICT DO NOTHING",
            [(d,) for d in dates],
        )


def is_upcoming_release_date(conn: psycopg.Connection, release_date: date) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM upcoming_release_dates WHERE release_date = %s", (release_date,))
        return cur.fetchone() is not None


def get_upcoming_release_dates(conn: psycopg.Connection, from_date: date) -> list[date]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT release_date FROM upcoming_release_dates WHERE release_date >= %s ORDER BY release_date",
            (from_date,),
        )
        return [row[0] for row in cur.fetchall()]


# ---- Wines + enrichments ------------------------------------------------


def upsert_wines(conn: psycopg.Connection, wines: list[Wine]) -> None:
    if not wines:
        return
    with conn.transaction(), conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO wines
                (release_date, sb_product_number, sb_product_id, name, producer, sb_url, price, wine_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (release_date, sb_product_number) DO UPDATE SET
                sb_product_id = EXCLUDED.sb_product_id,
                name          = EXCLUDED.name,
                producer      = EXCLUDED.producer,
                sb_url        = EXCLUDED.sb_url,
                price         = EXCLUDED.price,
                wine_type     = EXCLUDED.wine_type
            """,
            [
                (
                    w.release_date,
                    w.sb_product_number,
                    w.sb_product_id,
                    w.name,
                    w.producer,
                    w.sb_url,
                    w.price,
                    w.wine_type,
                )
                for w in wines
            ],
        )


def get_release_type_counts(conn: psycopg.Connection, release_date: date) -> dict[str, int]:
    """Distinct wine_type → count for a release. Missing wine_types are bucketed as 'Annat'."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COALESCE(wine_type, 'Annat'), COUNT(*) FROM wines "
            "WHERE release_date = %s GROUP BY 1",
            (release_date,),
        )
        return {row[0]: row[1] for row in cur.fetchall()}


def has_wines_for(conn: psycopg.Connection, release_date: date) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM wines WHERE release_date = %s LIMIT 1", (release_date,))
        return cur.fetchone() is not None


def get_wines(conn: psycopg.Connection, release_date: date) -> list[Wine]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT release_date, sb_product_number, sb_product_id, name, producer, sb_url, price, wine_type
            FROM wines WHERE release_date = %s ORDER BY sb_product_number
            """,
            (release_date,),
        )
        return [
            Wine(
                release_date=r[0],
                sb_product_number=r[1],
                sb_product_id=r[2],
                name=r[3],
                producer=r[4],
                sb_url=r[5],
                price=r[6],
                wine_type=r[7],
            )
            for r in cur.fetchall()
        ]


def upsert_enrichments(
    conn: psycopg.Connection,
    release_date: date,
    source: Source | str,
    results: list[EnrichmentResult],
) -> None:
    if not results:
        return
    source_value = str(Source(source))
    with conn.transaction(), conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO wine_enrichments
                (release_date, sb_product_number, source, confidence, payload, fetched_at)
            VALUES (%s, %s, %s, %s, %s, now())
            ON CONFLICT (release_date, sb_product_number, source) DO UPDATE SET
                confidence = EXCLUDED.confidence,
                payload    = EXCLUDED.payload,
                fetched_at = now()
            """,
            [
                (release_date, r.sb_product_number, source_value, r.confidence, Jsonb(r.payload))
                for r in results
            ],
        )


def record_enrichment_run(
    conn: psycopg.Connection, release_date: date, source: Source | str, matched: int, total: int
) -> None:
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO enrichment_runs (release_date, source, matched_count, total_count)
            VALUES (%s, %s, %s, %s)
            """,
            (release_date, str(Source(source)), matched, total),
        )


def get_last_run(
    conn: psycopg.Connection, release_date: date, source: Source | str
) -> tuple[datetime, int] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT run_at, matched_count FROM enrichment_runs
            WHERE release_date = %s AND source = %s
            ORDER BY run_at DESC LIMIT 1
            """,
            (release_date, str(Source(source))),
        )
        row = cur.fetchone()
        return (row[0], row[1]) if row else None


def get_wines_with_enrichments(
    conn: psycopg.Connection, release_date: date
) -> list[tuple[Wine, dict[str, dict[str, Any]]]]:
    """One row per wine; second tuple item is {source_name: payload_dict}."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                w.release_date, w.sb_product_number, w.sb_product_id,
                w.name, w.producer, w.sb_url, w.price, w.wine_type,
                COALESCE(
                    jsonb_object_agg(e.source, e.payload) FILTER (WHERE e.source IS NOT NULL),
                    '{}'::jsonb
                ) AS payloads
            FROM wines w
            LEFT JOIN wine_enrichments e
                ON e.release_date = w.release_date
                AND e.sb_product_number = w.sb_product_number
            WHERE w.release_date = %s
            GROUP BY w.release_date, w.sb_product_number, w.sb_product_id,
                     w.name, w.producer, w.sb_url, w.price, w.wine_type
            ORDER BY w.sb_product_number
            """,
            (release_date,),
        )
        out: list[tuple[Wine, dict[str, dict[str, Any]]]] = []
        for r in cur.fetchall():
            wine = Wine(
                release_date=r[0],
                sb_product_number=r[1],
                sb_product_id=r[2],
                name=r[3],
                producer=r[4],
                sb_url=r[5],
                price=r[6],
                wine_type=r[7],
            )
            payloads = r[8] if isinstance(r[8], dict) else json.loads(r[8])
            out.append((wine, payloads))
        return out


def get_available_sources_for(conn: psycopg.Connection, release_date: date) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT source FROM wine_enrichments WHERE release_date = %s ORDER BY source",
            (release_date,),
        )
        return [r[0] for r in cur.fetchall()]


def wipe_release(conn: psycopg.Connection, release_date: date) -> tuple[int, int]:
    """Delete wines (cascades to wine_enrichments) and enrichment_runs for a date.

    Preserves notified_subscribers and seen_releases so re-fetching does not
    trigger re-notifications. Returns (wines_deleted, runs_deleted).
    """
    with conn.transaction(), conn.cursor() as cur:
        cur.execute("DELETE FROM wines WHERE release_date = %s", (release_date,))
        wines_deleted = cur.rowcount
        cur.execute("DELETE FROM enrichment_runs WHERE release_date = %s", (release_date,))
        runs_deleted = cur.rowcount
    return wines_deleted, runs_deleted


def get_past_release_dates_with_data(conn: psycopg.Connection, since: date) -> list[date]:
    """Distinct release dates in [since, today) that have rows in `wines`."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT release_date FROM wines
            WHERE release_date >= %s AND release_date < CURRENT_DATE
            ORDER BY release_date
            """,
            (since,),
        )
        return [r[0] for r in cur.fetchall()]


# ---- Subscribers --------------------------------------------------------


def _row_to_subscriber(row: tuple) -> Subscriber:
    return Subscriber(
        chat_id=row[0],
        max_price=row[1],
        rank_source=row[2],
        value_filter=row[3],
        wine_type_filter=row[4],
    )


_SUB_COLS = "chat_id, max_price, rank_source, value_filter, wine_type_filter"


def get_subscribers(conn: psycopg.Connection) -> list[Subscriber]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT {_SUB_COLS} FROM subscribers")
        return [_row_to_subscriber(r) for r in cur.fetchall()]


def get_subscribers_to_notify_for(conn: psycopg.Connection, release_date: date) -> list[Subscriber]:
    """Subscribers who joined before `release_date` and haven't been notified for it.

    Excludes recently-joined subscribers — they were not eligible at the original
    notification window, and `_handle_start` already sent them the most recent release.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT {_SUB_COLS}
            FROM subscribers s
            WHERE s.created_at < %s
              AND NOT EXISTS (
                  SELECT 1 FROM notified_subscribers n
                  WHERE n.release_date = %s AND n.chat_id = s.chat_id
              )
            """,
            (release_date, release_date),
        )
        return [_row_to_subscriber(r) for r in cur.fetchall()]


def get_subscriber_budget(conn: psycopg.Connection, chat_id: int) -> float | None:
    with conn.cursor() as cur:
        cur.execute("SELECT max_price FROM subscribers WHERE chat_id = %s", (chat_id,))
        row = cur.fetchone()
        return row[0] if row else None


def set_subscriber_budget(conn: psycopg.Connection, chat_id: int, max_price: float | None) -> None:
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            "UPDATE subscribers SET max_price = %s WHERE chat_id = %s",
            (max_price, chat_id),
        )


def get_subscriber_rank_source(conn: psycopg.Connection, chat_id: int) -> Source:
    with conn.cursor() as cur:
        cur.execute("SELECT rank_source FROM subscribers WHERE chat_id = %s", (chat_id,))
        row = cur.fetchone()
        return Source(row[0]) if row else Source.MUNSKANKARNA


def set_subscriber_rank_source(
    conn: psycopg.Connection, chat_id: int, source: Source | str
) -> None:
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            "UPDATE subscribers SET rank_source = %s WHERE chat_id = %s",
            (str(Source(source)), chat_id),
        )


def get_subscriber_value_filter(conn: psycopg.Connection, chat_id: int) -> list[str] | None:
    with conn.cursor() as cur:
        cur.execute("SELECT value_filter FROM subscribers WHERE chat_id = %s", (chat_id,))
        row = cur.fetchone()
        return row[0] if row else None


def set_subscriber_value_filter(
    conn: psycopg.Connection, chat_id: int, values: list[str] | None
) -> None:
    # NULL or empty list both mean "no filter".
    stored = values if values else None
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            "UPDATE subscribers SET value_filter = %s WHERE chat_id = %s",
            (stored, chat_id),
        )


def get_subscriber_wine_type_filter(conn: psycopg.Connection, chat_id: int) -> list[str] | None:
    with conn.cursor() as cur:
        cur.execute("SELECT wine_type_filter FROM subscribers WHERE chat_id = %s", (chat_id,))
        row = cur.fetchone()
        return row[0] if row else None


def set_subscriber_wine_type_filter(
    conn: psycopg.Connection, chat_id: int, values: list[str] | None
) -> None:
    stored = values if values else None
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            "UPDATE subscribers SET wine_type_filter = %s WHERE chat_id = %s",
            (stored, chat_id),
        )


def add_subscriber(conn: psycopg.Connection, chat_id: int) -> bool:
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            "INSERT INTO subscribers (chat_id) VALUES (%s) ON CONFLICT DO NOTHING",
            (chat_id,),
        )
        return cur.rowcount == 1


def remove_subscriber(conn: psycopg.Connection, chat_id: int) -> bool:
    with conn.transaction(), conn.cursor() as cur:
        cur.execute("DELETE FROM subscribers WHERE chat_id = %s", (chat_id,))
        return cur.rowcount == 1


def set_subscriber_preview_date(conn: psycopg.Connection, chat_id: int, release_date: date) -> None:
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            "UPDATE subscribers SET last_preview_date = %s WHERE chat_id = %s",
            (release_date, chat_id),
        )


def get_subscriber_preview_date(conn: psycopg.Connection, chat_id: int) -> date | None:
    with conn.cursor() as cur:
        cur.execute("SELECT last_preview_date FROM subscribers WHERE chat_id = %s", (chat_id,))
        row = cur.fetchone()
        return row[0] if row else None


# ---- Last release helper (replaces get_last_release_wines) --------------


def get_last_release_with_data(
    conn: psycopg.Connection, max_age_days: int | None = None
) -> date | None:
    """Most recent release date that has wines stored, optionally bounded by age."""
    with conn.cursor() as cur:
        if max_age_days is not None:
            cur.execute(
                """
                SELECT release_date FROM wines
                WHERE release_date >= CURRENT_DATE - %s
                  AND release_date <= CURRENT_DATE
                GROUP BY release_date ORDER BY release_date DESC LIMIT 1
                """,
                (max_age_days,),
            )
        else:
            cur.execute(
                """
                SELECT release_date FROM wines
                WHERE release_date <= CURRENT_DATE
                GROUP BY release_date ORDER BY release_date DESC LIMIT 1
                """
            )
        row = cur.fetchone()
        return row[0] if row else None
