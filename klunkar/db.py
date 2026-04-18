import datetime
from contextlib import contextmanager

import psycopg

from klunkar import config


@contextmanager
def get_conn():
    with psycopg.connect(config.DATABASE_URL) as conn:
        yield conn


def migrate(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS subscribers (
                chat_id    BIGINT PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                max_price  FLOAT
            )
        """)
        cur.execute("""
            ALTER TABLE subscribers ADD COLUMN IF NOT EXISTS max_price FLOAT
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS seen_releases (
                release_date DATE PRIMARY KEY,
                notified_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                wine_count   INT,
                message      TEXT
            )
        """)
        cur.execute("""
            ALTER TABLE seen_releases ADD COLUMN IF NOT EXISTS message TEXT
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS release_wines (
                release_date  DATE    NOT NULL,
                rank          INT     NOT NULL,
                name          TEXT    NOT NULL,
                score         FLOAT   NOT NULL,
                vivino_url    TEXT    NOT NULL,
                sb_url        TEXT    NOT NULL,
                price         FLOAT,
                wine_type     TEXT,
                PRIMARY KEY (release_date, rank)
            )
        """)
        cur.execute("""
            ALTER TABLE release_wines ADD COLUMN IF NOT EXISTS price FLOAT
        """)
        cur.execute("""
            ALTER TABLE release_wines ADD COLUMN IF NOT EXISTS wine_type TEXT
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
    conn.commit()


def get_apim_key(conn: psycopg.Connection) -> str | None:
    with conn.cursor() as cur:
        cur.execute("SELECT key FROM apim_key WHERE id = 1")
        row = cur.fetchone()
        return row[0] if row else None


def set_apim_key(conn: psycopg.Connection, key: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO apim_key (id, key, updated_at)
            VALUES (1, %s, now())
            ON CONFLICT (id) DO UPDATE SET key = EXCLUDED.key, updated_at = now()
        """,
            (key,),
        )
    conn.commit()


def is_release_seen(conn: psycopg.Connection, release_date: datetime.date) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM seen_releases WHERE release_date = %s", (release_date,))
        return cur.fetchone() is not None


def mark_release_seen(
    conn: psycopg.Connection, release_date: datetime.date, wine_count: int
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO seen_releases (release_date, wine_count)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
        """,
            (release_date, wine_count),
        )
    conn.commit()


def save_release_wines(conn: psycopg.Connection, release_date: datetime.date, wines: list) -> None:
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO release_wines (release_date, rank, name, score, vivino_url, sb_url, price, wine_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """,
            [
                (
                    release_date,
                    w.rank,
                    w.name,
                    w.score,
                    w.vivino_url,
                    w.sb_url,
                    w.price,
                    w.wine_type,
                )
                for w in wines
            ],
        )
    conn.commit()


def get_release_wines(
    conn: psycopg.Connection, release_date: datetime.date
) -> list | None:
    """Return ranked wine rows for a specific date, or None if not cached."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT rank, name, score, vivino_url, sb_url, price, wine_type
            FROM release_wines
            WHERE release_date = %s
            ORDER BY rank
            """,
            (release_date,),
        )
        rows = cur.fetchall()
        return rows if rows else None


def get_last_release_wines(
    conn: psycopg.Connection, max_age_days: int | None = 7
) -> tuple[datetime.date, list] | None:
    with conn.cursor() as cur:
        if max_age_days is not None:
            cur.execute(
                """
                SELECT release_date FROM seen_releases
                WHERE release_date >= CURRENT_DATE - %s
                ORDER BY release_date DESC
                LIMIT 1
                """,
                (max_age_days,),
            )
        else:
            cur.execute(
                "SELECT release_date FROM seen_releases ORDER BY release_date DESC LIMIT 1"
            )
        row = cur.fetchone()
        if not row:
            return None
        release_date = row[0]
        cur.execute(
            """
            SELECT rank, name, score, vivino_url, sb_url, price, wine_type
            FROM release_wines
            WHERE release_date = %s
            ORDER BY rank
        """,
            (release_date,),
        )
        return release_date, cur.fetchall()


def get_subscribers(conn: psycopg.Connection) -> list[tuple[int, float | None]]:
    with conn.cursor() as cur:
        cur.execute("SELECT chat_id, max_price FROM subscribers")
        return [(row[0], row[1]) for row in cur.fetchall()]


def get_subscriber_budget(conn: psycopg.Connection, chat_id: int) -> float | None:
    with conn.cursor() as cur:
        cur.execute("SELECT max_price FROM subscribers WHERE chat_id = %s", (chat_id,))
        row = cur.fetchone()
        return row[0] if row else None


def set_subscriber_budget(
    conn: psycopg.Connection, chat_id: int, max_price: float | None
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE subscribers SET max_price = %s WHERE chat_id = %s",
            (max_price, chat_id),
        )
    conn.commit()


def add_subscriber(conn: psycopg.Connection, chat_id: int) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO subscribers (chat_id) VALUES (%s)
            ON CONFLICT DO NOTHING
        """,
            (chat_id,),
        )
        inserted = cur.rowcount == 1
    conn.commit()
    return inserted


def remove_subscriber(conn: psycopg.Connection, chat_id: int) -> bool:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM subscribers WHERE chat_id = %s", (chat_id,))
        deleted = cur.rowcount == 1
    conn.commit()
    return deleted


def save_release_dates(conn: psycopg.Connection, dates: list[datetime.date]) -> None:
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO upcoming_release_dates (release_date)
            VALUES (%s)
            ON CONFLICT DO NOTHING
            """,
            [(d,) for d in dates],
        )
    conn.commit()


def get_upcoming_release_dates(conn: psycopg.Connection, from_date: datetime.date) -> list[datetime.date]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT release_date FROM upcoming_release_dates WHERE release_date >= %s ORDER BY release_date",
            (from_date,),
        )
        return [row[0] for row in cur.fetchall()]
