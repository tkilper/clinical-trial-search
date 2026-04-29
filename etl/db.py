import os
from contextlib import contextmanager

import psycopg2


def _dsn() -> str:
    return os.environ["CLINICAL_TRIALS_DB_DSN"]


@contextmanager
def get_conn():
    conn = psycopg2.connect(_dsn())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
