"""
db.py — Shared asyncpg connection pool.

All services import get_pool() and named_args() from here.

Usage:
    from db import get_pool, named_args

    # Positional params — write $1, $2, ... directly in SQL
    async with get_pool().acquire() as conn:
        row  = await conn.fetchrow("SELECT * FROM t WHERE id=$1", some_id)
        rows = await conn.fetch("SELECT * FROM t WHERE org=$1", org_id)
        val  = await conn.fetchval("SELECT COUNT(*) FROM t WHERE org=$1", org_id)
        await conn.execute("DELETE FROM t WHERE id=$1", some_id)

    # Named params — convert :name → $N then unpack
    sql, args = named_args(
        "INSERT INTO t (a, b) VALUES (:a, :b)",
        {"a": 1, "b": 2}
    )
    async with get_pool().acquire() as conn:
        await conn.execute(sql, *args)
"""
from __future__ import annotations

import os
import re
from typing import Any

import asyncpg

_pool: asyncpg.Pool | None = None


async def init_pool() -> None:
    global _pool
    _pool = await asyncpg.create_pool(
        os.environ["DATABASE_URL"],
        min_size=10,
        max_size=100,
        command_timeout=300,  # 5 min — needed for bulk COPY to remote DB
    )


async def close_pool() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("DB pool not initialised — call init_pool() at startup")
    return _pool


def named_args(sql: str, params: dict[str, Any]) -> tuple[str, list[Any]]:
    """
    Convert :name-style placeholders to asyncpg's $N positional params.

    Returns (converted_sql, args_list).

    Example:
        sql, args = named_args(
            "INSERT INTO t (a, b) VALUES (:a, :b) ON CONFLICT(a) DO UPDATE SET b=EXCLUDED.b",
            {"a": 1, "b": 2},
        )
        await conn.execute(sql, *args)
    """
    keys: list[str] = []

    def _repl(m: re.Match) -> str:
        keys.append(m.group(1))
        return f"${len(keys)}"

    converted = re.sub(r":([a-zA-Z_]\w*)", _repl, sql)
    return converted, [params[k] for k in keys]
