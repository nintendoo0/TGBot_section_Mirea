import sqlite3
from datetime import datetime


def now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


class Database:
    def __init__(self, path: str):
        self.path = path
        self._init_db()

    def _connect(self):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS admins (
                    user_id INTEGER PRIMARY KEY,
                    role TEXT NOT NULL DEFAULT 'admin',
                    added_by INTEGER,
                    added_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS trainings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    training_date TEXT NOT NULL,
                    training_time TEXT NOT NULL,
                    capacity INTEGER NOT NULL,
                    level TEXT NOT NULL,
                    location TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'open',
                    channel_message_id INTEGER,
                    created_by INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    closed_at TEXT
                );

                CREATE TABLE IF NOT EXISTS registrations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    training_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    username TEXT,
                    full_name TEXT,
                    fio TEXT NOT NULL,
                    dm_chat_id INTEGER NOT NULL,
                    dm_topic_id INTEGER NOT NULL,
                    queue_number INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    cancelled_at TEXT,
                    FOREIGN KEY (training_id) REFERENCES trainings(id)
                );

                CREATE TABLE IF NOT EXISTS bans (
                    user_id INTEGER PRIMARY KEY,
                    banned_until TEXT,
                    reason TEXT,
                    banned_by INTEGER,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS reentry_permissions (
                    training_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    allowed_by INTEGER,
                    allowed_at TEXT NOT NULL,
                    PRIMARY KEY (training_id, user_id)
                );
                """
            )

            # Мягкая миграция (для существующей БД) — отложенная публикация
            columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(trainings)").fetchall()
            }
            if "publish_at" not in columns:
                conn.execute("ALTER TABLE trainings ADD COLUMN publish_at TEXT")
            if "publish_status" not in columns:
                conn.execute(
                    "ALTER TABLE trainings ADD COLUMN publish_status TEXT NOT NULL DEFAULT 'published'"
                )
            if "location" not in columns:
                conn.execute("ALTER TABLE trainings ADD COLUMN location TEXT")

            reg_columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(registrations)").fetchall()
            }
            if "cancel_source" not in reg_columns:
                conn.execute("ALTER TABLE registrations ADD COLUMN cancel_source TEXT")
            if "cancelled_by" not in reg_columns:
                conn.execute("ALTER TABLE registrations ADD COLUMN cancelled_by INTEGER")

    def allow_reregister(self, training_id: int, user_id: int, allowed_by: int | None = None):
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO reentry_permissions (training_id, user_id, allowed_by, allowed_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(training_id, user_id) DO UPDATE SET
                    allowed_by = excluded.allowed_by,
                    allowed_at = excluded.allowed_at
                """,
                (training_id, user_id, allowed_by, now_iso()),
            )

    def disallow_reregister(self, training_id: int, user_id: int) -> bool:
        with self._connect() as conn:
            cur = conn.execute(
                "DELETE FROM reentry_permissions WHERE training_id = ? AND user_id = ?",
                (training_id, user_id),
            )
            return cur.rowcount > 0

    def is_reregister_allowed(self, training_id: int, user_id: int) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM reentry_permissions
                WHERE training_id = ? AND user_id = ?
                LIMIT 1
                """,
                (training_id, user_id),
            ).fetchone()
            return row is not None

    def get_cancel_block_source(self, training_id: int, user_id: int) -> str | None:
        """Return blocking cancellation source: 'user' or 'admin', or None if not blocked.

        Rules:
        - If there is a cancelled record for this training => blocked unless re-entry is allowed
        """
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT COALESCE(cancel_source, 'user') AS src
                FROM registrations
                WHERE training_id = ?
                  AND user_id = ?
                  AND status = 'cancelled'
                """,
                (training_id, user_id),
            ).fetchall()
            sources = {str(r["src"]).lower() for r in rows}

        if not sources:
            return None

        # Admin can override any cancellation (including user's self-cancel).
        if self.is_reregister_allowed(training_id, user_id):
            return None

        # Prefer reporting a concrete source for messaging.
        if "admin" in sources:
            return "admin"
        return "user"

    def get_active_ban(self, user_id: int):
        """Return active ban dict or None.

        If the ban is expired it will be removed automatically.
        """
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM bans WHERE user_id = ?",
                (user_id,),
            ).fetchone()
            if not row:
                return None

            ban = dict(row)
            banned_until = ban.get("banned_until")
            if not banned_until:
                return ban

            try:
                until_dt = datetime.fromisoformat(banned_until)
            except ValueError:
                # If stored value is invalid, treat as permanent to avoid accidental bypass.
                return ban

            if until_dt <= datetime.utcnow():
                conn.execute("DELETE FROM bans WHERE user_id = ?", (user_id,))
                return None

            return ban

    def ban_user(
        self,
        user_id: int,
        banned_by: int | None,
        banned_until: str | None,
        reason: str | None,
    ):
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO bans (user_id, banned_until, reason, banned_by, created_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    banned_until = excluded.banned_until,
                    reason = excluded.reason,
                    banned_by = excluded.banned_by,
                    created_at = excluded.created_at
                """,
                (user_id, banned_until, reason, banned_by, now_iso()),
            )

    def unban_user(self, user_id: int) -> bool:
        with self._connect() as conn:
            cur = conn.execute("DELETE FROM bans WHERE user_id = ?", (user_id,))
            return cur.rowcount > 0

    def list_bans(self):
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT user_id, banned_until, reason, banned_by, created_at
                FROM bans
                ORDER BY created_at DESC
                """
            ).fetchall()

            now = datetime.utcnow()
            active: list[dict] = []
            to_delete: list[int] = []
            for row in rows:
                ban = dict(row)
                banned_until = ban.get("banned_until")
                if not banned_until:
                    active.append(ban)
                    continue
                try:
                    until_dt = datetime.fromisoformat(banned_until)
                except ValueError:
                    # Keep invalid values (treat as active) to avoid bypass.
                    active.append(ban)
                    continue
                if until_dt <= now:
                    to_delete.append(int(ban["user_id"]))
                else:
                    active.append(ban)

            if to_delete:
                conn.executemany(
                    "DELETE FROM bans WHERE user_id = ?",
                    [(uid,) for uid in to_delete],
                )

            return active

    def ensure_owner(self, owner_id: int):
        if not owner_id or owner_id <= 0:
            return

        with self._connect() as conn:
            existing = conn.execute(
                "SELECT user_id FROM admins WHERE user_id = ?",
                (owner_id,),
            ).fetchone()

            if existing:
                conn.execute(
                    "UPDATE admins SET role = 'owner' WHERE user_id = ?",
                    (owner_id,),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO admins (user_id, role, added_by, added_at)
                    VALUES (?, 'owner', ?, ?)
                    """,
                    (owner_id, owner_id, now_iso()),
                )

    def is_admin(self, user_id: int) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM admins WHERE user_id = ?",
                (user_id,),
            ).fetchone()
            return row is not None

    def is_owner(self, user_id: int) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT role FROM admins WHERE user_id = ?",
                (user_id,),
            ).fetchone()
            return row is not None and row["role"] == "owner"

    def add_admin(self, user_id: int, added_by: int):
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT role FROM admins WHERE user_id = ?",
                (user_id,),
            ).fetchone()

            if existing:
                if existing["role"] == "owner":
                    return False, "Нельзя изменить владельца."
                conn.execute(
                    "UPDATE admins SET role = 'admin' WHERE user_id = ?",
                    (user_id,),
                )
                return True, "Админ обновлён."
            else:
                conn.execute(
                    """
                    INSERT INTO admins (user_id, role, added_by, added_at)
                    VALUES (?, 'admin', ?, ?)
                    """,
                    (user_id, added_by, now_iso()),
                )
                return True, "Админ добавлен."

    def remove_admin(self, user_id: int):
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT role FROM admins WHERE user_id = ?",
                (user_id,),
            ).fetchone()

            if not existing:
                return False, "Такого админа нет."

            if existing["role"] == "owner":
                return False, "Нельзя удалить владельца."

            conn.execute(
                "DELETE FROM admins WHERE user_id = ?",
                (user_id,),
            )
            return True, "Админ удалён."

    def list_admins(self):
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT user_id, role, added_by, added_at
                FROM admins
                ORDER BY CASE role WHEN 'owner' THEN 0 ELSE 1 END, user_id
                """
            ).fetchall()
            return [dict(row) for row in rows]

    def create_training(
        self,
        training_date: str,
        training_time: str,
        capacity: int,
        level: str,
        location: str,
        created_by: int,
        publish_at: str | None = None,
        publish_status: str = "published",
    ) -> int:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO trainings (
                    training_date, training_time, capacity, level, location,
                    status, created_by, created_at,
                    publish_at, publish_status
                )
                VALUES (?, ?, ?, ?, ?, 'open', ?, ?, ?, ?)
                """,
                (
                    training_date,
                    training_time,
                    capacity,
                    level,
                    location,
                    created_by,
                    now_iso(),
                    publish_at,
                    publish_status,
                ),
            )
            return cursor.lastrowid

    def delete_training(self, training_id: int):
        with self._connect() as conn:
            conn.execute("DELETE FROM trainings WHERE id = ?", (training_id,))

    def get_training_by_id(self, training_id: int):
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM trainings WHERE id = ?",
                (training_id,),
            ).fetchone()
            return dict(row) if row else None

    def get_open_training(self):
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM trainings
                WHERE status = 'open'
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
            return dict(row) if row else None

    def set_channel_message_id(self, training_id: int, message_id: int):
        with self._connect() as conn:
            conn.execute(
                "UPDATE trainings SET channel_message_id = ? WHERE id = ?",
                (message_id, training_id),
            )

    def close_training(self, training_id: int):
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE trainings
                SET status = 'closed', closed_at = ?
                WHERE id = ?
                """,
                (now_iso(), training_id),
            )

    def set_capacity_and_rebalance(self, training_id: int, new_capacity: int):
        """Update training capacity and rebalance active/waiting registrations.

        Returns dict with lists:
        - demoted: registrations moved from active -> waiting
        - promoted: registrations moved from waiting -> active
        """
        if new_capacity <= 0 or new_capacity > 200:
            raise ValueError("new_capacity must be in 1..200")

        with self._connect() as conn:
            training = conn.execute(
                "SELECT * FROM trainings WHERE id = ?",
                (training_id,),
            ).fetchone()
            if not training:
                return {"ok": False, "reason": "training_not_found"}

            if training["status"] != "open":
                return {"ok": False, "reason": "training_closed"}

            conn.execute(
                "UPDATE trainings SET capacity = ? WHERE id = ?",
                (new_capacity, training_id),
            )

            active_count_row = conn.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM registrations
                WHERE training_id = ?
                  AND status = 'active'
                """,
                (training_id,),
            ).fetchone()
            active_count = int(active_count_row["cnt"]) if active_count_row else 0

            demoted_ids: list[int] = []
            promoted_ids: list[int] = []

            # If capacity decreased below active count: demote tail of active list.
            if active_count > new_capacity:
                to_demote = active_count - new_capacity
                rows = conn.execute(
                    """
                    SELECT id
                    FROM registrations
                    WHERE training_id = ?
                      AND status = 'active'
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (training_id, to_demote),
                ).fetchall()
                demoted_ids = [int(r["id"]) for r in rows]
                if demoted_ids:
                    conn.executemany(
                        "UPDATE registrations SET status = 'waiting' WHERE id = ?",
                        [(rid,) for rid in demoted_ids],
                    )

            # If capacity increased: promote from waiting into active.
            # Recompute active count after possible demotions.
            active_count_row = conn.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM registrations
                WHERE training_id = ?
                  AND status = 'active'
                """,
                (training_id,),
            ).fetchone()
            active_count = int(active_count_row["cnt"]) if active_count_row else 0

            while active_count < new_capacity:
                promoted = self._promote_first_waiting_conn(conn, training_id)
                if not promoted:
                    break
                promoted_ids.append(int(promoted["id"]))
                active_count += 1

            self._normalize_numbers_conn(conn, training_id)

            demoted = []
            if demoted_ids:
                placeholders = ",".join(["?"] * len(demoted_ids))
                rows = conn.execute(
                    f"SELECT * FROM registrations WHERE id IN ({placeholders})",
                    demoted_ids,
                ).fetchall()
                demoted = [dict(r) for r in rows]

            promoted = []
            if promoted_ids:
                placeholders = ",".join(["?"] * len(promoted_ids))
                rows = conn.execute(
                    f"SELECT * FROM registrations WHERE id IN ({placeholders})",
                    promoted_ids,
                ).fetchall()
                promoted = [dict(r) for r in rows]

            # Sort by queue_number for nicer messaging.
            demoted.sort(key=lambda r: (r.get("queue_number") or 0, r.get("id") or 0))
            promoted.sort(key=lambda r: (r.get("queue_number") or 0, r.get("id") or 0))

            return {"ok": True, "demoted": demoted, "promoted": promoted}

    def get_counts(self, training_id: int):
        result = {"active": 0, "waiting": 0}

        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT status, COUNT(*) AS cnt
                FROM registrations
                WHERE training_id = ?
                  AND status IN ('active', 'waiting')
                GROUP BY status
                """,
                (training_id,),
            ).fetchall()

        for row in rows:
            result[row["status"]] = row["cnt"]

        return result

    def get_registration_for_user(self, training_id: int, user_id: int):
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM registrations
                WHERE training_id = ?
                  AND user_id = ?
                  AND status IN ('active', 'waiting')
                ORDER BY id DESC
                LIMIT 1
                """,
                (training_id, user_id),
            ).fetchone()
            return dict(row) if row else None

    def has_cancelled_registration(self, training_id: int, user_id: int) -> bool:
        # Kept for backward compatibility; use get_cancel_block_source() in new logic.
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM registrations
                WHERE training_id = ?
                  AND user_id = ?
                  AND status = 'cancelled'
                LIMIT 1
                """,
                (training_id, user_id),
            ).fetchone()
            return row is not None

    def register_user(
        self,
        training_id: int,
        user_id: int,
        username: str | None,
        full_name: str | None,
        fio: str,
        dm_chat_id: int,
        dm_topic_id: int,
    ):
        training = self.get_training_by_id(training_id)
        if not training or training["status"] != "open":
            return {"ok": False, "reason": "training_closed"}

        cancel_block_source = self.get_cancel_block_source(training_id, user_id)
        if cancel_block_source:
            return {
                "ok": False,
                "reason": "cancelled_block",
                "cancel_source": cancel_block_source,
            }

        existing = self.get_registration_for_user(training_id, user_id)
        if existing:
            return {
                "ok": False,
                "reason": "already_registered",
                "status": existing["status"],
                "number": existing["queue_number"],
                "fio": existing["fio"],
            }

        counts = self.get_counts(training_id)
        active_count = counts["active"]
        waiting_count = counts["waiting"]

        if active_count < training["capacity"]:
            status = "active"
            number = active_count + 1
        else:
            status = "waiting"
            number = waiting_count + 1

        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO registrations (
                    training_id, user_id, username, full_name, fio,
                    dm_chat_id, dm_topic_id, queue_number, status, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    training_id,
                    user_id,
                    username,
                    full_name,
                    fio,
                    dm_chat_id,
                    dm_topic_id,
                    number,
                    status,
                    now_iso(),
                ),
            )
            registration_id = cursor.lastrowid

        return {
            "ok": True,
            "registration_id": registration_id,
            "status": status,
            "number": number,
        }

    def cancel_registration(self, training_id: int, user_id: int):
        registration = self.get_registration_for_user(training_id, user_id)
        if not registration:
            return {"ok": False, "reason": "not_found"}

        promoted = None

        with self._connect() as conn:
            conn.execute(
                """
                UPDATE registrations
                SET status = 'cancelled', cancelled_at = ?, cancel_source = 'user', cancelled_by = ?
                WHERE id = ?
                """,
                (now_iso(), user_id, registration["id"]),
            )

            if registration["status"] == "active":
                promoted = self._promote_first_waiting_conn(conn, training_id)

            self._normalize_numbers_conn(conn, training_id)

            if promoted:
                promoted = conn.execute(
                    "SELECT * FROM registrations WHERE id = ?",
                    (promoted["id"],),
                ).fetchone()
                promoted = dict(promoted)

        return {
            "ok": True,
            "cancelled_status": registration["status"],
            "cancelled_fio": registration["fio"],
            "promoted": promoted,
        }

    def list_registrations(self, training_id: int, status: str):
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM registrations
                WHERE training_id = ?
                  AND status = ?
                ORDER BY queue_number, id
                """,
                (training_id, status),
            ).fetchall()
            return [dict(row) for row in rows]

    def _normalize_numbers_conn(self, conn, training_id: int):
        active_rows = conn.execute(
            """
            SELECT id
            FROM registrations
            WHERE training_id = ?
              AND status = 'active'
            ORDER BY id
            """,
            (training_id,),
        ).fetchall()

        for index, row in enumerate(active_rows, start=1):
            conn.execute(
                "UPDATE registrations SET queue_number = ? WHERE id = ?",
                (index, row["id"]),
            )

        waiting_rows = conn.execute(
            """
            SELECT id
            FROM registrations
            WHERE training_id = ?
              AND status = 'waiting'
            ORDER BY id
            """,
            (training_id,),
        ).fetchall()

        for index, row in enumerate(waiting_rows, start=1):
            conn.execute(
                "UPDATE registrations SET queue_number = ? WHERE id = ?",
                (index, row["id"]),
            )

    def _promote_first_waiting_conn(self, conn, training_id: int):
        waiting_row = conn.execute(
            """
            SELECT *
            FROM registrations
            WHERE training_id = ?
              AND status = 'waiting'
            ORDER BY id
            LIMIT 1
            """,
            (training_id,),
        ).fetchone()

        if not waiting_row:
            return None

        conn.execute(
            "UPDATE registrations SET status = 'active' WHERE id = ?",
            (waiting_row["id"],),
        )
        return dict(waiting_row)

    # --- отложенная публикация ---
    def get_scheduled_trainings_due(self, now_iso_value: str):
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM trainings
                WHERE status = 'open'
                  AND publish_status = 'scheduled'
                  AND publish_at IS NOT NULL
                  AND publish_at <= ?
                  AND channel_message_id IS NULL
                ORDER BY publish_at ASC, id ASC
                """,
                (now_iso_value,),
            ).fetchall()
            return [dict(row) for row in rows]

    def mark_training_published(self, training_id: int):
        with self._connect() as conn:
            conn.execute(
                "UPDATE trainings SET publish_status = 'published' WHERE id = ?",
                (training_id,),
            )

    # --- kick ---
    def admin_kick_by_queue(
        self,
        training_id: int,
        list_name: str,  # 'active' | 'waiting'
        queue_number: int,
        cancelled_by: int | None = None,
    ):
        if list_name not in ("active", "waiting"):
            raise ValueError("list_name must be 'active' or 'waiting'")

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM registrations
                WHERE training_id = ?
                  AND status = ?
                  AND queue_number = ?
                """,
                (training_id, list_name, queue_number),
            ).fetchone()

            if not row:
                return {"ok": False, "reason": "not_found"}

            row = dict(row)

            conn.execute(
                """
                UPDATE registrations
                SET status = 'cancelled', cancelled_at = ?, cancel_source = 'admin', cancelled_by = ?
                WHERE id = ?
                """,
                (now_iso(), cancelled_by, row["id"]),
            )

            promoted = None
            if list_name == "active":
                promoted = self._promote_first_waiting_conn(conn, training_id)

            self._normalize_numbers_conn(conn, training_id)

            if promoted:
                promoted = conn.execute(
                    "SELECT * FROM registrations WHERE id = ?",
                    (promoted["id"],),
                ).fetchone()
                promoted = dict(promoted)

            return {"ok": True, "removed": row, "promoted": promoted}