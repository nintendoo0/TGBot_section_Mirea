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
        created_by: int,
        publish_at: str | None = None,
        publish_status: str = "published",
    ) -> int:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO trainings (
                    training_date, training_time, capacity, level,
                    status, created_by, created_at,
                    publish_at, publish_status
                )
                VALUES (?, ?, ?, ?, 'open', ?, ?, ?, ?)
                """,
                (
                    training_date,
                    training_time,
                    capacity,
                    level,
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
                SET status = 'cancelled', cancelled_at = ?
                WHERE id = ?
                """,
                (now_iso(), registration["id"]),
            )

            if registration["status"] == "active":
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

                if waiting_row:
                    conn.execute(
                        """
                        UPDATE registrations
                        SET status = 'active'
                        WHERE id = ?
                        """,
                        (waiting_row["id"],),
                    )
                    promoted = dict(waiting_row)

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