from importies import *

class VouchDB:

    def __init__(self, path: str, timeout: float = 30.0, pragmas: Optional[List[Tuple[str, str]]] = None):
        self.path = path
        self.timeout = timeout
        self._lock = threading.RLock()
        self._conn: sqlite3.Connection | None = None
        self._connect(pragmas or [("journal_mode", "WAL"), ("synchronous", "NORMAL")])
        self.ensure_schema()
        self.lock = self._lock
        self.conn = self._conn

    def _connect(self, pragmas: List[Tuple[str, str]]):
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        try:
            conn = sqlite3.connect(self.path, timeout=self.timeout, check_same_thread=False, isolation_level=None)
            for name, value in pragmas:
                conn.execute(f"PRAGMA {name}={value};")
            conn.execute("PRAGMA foreign_keys=ON;")
            self._conn = conn
        except Exception:
            self._conn = sqlite3.connect(self.path, timeout=self.timeout, check_same_thread=False, isolation_level=None)

    def _table_info(self, table: str) -> List[Tuple]:
        with self._lock:
            cur = self._conn.execute(f"PRAGMA table_info({table});")
            return cur.fetchall()

    def ensure_schema(self):
        """
        Create/upgrade tables while preserving existing data (best-effort).
        """
        with self._lock:
            c = self._conn

            c.execute(
                """
                CREATE TABLE IF NOT EXISTS vouches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    seller_id TEXT NOT NULL,
                    buyer_id TEXT NOT NULL,
                    guild_id TEXT NOT NULL,
                    rating INTEGER NOT NULL,
                    text TEXT NOT NULL,
                    img_hash TEXT NOT NULL,
                    desc_hash TEXT NOT NULL,
                    image_path TEXT,
                    image_url TEXT,
                    notify_seller INTEGER DEFAULT 1,
                    timestamp TEXT NOT NULL
                );
                """
            )

            try:
                c.execute("DROP INDEX IF EXISTS uniq_vouch_triple;")
            except Exception:
                pass
            c.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ux_vouches_seller_guild_img_desc
                ON vouches(seller_id, guild_id, img_hash, desc_hash);
                """
            )
            c.execute("CREATE INDEX IF NOT EXISTS ix_vouches_guild_created ON vouches(guild_id, timestamp DESC);")
            c.execute("CREATE INDEX IF NOT EXISTS ix_vouches_seller_guild ON vouches(seller_id, guild_id);")
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS blacklist (
                    user_id TEXT NOT NULL,
                    guild_id TEXT NOT NULL,
                    reason TEXT,
                    banned_at TEXT,
                    PRIMARY KEY (user_id, guild_id)
                );
                """
            )

            try:
                info = self._table_info("blacklist")
                cols = [r[1] for r in info]
                if "guild_id" not in cols:
                    c.execute(
                        """
                        CREATE TABLE IF NOT EXISTS blacklist_new(
                            user_id TEXT NOT NULL,
                            guild_id TEXT NOT NULL,
                            reason TEXT,
                            banned_at TEXT,
                            PRIMARY KEY (user_id, guild_id)
                        );
                        """
                    )
                    c.execute("INSERT OR IGNORE INTO blacklist_new(user_id, guild_id, reason, banned_at) SELECT user_id, '0', reason, banned_at FROM blacklist;")
                    c.execute("DROP TABLE blacklist;")
                    c.execute("ALTER TABLE blacklist_new RENAME TO blacklist;")
            except Exception:
                pass

            # whitelist: seller-targeted list and toggle/roles
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS whitelist (
                    guild_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    PRIMARY KEY (guild_id, user_id)
                );
                """
            )
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS whitelist_settings (
                    guild_id TEXT PRIMARY KEY,
                    enabled INTEGER NOT NULL DEFAULT 0 CHECK (enabled IN (0,1))
                );
                """
            )
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS whitelist_roles (
                    guild_id TEXT NOT NULL,
                    role_id TEXT NOT NULL,
                    PRIMARY KEY (guild_id, role_id)
                );
                """
            )

            # guild settings (keep as-is if present)
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS guild_settings (
                    guild_id TEXT PRIMARY KEY,
                    notify_channel_id TEXT DEFAULT NULL,
                    notify_enabled INTEGER DEFAULT 1
                );
                """
            )

            # profiles => migrate to per-guild
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS user_profiles (
                    user_id TEXT NOT NULL,
                    guild_id TEXT NOT NULL,
                    banner_path TEXT,
                    stats_public INTEGER NOT NULL DEFAULT 1,
                    PRIMARY KEY (user_id, guild_id)
                );
                """
            )
            # migration from legacy (user_id PK only)
            try:
                info = self._table_info("user_profiles")
                cols = [r[1] for r in info]
                if "guild_id" not in cols:
                    c.execute(
                        """
                        CREATE TABLE IF NOT EXISTS user_profiles_new (
                            user_id TEXT NOT NULL,
                            guild_id TEXT NOT NULL,
                            banner_path TEXT,
                            stats_public INTEGER NOT NULL DEFAULT 1,
                            PRIMARY KEY (user_id, guild_id)
                        );
                        """
                    )
                    c.execute("INSERT OR IGNORE INTO user_profiles_new(user_id, guild_id, banner_path, stats_public) SELECT user_id, '0', banner_path, 1 FROM user_profiles;")
                    c.execute("DROP TABLE user_profiles;")
                    c.execute("ALTER TABLE user_profiles_new RENAME TO user_profiles;")
            except Exception:
                pass

            # notify prefs per user per guild
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS notify_prefs (
                    user_id TEXT NOT NULL,
                    guild_id TEXT NOT NULL,
                    vouch_dm INTEGER NOT NULL DEFAULT 1,
                    reply_dm INTEGER NOT NULL DEFAULT 1,
                    PRIMARY KEY (user_id, guild_id)
                );
                """
            )

            # mutes (per user per guild per type)
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS mutes (
                    user_id TEXT NOT NULL,
                    guild_id TEXT NOT NULL,
                    type TEXT NOT NULL, -- 'vouch' | 'reply'
                    PRIMARY KEY (user_id, guild_id, type)
                );
                """
            )

            # replies linked to vouches (per guild)
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS vouch_replies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    vouch_id INTEGER NOT NULL,
                    guild_id TEXT NOT NULL,
                    seller_id TEXT NOT NULL,
                    buyer_id TEXT NOT NULL,
                    text TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (vouch_id) REFERENCES vouches(id) ON DELETE CASCADE
                );
                """
            )
            c.execute("CREATE INDEX IF NOT EXISTS ix_replies_vouch ON vouch_replies(vouch_id);")
            c.execute("CREATE INDEX IF NOT EXISTS ix_replies_guild ON vouch_replies(guild_id);")

    # ---------------------------
    # low-level helpers
    # ---------------------------
    def _execute(self, sql: str, params: Iterable[Any] = ()):
        with self._lock:
            return self._conn.execute(sql, tuple(params))

    def _executemany(self, sql: str, seq_params: Iterable[Iterable[Any]]):
        with self._lock:
            return self._conn.executemany(sql, [tuple(p) for p in seq_params])

    def _fetchone(self, sql: str, params: Iterable[Any] = ()):
        cur = self._execute(sql, params)
        return cur.fetchone()

    def _fetchall(self, sql: str, params: Iterable[Any] = ()):
        cur = self._execute(sql, params)
        return cur.fetchall()

    async def aexecute(self, sql: str, params: Iterable[Any] = ()):
        return await asyncio.to_thread(self._execute, sql, params)

    async def aexecutemany(self, sql: str, seq_params: Iterable[Iterable[Any]]):
        return await asyncio.to_thread(self._executemany, sql, seq_params)

    async def afetchone(self, sql: str, params: Iterable[Any] = ()):
        return await asyncio.to_thread(self._fetchone, sql, params)

    async def afetchall(self, sql: str, params: Iterable[Any] = ()):
        return await asyncio.to_thread(self._fetchall, sql, params)

    # ---------------------------
    # time utilities
    # ---------------------------
    @staticmethod
    def _now_iso() -> str:
        return datetime.utcnow().isoformat()

    # ---------------------------
    # vouch APIs (preserve, extend)
    # ---------------------------
    async def add_vouch(
        self,
        seller_id: str,
        buyer_id: str,
        guild_id: str,
        rating: int,
        text: str,
        img_hash: str,
        desc_hash: str,
        image_path: Optional[str] = None,
        image_url: Optional[str] = None,
        notify_seller: int = 1,
    ) -> Tuple[bool, Optional[int], Optional[str]]:
        # duplicate prevention: same seller + same guild + same img_hash + same desc_hash
        dup = await self.afetchone(
            """
            SELECT 1 FROM vouches
            WHERE seller_id=? AND guild_id=? AND img_hash=? AND desc_hash=?
            LIMIT 1
            """,
            (seller_id, guild_id, img_hash, desc_hash),
        )
        if dup:
            return False, None, "🚫 Duplicate vouch detected (same seller, image and text)."

        try:
            ts = self._now_iso()
            def _insert():
                cur = self._conn.execute(
                    """
                    INSERT INTO vouches (seller_id, buyer_id, guild_id, rating, text, img_hash, desc_hash, image_path, image_url, notify_seller, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (seller_id, buyer_id, guild_id, rating, text, img_hash, desc_hash, image_path, image_url, notify_seller, ts),
                )
                return cur.lastrowid
            vouch_id = await asyncio.to_thread(_insert)
            return True, vouch_id, None
        except sqlite3.IntegrityError:
            return False, None, "🚫 Duplicate vouch detected (same seller, image and text)."
        except Exception as e:
            return False, None, str(e)

    async def is_duplicate_vouch(self, seller_id: str, guild_id: str, img_hash: str, desc_hash: str) -> bool:
        row = await self.afetchone(
            """
            SELECT 1 FROM vouches WHERE seller_id=? AND guild_id=? AND img_hash=? AND desc_hash=? LIMIT 1
            """,
            (seller_id, guild_id, img_hash, desc_hash),
        )
        return row is not None

    async def get_vouch(self, vouch_id: int, guild_id: str) -> Optional[Dict[str, Any]]:
        row = await self.afetchone(
            """
            SELECT id, seller_id, buyer_id, guild_id, rating, text, img_hash, desc_hash, image_path, image_url, notify_seller, timestamp
            FROM vouches WHERE id=? AND guild_id=?
            """,
            (vouch_id, guild_id),
        )
        if not row:
            return None
        keys = ["id", "seller_id", "buyer_id", "guild_id", "rating", "text", "img_hash", "desc_hash", "image_path", "image_url", "notify_seller", "timestamp"]
        return dict(zip(keys, row))

    async def get_vouches_for_seller_in_guild(self, seller_id: int, guild_id: int) -> List[Dict[str, Any]]:
        rows = await self.afetchall(
            """
            SELECT id, seller_id, buyer_id, guild_id, rating, text, img_hash, desc_hash, image_path, image_url, timestamp
            FROM vouches WHERE seller_id=? AND guild_id=? ORDER BY timestamp DESC
            """,
            (str(seller_id), str(guild_id)),
        )
        keys = ["id", "seller_id", "buyer_id", "guild_id", "rating", "text", "img_hash", "desc_hash", "image_path", "image_url", "timestamp"]
        return [dict(zip(keys, r)) for r in rows]

    async def get_aggregates_in_guild(self, seller_id: int, guild_id: int) -> Tuple[int, float]:
        row = await self.afetchone(
            """
            SELECT COUNT(*), COALESCE(AVG(rating),0) FROM vouches WHERE seller_id=? AND guild_id=?
            """,
            (str(seller_id), str(guild_id)),
        )
        return int(row[0]), float(row[1])

    async def delete_vouch(self, vouch_id: int, guild_id: str) -> bool:
        cur = await self.aexecute("DELETE FROM vouches WHERE id=? AND guild_id=?", (vouch_id, guild_id))
        return (cur.rowcount or 0) > 0

    async def leaderboard(self, guild_id: str, limit: int = 10, min_vouches: int = 1) -> List[Dict[str, Any]]:
        rows = await self.afetchall(
            """
            SELECT seller_id, COUNT(*) AS total_vouches, AVG(rating) AS avg_rating
            FROM vouches WHERE guild_id=?
            GROUP BY seller_id
            HAVING COUNT(*) >= ?
            ORDER BY avg_rating DESC, total_vouches DESC
            LIMIT ?
            """,
            (guild_id, min_vouches, limit),
        )
        return [{"seller_id": r[0], "total_vouches": int(r[1]), "avg_rating": float(r[2]) if r[2] is not None else 0.0} for r in rows]

    # ---------------------------
    # replies
    # ---------------------------
    async def add_reply(self, vouch_id: int, guild_id: str, seller_id: str, buyer_id: str, text: str) -> Tuple[bool, Optional[int], Optional[str]]:
        try:
            ts = self._now_iso()
            def _insert():
                cur = self._conn.execute(
                    """
                    INSERT INTO vouch_replies (vouch_id, guild_id, seller_id, buyer_id, text, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (vouch_id, guild_id, seller_id, buyer_id, text, ts),
                )
                return cur.lastrowid
            rid = await asyncio.to_thread(_insert)
            return True, rid, None
        except Exception as e:
            return False, None, str(e)

    async def list_replies(self, vouch_id: int, guild_id: str) -> List[Dict[str, Any]]:
        rows = await self.afetchall(
            """
            SELECT id, vouch_id, guild_id, seller_id, buyer_id, text, created_at
            FROM vouch_replies WHERE vouch_id=? AND guild_id=? ORDER BY id ASC
            """,
            (vouch_id, guild_id),
        )
        keys = ["id", "vouch_id", "guild_id", "seller_id", "buyer_id", "text", "created_at"]
        return [dict(zip(keys, r)) for r in rows]

    # ---------------------------
    # blacklist (admin only) - per guild
    # Preserve compatibility with older method names
    # ---------------------------
    async def ban_user(self, user_id: int, guild_id: int, reason: Optional[str] = None):
        await self.aexecute(
            """
            INSERT INTO blacklist (user_id, guild_id, reason, banned_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id, guild_id) DO UPDATE SET reason=excluded.reason, banned_at=excluded.banned_at
            """,
            (str(user_id), str(guild_id), reason, self._now_iso()),
        )

    async def unban_user(self, user_id: int, guild_id: int):
        await self.aexecute("DELETE FROM blacklist WHERE user_id=? AND guild_id=?", (str(user_id), str(guild_id)))

    async def is_banned(self, user_id: int, guild_id: int) -> bool:
        row = await self.afetchone("SELECT 1 FROM blacklist WHERE user_id=? AND guild_id=? LIMIT 1", (str(user_id), str(guild_id)))
        return row is not None

    # aliases to match other possible call sites
    async def blacklist_add(self, user_id: str, guild_id: str, admin_id: Optional[str] = None, reason: Optional[str] = None):
        await self.ban_user(int(user_id), int(guild_id), reason)

    async def blacklist_remove(self, user_id: str, guild_id: str):
        await self.unban_user(int(user_id), int(guild_id))

    async def is_blacklisted(self, user_id: str, guild_id: str) -> bool:
        return await self.is_banned(int(user_id), int(guild_id))

    async def get_banned_users(self, guild_id: int) -> List[Tuple[str, Optional[str], Optional[str]]]:
        rows = await self.afetchall(
            "SELECT user_id, reason, banned_at FROM blacklist WHERE guild_id=? ORDER BY banned_at DESC",
            (str(guild_id),),
        )
        return [(r[0], r[1], r[2]) for r in rows]

    # ---------------------------
    # whitelist (seller-targeted)
    # ---------------------------
    async def set_whitelist_enabled(self, guild_id: int, on: bool):
        await self.aexecute(
            """
            INSERT INTO whitelist_settings (guild_id, enabled)
            VALUES (?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET enabled=excluded.enabled
            """,
            (str(guild_id), 1 if on else 0),
        )

    async def is_whitelist_enabled(self, guild_id: int) -> bool:
        row = await self.afetchone("SELECT enabled FROM whitelist_settings WHERE guild_id=?", (str(guild_id),))
        return bool(row[0]) if row else False

    async def whitelist_add(self, guild_id: int, user_id: int):
        await self.aexecute("INSERT OR IGNORE INTO whitelist (guild_id, user_id) VALUES (?, ?)", (str(guild_id), str(user_id)))

    async def whitelist_remove(self, guild_id: int, user_id: int):
        await self.aexecute("DELETE FROM whitelist WHERE guild_id=? AND user_id=?", (str(guild_id), str(user_id)))

    async def is_whitelisted(self, guild_id: int, user_id: int) -> bool:
        row = await self.afetchone("SELECT 1 FROM whitelist WHERE guild_id=? AND user_id=? LIMIT 1", (str(guild_id), str(user_id)))
        return row is not None

    async def whitelist_add_role(self, guild_id: int, role_id: int):
        await self.aexecute("INSERT OR IGNORE INTO whitelist_roles (guild_id, role_id) VALUES (?, ?)", (str(guild_id), str(role_id)))

    async def whitelist_remove_role(self, guild_id: int, role_id: int):
        await self.aexecute("DELETE FROM whitelist_roles WHERE guild_id=? AND role_id=?", (str(guild_id), str(role_id)))

    async def get_whitelist_role_ids(self, guild_id: int) -> List[str]:
        rows = await self.afetchall("SELECT role_id FROM whitelist_roles WHERE guild_id=?", (str(guild_id),))
        return [r[0] for r in rows]

    # ---------------------------
    # guild settings / notify channel
    # ---------------------------
    async def set_guild_notify_channel(self, guild_id: int, channel_id: Optional[int]):
        await self.aexecute(
            """
            INSERT INTO guild_settings (guild_id, notify_channel_id, notify_enabled)
            VALUES (?, ?, COALESCE((SELECT notify_enabled FROM guild_settings WHERE guild_id=?), 1))
            ON CONFLICT(guild_id) DO UPDATE SET notify_channel_id=excluded.notify_channel_id
            """,
            (str(guild_id), str(channel_id) if channel_id is not None else None, str(guild_id)),
        )

    async def get_guild_settings(self, guild_id: int) -> Dict[str, Any]:
        row = await self.afetchone("SELECT notify_channel_id, notify_enabled FROM guild_settings WHERE guild_id=?", (str(guild_id),))
        if not row:
            return {"notify_channel_id": None, "notify_enabled": 1}
        return {"notify_channel_id": row[0], "notify_enabled": int(row[1])}

    # ---------------------------
    # profiles / banners (per-guild) + privacy flag
    # ---------------------------
    async def set_user_banner(self, user_id: int, guild_id: int, banner_path: Optional[str]):
        await self.aexecute(
            """
            INSERT INTO user_profiles (user_id, guild_id, banner_path, stats_public)
            VALUES (?, ?, ?, COALESCE((SELECT stats_public FROM user_profiles WHERE user_id=? AND guild_id=?), 1))
            ON CONFLICT(user_id, guild_id) DO UPDATE SET banner_path=excluded.banner_path
            """,
            (str(user_id), str(guild_id), banner_path, str(user_id), str(guild_id)),
        )

    async def get_user_banner(self, user_id: int, guild_id: int) -> Optional[str]:
        row = await self.afetchone("SELECT banner_path FROM user_profiles WHERE user_id=? AND guild_id=?", (str(user_id), str(guild_id)))
        return row[0] if row and row[0] else None

    # aliases to match alternate names used elsewhere
    async def set_banner_path(self, user_id: str, guild_id: str, banner_path: Optional[str]):
        await self.set_user_banner(int(user_id), int(guild_id), banner_path)

    async def get_banner_path(self, user_id: str, guild_id: str) -> Optional[str]:
        return await self.get_user_banner(int(user_id), int(guild_id))

    # privacy
    async def get_profile_privacy(self, user_id: int, guild_id: int) -> bool:
        row = await self.afetchone("SELECT stats_public FROM user_profiles WHERE user_id=? AND guild_id=?", (str(user_id), str(guild_id)))
        return bool(row[0]) if row else True

    async def set_profile_privacy(self, user_id: int, guild_id: int, public: bool):
        await self.aexecute(
            """
            INSERT INTO user_profiles (user_id, guild_id, banner_path, stats_public)
            VALUES (?, ?, COALESCE((SELECT banner_path FROM user_profiles WHERE user_id=? AND guild_id=?), NULL), ?)
            ON CONFLICT(user_id, guild_id) DO UPDATE SET stats_public=excluded.stats_public
            """,
            (str(user_id), str(guild_id), str(user_id), str(guild_id), 1 if public else 0),
        )

    # ---------------------------
    # notify preferences + mutes
    # ---------------------------
    async def get_notify_prefs(self, user_id: int, guild_id: int) -> Dict[str, int]:
        row = await self.afetchone("SELECT vouch_dm, reply_dm FROM notify_prefs WHERE user_id=? AND guild_id=?", (str(user_id), str(guild_id)))
        if not row:
            return {"vouch_dm": 1, "reply_dm": 1}
        return {"vouch_dm": int(row[0]), "reply_dm": int(row[1])}

    async def set_notify_prefs(self, user_id: int, guild_id: int, vouch_dm: Optional[bool] = None, reply_dm: Optional[bool] = None):
        current = await self.get_notify_prefs(user_id, guild_id)
        vouch_val = 1 if (vouch_dm if vouch_dm is not None else current["vouch_dm"]) else 0
        reply_val = 1 if (reply_dm if reply_dm is not None else current["reply_dm"]) else 0
        await self.aexecute(
            """
            INSERT INTO notify_prefs (user_id, guild_id, vouch_dm, reply_dm)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id, guild_id) DO UPDATE SET vouch_dm=excluded.vouch_dm, reply_dm=excluded.reply_dm
            """,
            (str(user_id), str(guild_id), vouch_val, reply_val),
        )

    # mute buttons (per type)
    async def mute_set(self, user_id: str, guild_id: str, mute_type: str, muted: bool):
        if muted:
            await self.aexecute("INSERT OR IGNORE INTO mutes (user_id, guild_id, type) VALUES (?, ?, ?)", (user_id, guild_id, mute_type))
        else:
            await self.aexecute("DELETE FROM mutes WHERE user_id=? AND guild_id=? AND type=?", (user_id, guild_id, mute_type))

    async def is_muted(self, user_id: str, guild_id: str, mute_type: str) -> bool:
        row = await self.afetchone("SELECT 1 FROM mutes WHERE user_id=? AND guild_id=? AND type=? LIMIT 1", (user_id, guild_id, mute_type))
        return row is not None

    # convenience: DM enabled check using notify_prefs
    async def is_dm_enabled(self, user_id: str, guild_id: str, kind: str = "vouch") -> bool:
        prefs = await self.get_notify_prefs(int(user_id), int(guild_id))
        return bool(prefs["vouch_dm"] if kind == "vouch" else prefs["reply_dm"])

    # ---------------------------
    # history / counts
    # ---------------------------
    async def history_for_user(self, user_id: str, guild_id: str, limit: int = 20, offset: int = 0) -> List[Dict[str, Any]]:
        rows = await self.afetchall(
            """
            SELECT id, seller_id, buyer_id, guild_id, rating, text, img_hash, desc_hash, image_path, image_url, timestamp
            FROM vouches
            WHERE guild_id=? AND (seller_id=? OR buyer_id=?)
            ORDER BY timestamp DESC
            LIMIT ? OFFSET ?
            """,
            (guild_id, user_id, user_id, limit, offset),
        )
        keys = ["id", "seller_id", "buyer_id", "guild_id", "rating", "text", "img_hash", "desc_hash", "image_path", "image_url", "timestamp"]
        return [dict(zip(keys, r)) for r in rows]

    async def count_vouches(self, guild_id: str) -> int:
        row = await self.afetchone("SELECT COUNT(*) FROM vouches WHERE guild_id=?", (guild_id,))
        return int(row[0]) if row else 0

    # ---------------------------
    # merge operations
    # ---------------------------
    async def guild_merge(self, from_guild_id: str, to_guild_id: str) -> Dict[str, int]:
        """
        Transfers all vouches, profiles, replies, blacklist, whitelist, roles, prefs, mutes, guild_settings
        from one guild to another. Duplicate vouches are skipped by unique index.
        """
        def _merge():
            moved = {"vouches": 0, "replies": 0, "profiles": 0, "blacklist": 0, "whitelist": 0, "roles": 0, "prefs": 0, "mutes": 0, "gsettings": 0}
            with self._lock:
                c = self._conn
                c.execute("BEGIN;")
                try:
                    # vouches
                    c.execute(
                        """
                        INSERT OR IGNORE INTO vouches (seller_id, buyer_id, guild_id, rating, text, img_hash, desc_hash, image_path, image_url, notify_seller, timestamp)
                        SELECT seller_id, buyer_id, ?, rating, text, img_hash, desc_hash, image_path, image_url, notify_seller, timestamp
                        FROM vouches WHERE guild_id=?
                        """,
                        (to_guild_id, from_guild_id),
                    )
                    moved["vouches"] += c.total_changes

                    # map source->target vouches for replies by keys
                    c.execute("DROP TABLE IF EXISTS _tmp_src_v;")
                    c.execute(
                        """
                        CREATE TEMP TABLE _tmp_src_v AS
                        SELECT id AS src_id, seller_id, buyer_id, img_hash, desc_hash, timestamp
                        FROM vouches WHERE guild_id=?
                        """,
                        (from_guild_id,),
                    )
                    c.execute("DROP TABLE IF EXISTS _tmp_tgt_v;")
                    c.execute(
                        """
                        CREATE TEMP TABLE _tmp_tgt_v AS
                        SELECT id AS tgt_id, seller_id, buyer_id, img_hash, desc_hash, timestamp
                        FROM vouches WHERE guild_id=?
                        """,
                        (to_guild_id,),
                    )

                    # replies
                    c.execute(
                        """
                        INSERT OR IGNORE INTO vouch_replies (vouch_id, guild_id, seller_id, buyer_id, text, created_at)
                        SELECT t.tgt_id, ?, r.seller_id, r.buyer_id, r.text, r.created_at
                        FROM vouch_replies r
                        JOIN _tmp_src_v s ON s.src_id=r.vouch_id
                        JOIN _tmp_tgt_v t ON t.seller_id=s.seller_id AND t.buyer_id=s.buyer_id AND t.img_hash=s.img_hash AND t.desc_hash=s.desc_hash AND t.timestamp=s.timestamp
                        WHERE r.guild_id=?
                        """,
                        (to_guild_id, from_guild_id),
                    )
                    moved["replies"] += c.total_changes

                    # profiles
                    c.execute(
                        """
                        INSERT OR REPLACE INTO user_profiles (user_id, guild_id, banner_path, stats_public)
                        SELECT user_id, ?, banner_path, stats_public FROM user_profiles WHERE guild_id=?
                        """,
                        (to_guild_id, from_guild_id),
                    )
                    moved["profiles"] += c.total_changes

                    # blacklist
                    c.execute(
                        """
                        INSERT OR REPLACE INTO blacklist (user_id, guild_id, reason, banned_at)
                        SELECT user_id, ?, reason, banned_at FROM blacklist WHERE guild_id=?
                        """,
                        (to_guild_id, from_guild_id),
                    )
                    moved["blacklist"] += c.total_changes

                    # whitelist
                    c.execute(
                        "INSERT OR IGNORE INTO whitelist (guild_id, user_id) SELECT ?, user_id FROM whitelist WHERE guild_id=?",
                        (to_guild_id, from_guild_id),
                    )
                    moved["whitelist"] += c.total_changes

                    # whitelist roles
                    c.execute(
                        "INSERT OR IGNORE INTO whitelist_roles (guild_id, role_id) SELECT ?, role_id FROM whitelist_roles WHERE guild_id=?",
                        (to_guild_id, from_guild_id),
                    )
                    moved["roles"] += c.total_changes

                    # notify prefs
                    c.execute(
                        """
                        INSERT OR REPLACE INTO notify_prefs (user_id, guild_id, vouch_dm, reply_dm)
                        SELECT user_id, ?, vouch_dm, reply_dm FROM notify_prefs WHERE guild_id=?
                        """,
                        (to_guild_id, from_guild_id),
                    )
                    moved["prefs"] += c.total_changes

                    # mutes
                    c.execute(
                        "INSERT OR IGNORE INTO mutes (user_id, guild_id, type) SELECT user_id, ?, type FROM mutes WHERE guild_id=?",
                        (to_guild_id, from_guild_id),
                    )
                    moved["mutes"] += c.total_changes

                    # guild settings: keep target if exists, otherwise copy
                    c.execute(
                        """
                        INSERT OR IGNORE INTO guild_settings (guild_id, notify_channel_id, notify_enabled)
                        SELECT ?, notify_channel_id, notify_enabled FROM guild_settings WHERE guild_id=?
                        """,
                        (to_guild_id, from_guild_id),
                    )
                    moved["gsettings"] += c.total_changes

                    # cleanup source
                    c.execute("DELETE FROM vouch_replies WHERE guild_id=?", (from_guild_id,))
                    c.execute("DELETE FROM vouches WHERE guild_id=?", (from_guild_id,))
                    c.execute("DELETE FROM user_profiles WHERE guild_id=?", (from_guild_id,))
                    c.execute("DELETE FROM blacklist WHERE guild_id=?", (from_guild_id,))
                    c.execute("DELETE FROM whitelist WHERE guild_id=?", (from_guild_id,))
                    c.execute("DELETE FROM whitelist_roles WHERE guild_id=?", (from_guild_id,))
                    c.execute("DELETE FROM notify_prefs WHERE guild_id=?", (from_guild_id,))
                    c.execute("DELETE FROM mutes WHERE guild_id=?", (from_guild_id,))
                    c.execute("DELETE FROM guild_settings WHERE guild_id=?", (from_guild_id,))

                    c.execute("COMMIT;")
                    return moved
                except Exception:
                    c.execute("ROLLBACK;")
                    raise
        return await asyncio.to_thread(_merge)

    async def seller_merge(self, seller_id: str, from_guild_id: str, to_guild_id: str) -> Dict[str, int]:
        """
        Moves only that seller's vouches (and their replies) between guilds; de-dupe preserved.
        """
        def _merge():
            moved = {"vouches": 0, "replies": 0}
            with self._lock:
                c = self._conn
                c.execute("BEGIN;")
                try:
                    c.execute(
                        """
                        INSERT OR IGNORE INTO vouches (seller_id, buyer_id, guild_id, rating, text, img_hash, desc_hash, image_path, image_url, notify_seller, timestamp)
                        SELECT seller_id, buyer_id, ?, rating, text, img_hash, desc_hash, image_path, image_url, notify_seller, timestamp
                        FROM vouches WHERE guild_id=? AND seller_id=?
                        """,
                        (to_guild_id, from_guild_id, seller_id),
                    )
                    moved["vouches"] += c.total_changes

                    c.execute("DROP TABLE IF EXISTS _tmp_src;")
                    c.execute(
                        """
                        CREATE TEMP TABLE _tmp_src AS
                        SELECT id AS src_id, seller_id, buyer_id, img_hash, desc_hash, timestamp
                        FROM vouches WHERE guild_id=? AND seller_id=?
                        """,
                        (from_guild_id, seller_id),
                    )
                    c.execute("DROP TABLE IF EXISTS _tmp_tgt;")
                    c.execute(
                        """
                        CREATE TEMP TABLE _tmp_tgt AS
                        SELECT id AS tgt_id, seller_id, buyer_id, img_hash, desc_hash, timestamp
                        FROM vouches WHERE guild_id=? AND seller_id=?
                        """,
                        (to_guild_id, seller_id),
                    )
                    c.execute(
                        """
                        INSERT OR IGNORE INTO vouch_replies (vouch_id, guild_id, seller_id, buyer_id, text, created_at)
                        SELECT t.tgt_id, ?, r.seller_id, r.buyer_id, r.text, r.created_at
                        FROM vouch_replies r
                        JOIN _tmp_src s ON s.src_id=r.vouch_id
                        JOIN _tmp_tgt t ON t.seller_id=s.seller_id AND t.buyer_id=s.buyer_id AND t.img_hash=s.img_hash AND t.desc_hash=s.desc_hash AND t.timestamp=s.timestamp
                        WHERE r.guild_id=?
                        """,
                        (to_guild_id, from_guild_id),
                    )
                    moved["replies"] += c.total_changes

                    c.execute(
                        """
                        DELETE FROM vouch_replies
                        WHERE guild_id=? AND vouch_id IN (SELECT id FROM vouches WHERE guild_id=? AND seller_id=?)
                        """,
                        (from_guild_id, from_guild_id, seller_id),
                    )
                    c.execute("DELETE FROM vouches WHERE guild_id=? AND seller_id=?", (from_guild_id, seller_id))

                    c.execute("COMMIT;")
                    return moved
                except Exception:
                    c.execute("ROLLBACK;")
                    raise
        return await asyncio.to_thread(_merge)

    # ---------------------------
    # export / import (optional helpers)
    # ---------------------------
    async def export_vouches(self, guild_id: str) -> List[Dict[str, Any]]:
        rows = await self.afetchall(
            """
            SELECT id, seller_id, buyer_id, guild_id, rating, text, img_hash, desc_hash, image_path, image_url, notify_seller, timestamp
            FROM vouches WHERE guild_id=? ORDER BY id ASC
            """,
            (guild_id,),
        )
        keys = ["id", "seller_id", "buyer_id", "guild_id", "rating", "text", "img_hash", "desc_hash", "image_path", "image_url", "notify_seller", "timestamp"]
        return [dict(zip(keys, r)) for r in rows]

    async def import_vouches(self, guild_id: str, records: List[Dict[str, Any]]) -> Dict[str, int]:
        ts = self._now_iso()
        rows = []
        for rec in records:
            rows.append(
                (
                    rec["seller_id"],
                    rec["buyer_id"],
                    guild_id,
                    int(rec.get("rating", 0)),
                    rec.get("text", ""),
                    rec["img_hash"],
                    rec["desc_hash"],
                    rec.get("image_path"),
                    rec.get("image_url"),
                    int(rec.get("notify_seller", 1)),
                    rec.get("timestamp", ts),
                )
            )
        def _bulk():
            with self._lock:
                self._conn.executemany(
                    """
                    INSERT OR IGNORE INTO vouches (seller_id, buyer_id, guild_id, rating, text, img_hash, desc_hash, image_path, image_url, notify_seller, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )
                return self._conn.total_changes
        inserted = await asyncio.to_thread(_bulk)
        return {"inserted_or_ignored": inserted}
    
    def close(self):
        try:
            with self._lock:
                if self._conn:
                    self._conn.close()
                    self._conn = None
        except Exception:
            pass
