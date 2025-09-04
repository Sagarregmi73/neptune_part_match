# lib/app/adapter/output/persistence/postgres/postgres_repository.py
from lib.app.application.services.repository_interface import RepositoryInterface
from lib.app.domain.entities.part_number import PartNumber
from lib.app.domain.entities.match import Match
from lib.core.db.psql.postgres_client import get_postgres_connection
from lib.core.logging import logger

class PostgresRepository(RepositoryInterface):
    def __init__(self):
        self.conn = get_postgres_connection()
        self.cur = self.conn.cursor()

    # ----- PART CRUD -----
    def create_part(self, part: PartNumber) -> PartNumber:
        self.cur.execute(
            "INSERT INTO parts (id, specs, notes) VALUES (%s, %s, %s) ON CONFLICT (id) DO NOTHING",
            (part.part_number, part.specs, part.notes)
        )
        self.conn.commit()
        logger.info(f"Created PartNumber in Postgres: {part.part_number}")
        return part

    def get_part(self, part_number: str):
        self.cur.execute("SELECT id, specs, notes FROM parts WHERE id=%s", (part_number,))
        row = self.cur.fetchone()
        if row:
            return PartNumber(*row)
        return None

    def update_part(self, part: PartNumber):
        self.cur.execute(
            "UPDATE parts SET specs=%s, notes=%s WHERE id=%s",
            (part.specs, part.notes, part.part_number)
        )
        self.conn.commit()
        return part

    def delete_part(self, part_number: str):
        self.cur.execute("DELETE FROM parts WHERE id=%s", (part_number,))
        self.conn.commit()
        return True

    def list_parts(self):
        self.cur.execute("SELECT id, specs, notes FROM parts")
        rows = self.cur.fetchall()
        return [PartNumber(*row) for row in rows]

    # ----- MATCH CRUD -----
    def create_match(self, match: Match) -> Match:
        self.cur.execute(
            "INSERT INTO matches (source, target, match_type) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            (match.source, match.target, match.match_type)
        )
        self.conn.commit()
        return match

    def get_match(self, source: str, target: str):
        self.cur.execute(
            "SELECT source, target, match_type FROM matches WHERE source=%s AND target=%s",
            (source, target)
        )
        row = self.cur.fetchone()
        if row:
            return Match(*row)
        return None

    def update_match(self, match: Match):
        self.cur.execute(
            "UPDATE matches SET match_type=%s WHERE source=%s AND target=%s",
            (match.match_type, match.source, match.target)
        )
        self.conn.commit()
        return match

    def delete_match(self, source: str, target: str):
        self.cur.execute("DELETE FROM matches WHERE source=%s AND target=%s", (source, target))
        self.conn.commit()
        return True

    def list_matches(self):
        self.cur.execute("SELECT source, target, match_type FROM matches")
        rows = self.cur.fetchall()
        return [Match(*row) for row in rows]
