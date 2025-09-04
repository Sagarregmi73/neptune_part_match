# lib/app/adapter/output/persistence/neptune/neptune_repository.py
from lib.app.application.services.repository_interface import RepositoryInterface
from gremlin_python.process.graph_traversal import __
from lib.app.domain.entities.part_number import PartNumber
from lib.app.domain.entities.match import Match
from lib.core.aws.neptune_client import get_neptune_connection
from lib.core.logging import logger

class NeptuneRepository(RepositoryInterface):
    def __init__(self):
        self.g, self.connection = get_neptune_connection()

    # ----- PART CRUD -----
    def create_part(self, part: PartNumber) -> PartNumber:
        try:
            self.g.addV("PartNumber")\
                .property("id", part.part_number)\
                .property("specs", part.specs)\
                .property("notes", part.notes).next()
            logger.info(f"Created PartNumber in Neptune: {part.part_number}")
            return part
        except Exception as e:
            logger.error(f"Failed to create PartNumber: {e}")
            raise

    def get_part(self, part_number: str):
        try:
            v = self.g.V().has("PartNumber", "id", part_number).next()
            return PartNumber(v["id"], v["specs"], v["notes"])
        except StopIteration:
            return None

    def update_part(self, part: PartNumber):
        self.g.V().has("PartNumber", "id", part.part_number)\
            .property("specs", part.specs)\
            .property("notes", part.notes).next()
        return part

    def delete_part(self, part_number: str):
        self.g.V().has("PartNumber", "id", part_number).drop().iterate()
        return True

    def list_parts(self):
        vertices = self.g.V().hasLabel("PartNumber").toList()
        return [PartNumber(v["id"], v["specs"], v["notes"]) for v in vertices]
    
    def list_matches(self):
        edges = self.g.E().hasLabel("MATCHED").toList()
        return [Match(e.outV.id, e.inV.id, e["match_type"]) for e in edges]


    # ----- MATCH CRUD -----
    def create_match(self, match: Match) -> Match:
        self.g.V().has("PartNumber", "id", match.source)\
            .addE("MATCHED").to(self.g.V().has("PartNumber", "id", match.target))\
            .property("match_type", match.match_type).next()
        return match

    def get_match(self, source: str, target: str):
        e = self.g.E().hasLabel("MATCHED")\
            .where(__.outV().has("id", source))\
            .where(__.inV().has("id", target)).toList()
        if e:
            return Match(source, target, e[0]["match_type"])
        return None

    def update_match(self, match: Match):
        e = self.g.E().hasLabel("MATCHED")\
            .where(__.outV().has("id", match.source))\
            .where(__.inV().has("id", match.target)).next()
        e.property("match_type", match.match_type)
        return match

    def delete_match(self, source: str, target: str):
        self.g.E().hasLabel("MATCHED")\
            .where(__.outV().has("id", source))\
            .where(__.inV().has("id", target)).drop().iterate()
        return True
