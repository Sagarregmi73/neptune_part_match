# lib/app/adapter/output/persistence/neptune/neptune_repository.py

from lib.app.application.services.repository_interface import RepositoryInterface
from lib.app.domain.entities.part_number import PartNumber
from lib.app.domain.entities.match import Match
from lib.core.aws.neptune_client import get_neptune_connection
from gremlin_python.process.graph_traversal import __

class NeptuneRepository(RepositoryInterface):
    def __init__(self):
        self.g, self.connection = get_neptune_connection()

    # -------------------- PART CRUD --------------------
    def create_part(self, part: PartNumber) -> PartNumber:
        self.g.addV("PartNumber")\
            .property("id", part.part_number)\
            .property("specs", str(part.specs))\
            .property("notes", str(part.notes)).next()
        return part

    def get_part(self, part_number: str):
        try:
            v = self.g.V().has("PartNumber", "id", part_number).next()
            specs = eval(v.properties["specs"][0].value)
            notes = eval(v.properties["notes"][0].value)
            return PartNumber(v.id, specs, notes)
        except StopIteration:
            return None

    def update_part(self, part: PartNumber) -> PartNumber:
        self.g.V().has("PartNumber", "id", part.part_number)\
            .property("specs", str(part.specs))\
            .property("notes", str(part.notes)).next()
        return part

    def delete_part(self, part_number: str) -> bool:
        self.g.V().has("PartNumber", "id", part_number).drop().iterate()
        return True

    def list_parts(self):
        vertices = self.g.V().hasLabel("PartNumber").toList()
        parts = []
        for v in vertices:
            specs = eval(v.properties["specs"][0].value)
            notes = eval(v.properties["notes"][0].value)
            parts.append(PartNumber(v.id, specs, notes))
        return parts

    # -------------------- MATCH CRUD --------------------
    def create_match(self, match: Match) -> Match:
        self.g.V().has("PartNumber", "id", match.source).as_("a")\
            .V().has("PartNumber", "id", match.target)\
            .coalesce(
                __.inE("MATCHED").where(__.outV().as_("a")),
                __.addE("MATCHED").from_("a").property("match_type", match.match_type)
            ).next()
        return match

    def get_match(self, source: str, target: str):
        e_list = self.g.E().hasLabel("MATCHED")\
            .where(__.outV().has("id", source))\
            .where(__.inV().has("id", target)).toList()
        if e_list:
            e = e_list[0]
            match_type = e.properties["match_type"][0].value
            return Match(e.outV.id, e.inV.id, match_type)
        return None

    def update_match(self, match: Match) -> Match:
        e = self.g.E().hasLabel("MATCHED")\
            .where(__.outV().has("id", match.source))\
            .where(__.inV().has("id", match.target)).next()
        e.property("match_type", match.match_type)
        return match

    def delete_match(self, source: str, target: str) -> bool:
        self.g.E().hasLabel("MATCHED")\
            .where(__.outV().has("id", source))\
            .where(__.inV().has("id", target)).drop().iterate()
        return True

    def list_matches(self):
        edges = self.g.E().hasLabel("MATCHED").toList()
        matches = []
        for e in edges:
            match_type = e.properties["match_type"][0].value
            matches.append(Match(e.outV.id, e.inV.id, match_type))
        return matches

    # -------------------- BIDIRECTIONAL SEARCH --------------------
    def get_matches_for_part(self, part_number: str):
        edges_out = self.g.V().has("PartNumber", "id", part_number)\
                        .outE("MATCHED").as_("e").inV().as_("v").select("e","v").toList()
        edges_in = self.g.V().has("PartNumber", "id", part_number)\
                        .inE("MATCHED").as_("e").outV().as_("v").select("e","v").toList()
        matches = []
        for e in edges_out + edges_in:
            edge = e["e"]
            match_type = edge.properties["match_type"][0].value
            matches.append(Match(edge.outV.id, edge.inV.id, match_type))
        return matches
