from lib.app.application.services.repository_interface import RepositoryInterface
from lib.app.domain.entities.part_number import PartNumber
from lib.app.domain.entities.match import Match
from lib.core.aws.neptune_client import get_neptune_connection
from gremlin_python.process.graph_traversal import __


def _get_prop(obj, key: str, default=None):
    """Safely get property from Neptune Vertex/Edge."""
    try:
        if hasattr(obj, "properties") and key in obj.properties:
            props = obj.properties[key]
            if isinstance(props, list) and len(props) > 0:
                return props[0].value
    except Exception:
        pass
    return default


class NeptuneRepository(RepositoryInterface):
    def __init__(self):
        self.g, self.connection = get_neptune_connection()

    # ---------- PART CRUD ----------
    def create_part(self, part: PartNumber) -> PartNumber:
        self.g.addV("PartNumber")\
            .property("id", part.part_number)\
            .property("spec1", part.spec1)\
            .property("spec2", part.spec2)\
            .property("spec3", part.spec3)\
            .property("spec4", part.spec4)\
            .property("spec5", part.spec5)\
            .property("note1", part.note1)\
            .property("note2", part.note2)\
            .property("note3", part.note3)\
            .next()
        return part

    def get_part(self, part_number: str):
        try:
            v = self.g.V().has("PartNumber", "id", part_number).next()
            return PartNumber(
                _get_prop(v, "id", v.id),
                _get_prop(v, "spec1"),
                _get_prop(v, "spec2"),
                _get_prop(v, "spec3"),
                _get_prop(v, "spec4"),
                _get_prop(v, "spec5"),
                _get_prop(v, "note1", ""),
                _get_prop(v, "note2", ""),
                _get_prop(v, "note3", "")
            )
        except StopIteration:
            return None

    def update_part(self, part: PartNumber) -> PartNumber:
        self.g.V().has("PartNumber", "id", part.part_number)\
            .property("spec1", part.spec1)\
            .property("spec2", part.spec2)\
            .property("spec3", part.spec3)\
            .property("spec4", part.spec4)\
            .property("spec5", part.spec5)\
            .property("note1", part.note1)\
            .property("note2", part.note2)\
            .property("note3", part.note3)\
            .next()
        return part

    def delete_part(self, part_number: str) -> bool:
        self.g.V().has("PartNumber", "id", part_number).drop().iterate()
        return True

    def list_parts(self):
        vertices = self.g.V().hasLabel("PartNumber").toList()
        parts = []
        for v in vertices:
            parts.append(
                PartNumber(
                    _get_prop(v, "id", v.id),
                    _get_prop(v, "spec1"),
                    _get_prop(v, "spec2"),
                    _get_prop(v, "spec3"),
                    _get_prop(v, "spec4"),
                    _get_prop(v, "spec5"),
                    _get_prop(v, "note1", ""),
                    _get_prop(v, "note2", ""),
                    _get_prop(v, "note3", "")
                )
            )
        return parts

    # ---------- MATCH CRUD ----------
    def create_match(self, match: Match) -> Match:
        self.g.V().has("PartNumber", "id", match.source).as_("a")\
            .V().has("PartNumber", "id", match.target)\
            .coalesce(
                __.inE("MATCHED").where(__.outV().as_("a")),
                __.addE("MATCHED").from_("a").property("match_type", match.match_type)
            ).next()
        return match

    def get_match(self, source: str, target: str):
        edges = self.g.E().hasLabel("MATCHED")\
            .where(__.outV().has("id", source))\
            .where(__.inV().has("id", target)).toList()
        if edges:
            return Match(source, target, _get_prop(edges[0], "match_type"))
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
        return [Match(e.outV.id, e.inV.id, _get_prop(e, "match_type")) for e in edges]

    def get_matches_for_part(self, part_number: str):
        edges_out = self.g.V().has("PartNumber", "id", part_number)\
            .outE("MATCHED").as_("e").inV().as_("v").select("e", "v").toList()
        edges_in = self.g.V().has("PartNumber", "id", part_number)\
            .inE("MATCHED").as_("e").outV().as_("v").select("e", "v").toList()
        matches = []
        for e in edges_out + edges_in:
            matches.append(Match(e["e"].outV.id, e["e"].inV.id, _get_prop(e["e"], "match_type")))
        return matches

    def close(self):
        try:
            if self.connection:
                self.connection.close()
        except Exception as e:
            print("Error closing Neptune connection:", e)
