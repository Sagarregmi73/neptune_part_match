# lib/app/adapter/output/persistence/neptune/neptune_repository.py

from lib.app.application.services.repository_interface import RepositoryInterface
from lib.app.domain.entities.part_number import PartNumber
from lib.app.domain.entities.match import Match
from lib.core.aws.neptune_client import get_neptune_connection
from gremlin_python.process.graph_traversal import __
import hashlib
import time

def _get_prop(obj, key: str, default=""):
    try:
        if hasattr(obj, "properties") and key in obj.properties:
            props = obj.properties[key]
            if isinstance(props, list) and len(props) > 0:
                return props[0].value or ""
    except Exception:
        pass
    return default

class NeptuneRepository(RepositoryInterface):
    def __init__(self):
        self.g, self.connection = get_neptune_connection()

    # ---------- PART CRUD ----------
    def create_part(self, part: PartNumber) -> PartNumber:
        composite_id = f"{part.part_number}_{hashlib.md5(str(time.time()).encode()).hexdigest()[:8]}"
        self.g.addV("PartNumber")\
            .property("id", composite_id)\
            .property("part_number", part.part_number)\
            .property("spec1", part.spec1)\
            .property("spec2", part.spec2)\
            .property("spec3", part.spec3)\
            .property("spec4", part.spec4)\
            .property("spec5", part.spec5)\
            .property("note1", part.note1)\
            .property("note2", part.note2)\
            .property("note3", part.note3)\
            .next()
        part.id = composite_id
        return part

    def get_part(self, part_number: str):
        try:
            v = self.g.V().has("PartNumber", "part_number", part_number).next()
            return PartNumber(
                id=_get_prop(v, "id", v.id),
                part_number=_get_prop(v, "part_number", v.id),
                spec1=_get_prop(v, "spec1"),
                spec2=_get_prop(v, "spec2"),
                spec3=_get_prop(v, "spec3"),
                spec4=_get_prop(v, "spec4"),
                spec5=_get_prop(v, "spec5"),
                note1=_get_prop(v, "note1"),
                note2=_get_prop(v, "note2"),
                note3=_get_prop(v, "note3")
            )
        except StopIteration:
            return None

    def update_part(self, part: PartNumber) -> PartNumber:
        v = self.g.V().has("PartNumber", "part_number", part.part_number).next()
        self.g.V(v.id)\
            .property("spec1", part.spec1)\
            .property("spec2", part.spec2)\
            .property("spec3", part.spec3)\
            .property("spec4", part.spec4)\
            .property("spec5", part.spec5)\
            .property("note1", part.note1)\
            .property("note2", part.note2)\
            .property("note3", part.note3)\
            .next()
        part.id = _get_prop(v, "id", v.id)
        return part

    def delete_part(self, part_number: str) -> bool:
        self.g.V().has("PartNumber", "part_number", part_number).drop().iterate()
        return True

    def list_parts(self):
        vertices = self.g.V().hasLabel("PartNumber").toList()
        parts = []
        for v in vertices:
            parts.append(
                PartNumber(
                    id=_get_prop(v, "id", v.id),
                    part_number=_get_prop(v, "part_number", v.id),
                    spec1=_get_prop(v, "spec1"),
                    spec2=_get_prop(v, "spec2"),
                    spec3=_get_prop(v, "spec3"),
                    spec4=_get_prop(v, "spec4"),
                    spec5=_get_prop(v, "spec5"),
                    note1=_get_prop(v, "note1"),
                    note2=_get_prop(v, "note2"),
                    note3=_get_prop(v, "note3")
                )
            )
        return parts

    # ---------- MATCH CRUD ----------
    def create_match(self, match: Match) -> Match:
        self.g.V().has("PartNumber", "part_number", match.source).as_("a")\
            .V().has("PartNumber", "part_number", match.target)\
            .coalesce(
                __.inE("MATCHED").where(__.outV().as_("a")),
                __.addE("MATCHED").from_("a").property("match_type", match.match_type)
            ).next()
        return match

    def get_match(self, source: str, target: str):
        edges = self.g.E().hasLabel("MATCHED")\
            .where(__.outV().has("part_number", source))\
            .where(__.inV().has("part_number", target)).toList()
        if edges:
            return Match(source, target, _get_prop(edges[0], "match_type"))
        return None

    def update_match(self, match: Match) -> Match:
        e = self.g.E().hasLabel("MATCHED")\
            .where(__.outV().has("part_number", match.source))\
            .where(__.inV().has("part_number", match.target)).next()
        e.property("match_type", match.match_type)
        return match

    def delete_match(self, source: str, target: str) -> bool:
        self.g.E().hasLabel("MATCHED")\
            .where(__.outV().has("part_number", source))\
            .where(__.inV().has("part_number", target)).drop().iterate()
        return True

    def list_matches(self):
        edges = self.g.E().hasLabel("MATCHED").toList()
        return [Match(e.outV.id, e.inV.id, _get_prop(e, "match_type")) for e in edges]

    # ---------- GET MATCHES FOR PART ----------
    def get_matches_for_part(self, part_number: str):
        try:
            part_vertex = self.g.V().has("PartNumber", "part_number", part_number).next()
            part_data = PartNumber(
                id=_get_prop(part_vertex, "id", part_vertex.id),
                part_number=_get_prop(part_vertex, "part_number", part_vertex.id),
                spec1=_get_prop(part_vertex, "spec1"),
                spec2=_get_prop(part_vertex, "spec2"),
                spec3=_get_prop(part_vertex, "spec3"),
                spec4=_get_prop(part_vertex, "spec4"),
                spec5=_get_prop(part_vertex, "spec5"),
                note1=_get_prop(part_vertex, "note1"),
                note2=_get_prop(part_vertex, "note2"),
                note3=_get_prop(part_vertex, "note3")
            )
        except StopIteration:
            return None

        edges_out = self.g.V().has("PartNumber", "part_number", part_number)\
            .outE("MATCHED").as_("e").inV().as_("v").select("e", "v").toList()
        edges_in = self.g.V().has("PartNumber", "part_number", part_number)\
            .inE("MATCHED").as_("e").outV().as_("v").select("e", "v").toList()

        matches = []
        for e in edges_out + edges_in:
            matched_vertex = e["v"]
            matched_part = PartNumber(
                id=_get_prop(matched_vertex, "id", matched_vertex.id),
                part_number=_get_prop(matched_vertex, "part_number", matched_vertex.id),
                spec1=_get_prop(matched_vertex, "spec1"),
                spec2=_get_prop(matched_vertex, "spec2"),
                spec3=_get_prop(matched_vertex, "spec3"),
                spec4=_get_prop(matched_vertex, "spec4"),
                spec5=_get_prop(matched_vertex, "spec5"),
                note1=_get_prop(matched_vertex, "note1"),
                note2=_get_prop(matched_vertex, "note2"),
                note3=_get_prop(matched_vertex, "note3")
            )
            matches.append({
                "replacement_part": matched_part,
                "match_type": _get_prop(e["e"], "match_type")
            })

        return {
            "part": part_data,
            "matches": matches
        }

    def close(self):
        try:
            if self.connection:
                self.connection.close()
        except Exception as e:
            print("Error closing Neptune connection:", e)
