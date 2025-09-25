from lib.app.application.services.repository_interface import RepositoryInterface
from lib.app.domain.entities.part_number import PartNumber
from lib.app.domain.entities.match import Match
from lib.core.aws.neptune_client import get_neptune_connection
from gremlin_python.process.graph_traversal import __

class NeptuneRepository(RepositoryInterface):
    def __init__(self):
        self.g, self.connection = get_neptune_connection()

    # ---------- PART CRUD ----------
    async def create_part(self, part: PartNumber) -> PartNumber:
        await self.g.addV("PartNumber")\
            .property("id", part.part_number)\
            .property("spec1", part.spec1)\
            .property("spec2", part.spec2)\
            .property("spec3", part.spec3)\
            .property("spec4", part.spec4)\
            .property("spec5", part.spec5)\
            .property("note1", part.note1)\
            .property("note2", part.note2)\
            .property("note3", part.note3)\
            .nextAsync()
        return part

    async def get_part(self, part_number: str):
        try:
            v = await self.g.V().has("PartNumber", "id", part_number).nextAsync()
            return PartNumber(
                v.id,
                v.properties["spec1"][0].value,
                v.properties["spec2"][0].value,
                v.properties["spec3"][0].value,
                v.properties["spec4"][0].value,
                v.properties["spec5"][0].value,
                v.properties.get("note1", [{}])[0].get("value", ""),
                v.properties.get("note2", [{}])[0].get("value", ""),
                v.properties.get("note3", [{}])[0].get("value", "")
            )
        except StopIteration:
            return None

    async def update_part(self, part: PartNumber) -> PartNumber:
        await self.g.V().has("PartNumber", "id", part.part_number)\
            .property("spec1", part.spec1)\
            .property("spec2", part.spec2)\
            .property("spec3", part.spec3)\
            .property("spec4", part.spec4)\
            .property("spec5", part.spec5)\
            .property("note1", part.note1)\
            .property("note2", part.note2)\
            .property("note3", part.note3)\
            .nextAsync()
        return part

    async def delete_part(self, part_number: str) -> bool:
        await self.g.V().has("PartNumber", "id", part_number).drop().iterateAsync()
        return True

    async def list_parts(self):
        vertices = await self.g.V().hasLabel("PartNumber").toListAsync()
        parts = []
        for v in vertices:
            parts.append(
                PartNumber(
                    v.id,
                    v.properties["spec1"][0].value,
                    v.properties["spec2"][0].value,
                    v.properties["spec3"][0].value,
                    v.properties["spec4"][0].value,
                    v.properties["spec5"][0].value,
                    v.properties.get("note1", [{}])[0].get("value", ""),
                    v.properties.get("note2", [{}])[0].get("value", ""),
                    v.properties.get("note3", [{}])[0].get("value", "")
                )
            )
        return parts

    # ---------- MATCH CRUD ----------
    async def create_match(self, match: Match) -> Match:
        await self.g.V().has("PartNumber", "id", match.source).as_("a")\
            .V().has("PartNumber", "id", match.target)\
            .coalesce(
                __.inE("MATCHED").where(__.outV().as_("a")),
                __.addE("MATCHED").from_("a").property("match_type", match.match_type)
            ).nextAsync()
        return match

    async def get_match(self, source: str, target: str):
        edges = await self.g.E().hasLabel("MATCHED")\
            .where(__.outV().has("id", source))\
            .where(__.inV().has("id", target)).toListAsync()
        if edges:
            return Match(source, target, edges[0].properties["match_type"][0].value)
        return None

    async def update_match(self, match: Match) -> Match:
        e = await self.g.E().hasLabel("MATCHED")\
            .where(__.outV().has("id", match.source))\
            .where(__.inV().has("id", match.target)).nextAsync()
        await e.property("match_type", match.match_type)
        return match

    async def delete_match(self, source: str, target: str) -> bool:
        await self.g.E().hasLabel("MATCHED")\
            .where(__.outV().has("id", source))\
            .where(__.inV().has("id", target)).drop().iterateAsync()
        return True

    async def list_matches(self):
        edges = await self.g.E().hasLabel("MATCHED").toListAsync()
        return [Match(e.outV.id, e.inV.id, e.properties["match_type"][0].value) for e in edges]

    async def get_matches_for_part(self, part_number: str):
        edges_out = await self.g.V().has("PartNumber", "id", part_number)\
            .outE("MATCHED").as_("e").inV().as_("v").select("e","v").toListAsync()
        edges_in  = await self.g.V().has("PartNumber", "id", part_number)\
            .inE("MATCHED").as_("e").outV().as_("v").select("e","v").toListAsync()
        matches = []
        for e in edges_out + edges_in:
            matches.append(Match(e["e"].outV.id, e["e"].inV.id, e["e"].properties["match_type"][0].value))
        return matches

    async def close(self):
        try:
            if self.connection:
                await self.connection.closeAsync()
        except Exception as e:
            print("Error closing Neptune connection:", e)
