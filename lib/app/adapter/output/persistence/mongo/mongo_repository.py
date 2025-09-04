# lib/app/adapter/output/persistence/mongo/mongo_repository.py
from lib.app.application.services.repository_interface import RepositoryInterface
from lib.app.domain.entities.part_number import PartNumber
from lib.app.domain.entities.match import Match
from lib.core.db.mongodb.mongo_client import get_mongo_client
from lib.core.logging import logger

class MongoRepository(RepositoryInterface):
    def __init__(self):
        self.db = get_mongo_client()
        self.parts = self.db["parts"]
        self.matches = self.db["matches"]

    # ----- PART CRUD -----
    def create_part(self, part: PartNumber) -> PartNumber:
        self.parts.update_one(
            {"_id": part.part_number},
            {"$set": {"specs": part.specs, "notes": part.notes}},
            upsert=True
        )
        logger.info(f"Created PartNumber in Mongo: {part.part_number}")
        return part

    def get_part(self, part_number: str):
        doc = self.parts.find_one({"_id": part_number})
        if doc:
            return PartNumber(doc["_id"], doc["specs"], doc.get("notes", ""))
        return None

    def update_part(self, part: PartNumber):
        self.parts.update_one(
            {"_id": part.part_number},
            {"$set": {"specs": part.specs, "notes": part.notes}}
        )
        return part

    def delete_part(self, part_number: str):
        self.parts.delete_one({"_id": part_number})
        return True

    def list_parts(self):
        return [PartNumber(doc["_id"], doc["specs"], doc.get("notes", "")) for doc in self.parts.find()]

    # ----- MATCH CRUD -----
    def create_match(self, match: Match) -> Match:
        self.matches.update_one(
            {"source": match.source, "target": match.target},
            {"$set": {"match_type": match.match_type}},
            upsert=True
        )
        return match

    def get_match(self, source: str, target: str):
        doc = self.matches.find_one({"source": source, "target": target})
        if doc:
            return Match(doc["source"], doc["target"], doc["match_type"])
        return None

    def update_match(self, match: Match):
        self.matches.update_one(
            {"source": match.source, "target": match.target},
            {"$set": {"match_type": match.match_type}}
        )
        return match

    def delete_match(self, source: str, target: str):
        self.matches.delete_one({"source": source, "target": target})
        return True

    def list_matches(self):
        return [Match(doc["source"], doc["target"], doc["match_type"]) for doc in self.matches.find()]
