import json
import os

class SchemaParser:
    SCALAR_MAP = {
        "String": "VARCHAR(255)",
        "Int": "INTEGER",
        "Float": "DECIMAL(10,2)",
        "Boolean": "BOOLEAN",
        "ID": "UUID PRIMARY KEY"
    }

    def __init__(self, schema_path):
        """Initialize SchemaParser with the path to the GraphQL schema JSON."""
        self.schema_path = schema_path
        self.types = {}  # Full type definitions
        self.mappings = {}  # Object mappings
        self.relations = {}  # Relationship mappings
        self.visited = set()  # Visited types

        schema_filename = os.path.basename(self.schema_path)
        schema_hash = os.path.splitext(schema_filename)[0].replace("schema_", "")
        self.schema_hash = schema_hash
        
        self._load_schema()

    def _load_schema(self):
        """Loads and processes the GraphQL schema JSON file."""
        with open(self.schema_path, "r") as file:
            schema_data = json.load(file)
        for entry in schema_data:
            self.types[entry["name"]] = entry

    def parse(self):
        """Parses the GraphQL schema into mappings & relations."""
        for type_name, type_info in self.types.items():
            if type_info["kind"] == "OBJECT":
                self._parse_object(type_name, type_info)
        self._save_mappings()
        self._save_relations()

    def _parse_object(self, name, obj_info):
        """Parses a GraphQL object type and detects relationships correctly."""
        if name in self.visited:
            return
        self.visited.add(name)
        self.mappings[name] = {}
        for field in obj_info.get("fields") or []:
            field_name = field["name"]
            field_type = self._resolve_type(field["type"])
            if isinstance(field_type, dict) and "LIST" in field_type:
                related_type = list(field_type["LIST"].keys())[0]
                if related_type in ["VARCHAR(255)", "INTEGER", "DECIMAL(10,2)", "BOOLEAN"]:
                    if related_type == "VARCHAR(255)":
                        self.mappings[name][field_name] = "VARCHAR(255) ARRAY"
                    elif related_type == "INTEGER":
                        self.mappings[name][field_name] = "INTEGER ARRAY"
                    else:
                        self.mappings[name][field_name] = "JSON" 
                    continue
                self.relations.setdefault(name, []).append(
                    {"field": field_name, "relation": "one-to-many", "target": related_type}
                )
                continue
            elif isinstance(field_type, dict):
                related_type = list(field_type.keys())[0]
                self.relations.setdefault(name, []).append(
                    {"field": field_name, "relation": "many-to-one", "target": related_type}
                )
            else:
                self.mappings[name][field_name] = field_type

    def _resolve_type(self, type_obj):
        """Safely resolves GraphQL types recursively and prevents missing keys."""
        if not isinstance(type_obj, dict) or "kind" not in type_obj:
            return "UNKNOWN"
        kind = type_obj["kind"]
        if kind == "SCALAR":
            resolved_type = self.SCALAR_MAP.get(type_obj.get("name"), "TEXT")
            return resolved_type
        if kind == "NON_NULL":
            of_type = self._resolve_type(type_obj.get("ofType", {}))
            resolved_type = f"{of_type} NOT NULL" if isinstance(of_type, str) else of_type
            return resolved_type
        if kind == "LIST":
            of_type = self._resolve_type(type_obj.get("ofType", {}))
            if of_type in ["VARCHAR(255)", "INTEGER", "DECIMAL(10,2)", "BOOLEAN"]:
                resolved_type = f"{of_type} ARRAY"
                return resolved_type
            resolved_type = f"LIST<{of_type}>" if isinstance(of_type, str) else {"LIST": of_type}
            return resolved_type
        if kind == "OBJECT":
            object_name = type_obj.get("name", "UNKNOWN_OBJECT")
            resolved_type = {object_name: "RELATION"}
            return resolved_type
        return "UNKNOWN"

    def _save_mappings(self):
        """Saves mappings to a JSON file with schema hash in the filename."""
        mappings_path = f"schemas/mappings_{self.schema_hash}.json"
        with open(mappings_path, "w") as file:
            json.dump(self.mappings, file, indent=2)
        print(f"✅ Mappings saved to {mappings_path}")

    def _save_relations(self):
        """Saves relationships to a JSON file with schema hash in the filename."""
        relations_path = f"schemas/relations_{self.schema_hash}.json"
        with open(relations_path, "w") as file:
            json.dump(self.relations, file, indent=2)
        print(f"✅ Relations saved to {relations_path}")