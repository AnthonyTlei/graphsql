import json
import os

class SchemaParser:
    """Parses the JSON from a full GraphQL introspection query and produces:
       1. A 'mappings' JSON that maps object fields to SQL-like columns.
       2. A 'relations' JSON that describes relationships between objects (tables).
    """
    
    SCALAR_MAP = {
        "String": "VARCHAR(255)",
        "Int": "INTEGER",
        "Float": "DECIMAL(10,2)",
        "Boolean": "BOOLEAN",
        "ID": "UUID PRIMARY KEY"
    }
    
    BUILTIN_SCALARS = set(SCALAR_MAP.keys())
    BUILTIN_TYPES = {
        "Boolean", "String", "Float", "Int", "ID",
        "__Schema", "__Type", "__Field", "__InputValue", 
        "__EnumValue", "__Directive"
    }

    def __init__(self, full_schema_path):
        """
        :param full_schema_path: Path to the JSON file containing the 
                                 full introspection result:
                                 {
                                   "data": {
                                     "__schema": {
                                       "queryType": {...},
                                       "mutationType": {...},
                                       "types": [...],
                                       "directives": [...]
                                     }
                                   }
                                 }
        """
        self.full_schema_path = full_schema_path
        self.types_dict = {}   # Store the full type definitions keyed by name
        self.mappings = {}     # Object (type) -> field mappings
        self.relations = {}    # Object (type) -> array of relation definitions
        self.visited = set()   # Keep track of visited object types
        
        schema_filename = os.path.basename(self.full_schema_path)
        schema_hash = os.path.splitext(schema_filename)[0].replace("schema_", "")
        self.schema_hash = schema_hash
        
        self._load_full_schema()
    
    def _load_full_schema(self):
        """Loads the full introspection JSON file from disk and preps self.types_dict."""
        with open(self.full_schema_path, "r", encoding="utf-8") as f:
            schema_data = json.load(f)
        
        if "data" not in schema_data or "__schema" not in schema_data["data"]:
            raise ValueError("Invalid schema JSON structure: Could not find 'data.__schema'")
        
        schema = schema_data["data"]["__schema"]
        
        for tdef in schema["types"]:
            type_name = tdef.get("name")
            if type_name:
                self.types_dict[type_name] = tdef
    
    def parse(self):
        """Main entry point to parse the schema and produce mappings & relations."""
        for type_name, type_def in self.types_dict.items():
            if (
                type_def.get("kind") == "OBJECT" 
                and type_name not in self.BUILTIN_TYPES
            ):
                self._parse_object(type_name, type_def)
        
        self._save_mappings()
        self._save_relations()
    
    def _parse_object(self, name, type_def):
        """
        Parses a GraphQL object type (which we treat like a "table") 
        and detects its fields/relationships.
        """
        if name in self.visited:
            return
        self.visited.add(name)
        
        self.mappings[name] = {}
        
        fields = type_def.get("fields", [])
        for field_def in fields:
            field_name = field_def["name"]
            graphql_type = field_def["type"]
            resolved = self._resolve_type(graphql_type)
            
            if isinstance(resolved, dict):
                self._handle_complex_field(name, field_name, resolved)
            else:
                self.mappings[name][field_name] = resolved
    
    def _handle_complex_field(self, parent_type_name, field_name, resolved):
        if "LIST" in resolved:
            inner_type = resolved["LIST"]
            if isinstance(inner_type, str):
                self.mappings[parent_type_name][field_name] = f"{inner_type} ARRAY"
            else:
                key = list(inner_type.keys())[0]
                val = inner_type[key]
                if val == "RELATION":
                    self.relations.setdefault(parent_type_name, []).append({
                        "field": field_name,
                        "relation": "one-to-many",
                        "target": key
                    })
                elif val == "UNION_RELATION":
                    self._append_union_relation(
                        parent_type_name, field_name, key, is_list=True
                    )
                elif val == "INTERFACE_RELATION":
                    # TODO - handle interface
                    pass
                else:
                    self.mappings[parent_type_name][field_name] = "TEXT ARRAY"
                    
        else:
            key = list(resolved.keys())[0]
            val = resolved[key]
            
            if val == "RELATION":
                self.relations.setdefault(parent_type_name, []).append({
                    "field": field_name,
                    "relation": "many-to-one",
                    "target": key
                })
            elif val == "UNION_RELATION":
                self._append_union_relation(
                    parent_type_name, field_name, key, is_list=False
                )
            elif val == "INTERFACE_RELATION":
                # TODO - handle interface
                pass
            else:
                self.mappings[parent_type_name][field_name] = val
                
    def _append_union_relation(self, parent_type_name, field_name, union_name, is_list=False):
        """Create a polymorphic-union relation entry."""
        union_def = self.types_dict.get(union_name, {})
        possible_types = union_def.get("possibleTypes", [])
        target_names = [t["name"] for t in possible_types if "name" in t]
        
        self.relations.setdefault(parent_type_name, []).append({
            "field": field_name,
            "relation": "polymorphic-union",
            "union": union_name,
            "possibleTargets": target_names,
            "isList": is_list
        })
    
    def _resolve_type(self, type_ref):
        if not type_ref or not isinstance(type_ref, dict):
            return "UNKNOWN"
        
        kind = type_ref.get("kind")
        name = type_ref.get("name")
        of_type = type_ref.get("ofType")
        
        if kind == "SCALAR":
            return self.SCALAR_MAP.get(name, "TEXT")
        
        if kind == "NON_NULL":
            unwrapped = self._resolve_type(of_type)
            if isinstance(unwrapped, str):
                return f"{unwrapped} NOT NULL"
            return unwrapped
        
        if kind == "LIST":
            inner_resolved = self._resolve_type(of_type)
            if isinstance(inner_resolved, str):
                return {"LIST": inner_resolved}
            elif isinstance(inner_resolved, dict):
                return {"LIST": inner_resolved}
            return {"LIST": "UNKNOWN"}
        
        if kind == "OBJECT":
            if not name:
                return "UNKNOWN"
            if name not in self.BUILTIN_TYPES:
                return {name: "RELATION"}
            else:
                return self.SCALAR_MAP.get(name, "TEXT")
        
        if kind == "INTERFACE":
            return {name: "INTERFACE_RELATION"}
        
        if kind == "UNION":
            return {name: "UNION_RELATION"}
        
        if kind == "ENUM":
            return f"TEXT /* ENUM: {name} */"
        
        if kind == "INPUT_OBJECT":
            return "UNKNOWN"
        
        return "UNKNOWN"
    
    def _save_mappings(self):
        """Saves the computed field-to-SQL-type mappings to a JSON file."""
        os.makedirs("schemas", exist_ok=True)
        mappings_path = f"schemas/mappings_{self.schema_hash}.json"
        with open(mappings_path, "w", encoding="utf-8") as f:
            json.dump(self.mappings, f, indent=2)
        print(f"✅ Mappings saved to {mappings_path}")
    
    def _save_relations(self):
        """Saves the computed relationships to a JSON file."""
        os.makedirs("schemas", exist_ok=True)
        relations_path = f"schemas/relations_{self.schema_hash}.json"
        with open(relations_path, "w", encoding="utf-8") as f:
            json.dump(self.relations, f, indent=2)
        print(f"✅ Relations saved to {relations_path}")