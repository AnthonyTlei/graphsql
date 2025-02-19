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
    
    BUILTIN_SCALARS = set(SCALAR_MAP.keys())  # for quick membership checks
    BUILTIN_TYPES = {
        "Boolean", "String", "Float", "Int", "ID",
        "__Schema", "__Type", "__Field", "__InputValue", 
        "__EnumValue", "__Directive"  # built-in introspection types
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
        
        # The actual schema data is typically under schema_data["data"]["__schema"]
        if "data" not in schema_data or "__schema" not in schema_data["data"]:
            raise ValueError("Invalid schema JSON structure: Could not find 'data.__schema'")
        
        schema = schema_data["data"]["__schema"]
        
        # Loop through all types in the schema
        for tdef in schema["types"]:
            type_name = tdef.get("name")
            if type_name:
                self.types_dict[type_name] = tdef
    
    def parse(self):
        """Main entry point to parse the schema and produce mappings & relations."""
        for type_name, type_def in self.types_dict.items():
            # We only consider OBJECT types as "tables", skipping built-in scalars, interfaces, etc.
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
        
        # Initialize the table schema (mappings) and relations container.
        self.mappings[name] = {}
        
        fields = type_def.get("fields", [])
        for field_def in fields:
            field_name = field_def["name"]
            graphql_type = field_def["type"]  # The top-level type definition for this field
            resolved = self._resolve_type(graphql_type)
            
            # If resolved is a dict that indicates a relationship
            # Example from your code:  {"LIST": {...}} or {"ObjectName": "RELATION"}
            if isinstance(resolved, dict):
                self._handle_complex_field(name, field_name, resolved)
            else:
                # It's a scalar or a scalar with a NOT NULL or array suffix, etc.
                self.mappings[name][field_name] = resolved
    
    def _handle_complex_field(self, parent_type_name, field_name, resolved):
        """
        Handle dictionaries returned by _resolve_type that might indicate relationships.
        For example:
          - {"LIST": "VARCHAR(255)"} or {"LIST": {"SomeObject": "RELATION"}}
          - {"SomeObject": "RELATION"}
        """
        # If it's a list wrapper
        if "LIST" in resolved:
            inner_type = resolved["LIST"]
            # If the inner_type is a string like "VARCHAR(255)", it's a scalar array
            if isinstance(inner_type, str):
                # Map to a SQL array or JSON, etc.
                self.mappings[parent_type_name][field_name] = f"{inner_type} ARRAY"
            elif isinstance(inner_type, dict):
                # That means it's a list of objects => "one-to-many" relationship
                # e.g. {"SomeObject": "RELATION"}
                target_type = list(inner_type.keys())[0]  # "SomeObject"
                
                # We skip if it's unknown or a built-in
                if target_type in self.BUILTIN_SCALARS:
                    # It's a list of a built-in scalar
                    scalar_sql = self.SCALAR_MAP.get(target_type, "TEXT")
                    self.mappings[parent_type_name][field_name] = f"{scalar_sql} ARRAY"
                else:
                    # Many objects
                    rel_item = {
                        "field": field_name,
                        "relation": "one-to-many",
                        "target": target_type
                    }
                    self.relations.setdefault(parent_type_name, []).append(rel_item)
        else:
            # Not a list => single object => many-to-one relationship
            target_type = list(resolved.keys())[0]  # e.g. "SomeObject"
            if target_type not in self.BUILTIN_SCALARS:
                rel_item = {
                    "field": field_name,
                    "relation": "many-to-one",
                    "target": target_type
                }
                self.relations.setdefault(parent_type_name, []).append(rel_item)
            else:
                # If it's a built-in scalar or unknown for some reason
                self.mappings[parent_type_name][field_name] = self.SCALAR_MAP.get(target_type, "TEXT")
    
    def _resolve_type(self, type_ref):
        """
        Recursively resolves a GraphQL field type, returning:
        
        - A *string* if it's a scalar (possibly decorated with NOT NULL).
        - A dict if it's an OBJECT or a LIST type => 
            e.g. {"MyObjectType": "RELATION"} 
                 means we have an object relationship 
            e.g. {"LIST": "VARCHAR(255)"} 
                 means we have a list of scalars
            e.g. {"LIST": {"MyObjectType": "RELATION"}}
                 means we have a list of objects
        """
        if not type_ref or not isinstance(type_ref, dict):
            return "UNKNOWN"
        
        kind = type_ref.get("kind")
        name = type_ref.get("name")  # can be None for LIST/NON_NULL
        of_type = type_ref.get("ofType")  # nested type
        
        # 1. SCALAR
        if kind == "SCALAR":
            # Map it to SQL type
            return self.SCALAR_MAP.get(name, "TEXT")
        
        # 2. NON_NULL => unwrap it, but note it's NOT NULL
        if kind == "NON_NULL":
            unwrapped = self._resolve_type(of_type)
            # If it's already a string, we can append "NOT NULL"
            if isinstance(unwrapped, str):
                # optionally: you can store the not-null constraint in a structured way 
                return f"{unwrapped} NOT NULL"
            # If it's a dict, just pass it up so we can keep the structure
            return unwrapped
        
        # 3. LIST => unwrap the inner type
        if kind == "LIST":
            inner_resolved = self._resolve_type(of_type)
            # If the inner is a string (scalar), return a dict or string that indicates an array
            if isinstance(inner_resolved, str):
                # e.g. "VARCHAR(255)" => we store "LIST<VARCHAR(255)>" or 
                # a dict to indicate it’s a list.
                return {"LIST": inner_resolved}
            elif isinstance(inner_resolved, dict):
                # e.g. {"SomeObject": "RELATION"}
                # So the final is {"LIST": {"SomeObject": "RELATION"}}
                return {"LIST": inner_resolved}
            return {"LIST": "UNKNOWN"}
        
        # 4. OBJECT => relationship
        if kind == "OBJECT":
            # This indicates a reference to another object type => many-to-one or one-to-many
            if not name:
                return "UNKNOWN"
            # If it's not a built-in type, we return a dict to indicate a relation
            if name not in self.BUILTIN_TYPES:
                return {name: "RELATION"}
            else:
                # Built-in or introspection object => treat as scalar? or skip
                return self.SCALAR_MAP.get(name, "TEXT")
        
        # 5. INTERFACE, UNION => you can skip or treat them specially
        if kind in ("INTERFACE", "UNION"):
            # For now, we treat them as "UNKNOWN" or skip them.
            # Production code might need special handling or to build separate inheritance tables.
            return "UNKNOWN"
        
        # 6. ENUM => we can map to a TEXT or a special "ENUM" column
        if kind == "ENUM":
            # Typically you'll store an enum in a TEXT or use a real DB enum type
            return "TEXT /* ENUM: {} */".format(name)
        
        # 7. INPUT_OBJECT => For writing data only (GraphQL input), no direct table
        if kind == "INPUT_OBJECT":
            return "UNKNOWN"  # or skip it entirely
        
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