import os
import json
import hashlib
import pandas as pd
import numpy as np

class JSONToTabular:
    """
    Converts multiple JSON responses from GraphQL queries into a single tabular format,
    flattening nested structures and exploding lists into rows.
    """

    def __init__(self, depth_cutoff=2, output_format="csv", output_dir="data"):
        """
        Initialize the JSONToTabular processor.
        :param depth_cutoff: Maximum depth for flattening nested objects.
        :param output_format: Output file format (csv, parquet, jsonl).
        :param output_dir: Directory where tabular files will be stored.
        """
        self.depth_cutoff = depth_cutoff
        self.output_format = output_format.lower()
        self.output_dir = os.path.join(output_dir, "tabular")
        os.makedirs(self.output_dir, exist_ok=True)
        
    def flatten_json(self, obj, parent_key="", depth=0, root_key=None):
        """
        Flatten JSON into a list of dict rows.
        - If the top-level key is a list (like 'products'), remove it from column names.
        - 'depth_cutoff' controls how deep we keep flattening nested objects.
        - If we hit a list, we replicate rows for each item.
        """
        if not isinstance(obj, (dict, list)) or depth >= self.depth_cutoff:
            return [{parent_key: obj}] if parent_key else [{}]

        if isinstance(obj, list):
            all_rows = []
            for item in obj:
                flattened_item = self.flatten_json(item, "" if depth == 0 else parent_key, depth + 1, root_key)
                all_rows.extend(flattened_item)
            return all_rows

        rows = [{}]

        if depth == 0:
            root_key = parent_key

        for key, value in obj.items():
            new_key = f"{parent_key}.{key}" if parent_key else key
            if root_key and new_key.startswith(root_key + "."):
                new_key = new_key[len(root_key) + 1:]

            flattened_value = self.flatten_json(value, new_key, depth + 1, root_key)

            if isinstance(flattened_value, list) and flattened_value and isinstance(flattened_value[0], dict):
                new_rows = []
                for existing_row in rows:
                    for fv in flattened_value:
                        merged = dict(existing_row, **fv)
                        new_rows.append(merged)
                rows = new_rows
            else:
                for r in rows:
                    for fv in flattened_value:
                        r.update(fv)

        return rows

    def _generate_output_filename(self, json_paths):
        """
        Generate a unique output filename based on a hash of the input file paths.
        """
        hash_input = "|".join(sorted(json_paths)).encode()
        file_hash = hashlib.md5(hash_input).hexdigest()
        return f"output_{file_hash}.{self.output_format}"

    def convert(self, json_paths):
        """
        Converts multiple JSON files to a single tabular format, handling aggregations.
        :param json_paths: List of paths to JSON files containing GraphQL responses.
        :return: Path of the saved combined tabular file.
        """
        
        if not json_paths:
            raise ValueError("No input JSON files provided.")
        
        combined_records = []
        valid_paths = []

        for json_path in json_paths:
            if not json_path or not os.path.exists(json_path):
                print(f"⚠️ Skipping missing file: {json_path}")
                continue
            
            valid_paths.append(json_path)

            with open(json_path, "r") as file:
                data = json.load(file)

            if "data" not in data:
                raise ValueError(f"Invalid GraphQL response format in {json_path}: 'data' field missing.")

            for key, value in data["data"].items():
                flattened_data = self.flatten_json(value, parent_key=key)
                combined_records.extend(flattened_data)
                    
        df = pd.DataFrame(combined_records)

        if df.empty:
            df = pd.DataFrame([{}]) 

        output_filename = self._generate_output_filename(valid_paths)
        output_path = os.path.join(self.output_dir, output_filename)

        if self.output_format == "csv":
            df.to_csv(output_path, index=False)
        elif self.output_format == "parquet":
            df.to_parquet(output_path, index=False)
        elif self.output_format == "jsonl":
            df.to_json(output_path, orient="records", lines=True)
        else:
            raise ValueError("Unsupported output format.")

        print(f"✅ Combined data saved to {output_path}")
        return output_path
        