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
        
    def flatten_json(self, obj, parent_key="", depth=0):
        """
        Flatten JSON into a list of dict rows.
        - 'depth_cutoff' decides how deep we keep flattening lists/dicts.
        - If we hit a list, we replicate all existing rows for each item in that list.
        """
        if not isinstance(obj, (dict, list)) or depth >= self.depth_cutoff:
            return [{parent_key: obj}] if parent_key else [{}]
        
        if isinstance(obj, list):
            all_rows = []
            for item in obj:
                flattened_item = self.flatten_json(item, parent_key, depth + 1)
                all_rows.extend(flattened_item)
            return all_rows

        rows = [{}]
        for key, value in obj.items():
            new_key = f"{parent_key}.{key}" if parent_key else key
            flattened_value = self.flatten_json(value, new_key, depth + 1)

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
        aggregation_results = {}

        for json_path in json_paths:
            if not json_path or not os.path.exists(json_path):
                print(f"⚠️ Skipping missing file: {json_path}")
                continue
            
            valid_paths.append(json_path)

            with open(json_path, "r") as file:
                data = json.load(file)

            if "data" not in data:
                raise ValueError(f"Invalid GraphQL response format in {json_path}: 'data' field missing.")

            operation = data.get("operation", "DISPLAY")

            for key, value in data["data"].items():
                flattened_data = self.flatten_json(value, parent_key=key)

                if operation == "DISPLAY":
                    combined_records.extend(flattened_data)
                else:
                    self._process_aggregation(operation, flattened_data, aggregation_results)
                    
        if not combined_records and not aggregation_results:
            raise ValueError("Flattening resulted in an empty DataFrame. Check input JSON structure.")

        df = pd.DataFrame(combined_records)

        if df.empty:
            df = pd.DataFrame([{}]) 

        if aggregation_results:
            for agg_key, agg_value in aggregation_results.items():
                df[agg_key] = agg_value
                
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

    def _process_aggregation(self, operation, data, aggregation_results):
        """
        Processes aggregation functions like COUNT, SUM, AVG, MIN, MAX.
        :param operation: Aggregation type (e.g., COUNT, SUM, AVG).
        :param data: Flattened JSON data extracted from GraphQL response.
        :param aggregation_results: Dictionary storing computed aggregation results.
        """
        if not data:
            return

        def extract_scalar_field(record):
            """
            Recursively extracts the deepest scalar field from nested JSON.
            Assumes that only one scalar value exists per record.
            """
            if isinstance(record, dict):
                for key, value in record.items():
                    return extract_scalar_field(value)
            elif isinstance(record, list) and record:
                return extract_scalar_field(record[0])
            else:
                return record

        values = [extract_scalar_field(record) for record in data]

        first_key = list(data[0].keys())[0]
        field_name = first_key.replace(".", "_")

        numeric_values = np.array([v for v in values if isinstance(v, (int, float))], dtype=float)

        if operation == "COUNT":
            aggregation_results[f"{operation}({first_key})"] = len(values)
        elif operation == "SUM":
            aggregation_results[f"{operation}({first_key})"] = np.nansum(numeric_values)
        elif operation == "AVG":
            aggregation_results[f"{operation}({first_key})"] = np.nanmean(numeric_values) if len(numeric_values) > 0 else None
        elif operation == "MIN":
            aggregation_results[f"{operation}({first_key})"] = np.nanmin(numeric_values) if len(numeric_values) > 0 else None
        elif operation == "MAX":
            aggregation_results[f"{operation}({first_key})"] = np.nanmax(numeric_values) if len(numeric_values) > 0 else None