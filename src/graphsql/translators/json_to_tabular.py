import os
import json
import pandas as pd

class JSONToTabular:
    """
    Converts a JSON response from a GraphQL query into a tabular format,
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
        
    def flatten_json(self, obj, parent_key="", depth=0, depth_cutoff=2):
        """
        Flatten JSON into a list of dict rows. 
        - 'depth_cutoff' decides how deep we keep flattening lists/dicts.
        - If we hit a list, we replicate all existing rows for each item in that list.
        """
        if not isinstance(obj, (dict, list)) or depth >= depth_cutoff:
            return [{parent_key: obj}] if parent_key else [{}]
        
        if isinstance(obj, list):
            all_rows = []
            for item in obj:
                flattened_item = self.flatten_json(item, parent_key, depth + 1, depth_cutoff)
                all_rows.extend(flattened_item)
            return all_rows

        rows = [ {} ]
        for key, value in obj.items():
            new_key = f"{parent_key}.{key}" if parent_key else key
            
            flattened_value = self.flatten_json(value, new_key, depth + 1, depth_cutoff)

            if len(flattened_value) > 0 and isinstance(flattened_value, list):
                if isinstance(flattened_value[0], dict):
                    new_rows = []
                    for existing_row in rows:
                        for fv in flattened_value:
                            merged = dict(existing_row, **fv)
                            new_rows.append(merged)
                    rows = new_rows
                else:
                    pass
            else:
                for r in rows:
                    for fv in flattened_value:
                        r.update(fv)
        
        return rows

    def convert(self, json_path):
        """
        Converts a JSON file to a tabular format.
        :param json_path: Path to the JSON file containing the GraphQL response.
        :return: Path of the saved tabular file.
        """
        
        if not json_path:
            return ""
        
        with open(json_path, "r") as file:
            data = json.load(file)

        if "data" not in data:
            raise ValueError("Invalid GraphQL response format: 'data' field missing.")

        extracted_records = []
        for key, value in data["data"].items():
            flattened_data = self.flatten_json(value, parent_key=key, depth_cutoff=self.depth_cutoff)
            extracted_records.extend(flattened_data)

        df = pd.DataFrame(extracted_records)

        if df.empty:
            raise ValueError("Flattening resulted in an empty DataFrame. Check input JSON structure.")

        filename = os.path.basename(json_path).replace(".json", f".{self.output_format}")
        output_path = os.path.join(self.output_dir, filename)

        if self.output_format == "csv":
            df.to_csv(output_path, index=False)
        elif self.output_format == "parquet":
            df.to_parquet(output_path, index=False)
        elif self.output_format == "jsonl":
            df.to_json(output_path, orient="records", lines=True)
        else:
            raise ValueError("Unsupported output format.")

        print(f"✅ Data saved to {output_path}")
        return output_path

if __name__ == "__main__":
    converter = JSONToTabular(output_format="parquet", depth_cutoff=10)
    sample_json_path = "/Users/anthonytleiji/dev/graphsql/data/query_9d913ce1f5_1739388150.json"
    output_path = converter.convert(sample_json_path)
    
    import pandas as pd
    df = pd.read_parquet(output_path, engine='pyarrow')
    print(df.head())