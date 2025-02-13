from graphsql.translators.json_to_tabular import JSONToTabular

data_path = "./data/query_46f9dbf2f3_1739277054.json"
csv_converter = JSONToTabular(depth_cutoff=2, output_format="csv")
parquet_converter = JSONToTabular(depth_cutoff=2, output_format="parquet")
jsonl_converter = JSONToTabular(depth_cutoff=2, output_format="jsonl")

csv_converter.convert(data_path)
parquet_converter.convert(data_path)
jsonl_converter.convert(data_path)