from graphsql.datafetch.data_fetch import DataFetch

fetcher = DataFetch("https://spacex-production.up.railway.app/")
sample_query = """
query GetRockets {
    rockets {
        id
        name
        diameter {
        feet
        meters
        }
    }
}
"""
file_path = fetcher.fetch_data(sample_query)
print(f"Data saved at: {file_path}")