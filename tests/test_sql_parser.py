import unittest

from graphsql.translators.sql_parser import SQLParser

class TestSQLToGraphQL(unittest.TestCase):
    """Unit tests for SQLToGraphQL conversion"""

    @classmethod
    def setUpClass(cls):
        """Initialize SQLToGraphQL converter before tests."""
        cls.sql_to_graphql = SQLParser(
            mappings_path="schemas/mappings.json",
            relations_path="schemas/relations.json"
        )

    def assertGraphQL(self, sql_query, expected_graphql):
        """Helper function to compare SQL-to-GraphQL conversion."""
        graphql_query = self.sql_to_graphql.convert_to_graphql(sql_query)
        self.assertEqual(graphql_query, expected_graphql, f"\n🔴 Expected:\n{expected_graphql}\n🟢 Got:\n{graphql_query}")

    def test_basic_select(self):
        """Test simple SELECT query with WHERE condition"""
        sql_query = "SELECT id, name FROM Mission WHERE id = '123';"
        expected_graphql = 'query mission($id: ID!) { mission(id: $id) { id, name } }'
        self.assertGraphQL(sql_query, expected_graphql)

    def test_select_all(self):
        """Test wildcard SELECT * query"""
        sql_query = "SELECT * FROM Mission WHERE id = '123';"
        expected_graphql = (
            'query mission($id: ID!) { mission(id: $id) { '
            'description, id, manufacturers, name, twitter, website, wikipedia, '
            'payloads { graphsql_id } } }'
        )
        self.assertGraphQL(sql_query, expected_graphql)

if __name__ == "__main__":
    unittest.main()