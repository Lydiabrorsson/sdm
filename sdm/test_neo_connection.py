from neo4j import GraphDatabase
import os

URI = "neo4j+s://f4cce3df.databases.neo4j.io"
USER = "f4cce3df"
PASSWORD = os.environ["NEO4J_PASSWORD"]  # Ensure this environment variable is set with your Neo4j password
driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

with driver.session() as session:
    result = session.run("RETURN 'connected' AS msg")
    print(result.single()["msg"])

driver.close()