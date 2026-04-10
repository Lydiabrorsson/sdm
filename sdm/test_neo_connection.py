from neo4j import GraphDatabase
import os

URI = "neo4j://127.0.0.1:7687"
USER = "neo4j"
PASSWORD = os.environ["NEO4J_PASSWORD"]  # Ensure this environment variable is set with your Neo4j password
driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

with driver.session() as session:
    result = session.run("RETURN 'connected' AS msg")
    print(result.single()["msg"])

driver.close()