from neo4j import GraphDatabase
import os

URI = "neo4j+s://e903ba21.databases.neo4j.io"
USER = "e903ba21"
PASSWORD = os.environ["NEO4J_PASSWORD"]


def create_constraints(tx):
    tx.run("""
        CREATE CONSTRAINT review_id IF NOT EXISTS
        FOR (r:Review) REQUIRE r.id IS UNIQUE
    """)
    tx.run("""
        CREATE CONSTRAINT organization_id IF NOT EXISTS
        FOR (o:Organization) REQUIRE o.id IS UNIQUE
    """)


def migrate_review_relationships(tx):
    tx.run("""
        MATCH (a:Authors)-[old:REVIEWS]->(p:Paper)
        WITH a, old, p,
             CASE
                 WHEN rand() < 0.5 THEN "accept"
                 ELSE "reject"
             END AS generatedDecision
        CREATE (rev:Review {
            id: randomUUID(),
            content: coalesce(
                old.content,
                CASE
                    WHEN generatedDecision = "accept"
                    THEN "Synthetic review: the paper is relevant and should be accepted."
                    ELSE "Synthetic review: the paper needs improvement and should be rejected."
                END
            ),
            decision: generatedDecision
        })
        CREATE (a)-[:WROTE_REVIEW]->(rev)
        CREATE (rev)-[:REVIEWS]->(p)
        DELETE old
    """)


def create_synthetic_organizations(tx):
    tx.run("""
        UNWIND [
            {id: "org_001", name: "Stanford University", type: "university"},
            {id: "org_002", name: "Massachusetts Institute of Technology", type: "university"},
            {id: "org_003", name: "University of Oxford", type: "university"},
            {id: "org_004", name: "Google", type: "company"},
            {id: "org_005", name: "Microsoft", type: "company"},
            {id: "org_006", name: "Amazon", type: "company"},
            {id: "org_007", name: "Meta", type: "company"}
        ] AS org
        MERGE (o:Organization {id: org.id})
        ON CREATE SET
            o.name = org.name,
            o.type = org.type
    """)


def assign_authors_to_organizations(tx):
    tx.run("""
        MATCH (a:Authors)
        WHERE NOT (a)-[:AFFILIATED_WITH]->(:Organization)
        WITH a, toInteger(rand() * 7) AS idx
        MATCH (o:Organization)
        WITH a, idx, o
        ORDER BY o.id
        WITH a, idx, collect(o) AS orgs
        WITH a, orgs[idx] AS org
        MERGE (a)-[:AFFILIATED_WITH]->(org)
    """)


def compute_paper_acceptance(tx):
    tx.run("""
        MATCH (p:Paper)<-[:REVIEWS]-(r:Review)
        WITH p,
             count(r) AS reviewCount,
             sum(CASE WHEN r.decision = "accept" THEN 1 ELSE 0 END) AS acceptCount,
             sum(CASE WHEN r.decision = "reject" THEN 1 ELSE 0 END) AS rejectCount
        SET p.review_count = reviewCount,
            p.accept_count = acceptCount,
            p.reject_count = rejectCount,
            p.accepted = acceptCount > rejectCount
    """)


def remove_publication_for_rejected_papers(tx):
    tx.run("""
        MATCH (p:Paper)-[pub:PUBLISHED_IN]->()
        WHERE p.accepted IS NOT NULL AND p.accepted = false
        DELETE pub
    """)


def main():
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

    with driver.session() as session:
        session.execute_write(create_constraints)
        session.execute_write(migrate_review_relationships)
        session.execute_write(create_synthetic_organizations)
        session.execute_write(assign_authors_to_organizations)
        session.execute_write(compute_paper_acceptance)
        session.execute_write(remove_publication_for_rejected_papers)

    driver.close()


if __name__ == "__main__":
    main()