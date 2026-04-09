from neo4j import GraphDatabase
import os

URI = "neo4j+s://f4cce3df.databases.neo4j.io"
USER = "f4cce3df"
PASSWORD = os.environ["NEO4J_PASSWORD"]


def create_constraints(tx):
    tx.run("""
        CREATE CONSTRAINT community_name IF NOT EXISTS
        FOR (c:Community) REQUIRE c.name IS UNIQUE
    """)


def define_database_community(tx):
    tx.run("""
        MERGE (c:Community {name: "Database"})
        WITH c, [
            "data management",
            "indexing",
            "data modeling",
            "big data",
            "data processing",
            "data storage",
            "data querying"
        ] AS dbTopics
        UNWIND dbTopics AS topicId
        MATCH (t:Topic {topicId: topicId})
        MERGE (c)-[:DEFINED_BY]->(t)
    """)


def identify_database_venues(tx):
    tx.run("""
        MATCH (c:Community {name: "Database"})

        CALL (c) {
            MATCH (v)-[:HAS_EDITION]->(pr:Proceeding)
            WHERE v:Conference OR v:Workshop
            MATCH (p:Paper)-[:PUBLISHED_IN]->(pr)
            WITH c, v, collect(DISTINCT p) AS papers
            WITH c, v, papers, size(papers) AS totalPapers
            UNWIND papers AS p
            OPTIONAL MATCH (p)-[:ABOUT]->(t:Topic)<-[:DEFINED_BY]-(c)
            WITH c, v, totalPapers, p, count(t) AS matchedTopics
            WITH c, v, totalPapers,
                 count(CASE WHEN matchedTopics > 0 THEN 1 END) AS dbPapers
            WHERE totalPapers > 0 AND (1.0 * dbPapers / totalPapers) >= 0.9
            MERGE (v)-[r:BELONGS_TO_COMMUNITY]->(c)
            SET r.paperCount = totalPapers,
                r.communityPaperCount = dbPapers,
                r.ratio = 1.0 * dbPapers / totalPapers
            RETURN count(*) AS done1
        }

        WITH c, coalesce(done1, 0) AS done1

        CALL (c) {
            MATCH (j:Journal)-[:HAS_VOLUME]->(vol:Volume)
            MATCH (p:Paper)-[:PUBLISHED_IN]->(vol)
            WITH c, j, collect(DISTINCT p) AS papers
            WITH c, j, papers, size(papers) AS totalPapers
            UNWIND papers AS p
            OPTIONAL MATCH (p)-[:ABOUT]->(t:Topic)<-[:DEFINED_BY]-(c)
            WITH c, j, totalPapers, p, count(t) AS matchedTopics
            WITH c, j, totalPapers,
                 count(CASE WHEN matchedTopics > 0 THEN 1 END) AS dbPapers
            WHERE totalPapers > 0 AND (1.0 * dbPapers / totalPapers) >= 0.9
            MERGE (j)-[r:BELONGS_TO_COMMUNITY]->(c)
            SET r.paperCount = totalPapers,
                r.communityPaperCount = dbPapers,
                r.ratio = 1.0 * dbPapers / totalPapers
            RETURN count(*) AS done2
        }

        RETURN done1, coalesce(done2, 0) AS done2
    """)


def identify_top_100_database_papers(tx):
    tx.run("""
            MATCH (c:Community {name: "Database"})

            CALL (c) {
                MATCH (p:Paper)-[:PUBLISHED_IN]->(pr:Proceeding)<-[:HAS_EDITION]-(v)
                WHERE (v:Conference OR v:Workshop)
                AND EXISTS { MATCH (v)-[:BELONGS_TO_COMMUNITY]->(c) }
                RETURN p

                UNION

                MATCH (p:Paper)-[:PUBLISHED_IN]->(vol:Volume)<-[:HAS_VOLUME]-(j:Journal)
                WHERE EXISTS { MATCH (j)-[:BELONGS_TO_COMMUNITY]->(c) }
                RETURN p
            }

            WITH DISTINCT c, p

            OPTIONAL MATCH (citing:Paper)-[:CITES]->(p)
            WHERE
                EXISTS {
                    MATCH (citing)-[:PUBLISHED_IN]->(pr2:Proceeding)<-[:HAS_EDITION]-(v2)
                    WHERE (v2:Conference OR v2:Workshop)
                    AND EXISTS { MATCH (v2)-[:BELONGS_TO_COMMUNITY]->(c) }
                }
                OR
                EXISTS {
                    MATCH (citing)-[:PUBLISHED_IN]->(vol2:Volume)<-[:HAS_VOLUME]-(j2:Journal)
                    WHERE EXISTS { MATCH (j2)-[:BELONGS_TO_COMMUNITY]->(c) }
                }

            WITH c, p, count(DISTINCT citing) AS dbCitationCount
            ORDER BY dbCitationCount DESC, p.title ASC
            LIMIT 100

            WITH c, collect({paper: p, cites: dbCitationCount}) AS topPapers
            UNWIND range(0, size(topPapers) - 1) AS i
            WITH c, i, topPapers[i] AS row
            WITH c, i, row.paper AS p, row.cites AS cites
            MERGE (p)-[r:IN_TOP_COMMUNITY_PAPERS]->(c)
            SET r.rank = i + 1,
                r.dbCitationCount = cites;   
            """)

def identify_reviewer_candidates_and_gurus(tx):
    tx.run("""
        MATCH (c:Community {name: "Database"})
        MATCH (a:Author)-[:WRITES]->(p:Paper)-[:IN_TOP_COMMUNITY_PAPERS]->(c)

        WITH c, a, count(DISTINCT p) AS topPaperCount

        MERGE (a)-[r:GOOD_REVIEWER_MATCH_FOR]->(c)
        SET r.topPaperCount = topPaperCount

        FOREACH (_ IN CASE WHEN topPaperCount >= 2 THEN [1] ELSE [] END |
            MERGE (a)-[g:GURU_IN]->(c)
            SET g.topPaperCount = topPaperCount
        )

        RETURN count(a) AS processed;
        )
    """)


def main():
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

    with driver.session() as session:
        session.execute_write(create_constraints)
        session.execute_write(define_database_community)
        session.execute_write(identify_database_venues)
        session.execute_write(identify_top_100_database_papers)
        session.execute_write(identify_reviewer_candidates_and_gurus)

    driver.close()


if __name__ == "__main__":
    main()