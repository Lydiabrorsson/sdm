import json
from pathlib import Path
import random
import csv
import hashlib

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/complete")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

INPUT_FILE = RAW_DIR / "papers.json"

TOPIC_KEYWORDS = [
    "data management",
    "database management",
    "data governance",

    "data querying",
    "query processing",
    "query optimization",
    "query execution",
    "sql query",

    "data modeling",
    "data model",
    "schema design",
    "schema evolution",
    "entity relationship",

    "big data",
    "large scale data",
    "large-scale data",
    "data analytics",
    "data mining",
    "distributed data processing",

    "indexing",
    "database index",
    "index structure",
    "b-tree",
    "hash index",
    "inverted index"
]


def get_topics(title, abstract):
    text = f"{title or ''} {abstract or ''}".lower()
    return [topic for topic in TOPIC_KEYWORDS if topic in text]


def classify_venue(venue_name, publication_types):
    venue_lower = (venue_name or "").lower()
    publication_types = publication_types or []

    if "workshop" in venue_lower:
        return "Workshop"
    if "JournalArticle" in publication_types:
        return "Journal"
    return "Conference"


def make_conference_id(name):
    return f"conference::{name.lower()}"


def make_workshop_id(name):
    return f"workshop::{name.lower()}"


def make_journal_id(name):
    return f"journal::{name.lower()}"


def make_proceeding_id(name, year):
    return f"proceeding::{name.lower()}::{year}"


def make_volume_id(name, year, volume_nr):
    return f"volume::{name.lower()}::{year}::{volume_nr}"

def make_edition_id(name, year):
    return f"edition::{name.lower()}::{year}"

def make_isbn(venue_name, year):
    key = f"{venue_name.lower()}-{year}"
    hash_val = hashlib.md5(key.encode()).hexdigest()[:12] 
    digits = ''.join(str(int(c, 16) % 10) for c in hash_val)
    return f"978-{digits[:3]}-{digits[3:6]}-{digits[6:9]}-{digits[9:]}"

def process_graph(raw_papers):
    papers = {}
    authors = {}
    topics = {}

    conferences = {}
    workshops = {}
    editions = {}
    journals = {}
    proceedings = {}
    volumes = {}

    review_edges = []
    wrote_edges = []
    has_topic_edges = []
    has_proceeding_edges = []

    published_in_proceeding_edges = []
    published_in_volume_edges = []
    has_edition_edges = []
    has_volume_edges = []

    # To avoid duplicate edges in case of multiple papers from same venue/year
    seen_has_volume_edges = set()
    seen_has_edition_edges = set()

    for p in raw_papers:
        paper_id = p.get("paperId")
        title = p.get("title")
        year = p.get("year")
        venue_name = p.get("venue")
        abstract = p.get("abstract")
        citation_count = p.get("citationCount", 0)
        external_ids = p.get("externalIds", {}) or {}
        doi = external_ids.get("DOI")
        author_list = p.get("authors", []) or []
        publication_types = p.get("publicationTypes", []) or []
        papers_topics = get_topics(title, abstract)

        if not paper_id or not doi or not year or not venue_name:
            continue

        # Paper node
        papers[paper_id] = {
            "paperId": paper_id,
            "title": title,
            "year": year,
            "abstract": abstract,
            "pages": random.randint(6, 15),
            "doi": doi,
            "citationCount": citation_count
        }

        # Topic nodes + has_topic edges
        for topic_name in papers_topics:
            topic_id = topic_name.lower()

            if topic_id not in topics:
                topics[topic_id] = {
                    "topicId": topic_id,
                    "name": topic_name
                }

            has_topic_edges.append({
                "from": paper_id,
                "to": topic_id,
            })

        # Publication structure
        venue_type = classify_venue(venue_name, publication_types)

        if venue_type == "Journal":
            volume_nr = random.randint(1, 20)
            journal_id = make_journal_id(venue_name)
            volume_id = make_volume_id(venue_name, year, volume_nr)

            if journal_id not in journals:
                journals[journal_id] = {
                    "journalId": journal_id,
                    "name": venue_name
                }

            if volume_id not in volumes:
                volumes[volume_id] = {
                    "volumeId": volume_id,
                    "volumeNumber": volume_nr,
                    "year": year
                }

            edge_key = (journal_id, volume_id, "HAS_VOLUME")
            if edge_key not in seen_has_volume_edges:
                seen_has_volume_edges.add(edge_key)
                has_volume_edges.append({
                    "from": journal_id,
                    "to": volume_id,
                })

            published_in_volume_edges.append({
                "from": paper_id,
                "to": volume_id,
            })

        else:
            edition_id = make_edition_id(venue_name, year)
            isbn = make_isbn(venue_name, year)

            cities = ["Barcelona", "New York", "London", "Tokyo","Paris", "Berlin", "Singapore", "Sydney"]

            if edition_id not in editions:
                editions[edition_id] = {
                    "editionId": edition_id,
                    "year": year,
                    "city": random.choice(cities)
                }

            publishers = ["Springer", "IEEE", "ACM", "Elsevier", "Wiley", "Oxford University Press"]

            if isbn not in proceedings:
                proceedings[isbn] = {
                    "isbn": isbn,
                    "publisher": random.choice(publishers)
                }

            if venue_type == "Workshop":
                workshop_id = make_workshop_id(venue_name)

                if workshop_id not in workshops:
                    workshops[workshop_id] = {
                        "workshopId": workshop_id,
                        "name": venue_name
                    }

                edge_key = (workshop_id, edition_id, "HAS_EDITION")
                if edge_key not in seen_has_edition_edges:
                    seen_has_edition_edges.add(edge_key)
                    has_edition_edges.append({
                        "from": workshop_id,
                        "to": edition_id,
                    })
                    has_proceeding_edges.append({
                        "from": edition_id,
                        "to": isbn,
                    })

                published_in_proceeding_edges.append({
                    "from": paper_id,
                    "to": isbn,
                })

            else:
                conference_id = make_conference_id(venue_name)

                if conference_id not in conferences:
                    conferences[conference_id] = {
                        "conferenceId": conference_id,
                        "name": venue_name
                    }

                edge_key = (conference_id, edition_id, "HAS_EDITION")
                if edge_key not in seen_has_edition_edges:
                    seen_has_edition_edges.add(edge_key)
                    has_edition_edges.append({
                        "from": conference_id,
                        "to": edition_id,
                    })
                    has_proceeding_edges.append({
                        "from": edition_id,
                        "to": isbn,
                    })

                published_in_proceeding_edges.append({
                    "from": paper_id,
                    "to": isbn,
                })

        # Author nodes + wrote edges
        for idx, author in enumerate(author_list, start=1):
            author_id = author.get("authorId")
            author_name = author.get("name")

            if not author_id or not author_name:
                continue

            if author_id not in authors:
                parts = author_name.split()
                first_name = parts[0] if parts else ""
                last_name = " ".join(parts[1:]) if len(parts) > 1 else ""

                authors[author_id] = {
                    "authorId": author_id,
                    "firstName": first_name,
                    "lastName": last_name
                }

            wrote_edges.append({
                "from": author_id,
                "to": paper_id,
                "authorOrder": idx,
                "corresponding": idx == 1
            })

    # Generate random review edges 
    all_author_ids = list(authors.keys())
    num_reviewers = random.randint(1, 5)
    # map papers to their authors
    paper_authors = {}
    for edge in wrote_edges:
        paper_id = edge["to"]
        author_id = edge["from"]
        if paper_id not in paper_authors:
          paper_authors[paper_id] = set()
        paper_authors[paper_id].add(author_id)

    for paper_id in papers.keys():
        chosen_reviewers = set()
        num_reviewers = random.randint(1, 5)

        while len(chosen_reviewers) < num_reviewers:
            reviewer_id = random.choice(all_author_ids)

            if reviewer_id in paper_authors.get(paper_id, set()):
                continue

            if reviewer_id in chosen_reviewers:
                continue

            chosen_reviewers.add(reviewer_id)

        for reviewer_id in chosen_reviewers:
            review_edges.append({
                "from": reviewer_id,
                "to": paper_id,
            })

    # generate cite edges
    cites_edges = []
    paper_list = list(papers.values())
    for source_paper in paper_list:
        source_id = source_paper["paperId"]
        source_year = source_paper["year"]

        candidates = [
            p for p in paper_list
            if p["paperId"] != source_id and p["year"] > source_year
        ]

        if not candidates:
            continue

        citations = min(len(candidates), source_paper.get("citationCount", 0))
        chosen = random.sample(candidates, citations)

        for target_paper in chosen:
            cites_edges.append({
                "from": target_paper["paperId"],
                "to": source_id,
            })

    return {
        "papers": list(papers.values()),
        "authors": list(authors.values()),
        "topics": list(topics.values()),
        "conferences": list(conferences.values()),
        "workshops": list(workshops.values()),
        "journals": list(journals.values()),
        "proceedings": list(proceedings.values()),
        "volumes": list(volumes.values()),
        "editions": list(editions.values()),
        "wrote_edges": wrote_edges,
        "has_topic_edges": has_topic_edges,
        "published_in_proceeding_edges": published_in_proceeding_edges,
        "has_proceeding_edges": has_proceeding_edges,
        "published_in_volume_edges": published_in_volume_edges,
        "has_edition_edges": has_edition_edges,
        "has_volume_edges": has_volume_edges,
        "review_edges": review_edges,
        "cites_edges": cites_edges
    }


def save_csv(filename, data):
    if not data:
        print(f"No data for {filename}")
        return

    out_file = PROCESSED_DIR / filename

    columns = set()
    for row in data:
        columns.update(row.keys())
    columns = sorted(columns)

    with open(out_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(data)

    print(f"Saved {out_file}")


if __name__ == "__main__":
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        raw_papers = json.load(f)

    graph = process_graph(raw_papers)

    save_csv("papers_nodes.csv", graph["papers"])
    save_csv("authors_nodes.csv", graph["authors"])
    save_csv("topics_nodes.csv", graph["topics"])

    save_csv("conferences_nodes.csv", graph["conferences"])
    save_csv("workshops_nodes.csv", graph["workshops"])
    save_csv("editions_nodes.csv", graph["editions"])
    save_csv("journals_nodes.csv", graph["journals"])
    save_csv("proceedings_nodes.csv", graph["proceedings"])
    save_csv("volumes_nodes.csv", graph["volumes"])

    save_csv("wrote_edges.csv", graph["wrote_edges"])
    save_csv("has_topic_edges.csv", graph["has_topic_edges"])
    save_csv("has_proceeding_edges.csv", graph["has_proceeding_edges"])
    save_csv("published_in_proceeding_edges.csv", graph["published_in_proceeding_edges"])
    save_csv("published_in_volume_edges.csv", graph["published_in_volume_edges"])
    save_csv("has_edition_edges.csv", graph["has_edition_edges"])
    save_csv("has_volume_edges.csv", graph["has_volume_edges"])
    save_csv("review_edges.csv", graph["review_edges"])
    save_csv("cites_edges.csv", graph["cites_edges"])