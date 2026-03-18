import json
from pathlib import Path
import random

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")
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


def make_volume_id(name, year):
    return f"volume::{name.lower()}::{year}"


def process_graph(raw_papers):
    papers = {}
    authors = {}
    topics = {}

    conferences = {}
    workshops = {}
    journals = {}
    proceedings = {}
    volumes = {}

    review_edges = []
    wrote_edges = []
    has_topic_edges = []

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
            "citationCount": citation_count if citation_count is not None else 0,
            "doi": doi
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
                "type": "HAS_TOPIC"
            })

        # Publication structure
        venue_type = classify_venue(venue_name, publication_types)

        if venue_type == "Journal":
            journal_id = make_journal_id(venue_name)
            volume_id = make_volume_id(venue_name, year)

            if journal_id not in journals:
                journals[journal_id] = {
                    "journalId": journal_id,
                    "name": venue_name
                }

            if volume_id not in volumes:
                volumes[volume_id] = {
                    "volumeId": volume_id,
                    "volumeNumber": str(year),
                    "year": year
                }

            edge_key = (journal_id, volume_id, "HAS_VOLUME")
            if edge_key not in seen_has_volume_edges:
                seen_has_volume_edges.add(edge_key)
                has_volume_edges.append({
                    "from": journal_id,
                    "to": volume_id,
                    "type": "HAS_VOLUME"
                })

            published_in_volume_edges.append({
                "from": paper_id,
                "to": volume_id,
                "type": "PUBLISHED_IN"
            })

        else:
            proceeding_id = make_proceeding_id(venue_name, year)

            if proceeding_id not in proceedings:
                proceedings[proceeding_id] = {
                    "proceedingId": proceeding_id,
                    "editionNumber": str(year),
                    "year": year,
                    "city": ""
                }

            published_in_proceeding_edges.append({
                "from": paper_id,
                "to": proceeding_id,
                "type": "PUBLISHED_IN"
            })

            if venue_type == "Workshop":
                workshop_id = make_workshop_id(venue_name)

                if workshop_id not in workshops:
                    workshops[workshop_id] = {
                        "workshopId": workshop_id,
                        "name": venue_name
                    }

                edge_key = (workshop_id, proceeding_id, "HAS_EDITION")
                if edge_key not in seen_has_edition_edges:
                    seen_has_edition_edges.add(edge_key)
                    has_edition_edges.append({
                        "from": workshop_id,
                        "to": proceeding_id,
                        "type": "HAS_EDITION"
                    })

            else:
                conference_id = make_conference_id(venue_name)

                if conference_id not in conferences:
                    conferences[conference_id] = {
                        "conferenceId": conference_id,
                        "name": venue_name
                    }

                has_edition_edges.append({
                    "from": conference_id,
                    "to": proceeding_id,
                    "type": "HAS_EDITION"
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
                    "name": author_name,
                    "firstName": first_name,
                    "lastName": last_name
                }

            wrote_edges.append({
                "from": author_id,
                "to": paper_id,
                "type": "WROTE",
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
                "type": "REVIEWS"
            })

    # generate cite edges

    cites_edges = []
    seen_cites = set()
    paper_list = list(papers.values())
    for source_paper in paper_list:
        source_id = source_paper["paperId"]
        source_year = source_paper["year"]

        candidates = [
            p for p in paper_list
            if p["paperId"] != source_id and p["year"] <= source_year
        ]

        if not candidates:
            continue

        num_references = random.randint(1, min(5, len(candidates)))
        chosen = random.sample(candidates, num_references)

        for target_paper in chosen:
            edge_key = (source_id, target_paper["paperId"])
            if edge_key not in seen_cites:
                seen_cites.add(edge_key)
                cites_edges.append({
                    "from": source_id,
                    "to": target_paper["paperId"],
                    "type": "CITES"
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
        "wrote_edges": wrote_edges,
        "has_topic_edges": has_topic_edges,
        "published_in_proceeding_edges": published_in_proceeding_edges,
        "published_in_volume_edges": published_in_volume_edges,
        "has_edition_edges": has_edition_edges,
        "has_volume_edges": has_volume_edges,
        "review_edges": review_edges,
        "cites_edges": cites_edges
    }


def save_json(filename, data):
    out_file = PROCESSED_DIR / filename
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Saved {out_file}")


if __name__ == "__main__":
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        raw_papers = json.load(f)

    graph = process_graph(raw_papers)

    save_json("papers_nodes.json", graph["papers"])
    save_json("authors_nodes.json", graph["authors"])
    save_json("topics_nodes.json", graph["topics"])

    save_json("conferences_nodes.json", graph["conferences"])
    save_json("workshops_nodes.json", graph["workshops"])
    save_json("journals_nodes.json", graph["journals"])
    save_json("proceedings_nodes.json", graph["proceedings"])
    save_json("volumes_nodes.json", graph["volumes"])

    save_json("wrote_edges.json", graph["wrote_edges"])
    save_json("has_topic_edges.json", graph["has_topic_edges"])
    save_json("published_in_proceeding_edges.json", graph["published_in_proceeding_edges"])
    save_json("published_in_volume_edges.json", graph["published_in_volume_edges"])
    save_json("has_edition_edges.json", graph["has_edition_edges"])
    save_json("has_volume_edges.json", graph["has_volume_edges"])
    save_json("review_edges.json", graph["review_edges"])
    save_json("cites_edges.json", graph["cites_edges"])