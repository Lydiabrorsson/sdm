import os
import requests
import json
import time
from pathlib import Path

API_KEY = os.environ["S2_API_KEY"]
HEADERS = {"x-api-key": API_KEY}

BASE_URL = "https://api.semanticscholar.org/graph/v1/paper/search/bulk"
RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

QUERIES = [
    "data management",
    "data querying",
    "data modeling",
    "big data",
    "indexing"
]

YEAR_RANGE = "2019-2024"
FIELDS = "paperId,title,abstract,year,venue,authors,externalIds,citationCount,publicationTypes,references.paperId"
REQUEST_DELAY = 1.1 # To respect rate limits (1 request per second)


def fetch_bulk_papers(queries, year_range):
    papers_by_id = {}

    for query in queries:
        print(f"\nCollecting papers for query: {query}")
        params = {
            "query": query,
            "year": year_range,
            "fields": FIELDS,
        }

        response = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
        data = response.json()

        papers = data.get("data", [])


        # To avoid duplicates across queries
        for p in papers:
            pid = p.get("paperId")
            if pid not in papers_by_id:
                papers_by_id[pid] = p

        time.sleep(REQUEST_DELAY)

    return list(papers_by_id.values())


if __name__ == "__main__":
    papers = fetch_bulk_papers(QUERIES, YEAR_RANGE)

    out_file = RAW_DIR / "papers.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(papers, f, ensure_ascii=False, indent=2)

    print(f"\nTotal unique papers collected: {len(papers)}")
    print(f"Saved all papers to {out_file}")