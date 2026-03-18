import os
import json
import time
import requests
from pathlib import Path

# Made in own file because different API endpoint 

API_KEY = os.environ["S2_API_KEY"]
HEADERS = {"x-api-key": API_KEY}

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

INPUT_FILE = RAW_DIR / "papers.json"
OUTPUT_FILE = PROCESSED_DIR / "cites_edges.json"

BATCH_URL = "https://api.semanticscholar.org/graph/v1/paper/batch"
FIELDS = "paperId,references.paperId"

REQUEST_DELAY = 1.1
BATCH_SIZE = 500 # Max batch size allowed by API


def chunk_list(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def fetch_cites_edges():
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        papers = json.load(f)

    paper_ids = [p["paperId"] for p in papers]
    #paper_id_set = set(paper_ids)

    cites_edges = []

    for batch_num, batch_ids in enumerate(chunk_list(paper_ids, BATCH_SIZE), start=1):
        print(f"Fetching batch {batch_num} with {len(batch_ids)} papers")

        params = {"fields": FIELDS}
        payload = {"ids": batch_ids}

        response = requests.post(BATCH_URL, params=params, json=payload, headers=HEADERS)

        batch_data = response.json()

        for paper in batch_data:
            references = paper.get("references", []) or []

            for ref in references:
                if ref.get("paperId") is None:
                    continue
                #if ref.get("paperId") in paper_id_set:
                cites_edges.append({
                    "from": paper.get("paperId"),
                    "to": ref.get("paperId"),
                    "type": "CITES"
                })

        time.sleep(REQUEST_DELAY)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(cites_edges, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(cites_edges)} edges to {OUTPUT_FILE}")


if __name__ == "__main__":
    fetch_cites_edges()