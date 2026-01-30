import os
import json
import numpy as np
import lancedb
from dotenv import load_dotenv
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    VectorParams,
    PointStruct,
    CollectionInfo,
)

load_dotenv()

QDRANT_URL = os.environ["QDRANT_URL"]
QDRANT_API_KEY = os.environ["QDRANT_API_KEY"]
LANCE_DB_PATH = "../cognee-minihack/cognee-minihack/cognee_export/system_databases/cognee.lancedb"
VECTOR_DIM = 768
BATCH_SIZE = 100


def main():
    # Connect to Lance
    lance_db = lancedb.connect(LANCE_DB_PATH)
    table_names = lance_db.table_names()
    print(f"Found {len(table_names)} Lance tables: {table_names}")

    # Connect to Qdrant Cloud
    qdrant = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)
    print(f"Connected to Qdrant at {QDRANT_URL}")

    for table_name in table_names:
        print(f"\n--- Migrating: {table_name} ---")
        tbl = lance_db.open_table(table_name)
        df = tbl.to_pandas()
        row_count = len(df)
        print(f"  Rows: {row_count}")

        if row_count == 0:
            print("  Skipping empty table")
            continue

        # Create collection (recreate if exists)
        collection_name = table_name
        try:
            qdrant.delete_collection(collection_name)
        except Exception:
            pass

        qdrant.create_collection(
            collection_name=collection_name,
            vectors_config=VectorParams(size=VECTOR_DIM, distance=Distance.COSINE),
        )
        print(f"  Created collection: {collection_name}")

        # Upload in batches
        points = []
        for idx, row in df.iterrows():
            vector = row["vector"].tolist() if isinstance(row["vector"], np.ndarray) else list(row["vector"])
            payload = row["payload"] if isinstance(row["payload"], dict) else json.loads(row["payload"])

            # Use the payload 'id' as point id via a hash, or use index
            point_id = row["id"] if isinstance(row["id"], str) else str(row["id"])

            points.append(
                PointStruct(
                    id=point_id,
                    vector=vector,
                    payload=payload,
                )
            )

            if len(points) >= BATCH_SIZE:
                qdrant.upsert(collection_name=collection_name, points=points)
                print(f"  Uploaded {idx + 1}/{row_count}")
                points = []

        if points:
            qdrant.upsert(collection_name=collection_name, points=points)

        print(f"  Done: {row_count} points uploaded to {collection_name}")

    # Verify
    print("\n=== Verification ===")
    for table_name in table_names:
        info = qdrant.get_collection(table_name)
        print(f"  {table_name}: {info.points_count} points")

    print("\nMigration complete!")


if __name__ == "__main__":
    main()
