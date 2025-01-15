import pandas as pd
import torch
from sentence_transformers import SentenceTransformer, util

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

def initialize_embedding_model():
    # Ensure all tensors are on the same device
    embedding_model = SentenceTransformer('paraphrase-MiniLM-L6-v2').to(device)
    return embedding_model


def load_table_embeddings():
    # Load table embeddings
    table_embeddings = torch.load('C:/Users/sjoer/Desktop/CBS-Statistical-Assistant/data/summaries_embeddings.pt', map_location=device, weights_only=True)
    return table_embeddings


# Function to find best matching tables using table embeddings
def find_best_matching_tables(query, table_embeddings, embedding_model, top_k=5):
    # Embed the user query and move to the same device
    query_embedding = embedding_model.encode(query, convert_to_tensor=True).to(device)

    # Ensure table embeddings are on the same device
    table_vectors = [t["embedding"].to(device) for t in table_embeddings]
    table_tensor = torch.stack(table_vectors)

    # Compute cosine similarity between query and table embeddings
    cosine_scores = util.pytorch_cos_sim(query_embedding, table_tensor).squeeze()

    # Retrieve top-k matching tables
    top_k_indices = torch.topk(cosine_scores, k=top_k).indices.tolist()
    top_k_scores = torch.topk(cosine_scores, k=top_k).values.tolist()
    top_k_tables = [table_embeddings[i]["table_id"] for i in top_k_indices]
    
    # Return top-k results with descriptions
    return [
        {
            "table_id": table_id,
            "score": score,
        }
        for table_id, score in zip(top_k_tables, top_k_scores)
    ]

# Function to find best matching tables using column embeddings
def find_best_matching_tables_from_columns(query, column_embeddings, embedding_model, top_k=5):
    # Embed the user query and move to the same device
    query_embedding = embedding_model.encode(query, convert_to_tensor=True).to(device)

    # Compute cosine similarity between query and column embeddings for each table
    table_scores = []
    for table in column_embeddings:
        table_id = table["table_id"]
        column_embeddings_tensor = table["column_embeddings"].to(device)  # Move column embeddings to the same device
        cosine_scores = util.pytorch_cos_sim(query_embedding, column_embeddings_tensor).squeeze()

        # Use the maximum score among all columns of the table
        max_score = torch.max(cosine_scores).item()
        table_scores.append({"table_id": table_id, "score": max_score})

    # Sort by score and get top-k tables
    top_k_results = sorted(table_scores, key=lambda x: x["score"], reverse=True)[:top_k]

    return [
        {
            "table_id": result["table_id"],
            "score": result["score"],
        }
        for result in top_k_results
    ]