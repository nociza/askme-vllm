import hashlib
import pandas as pd
import logging
from tqdm import tqdm
from askmevllm.models import Paragraph, Author, dataset


def load_csv_data_all(file, overwrite=False):
    try:
        df = pd.read_csv(file)
        df["within_page_order"] = df.groupby("page_name").cumcount()
        df = df.where(pd.notnull(df), None)

        # Rename 'id' to 'original_entry_id'
        df = df.rename(columns={"id": "original_entry_id"})

        # Sort by length of 'text' column in reverse order
        df["text_length"] = df["text"].str.len()
        df = df.sort_values("text_length", ascending=False).drop("text_length", axis=1)

        if overwrite:
            dataset.paragraphs.clear()
            dataset.paragraph_dict.clear()
            logging.info("Existing entries in the dataset have been removed.")

        # Convert DataFrame to Paragraph objects and add to dataset
        for _, row in tqdm(df.iterrows(), total=len(df), desc="Loading data"):
            paragraph = Paragraph(
                id=row["original_entry_id"],
                page_name=row["page_name"],
                section_name=row["section_name"],
                subsection_name=row["subsection_name"],
                subsubsection_name=row["subsubsection_name"],
                text=row["text"],
                section_hierarchy=row["section_hierarchy"],
                text_cleaned=row["text_cleaned"],
                word_count=row["word_count"],
                is_bad=row["is_bad"],
                within_page_order=row["within_page_order"],
                processed=False,
                original_entry_id=row["original_entry_id"],
            )
            dataset.paragraphs.append(paragraph)
            dataset.paragraph_dict[paragraph.id] = paragraph

        logging.info(f"Successfully loaded {len(df)} entries into the local dataset.")

    except Exception as e:
        logging.error(f"Error loading CSV data: {str(e)}")
        raise  # Re-raise the exception for further debugging if needed
    finally:
        logging.info("Data loading completed.")


def load_csv_data_rand_n(file, n, overwrite=False):
    try:
        df = pd.read_csv(file)
        df["within_page_order"] = df.groupby("page_name").cumcount()
        df = df.where(pd.notnull(df), None)

        # Rename 'id' to 'original_entry_id'
        df = df.rename(columns={"id": "original_entry_id"})

        # Sort by length of 'text' column in reverse order
        df["text_length"] = df["text"].str.len()
        df = df.sort_values("text_length", ascending=False).drop("text_length", axis=1)

        # Randomly select N entries
        if len(df) > n:
            df = df.sample(n=n)

        if overwrite:
            dataset.paragraphs.clear()
            dataset.paragraph_dict.clear()
            logging.info("Existing entries in the dataset have been removed.")

        # Convert DataFrame to Paragraph objects and add to dataset
        for _, row in tqdm(df.iterrows(), total=len(df), desc="Loading data"):
            paragraph = Paragraph(
                id=row["original_entry_id"],
                page_name=row["page_name"],
                section_name=row["section_name"],
                subsection_name=row["subsection_name"],
                subsubsection_name=row["subsubsection_name"],
                text=row["text"],
                section_hierarchy=row["section_hierarchy"],
                text_cleaned=row["text_cleaned"],
                word_count=row["word_count"],
                is_bad=row["is_bad"],
                within_page_order=row["within_page_order"],
                processed=False,
                original_entry_id=row["original_entry_id"],
            )
            dataset.paragraphs.append(paragraph)
            dataset.paragraph_dict[paragraph.id] = paragraph

        logging.info(f"Successfully loaded {len(df)} entries into the local dataset.")

    except Exception as e:
        logging.error(f"Error loading CSV data: {str(e)}")
        raise  # Re-raise the exception for further debugging if needed
    finally:
        logging.info("Data loading completed.")


def generate_hash(model: str, prompt: str) -> str:
    return hashlib.sha256(f"{model}:{prompt}".encode("utf-8")).hexdigest()


def create_author_if_not_exists(prompt: str, model: str) -> int:
    hash_value = generate_hash(model, prompt)

    # Check if the author already exists
    existing_author = next(
        (author for author in dataset.authors if author.hash == hash_value), None
    )

    if existing_author:
        return existing_author.id

    # If the author does not exist, insert a new one
    author_id = len(dataset.authors) + 1
    new_author = Author(id=author_id, model=model, prompt=prompt)
    dataset.authors.append(new_author)
    dataset.author_dict[new_author.id] = new_author
    return author_id
