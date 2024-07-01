import logging
import time
from tqdm import tqdm
from vllm import LLM
from .models import dataset
from .config import DATASET_PATH
from .dataset.common import load_csv_data_all
from .dataset.questions import generate_questions_single_turn, filter_questions
from .dataset.answers import generate_answer
from .dataset.ratings import generate_answer_rating


def process_all_paragraphs_s2s(batch_size=5):
    # Initialize the local LLM engine
    llm = LLM(model="path/to/llama3-70B-instruct")

    # Stage 1: Generate Questions
    logging.info("Starting stage 1: Generate Questions")
    stage_1_start_time = time.time()
    total_paragraphs = len([p for p in dataset.paragraphs if not p.processed])
    with tqdm(total=total_paragraphs, desc="Stage 1: Generate Questions") as pbar:
        while True:
            paragraphs = [p for p in dataset.paragraphs if not p.processed][:batch_size]
            if not paragraphs:
                logging.info("No unprocessed paragraphs found. Moving to next stage.")
                break
            all_questions = generate_questions_single_turn(paragraphs, llm)
            if all_questions:
                for questions in all_questions:
                    dataset.questions.extend(questions)
                    paragraph = questions[-1]
                    paragraph.processed = True
            pbar.update(len(paragraphs))
    stage_1_end_time = time.time()

    # Stage 2: Filter Questions
    logging.info("Starting stage 2: Filter Questions")
    stage_2_start_time = time.time()
    total_questions = len([q for q in dataset.questions if not q.filtered])
    with tqdm(total=total_questions, desc="Stage 2: Filter Questions") as pbar:
        while True:
            questions = [q for q in dataset.questions if not q.filtered][:batch_size]
            if not questions:
                logging.info("No unprocessed questions found. Moving to next stage.")
                break
            filtered_questions = filter_questions(questions, llm)
            pbar.update(len(questions))
    stage_2_end_time = time.time()

    # Stage 3: Generate Answers
    logging.info("Starting stage 3: Generate Answers")
    stage_3_start_time = time.time()
    total_questions = len([q for q in dataset.questions if not q.processed])
    with tqdm(total=total_questions, desc="Stage 3: Generate Answers") as pbar:
        while True:
            questions = [q for q in dataset.questions if not q.processed][:batch_size]
            if not questions:
                logging.info("No unprocessed questions found. Moving to next stage.")
                break
            all_answers = [generate_answer(question.id, llm) for question in questions]
            for answers in all_answers:
                dataset.answers.extend(answers)
                question = answers[-1]
                question.processed = True
            pbar.update(len(questions))
    stage_3_end_time = time.time()

    # Stage 4: Generate Ratings
    logging.info("Starting stage 4: Generate Ratings")
    stage_4_start_time = time.time()
    total_answers = len([a for a in dataset.answers if not a.processed])
    with tqdm(total=total_answers, desc="Stage 4: Generate Ratings") as pbar:
        while True:
            answers = [a for a in dataset.answers if not a.processed][:batch_size]
            if not answers:
                logging.info("No unprocessed answers found. Finishing process.")
                break
            all_ratings = [generate_answer_rating(answer.id, llm) for answer in answers]
            for ratings in all_ratings:
                dataset.ratings.extend(ratings)
                answer = ratings[-1]
                answer.processed = True
            pbar.update(len(answers))
    stage_4_end_time = time.time()

    times = {
        "stage_1_time": stage_1_end_time - stage_1_start_time,
        "stage_2_time": stage_2_end_time - stage_2_start_time,
        "stage_3_time": stage_3_end_time - stage_3_start_time,
        "stage_4_time": stage_4_end_time - stage_4_start_time,
    }
    logging.info(f"Process completed in {times}")
    return times


def start_background_process_s2s(batch_size=128):
    try:
        process_all_paragraphs_s2s(batch_size)
    except Exception as e:
        logging.error("Error in background process:")
        logging.error(str(e))


def main():
    # Load dataset (in-memory)
    load_csv_data_all(DATASET_PATH)

    start_background_process_s2s(128)


if __name__ == "__main__":
    main()
