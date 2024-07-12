import logging
import os
import time
from tqdm import tqdm
from vllm import LLM
from askmevllm.models import dataset
from askmevllm.config import DATASET_PATH, MODEL, SEED
from askmevllm.helpers import load_csv_data_all, load_csv_data_rand_n
from askmevllm.dataset.questions import generate_questions_single_turn, filter_questions
from askmevllm.dataset.answers import generate_answers
from askmevllm.dataset.ratings import generate_answer_ratings


def process_all_paragraphs_s2s(batch_size, llm):
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
                dataset.questions.extend(all_questions)
            for paragraph in paragraphs:
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
            filter_questions(questions, llm)
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
            all_answers = generate_answers(questions, setting="ic", llm=llm)
            all_answers.extend(generate_answers(questions, setting="zs", llm=llm))
            if all_answers:
                dataset.answers.extend(all_answers)
                for question in questions:
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
            all_ratings = generate_answer_ratings(answers, llm)
            dataset.ratings.extend(all_ratings)
            for answer in answers:
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


def start_background_process_s2s(batch_size, llm):
    try:
        process_all_paragraphs_s2s(batch_size, llm)
    except Exception as e:
        logging.error("Error in background process:")
        logging.error(str(e))


def main():
    load_csv_data_rand_n(DATASET_PATH, 64)

    os.environ["CUDA_VISIBLE_DEVICES"] = "0,1"
    llm = LLM(MODEL, tensor_parallel_size=2, seed=SEED)
    start_background_process_s2s(64, llm)


if __name__ == "__main__":
    main()
