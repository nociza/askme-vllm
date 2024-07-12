from datetime import datetime
from typing import List
import logging
import re

from vllm import SamplingParams
from askmevllm.models import Answer, Rating, dataset
from askmevllm.dataset.common import generate_fact_with_context
from askmevllm.helpers import create_author_if_not_exists
from askmevllm.config import MODEL


def generate_answer_ratings(answers: List[Answer], llm):
    try:
        ratings = []
        prompts = []

        prompt_template = "{PROMPT_PREFIX}Based on this fact: \n\n `{REFERENCE}` \n\n Rate the following answer to the question - Question: `{QUESTION}` \n\n Answer: `{ANSWER}`; give a number from 0-5 where 0 is 'No answer or completely irrelevant', 1 is 'Significantly incorrect or incomplete', 2 is 'Partially correct; major inaccuracies or omissions', 3 is 'Correct but lacks depth; minimal detail', 4 is 'Mostly correct; minor errors, includes relevant details', 5 is 'Fully accurate and detailed; clear and comprehensive'. Your answer should follow the form `Answer:<number> \n Rationale:<justify your judgment in a paragraph>`. \n{PROMPT_SUFFIX}"

        for answer in answers:
            question = next(q for q in dataset.questions if q.id == answer.question_id)
            paragraph = next(
                p for p in dataset.paragraphs if p.id == question.paragraph_id
            )
            _, reference = generate_fact_with_context(paragraph)

            prompt = prompt_template.format(
                REFERENCE=reference,
                QUESTION=question.text,
                ANSWER=answer.text,
                PROMPT_PREFIX="",
                PROMPT_SUFFIX="",
            )

            author_id = create_author_if_not_exists(prompt, MODEL)
            prompts.append((prompt, answer.id, author_id))

        # Generate ratings in batch
        batch_prompts = [p[0] for p in prompts]
        sampling_params = SamplingParams(max_tokens=100, temperature=0.7)
        outputs = llm.generate(batch_prompts, sampling_params)

        for i, output in enumerate(outputs):
            rating_raw = output.outputs[0].text.strip()
            prompt_data = prompts[i]

            if re.search(r"Rationale:", rating_raw, re.I) and re.search(
                r"[0-5]", rating_raw
            ):
                score = int(re.search(r"[0-5]", rating_raw).group())
                rationale = "".join(rating_raw.split("Rationale:", re.I)[1:]).strip()

                logging.debug(f"Score: {score}, Rationale: {rationale}")

                rating = Rating(
                    id=len(dataset.ratings) + 1,
                    text=rationale,
                    value=score,
                    answer_id=prompt_data[1],
                    author_id=prompt_data[2],
                    timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                )
                logging.debug(
                    f"Generated rating: {rating.value} for answer: {answer.text} with rationale: {rating.text}"
                )
                ratings.append(rating)
            else:
                logging.error(
                    f"Invalid rating generated for answer_id: {prompt_data[1]}"
                )

        return ratings

    except Exception as e:
        logging.error(f"An error occurred at generate_answer_ratings: {e}")
        return None
