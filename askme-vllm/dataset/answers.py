from datetime import datetime
import logging

from vllm import SamplingParams
from ..models import Answer, dataset
from .common import generate_fact_with_context
from ..helpers import create_author_if_not_exists


def generate_answer(
    question_id: int,
    setting: str,
    llm,
    model: str = "llama3-70B-instruct",
):
    try:
        # process prompt template
        question = next(q for q in dataset.questions if q.id == question_id)

        prompt_template = "{PROMPT_PREFIX}{CONTEXT_PROMPT}Answer the following question in a succinct manner: {QUESTION}\n{PROMPT_SUFFIX}"

        if setting == "ic":
            paragraph = next(
                p for p in dataset.paragraphs if p.id == question.paragraph_id
            )
            _, fact = generate_fact_with_context(paragraph)
            context_prompt = f"Using this fact: {fact} \n\n "
        elif setting == "zs":
            context_prompt = ""
        else:
            raise Exception("Invalid setting")

        prompt = prompt_template.format(
            CONTEXT_PROMPT=context_prompt,
            QUESTION=question.text,
            PROMPT_PREFIX="",
            PROMPT_SUFFIX="",
        )
        author_id = create_author_if_not_exists(prompt, model)

        sampling_params = SamplingParams(max_tokens=200, temperature=0.7)
        output = llm.generate([prompt], sampling_params)
        answer_text = output[0].text.strip()

        if answer_text:
            answer = Answer(
                id=len(dataset.answers) + 1,
                question_id=question.id,
                author_id=author_id,
                setting=setting,
                timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                text=answer_text,
            )
            logging.debug(f"Generated answer: {answer.text}")
            return answer
        else:
            logging.error("Empty answer generated")
            return None

    except Exception as e:
        logging.error(f"An error occurred at generate_answer: {e}")
