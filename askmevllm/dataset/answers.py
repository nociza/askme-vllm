from datetime import datetime
import logging
from typing import List

from vllm import SamplingParams
from askmevllm.models import Answer, Question, dataset
from askmevllm.dataset.common import generate_fact_with_context
from askmevllm.helpers import create_author_if_not_exists
from askmevllm.config import MODEL, TEMPERATURE


def generate_answers(questions: List[Question], setting: str, llm):
    try:
        answers = []
        prompts = []

        for question in questions:
            # Process prompt template
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
            author_id = create_author_if_not_exists(prompt, MODEL)
            sampling_params = SamplingParams(max_tokens=200, temperature=TEMPERATURE)

            # Collect all prompts to process in batch
            prompts.append((prompt, question.id, author_id))

        # Generate answers in batch
        batch_prompts = [p[0] for p in prompts]
        outputs = llm.generate(batch_prompts, sampling_params)

        for i, output in enumerate(outputs):
            answer_text = output.text.strip()
            if answer_text:
                prompt_data = prompts[i]
                answer = Answer(
                    id=len(dataset.answers) + 1,
                    question_id=prompt_data[1],
                    author_id=prompt_data[2],
                    setting=setting,
                    timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    text=answer_text,
                )
                logging.debug(f"Generated answer: {answer.text}")
                answers.append(answer)
            else:
                logging.error(
                    "Empty answer generated for question_id: {prompt_data[1]}"
                )

        return answers

    except Exception as e:
        logging.error(f"An error occurred at generate_answers: {e}")
        return None
