from datetime import datetime
import logging
import re
import time
import traceback
from typing import List

import torch
from vllm import SamplingParams
from askmevllm.models import Question, Paragraph, dataset
from askmevllm.dataset.common import generate_fact_with_context
from askmevllm.helpers import create_author_if_not_exists
from askmevllm.config import NUMQUESTIONS, TEMPERATURE


def generate_questions_single_turn(
    paragraphs: List[Paragraph], llm, k: int = NUMQUESTIONS
) -> List[List[Question]]:
    try:
        prompts = []
        for paragraph in paragraphs:
            prompt_template = "{PROMPT_PREFIX}Generate {NUM_QUESTIONS} short answer questions about the facts mentioned in the following paragraph. The questions should be self-contained; meaning you avoid using references such as 'it', 'the game', 'the person', etc., but should directly include the name of the referenced item instead. Remember to include relevant context in the question. Return a ordered list. \n\nParagraph: {PARAGRAPH}\n{PROMPT_SUFFIX}"
            context, fact = generate_fact_with_context(paragraph)
            prompt = prompt_template.format(
                PARAGRAPH=fact, PROMPT_PREFIX="", PROMPT_SUFFIX="", NUM_QUESTIONS=k
            )
            prompts.append(prompt)

        author_id = create_author_if_not_exists(prompts[0], "llama3-70B-instruct")

        logging.debug("Generating questions for paragraphs")

        sampling_params = SamplingParams(max_tokens=500, temperature=TEMPERATURE)
        outputs = llm.generate(prompts, sampling_params)
        all_question_objects = []

        for paragraph, output in zip(paragraphs, outputs):
            generated_text = output.outputs[0].text.strip()
            logging.debug(f"Generated questions: {generated_text}")

            new_questions = [
                re.sub(r"^\d\.", "", x).strip()
                for x in generated_text.split("\n")
                if re.match(r"^[0-9]\.", x)
            ]

            print(new_questions)

            question_objects = [
                Question(
                    id=len(dataset.questions) + 1,
                    paragraph_id=paragraph.id,
                    scope="single-paragraph",
                    text=q,
                    context=context,
                    author_id=author_id,
                    timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    upvote=0,
                    downvote=0,
                    turns="single",
                )
                for q in new_questions
            ]
            all_question_objects.extend(question_objects)

        return all_question_objects

    except Exception as e:
        logging.error(str(e))
        logging.error(traceback.format_exc())
        raise Exception("Error generating questions for paragraphs") from e


def filter_questions(questions: List[Question], llm) -> List[Question]:
    updated_questions = []

    questions_by_paragraph = {}
    for q in questions:
        if q.paragraph_id not in questions_by_paragraph:
            questions_by_paragraph[q.paragraph_id] = []
        questions_by_paragraph[q.paragraph_id].append(q)

    def check_questions_for_paragraph(paragraph_id, questions):
        paragraph = next(p for p in dataset.paragraphs if p.id == paragraph_id)
        context, fact = generate_fact_with_context(paragraph)

        for q in questions:
            logging.debug(f"Checking if answerable: {q.text}")
            q_is_answerable_ic = is_answerable_guided_choice(q.text, fact, llm)
            q_is_answerable_zs = is_answerable_guided_choice(q.text, "", llm)
            logging.debug(
                f"Answerable in IC: {q_is_answerable_ic}, Answerable in ZS: {q_is_answerable_zs}"
            )
            if not q_is_answerable_ic or not q_is_answerable_zs:
                q.rejected = True
                q.is_answerable_ic = q_is_answerable_ic
                q.is_answerable_zs = q_is_answerable_zs
            q.filtered = True
            updated_questions.append(q)

    for paragraph_id, qs in questions_by_paragraph.items():
        check_questions_for_paragraph(paragraph_id, qs)

    return updated_questions


def is_answerable(question, fact, llm):
    if not question.strip():
        logging.debug("No question seen in is_answerable: ", question.strip())
        return False

    if not fact:
        prompt = f"Is the following question: \n\n {question} \n\n a valid question without additional context? \n\n Reply 'YES' and 'NO' only."
    else:
        prompt = f"Is the following question: \n\n {question} \n\n answerable using only the following fact? \n\n Fact: {fact} \n\n Reply 'YES' and 'NO' only."

    sampling_params = SamplingParams(max_tokens=10, temperature=0.0)
    output = llm.generate([prompt], sampling_params)
    answer = output[0].text.strip()
    if answer == "NO":
        return False
    elif answer == "YES":
        return True
    logging.info("Question Malformed: ", answer)
    raise Exception("Question Malformed: ", answer)


def is_answerable_guided_choice(question, fact="", llm=None):
    if not question.strip():
        logging.debug("No question seen in is_answerable: ", question.strip())
        return False

    if not fact:
        prompt = f"Is the following question: \n\n {question} \n\n a valid question without additional context? \n\n Reply 'YES' and 'NO' only."
    else:
        prompt = f"Is the following question: \n\n {question} \n\n answerable using only the following fact? \n\n Fact: {fact} \n\n Reply 'YES' and 'NO' only."

    sampling_params = SamplingParams(max_tokens=10, temperature=0.0)
    output = llm.generate([prompt], sampling_params)

    logits = output[0].logits
    yes_token = llm.tokenizer.encode("YES")[0]
    no_token = llm.tokenizer.encode("NO")[0]

    yes_logit = logits[:, yes_token]
    no_logit = logits[:, no_token]

    yes_prob = torch.softmax(yes_logit, dim=-1).item()
    no_prob = torch.softmax(no_logit, dim=-1).item()

    logging.debug(f"YES probability: {yes_prob}, NO probability: {no_prob}")

    if yes_prob > no_prob:
        return True
    else:
        return False
