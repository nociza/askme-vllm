import random
import requests
import time
import together

from dotenv import dotenv_values

together.api_key = dotenv_values()["TOGETHER_API_KEY"]

WAIT = 0.5
MAX_RETRIES = 5
MAX_TOKEN = 512
TEMPERATURE = 0.7
TOP_P = 0.7
TOP_K = 50
REPETITION_PENALTY = 1.1


def randwait(wait, offset=0.1):
    return random.random() * wait + offset


def llm_safe_request(
    prompt,
    model,
    stop,
    max_tokens=MAX_TOKEN,
    temperature=TEMPERATURE,
    top_p=TOP_P,
    top_k=TOP_K,
    repetition_penalty=REPETITION_PENALTY,
    max_retries=MAX_RETRIES,
    prompt_prefix="",
    prompt_suffix="",
):
    try:
        if prompt_prefix:
            prompt = prompt_prefix + " " + prompt
        if prompt_suffix:
            prompt = prompt + " " + prompt_suffix
        return together.Complete.create(
            prompt=prompt,
            model=model,
            max_tokens=max_tokens,
            temperature=temperature,
            top_p=top_p,
            top_k=top_k,
            repetition_penalty=repetition_penalty,
            stop=stop,
        )
    except requests.exceptions.HTTPError:
        if max_retries <= 0:
            raise Exception(
                f"Cannot get the response after max_attempts. \n Prompt: ", prompt
            )
        time.sleep(randwait(WAIT))
        return llm_safe_request(
            prompt,
            model,
            max_tokens,
            temperature,
            top_p,
            top_k,
            repetition_penalty,
            stop,
            max_retries - 1,
        )
