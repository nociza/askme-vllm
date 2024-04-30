import logging
import random
import requests
import time
import together

from dotenv import dotenv_values

together.api_key = dotenv_values()["TOGETHER_API_KEY"]

WAIT = 0
MAX_RETRIES = 10
MAX_TOKEN = 512
TEMPERATURE = 0.7
TOP_P = 0.7
TOP_K = 50
REPETITION_PENALTY = 1.1


def randwait(wait, offset=0):
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
    service="gpublaze"
): 
    if service == "gpublaze":
        return gpublaze_safe_request(
            prompt,
            model,
            stop,
            max_tokens,
            temperature,
            top_p,
            top_k,
            repetition_penalty,
            max_retries,
            prompt_prefix,
            prompt_suffix,
        )
    elif service == "together":
        return together_safe_request(
            prompt,
            model,
            stop,
            max_tokens,
            temperature,
            top_p,
            top_k,
            repetition_penalty,
            max_retries,
            prompt_prefix,
            prompt_suffix,
        )
    else:
        raise Exception(f"Service {service} not supported")

def gpublaze_safe_request(
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
    # print out all the parameters
    # print(f"model: {model} \n stop: {stop} \n max_tokens: {max_tokens} \n temperature: {temperature} \n top_p: {top_p} \n top_k: {top_k} \n repetition_penalty: {repetition_penalty} \n max_retries: {max_retries} \n prompt_prefix: {prompt_prefix} \n prompt_suffix: {prompt_suffix}")
    try:
        if prompt_prefix:
            prompt = prompt_prefix + " " + prompt
        if prompt_suffix:
            prompt = prompt + " " + prompt_suffix
        res = requests.post(
            "http://gpublaze.ist.berkeley.edu:54321/v1/chat/completions",
            headers={
                "Content-Type": "application/json",
            },
            json={
                "model": model,
                "messages": [
                    {
                        "role": "user",
                        "content": prompt,
                    }
                ],
                "max_tokens": max_tokens,
                "temperature": temperature,
                "top_p": top_p,
                "top_k": top_k,
                "repetition_penalty": repetition_penalty,
                "stop": stop,
                "stream": False,
                "safe_prompt": False,
            },
        )
        res.raise_for_status()
        return res.json() 
    except requests.exceptions.HTTPError as e:
        logging.error(f"Error: {e}")
        if max_retries <= 0:
            raise Exception(
                f"Cannot get the response after max_attempts. \n Prompt: {prompt} \n response: {res.json()}"
            )
        time.sleep(randwait(WAIT))
        return gpublaze_safe_request(
            prompt,
            model,
            stop,
            max_tokens,
            temperature,
            top_p,
            top_k,
            repetition_penalty,
            max_retries - 1,
            prompt_prefix,
            prompt_suffix,
        )   

def together_safe_request(
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
        return together_safe_request(
            prompt,
            model,
            stop,
            max_tokens,
            temperature,
            top_p,
            top_k,
            repetition_penalty,
            max_retries - 1,
            prompt_prefix,
            prompt_suffix,
        )
    

def generate_prompts_from_template(template, variables):
    template_variables = {key: f"<{value.__class__.__name__}>" for key, value in variables.items() if key not in ['PROMPT_PREFIX', 'PROMPT_SUFFIX']}
    specific_prompt = template.format(**variables).strip()
    template_prompt = template.replace("{PROMPT_PREFIX}", "").replace("{PROMPT_SUFFIX}", "").format(**template_variables).strip()
    return specific_prompt, template_prompt
