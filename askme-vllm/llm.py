import random
from vllm import SamplingParams, LLM
from .config import MAX_TOKEN, TEMPERATURE, SEED, MODEL


sampling_params = SamplingParams(
    max_tokens=MAX_TOKEN,
    temperature=TEMPERATURE,
    seed=SEED,
)

llm = LLM(MODEL)


def randwait(wait, offset=0):
    return random.random() * wait + offset


def generate_prompts_from_template(template, variables):
    template_variables = {
        key: f"<{value.__class__.__name__}>"
        for key, value in variables.items()
        if key not in ["PROMPT_PREFIX", "PROMPT_SUFFIX"]
    }
    specific_prompt = template.format(**variables).strip()
    template_prompt = (
        template.replace("{PROMPT_PREFIX}", "")
        .replace("{PROMPT_SUFFIX}", "")
        .format(**template_variables)
        .strip()
    )
    return specific_prompt, template_prompt
