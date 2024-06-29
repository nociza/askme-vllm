from fleecekmbackend.db.models import (
    Paragraph,
)

WAIT = 0.1
MODEL = "meta-llama/Meta-Llama-3-70B-Instruct"
STOP = ["[/INST]", "</s>"]
PROMPT_PREFIX, PROMPT_SUFFIX = [
    "<|begin_of_text|><|start_header_id|>user<|end_header_id|>\n\n",
    "<|eot_id|><|start_header_id|>assistant<|end_header_id|>\n\n",
]

NUMQUESTIONS = 4
MAX_ATTEMPTS = 5


def generate_fact_with_context(paragraph: Paragraph):
    if paragraph.subsubsection_name and paragraph.subsection_name:
        context = f"In an article about '{paragraph.page_name}', section '{paragraph.section_name}', subsection '{paragraph.subsection_name}', paragraph '{paragraph.subsubsection_name}'"
    elif paragraph.subsection_name:
        context = f"In an article about '{paragraph.page_name}', section '{paragraph.section_name}', subsection '{paragraph.subsection_name}'"
    else:
        context = f"In an article about '{paragraph.page_name}', section '{paragraph.section_name}'"
    return context, f"{context} mentioned: \n {paragraph.text_cleaned}"
