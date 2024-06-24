import logging

DATABASE_URL = "mysql+aiomysql://root:fleecekm@localhost:13306/benchmark"
DATASET_PATH = "data/wiki_text_cleaned_v1.csv"
WAIT = 0.1
MODEL = "meta-llama/Meta-Llama-3-70B-Instruct"
STOP = ["[/INST]", "</s>"]
PROMPT_PREFIX, PROMPT_SUFFIX = [
    "<|begin_of_text|><|start_header_id|>user<|end_header_id|>\n\n",
    "<|eot_id|><|start_header_id|>assistant<|end_header_id|>\n\n",
]
NUMQUESTIONS = 4
MAX_ATTEMPTS = 5
LOGGING_LEVEL = logging.INFO
