'''
Author: Pengzirong Peng.Zirong@outlook.com
Date: 2024-09-06 14:21:58
LastEditors: Pengzirong
LastEditTime: 2024-09-06 14:43:00
Description: file content
'''
from fetch_langfuse import FetchLangfuse


def process_llm_batch(llm_observations):
    """Process a batch of LLM observations.

    Args:
        llm_observations (list): A list of LLM observations.

    Returns:
        dict: A dictionary containing the processed evaluation batch.
    """
    evaluation_batch = {
        "question": [],
        "contexts": [],
        "answer": [],
        "trace_id": [],
        "observation_id": []
    }
    for observation in llm_observations:
        d = dict(observation)
        question = ""
        context = []
        for item in d["input"]:
            if item["role"] == "user":
                question = item["content"]
            elif item["role"] == "system":
                context.append(item["content"])

        evaluation_batch["question"].append(question)
        evaluation_batch["contexts"].append(context)

        if "output" in d and "text" in d["output"]:
            evaluation_batch["answer"].append(d["output"]["text"])
        evaluation_batch["trace_id"].append(observation["traceId"])
        evaluation_batch["observation_id"].append(observation["id"])
    
    return evaluation_batch
