'''
Author: Pengzirong Peng.Zirong@outlook.com
Date: 2024-09-06 14:21:58
LastEditors: Pengzirong
LastEditTime: 2024-09-06 16:30:51
Description: file content
'''
# from fetch_langfuse import FetchLangfuse


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

def pull_scores_to_langfuse(langfuse, scores, scores_keys):
    """Pull scores to Langfuse.

    Args:
        langfuse (Langfuse): The Langfuse object.
        scores (dict): The scores dictionary.
        scores_keys (list): The list of keys for evaluation.

    """
    from datetime import datetime
    for _, row in scores.iterrows():
        for key in scores_keys:
            trace_id = row['trace_id']
            observation_id = row['observation_id']
            name = key
            score = row[key]
            langfuse.score(
                id=f"{trace_id}-{observation_id}-{name}",
                trace_id = trace_id,
                observation_id=observation_id,
                name=name,
                value=score,
                comment=f"Last updated at {datetime.now().strftime('%Y/%m/%d %H:%M:%S')}"
            )
