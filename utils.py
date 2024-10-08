'''
Author: Pengzirong Peng.Zirong@outlook.com
Date: 2024-09-06 14:21:58
LastEditors: Pengzirong
LastEditTime: 2024-09-12 15:29:30
Description: file content
'''
# from fetch_langfuse import FetchLangfuse
import aiohttp
import os

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

def process_retrieval_batch(retrieval_observations):
    """Process a batch of retrieval observations.

    Args:
        retrieval_observations (list): A list of retrieval observations.

    Returns:
        dict: A dictionary containing the processed evaluation batch.
    """
    evaluation_batch = {
        "query": [],
        "retrieval result": [],
        "trace_id": [],
        "observation_id": []
    }
    for observation in retrieval_observations:
        d = dict(observation)
        query = d["input"]["query"]
        retrieval_result = {"title": [], "content": []}

        evaluation_batch["query"].append(query)
        if "output" in d:
            for item in d["output"]["result"]:
                if "title" in item and "content" in item:
                    retrieval_result["title"].append(item["title"])
                    retrieval_result["content"].append(item["content"])
        evaluation_batch["retrieval result"].append(retrieval_result)
        evaluation_batch["trace_id"].append(observation["traceId"])
        evaluation_batch["observation_id"].append(observation["id"])
    
    return evaluation_batch

def pull_scores_to_langfuse(langfuse, scores, scores_keys, node_name=None):
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
            name = f"{key}-{node_name}" if node_name else key
            score = row[key]
            langfuse.score(
                id=f"{trace_id}-{observation_id}-{name}",
                trace_id = trace_id,
                observation_id=observation_id,
                name=name,
                value=score,
                comment=f"Last updated at {datetime.now().strftime('%Y/%m/%d %H:%M:%S')}"
            )
            
def pull_score_to_langfuse(langfuse, score, trace_id, observation_id, name):
    """Pull a single score to Langfuse.

    Args:
        langfuse (Langfuse): The Langfuse object.
        score (float): The score.
        trace_id (str): The trace ID.
        observation_id (str): The observation ID.
        name (str): The name of the score.

    """
    from datetime import datetime
    langfuse.score(
        id=f"{trace_id}-{observation_id}-{name}",
        trace_id = trace_id,
        observation_id=observation_id,
        name=name,
        value=score,
        comment=f"Last updated at {datetime.now().strftime('%Y/%m/%d %H:%M:%S')}"
    )

async def async_pull_score_to_langfuse(score, trace_id, observation_id, name):
    """Pull a single score to Langfuse asynchronously.

    Args:
        score (float): The score.
        trace_id (str): The trace ID.
        observation_id (str): The observation ID.
        name (str): The name of the score.

    """
    from datetime import datetime
    url = f"{os.getenv('LANGFUSE_HOST')}/api/score"
    screct_key = os.getenv('LANGFUSE_SECRET_KEY')
    public_key = os.getenv('LANGFUSE_PUBLIC_KEY')
    headers = {
        "Content-Type": "application/json"
    }
    payload = {
        "id": f"{trace_id}-{observation_id}-{name}",
        "traceId": trace_id,
        "name": name,
        "value": score,
        "observationId": observation_id,
        "comment": f"Last updated at {datetime.now().strftime('%Y/%m/%d %H:%M:%S')}",
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(url, auth=aiohttp.BasicAuth(public_key, screct_key), headers=headers, data=json.dumps(payload)) as response:
            return await response.text()
    

async def send_chat_message(
    url,
    api_key,
    query: str,
    inputs={},
    response_mode: ["streaming", "blocking"] = "blocking",
    user: str = "abc-123",
    file_array = []
    ):
    """Send a chat message.

    Args:
        url (str): The URL of the chat message API.
        api_key (str): The API key for authentication.
        query (str): The chat message query.
        inputs (dict, optional): Additional inputs for the chat message. Defaults to {}.
        response_mode (str, optional): The response mode for the chat message. Defaults to "blocking".
        user (str, optional): The user identifier. Defaults to "abc-123".
        file_array (list, optional): An array of files to be sent with the chat message. Defaults to [].

    Returns:
        dict: The response from the chat message API.
    """
    base_url = f"{url}/chat-messages"
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json'
    }
    payload = {
        "inputs": inputs,
        "query": query,
        "response_mode": response_mode,
        "conversation_id": "",
        "user": user,
        "files": file_array
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(base_url, headers=headers, json=payload) as response:
            return await response.json()

def get_ragas_llm_and_embeddings():
    """Get Ragas LLM and Embeddings.

    Returns:
        tuple: A tuple containing the LLM and embeddings objects.
    """
    from ragas.llms import LangchainLLMWrapper
    from ragas.embeddings import LangchainEmbeddingsWrapper
    from langchain_openai.chat_models import ChatOpenAI
    from langchain_openai.embeddings import OpenAIEmbeddings
    llm = LangchainLLMWrapper(
        ChatOpenAI(
            model=os.getenv("RAGAS_CRITIC_LLM"),
            openai_api_base=os.getenv("RAGAS_BASE_URL"),
            openai_api_key=os.getenv("RAGAS_API_KEY"),
            temperature=0,
            max_tokens=None,
            timeout=None,
            max_retries=2,
        )
    )
    embeddings = LangchainEmbeddingsWrapper(
        OpenAIEmbeddings(
            model=os.getenv("RAGAS_EMBEDDING"),
            base_url=os.getenv("RAGAS_BASE_URL"),
            api_key=os.getenv("RAGAS_API_KEY"),
        )
    )
    return llm, embeddings