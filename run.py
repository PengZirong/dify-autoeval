'''
Author: Pengzirong Peng.Zirong@outlook.com
Date: 2024-09-11 09:10:02
LastEditors: Pengzirong
LastEditTime: 2024-09-12 09:22:01
Description: file content
'''

import pandas as pd
import os
from langfuse import Langfuse
from async_langfuse import FetchLangfuse
import asyncio
from datetime import datetime
import yaml
import aiohttp
import json

from utils import send_chat_message, process_llm_batch
from rules import Rules

from datasets import Dataset 
from ragas import evaluate
from ragas.metrics import (
    answer_correctness,
    answer_relevancy,
    context_precision,
    context_recall,
    context_utilization,
    faithfulness,
)

############################################
# step 0: load config and init langfuse
with open('config.yaml', 'r') as file:
    config = yaml.load(file, Loader=yaml.FullLoader)

for key in config:
    os.environ[key] = config[key]
    print(f'{key}={config[key]}')

langfuse = Langfuse()
semaphore = asyncio.Semaphore(1)  # 控制并发任务数量
evaluation_lock = asyncio.Lock()


fetch_langfuse = FetchLangfuse(
        secret_key=os.getenv('LANGFUSE_SECRET_KEY'),
        public_key=os.getenv('LANGFUSE_PUBLIC_KEY'),
        host=os.getenv('LANGFUSE_HOST')
    )

############################################
# step 1: upload dataset to langfuse

def upload_dataset_to_langfuse():
    df = pd.read_csv('../MedEval/妇产科6-28000.csv', encoding='GB18030')
    df = df[df['department']=='妇产科']
    df = df.tail(100)
    ds_name_in_langfuse = "OAGD_妇产科"

    langfuse.create_dataset(
        name=ds_name_in_langfuse,
        # optional description
        description="My first dataset",
        # optional metadata
        metadata={
            "author": "Peng Zirong",
            "date": f"{datetime.now()}",
            "type": "benchmark"
        }
    )
    for i in tqdm(range(df.shape[0])):
        data = df.iloc[i]
        data = {
            "input": {
                "department": data['department'],
                "title": data['title'],
                "ask": data['ask']
            },
            "output": data['answer']
        }
        langfuse.create_dataset_item(
            dataset_name=ds_name_in_langfuse,
            input=data["input"],
            expected_output=data["output"]
        )


############################################
# step 2: load dataset from langfuse
# dataset = langfuse.get_dataset("OAGD_妇产科")


############################################
# step 3: send data to dify
# def send_data_to_dify():
#     dataset = langfuse.get_dataset("OAGD_妇产科")
#     item = dataset.items[0]
#     query = item.input['ask'] if item.input['ask'] != '无' else item.input['title']
#     responce = send_chat_message(url=config["DIFY_HOST"], 
#                                 api_key=config["DIFY_API_KEY"], 
#                                 query=query)
    

############################################
# step 4: link trace to data
# def link_trace_to_data():
#     session_id = responce['conversation_id']
#     trace_id = responce['message_id']
#     observation_id = ""
#     run_name = "run_name"
#     item.link(trace_or_observation=None, # 已弃用，但这个参数去掉会报错
#             run_name=run_name,
#             trace_id=trace_id,
#             observation_id=observation_id)

############################################
# step 5: process and evaluate data
async def process_dataset(
    dataset,
    run_name, 
    ragas_metrics, 
    ragas_llm, 
    ragas_embedding):
    tasks = []
    results = []
    for item in dataset.items[:1]:
        task = asyncio.create_task(
            process_item(
            item,
            run_name,
            ragas_metrics, 
            ragas_llm, 
            ragas_embedding))
        tasks.append(task)

    results = await asyncio.gather(*tasks)
    
    # 将所有 observations 和 expected_outputs 合并到两个列表中
    all_observations = []
    all_expected_outputs = []
    for observations, expected_outputs in results:
        all_observations.extend(observations)
        all_expected_outputs.extend(expected_outputs)
    
    return all_observations, all_expected_outputs



def ragas_evaluation(observations, expected_output, metrics, llm, embedding):
    # async with semaphore: # 控制并发任务数量
        # async with evaluation_lock:
    batch = process_llm_batch(observations)
    batch['ground_truth'] = expected_output
    batch_keys = batch.keys()
    batch = Dataset.from_dict(batch)
    scores = evaluate(batch, metrics=metrics, llm=llm, embeddings=embedding)
    scores['trace_id'] = batch['trace_id']
    scores['observation_id'] = batch['observation_id']
    score_keys = [key for key in scores.keys() if key not in batch_keys]
    return scores.to_pandas(), score_keys


async def run_dify_app(query):
    while True:
        try:
            response = await send_chat_message(
                url=os.getenv("DIFY_API_BASE"), 
                api_key=os.getenv("DIFY_API_KEY"), 
                query=query,
                user="autoeval_dev")
            session_id = response['conversation_id']
            trace_id = response['message_id']
            print(f"trace_id: {trace_id}")
            return session_id, trace_id
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            print("Retrying after 10 seconds...")
            await asyncio.sleep(10)

async def process_item(
    item,
    run_name,
    ragas_metrics, 
    ragas_llm, 
    ragas_embedding
    ):
    query = item.input['ask']+'\n'+item.input['title'] if item.input['ask'] != '无' else item.input['title']
    expected_output = item.expected_output
    
    session_id, trace_id = await run_dify_app(query)
    
    rules = Rules().llm_rules
    
    await asyncio.sleep(10)
    
    # max_retries = 5
    retry_delay = 10
    # observations = []
    
    while True:
        try:
            observations = await fetch_langfuse.get_trace_selected_observations(trace_id, rules)
            break
        except Exception as e:
            # if attempt < max_retries - 1:
            print(f"An error occurred: {str(e)}")
                # print(f"Retrying in {retry_delay} seconds... (Attempt {attempt + 1}/{max_retries})")
            await asyncio.sleep(retry_delay)
            # else:
            #     print(f"Failed to fetch observations after {max_retries} attempts. Skipping this item.")
            #     return
            
    # 在处理完所有 observations 后再调用 ragas_evaluation 进行批量处理
    for observation in observations:
        trace_id = observation['traceId']
        observation_id = observation['id']

        item.link(
            trace_or_observation=None,
            run_name=run_name,
            trace_id=trace_id,
            observation_id=observation_id
        )
    return observations, [expected_output] * len(observations)

async def process_eval(
    observations, 
    expected_outputs, 
    ragas_metrics, 
    ragas_llm, 
    ragas_embedding):
    scores, score_keys = ragas_evaluation(
        observations, expected_outputs, ragas_metrics, ragas_llm, ragas_embedding
    )
    print(f"Scores type: {type(scores)}")
    for _, row in scores.iterrows():
        trace_id = row['trace_id']
        observation_id = row['observation_id']
        for evaluation_key in score_keys:
            score = row[evaluation_key]
            await fetch_langfuse.pull_score_to_langfuse(
                score=score,
                trace_id=trace_id,
                observation_id=observation_id,
                name=evaluation_key
            )

        
async def main():
    from test_llm2 import llm, embedding
    
    dataset = langfuse.get_dataset("OAGD_妇产科")
    run_name = "dify_app 2"
    ragas_metrics = [
        answer_correctness,
        # answer_relevancy,
        # context_precision,
        # context_recall,
        # context_utilization,
        faithfulness,
    ]
    observations, expected_outputs = await process_dataset(
        dataset, run_name, 
        ragas_metrics, ragas_llm=llm, ragas_embedding=embedding)
    await process_eval(observations, expected_outputs, ragas_metrics, llm, embedding)
    # Flush the langfuse client to ensure all data is sent to the server at the end of the experiment run
    langfuse.flush()

if __name__ == "__main__":
    asyncio.run(main())

