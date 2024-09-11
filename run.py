'''
Author: Pengzirong Peng.Zirong@outlook.com
Date: 2024-09-11 09:10:02
LastEditors: Pengzirong
LastEditTime: 2024-09-11 10:03:38
Description: file content
'''

import pandas as pd
import os
from langfuse import Langfuse
import yaml

with open('config.yaml', 'r') as file:
    config = yaml.load(file, Loader=yaml.FullLoader)

for key in config:
    os.environ[key] = config[key]
    print(f'{key}={config[key]}')

langfuse = Langfuse(
  secret_key=os.getenv('LANGFUSE_SECRET_KEY'),
  public_key=os.getenv('LANGFUSE_PUBLIC_KEY'),
  host=os.getenv('LANGFUSE_HOST')
)


# step 1: upload dataset to langfuse
from dataset import upload_data_to_langfuse

df = pd.read_csv('../MedEval/妇产科6-28000.csv', encoding='GB18030')
df = df[df['department']=='妇产科']
df = df.tail(100)
for i in tqdm(range(test_data.shape[0])):
    data = test_data.iloc[i]
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

# step 2: load dataset from langfuse
dataset = langfuse.get_dataset("OAGD_妇产科")

# step 3: send data to dify
from utils import send_chat_message
item = dataset.items[0]
query = item.input['ask'] if item.input['ask'] != '无' else item.input['title']
responce = send_chat_message(url=config["DIFY_HOST"], 
                             api_key=config["DIFY_API_KEY"], 
                             query=query)

# step 4: link trace to data
session_id = responce['conversation_id']
trace_id = responce['message_id']
observation_id = ""
run_name = "run_name"
item.link(trace_or_observation=None, # 已弃用，但这个参数去掉会报错
          run_name=run_name,
          trace_id=trace_id,
          observation_id=observation_id)

# step 5: process and evaluate data
async def process_dataset(dataset):

    tasks = []
    for item in dataset.items:
        task = asyncio.create_task(process_item(item))
        tasks.append(task)
    
    await asyncio.gather(*tasks)

