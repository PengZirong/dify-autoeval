'''
Author: Pengzirong Peng.Zirong@outlook.com
Date: 2024-09-12 16:59:59
LastEditors: Pengzirong
LastEditTime: 2024-09-12 18:13:45
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
from tqdm import tqdm
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

class DatasetProcessor:
    def __init__(self, config):
        self.config = config
        
        for key in config:
            os.environ[key] = config[key]
            print(f'{key}={config[key]}')
        
        self.langfuse = Langfuse()
        self.fech_langfuse = FetchLangfuse()
        
    def upload_dataset_to_langfuse(
        self,
        file_path,
        ds_name_in_langfuse,
        encoding='utf-8',
        description=None,
        metadata=None,
        input_columns=None,
        output_columns=None,
        sample_size=-1,
        ):

        # Determine file extension and read accordingly
        file_extension = os.path.splitext(file_path)[1]
        if file_extension == '.csv':
            df = pd.read_csv(file_path, encoding=encoding)
        elif file_extension == '.xlsx':
            df = pd.read_excel(file_path, encoding=encoding)
        elif file_extension == '.txt':
            df = pd.read_csv(file_path, delimiter='\t', encoding=encoding)
        elif file_extension == '.json':
            df = pd.read_json(file_path, encoding=encoding)
        else:
            raise ValueError(f"Unsupported file format: {file_extension}")
        
        df = df.tail(sample_size)
        if input_columns is None:
            input_columns = df.keys()[:-2]
            # print(f"Input columns: {input_columns}")
        if output_columns is None:
            output_columns = [df.keys()[-1]]
            # print(f"Output columns: {output_columns}")
            
        print(f"Input columns: {input_columns}")
        print(f"Output columns: {output_columns}")
        
        if not input_columns and not output_columns:
            raise ValueError("Input columns and output columns are empty.")
            
        # Create dataset in Langfuse if it doesn't already exist
        try:
            self.langfuse.get_dataset(ds_name_in_langfuse)
            print(f"Dataset {ds_name_in_langfuse} already exists in Langfuse.")
            return
        except Exception as e:
            print(f"Creating dataset in Langfuse: {e}")
            self.langfuse.create_dataset(
                name=ds_name_in_langfuse,
                description=description,
                metadata=metadata
            )

        # Process each row and upload to Langfuse
        for i in tqdm(range(df.shape[0])):
            data = df.iloc[i].to_dict()
            
            input_data = {key: data[key] for key in input_columns if key in data}
            output_data = {key: data[key] for key in output_columns if key in data} if output_columns else None

            if len(input_columns) == 1:
                input_data = data.get(input_columns[0], None)
            if len(output_columns) == 1:
                output_data = data.get(output_columns[0], None)

            if not input_data or (output_columns and not output_data):
                print(f"Missing data in row {i}. Skipping row {i}")
                continue
            
            # print(f"Input data: {input_data}")
            # print(f"Output data: {output_data}")
            
            try:
                self.langfuse.create_dataset_item(
                    dataset_name=ds_name_in_langfuse,
                    input=input_data,
                    expected_output=output_data
                )
            except Exception as e:
                print(f"Error creating dataset item in Langfuse for row {i}: {e}")
                

if __name__ == "__main__":
    import yaml
    with open('config.yaml', 'r') as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
    
    dataset_processor = DatasetProcessor(config)
    dataset_processor.upload_dataset_to_langfuse(
        file_path='../MedEval/妇产科6-28000.csv',
        encoding='GB18030',
        sample_size=2,
        ds_name_in_langfuse="OAGD_妇产科",
        metadata={
            "author": "Peng Zirong",
            "date": f"{datetime.now()}",
            "type": "benchmark"
        },
        input_columns=['department', 'title', 'ask'],
        output_columns=['answer'],
    )