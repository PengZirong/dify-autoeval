'''
Author: Pengzirong Peng.Zirong@outlook.com
Date: 2024-09-06 14:56:52
LastEditors: Pengzirong
LastEditTime: 2024-09-06 17:08:24
Description: file content
'''

from typing import List, Dict
import pandas as pd


class LLMmetrics:
    def __init__(self, evaluation_batch: Dict):
        self.evaluation_batch = evaluation_batch

    def compute_metrics(self, metrics: List):
        """Compute metrics using the specified metrics.

        Args:
            metrics (List): A list of metrics to compute.

        Returns:
            pd.DataFrame: A DataFrame containing the computed metrics.
        """
        dataset = pd.DataFrame(self.evaluation_batch)
        scores = pd.DataFrame()
        for metric in metrics:
            score = metric(dataset)
            scores = scores.append(score)
        scores["trace_id"] = self.evaluation_batch["trace_id"]
        scores["observation_id"] = self.evaluation_batch["observation_id"]
        return scores
    
    def ragas_metrics(self, ragas_metrics: List, llm=None, embedding_model=None):
        """Compute metrics using the specified ragas metrics.

        Args:
            ragas_metrics (List): A list of ragas metrics to compute.
            llm (type, optional): The llm parameter. Defaults to None.
            embedding_model (type, optional): The embedding model parameter. Defaults to None.

        Returns:
            pd.DataFrame: A DataFrame containing the computed metrics.
        """
        from ragas import evaluate
        ragas_dataset = pd.DataFrame(ragas_metrics)
        scores = evaluate(ragas_dataset, metrics=ragas_metrics,
                          llm=llm, embedding_model=embedding_model)
        scores.to_pandas()
        scores["trace_id"] = self.evaluation_batch["trace_id"]
        scores["observation_id"] = self.evaluation_batch["observation_id"]
        return scores
    
    def toy_metrics(self, toy_metrics: Dict):
        """Compute metrics for the toy evaluation batch.
        example:
        toy_metrics = {
            "toy_metric1": random.random,
            "toy_metric2": random.random,
            "toy_metric3": random.random,
            }

        Returns:
            pd.DataFrame: A DataFrame containing the computed metrics.
        """
        # Your code here to compute the metrics for the toy evaluation batch
        toy_dataset = pd.DataFrame(self.evaluation_batch)
        for metric in toy_metrics:
            toy_dataset[metric] = [toy_metrics[metric]() for _ in range(len(toy_dataset))]
        
        return toy_dataset
    
class RetrievalMetrics:
    def __init__(self, evaluation_batch: Dict):
        self.evaluation_batch = evaluation_batch
    
    def compute_metrics(self, metrics: List):
        """Compute metrics using the specified metrics.

        Args:
            metrics (List): A list of metrics to compute.

        Returns:
            pd.DataFrame: A DataFrame containing the computed metrics.
        """
        dataset = pd.DataFrame(self.evaluation_batch)
        scores = pd.DataFrame()
        for metric in metrics:
            score = metric(dataset)
            scores = scores.append(score)
        scores["trace_id"] = self.evaluation_batch["trace_id"]
        scores["observation_id"] = self.evaluation_batch["observation_id"]
        return scores
    
    def toy_metrics(self, toy_metrics: Dict):
        """Compute metrics for the toy evaluation batch.
        example:
        toy_metrics = {
            "toy_metric1": random.random,
            "toy_metric2": random.random,
            "toy_metric3": random.random,
            }

        Returns:
            pd.DataFrame: A DataFrame containing the computed metrics.
        """
        # Your code here to compute the metrics for the toy evaluation batch
        toy_dataset = pd.DataFrame(self.evaluation_batch)
        for metric in toy_metrics:
            toy_dataset[metric] = [toy_metrics[metric]() for _ in range(len(toy_dataset))]
        
        return toy_dataset