'''
Author: Pengzirong Peng.Zirong@outlook.com
Date: 2024-09-05 15:14:02
LastEditors: Pengzirong
LastEditTime: 2024-09-05 15:16:40
Description: file content
'''

class Rules:
    def __init__(self):
        self.knowledge_retrieval_rules = [
            lambda item: item['name'] == 'knowledge-retrieval',
            lambda item: item['type'] == 'SPAN',
            lambda item: item['metadata']['node_name'] == '知识检索',
            lambda item: item['metadata']['node_type'] == 'knowledge-retrieval',
        ]
        self.llm_rules = [
            lambda item: item['name'] == 'llm',
            lambda item: item['type'] == 'SPAN',
            lambda item: item['metadata']['node_name'] == 'LLM',
            lambda item: item['metadata']['node_type'] == 'llm',
        ]
