'''
Author: Pengzirong Peng.Zirong@outlook.com
Date: 2024-09-06 14:21:58
LastEditors: Pengzirong
LastEditTime: 2024-09-06 14:24:12
Description: file content
'''
from fetch_langfuse import FetchLangfuse

class Utils(FetchLangfuse):
    def __init__(self):
        super().__init__()

    def get_selected_observations(self, rules):
        all_data = []
        sessions = self.fetch_sessions()['data']
        for session in sessions:
            session_id = session['id']
            traces = self.fetch_session_traces(
                self.fetch_session(session_id)
            )
            for trace in traces:
                trace_id = trace['id']
                observations = self.fetch_trace_observations(trace_id)
                selected_ids = self.select_ids(observations, rules)
                if selected_ids:
                    for selected_id in selected_ids:
                        all_data.append(
                            self.fetch_observation(selected_id[0])
                        )
        return all_data

def get_selected_observations(fetch_langfuse, rules):
    all_data = []
    sessions = fetch_langfuse.fetch_sessions()['data']
    for session in sessions:
        session_id = session['id']
        traces = fetch_langfuse.fetch_session_traces(
            fetch_langfuse.fetch_session(session_id)
        )
        for trace in traces:
            trace_id = trace['id']
            observations = fetch_langfuse.fetch_trace_observations(trace_id)
            selected_ids = fetch_langfuse.select_ids(observations, rules)
            if selected_ids:
                for selected_id in selected_ids:
                    all_data.append(
                        fetch_langfuse.fetch_observation(selected_id[0])
                    )
    return all_data
