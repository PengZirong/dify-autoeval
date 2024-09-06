import os
import requests

class FetchLangfuse:
    """
    A class for fetching data from Langfuse API
    """
    def __init__(self, secret_key, public_key, host):
        """
        Initialize FetchLangfuse with secret key, public key, and host

        Args:
            secret_key (str): The secret key for authentication
            public_key (str): The public key for authentication
            host (str): The host URL for the Langfuse API
        """
        self.secret_key = secret_key
        self.public_key = public_key
        self.host = host
        # print(f"secret_key: {self.secret_key}")
        # print(f"public_key: {self.public_key}")
        # print(f"host: {self.host}")
        
    def fetch_sessions(self):
        """
        Fetch all sessions from Langfuse API

        Returns:
            dict: The JSON response containing the sessions
        """
        url = f"{self.host}/api/public/sessions"
        response = requests.get(url, auth=(self.public_key, self.secret_key))
        return response.json()

    def fetch_session(self, session_id):
        """
        Fetch a specific session from Langfuse API

        Args:
            session_id (str): The ID of the session

        Returns:
            dict: The JSON response containing the session
        """
        url = f"{self.host}/api/public/sessions/{session_id}"
        response = requests.get(url, auth=(self.public_key, self.secret_key))
        return response.json()
    
    def fetch_session_traces(self, session):
        """
        Fetch traces from a specific session

        Args:
            session (dict): The session object

        Returns:
            list: A list of traces
        """
        traces = session["traces"]
        return traces
    
    def fetch_trace(self, trace_id):
        """
        Fetch a specific trace from Langfuse API

        Args:
            trace_id (str): The ID of the trace

        Returns:
            dict: The JSON response containing the trace
        """
        url = f"{self.host}/api/public/traces/{trace_id}"
        response = requests.get(url, auth=(self.public_key, self.secret_key))
        return response.json()
    
    def fetch_observations(self, page: int = None, limit: int = None, name: str = None, userId: str = None, type: str = None, traceId: str = None, parentObservationId: str = None, fromStartTime: str = None, toStartTime: str = None, version: str = None):
        """
        Fetch observations from Langfuse API

        Args:
            page (int, optional): The page number. Defaults to None.
            limit (int, optional): The limit of observations per page. Defaults to None.
            name (str, optional): The name of the observation. Defaults to None.
            userId (str, optional): The ID of the user. Defaults to None.
            type (str, optional): The type of the observation. Defaults to None.
            traceId (str, optional): The ID of the trace. Defaults to None.
            parentObservationId (str, optional): The ID of the parent observation. Defaults to None.
            fromStartTime (str, optional): The start time range. Defaults to None.
            toStartTime (str, optional): The end time range. Defaults to None.
            version (str, optional): The version of the observation. Defaults to None.

        Returns:
            dict: The JSON response containing the observations
        """
        url = f"{self.host}/api/public/observations"
        params = {
            "page": page,
            "limit": limit,
            "name": name,
            "userId": userId,
            "type": type,
            "traceId": traceId,
            "parentObservationId": parentObservationId,
            "fromStartTime": fromStartTime,
            "toStartTime": toStartTime,
            "version": version
        }
        response = requests.get(url, auth=(self.public_key, self.secret_key), params=params)
        return response.json()
    
    def fetch_observation(self, observation_id):
        """
        Fetch a specific observation from Langfuse API

        Args:
            observation_id (str): The ID of the observation

        Returns:
            dict: The JSON response containing the observation
        """
        url = f"{self.host}/api/public/observations/{observation_id}"
        response = requests.get(url, auth=(self.public_key, self.secret_key))
        return response.json()
    
    def fetch_session_traces_idx(self, session):
        """
        Fetch trace indices from a specific session

        Args:
            session (dict): The session object

        Returns:
            list: A list of trace indices
        """
        traces = self.fetch_session_traces(session)
        traces_idx = []
        for trace in traces:
            traces_idx.append(
                {"id": trace["id"],
                 "name": trace["name"],
                 "tags": trace["tags"],
                }
                )
        return traces_idx
    
    def fetch_trace_observations(self, trace_id):
        """
        Fetch observations for a specific trace

        Args:
            trace_id (str): The ID of the trace

        Returns:
            list: A list of observations
        """
        trace = self.fetch_trace(trace_id)
        observations = trace["observations"]
        return observations
    
    def fetch_trace_observations_idx(self, trace):
        """
        Fetch observation indices from a specific trace

        Args:
            trace (dict): The trace object

        Returns:
            list: A list of observation indices
        """
        observations = trace["observations"]
        observations_idx = []
        for observation in observations:
            observations_idx.append(
                {"id": observation["id"],
                 "name": observation["name"],
                 "type": observation["type"],
                 "metadata": observation["metadata"],
                }
                )
        return observations_idx
    
    def select_ids(self, data, rules):
        """
        Select IDs based on rules

        Args:
            data (list): The data to filter
            rules (list): A list of rules to filter the data

        Returns:
            list: A list of selected IDs
        """
        selected_ids = []
        for item in data:
            if all(rule(item) for rule in rules):
                selected_ids.append((item['id'], data.index(item)))
        return selected_ids
    
    def fetch_node_observations(self, session_id, rules):
        """
        Fetch node observations based on rules from a specific session

        Args:
            session_id (str): The ID of the session
            rules (list): A list of rules to filter the observations

        Returns:
            list: A list of node observations
        """
        node_observations = []
        session = self.fetch_session(session_id)
        session_traces = self.fetch_session_traces(session)
        for session_trace in session_traces:
            trace = self.fetch_trace(session_trace["id"])
            trace_observations = trace["observations"]
            selected_ids = self.select_ids(trace_observations, rules)
            if selected_ids:
                for selected_id in selected_ids:
                    observation_id = selected_id[0]
                    observation = self.fetch_observation(observation_id)
                    node_observations.append(observation)
        return node_observations
    
    def fetch_trace_scores(self, trace_id):
        """
        Fetch scores for a specific trace

        Args:
            trace_id (str): The ID of the trace

        Returns:
            dict: The JSON response containing the scores
        """
        trace = self.fetch_trace(trace_id)
        scores = trace["scores"]
        return scores
    
    
    


