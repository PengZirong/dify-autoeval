import aiohttp
import json
import os
class FetchLangfuse:
    """
    A class for fetching data from Langfuse API
    """
    def __init__(self, secret_key=None, public_key=None, host=None):
        """
        Initialize FetchLangfuse with secret key, public key, and host

        Args:
            secret_key (str): The secret key for authentication
            public_key (str): The public key for authentication
            host (str): The host URL for the Langfuse API
        """
        self.secret_key = secret_key or os.getenv('LANGFUSE_SECRET_KEY')
        self.public_key = public_key or os.getenv('LANGFUSE_PUBLIC_KEY')
        self.host = host or os.getenv('LANGFUSE_HOST')

    async def fetch_sessions(self):
        """
        Fetch all sessions from Langfuse API

        Returns:
            dict: The JSON response containing the sessions
        """
        url = f"{self.host}/api/public/sessions"
        async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(self.public_key, self.secret_key)) as session:
            async with session.get(url) as response:
                return await response.json()

    async def fetch_session(self, session_id):
        """
        Fetch a specific session from Langfuse API

        Args:
            session_id (str): The ID of the session

        Returns:
            dict: The JSON response containing the session
        """
        url = f"{self.host}/api/public/sessions/{session_id}"
        async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(self.public_key, self.secret_key)) as session:
            async with session.get(url) as response:
                return await response.json()

    async def fetch_trace(self, trace_id):
        """
        Fetch a specific trace from Langfuse API

        Args:
            trace_id (str): The ID of the trace

        Returns:
            dict: The JSON response containing the trace
        """
        url = f"{self.host}/api/public/traces/{trace_id}"
        async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(self.public_key, self.secret_key)) as session:
            async with session.get(url) as response:
                return await response.json()

    async def fetch_observations(self, page: int = None, limit: int = None, name: str = None, userId: str = None, type: str = None, traceId: str = None, parentObservationId: str = None, fromStartTime: str = None, toStartTime: str = None, version: str = None):
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
        async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(self.public_key, self.secret_key)) as session:
            async with session.get(url, params=params) as response:
                return await response.json()

    async def fetch_observation(self, observation_id):
        """
        Fetch a specific observation from Langfuse API

        Args:
            observation_id (str): The ID of the observation

        Returns:
            dict: The JSON response containing the observation
        """
        url = f"{self.host}/api/public/observations/{observation_id}"
        async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(self.public_key, self.secret_key)) as session:
            async with session.get(url) as response:
                return await response.json()

    async def fetch_node_observations(self, session_id, rules):
        """
        Fetch node observations based on rules from a specific session

        Args:
            session_id (str): The ID of the session
            rules (list): A list of rules to filter the observations

        Returns:
            list: A list of node observations
        """
        node_observations = []
        session = await self.fetch_session(session_id)
        session_traces = await self.fetch_session_traces(session)
        for session_trace in session_traces:
            trace = await self.fetch_trace(session_trace["id"])
            trace_observations = trace["observations"]
            selected_ids = self.select_ids(trace_observations, rules)
            if selected_ids:
                for selected_id in selected_ids:
                    observation_id = selected_id[0]
                    observation = await self.fetch_observation(observation_id)
                    node_observations.append(observation)
        return node_observations

    async def fetch_session_traces(self, session):
        """
        Fetch traces from a specific session

        Args:
            session (dict): The session object

        Returns:
            list: A list of traces
        """
        traces = session["traces"]
        return traces

    async def fetch_trace_observations(self, trace_id):
        """
        Fetch observations for a specific trace

        Args:
            trace_id (str): The ID of the trace

        Returns:
            list: A list of observations
        """
        trace = await self.fetch_trace(trace_id)
        observations = trace["observations"]
        return observations

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
    
    def select_data(self, data, rules):
        """
        Select data based on rules

        Args:
            data (list): The data to filter
            rules (list): A list of rules to filter the data

        Returns:
            list: A list of selected data
        """
        selected_data = []
        for item in data:
            if all(rule(item) for rule in rules):
                selected_data.append(item)
        return selected_data

    async def get_selected_observations(self, rules):
        """
        Fetches selected observations based on given rules.

        Args:
            rules (list): A list of rules to filter the observations

        Returns:
            list: A list of selected observations
        """
        selected_observations = []
        sessions = await self.fetch_sessions()
        for session in sessions['data']:
            session_id = session['id']
            traces = await self.fetch_session_traces(await self.fetch_session(session_id))
            for trace in traces:
                trace_id = trace['id']
                observations = await self.fetch_trace_observations(trace_id)
                selected_ids = self.select_ids(observations, rules)
                if selected_ids:
                    selected_observations.extend([await self.fetch_observation(selected_id[0]) for selected_id in selected_ids])
        return selected_observations

    async def get_sessions_selected_observations(self, sessions_ids, rules):
        """
        Fetches selected observations based on given rules from specific sessions.

        Args:
            sessions_ids (list): A list of session IDs
            rules (list): A list of rules to filter the observations

        Returns:
            list: A list of selected observations
        """
        selected_observations = []
        for session_id in sessions_ids:
            traces = await self.fetch_session_traces(await self.fetch_session(session_id))
            for trace in traces:
                trace_id = trace['id']
                observations = await self.fetch_trace_observations(trace_id)
                selected_ids = self.select_ids(observations, rules)
                if selected_ids:
                    selected_observations.extend([await self.fetch_observation(selected_id[0]) for selected_id in selected_ids])
        return selected_observations
    
    async def get_trace_selected_observations(self, trace_id, rules):
        """
        Fetches selected observations based on given rules from a specific trace.

        Args:
            trace_id (str): The ID of the trace
            rules (list): A list of rules to filter the observations

        Returns:
            list: A list of selected observations
        """
        observations = await self.fetch_trace_observations(trace_id)
        return self.select_data(observations, rules)
    
    async def pull_score_to_langfuse(self, score, trace_id, observation_id, name):
        """
        Pull a single score to Langfuse asynchronously.

        Args:
            score (float): The score.
            trace_id (str): The trace ID.
            observation_id (str): The observation ID.
            name (str): The name of the score.

        """
        from datetime import datetime
        url = f"{self.host}/api/public/scores"
        headers = {
            "Content-Type": "application/json"
        }
        payload = {
            "id": f"{trace_id}-{observation_id}-{name}" if observation_id else f"{trace_id}-{name}",
            "traceId": trace_id,
            "name": name,
            "value": score,
            "observationId": observation_id,
            "comment": f"Last updated at {datetime.now().strftime('%Y/%m/%d %H:%M:%S')}",
        }
        if observation_id is None:
            del payload["observationId"]
        async with aiohttp.ClientSession() as session:
            async with session.post(url, auth=aiohttp.BasicAuth(self.public_key, self.secret_key), headers=headers, data=json.dumps(payload)) as response:
                return await response.text()
        

