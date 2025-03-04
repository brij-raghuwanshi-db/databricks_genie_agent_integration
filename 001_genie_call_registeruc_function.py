# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION brij_catalog.genie_agent_integration._genie_query(
# MAGIC   question string
# MAGIC ) RETURNS STRING LANGUAGE PYTHON
# MAGIC COMMENT 'This is a agent that you can converse with to get answers to questions. Try to provide simple questions and provide history if you had prior conversations.' AS $$
# MAGIC     import json
# MAGIC     import os
# MAGIC     import time
# MAGIC     from dataclasses import dataclass
# MAGIC     from datetime import datetime
# MAGIC     import pandas as pd
# MAGIC     import requests
# MAGIC     from typing import Dict, Any, Optional
# MAGIC     from dataclasses import dataclass
# MAGIC
# MAGIC     @dataclass
# MAGIC     class DatabricksGenieTool():
# MAGIC         databricks_host: str = "https://YourCloudWorkspace.cloud.databricks.com"
# MAGIC         api_token: str = dbutils.secrets.get('brij_scope', 'brij_key')
# MAGIC         headers = {
# MAGIC         "Authorization": f"Bearer {api_token}",
# MAGIC         "Content-Type": "application/json",
# MAGIC         }
# MAGIC         base_url: str = f"{databricks_host}/api/2.0/genie/spaces"
# MAGIC         space_id: str = "01ef68cc62d715ebb1aee542871cd9da"
# MAGIC         conversation_id: str = "01ef75fe32f81046a77b2a04beaaeae7"
# MAGIC         genie_space_url: str = f"{base_url}/{space_id}"
# MAGIC         convo_url: str = f"conversations/{conversation_id}/messages"
# MAGIC
# MAGIC         def _create_conversation(self, payload: Dict[str, Any]) -> Dict[str, Any]:
# MAGIC             url      = f"{self.genie_space_url}/start-conversation"
# MAGIC             response = requests.post(url, headers=self.headers, json=payload)
# MAGIC             return response.json()
# MAGIC         
# MAGIC         def _followup_conversation(self, payload: Dict[str, Any]) -> Dict[str, Any]:
# MAGIC             url      = f"{self.genie_space_url}/{self.convo_url}"
# MAGIC             response = requests.post(url, headers=self.headers, data=json.dumps(payload))
# MAGIC             return response.json()
# MAGIC         
# MAGIC         def _get_query_result(self, message_id: str) -> Dict[str, Any]:
# MAGIC             url = f"{self.genie_space_url}/{self.convo_url}/{message_id}/query-result"
# MAGIC             response = requests.get(url, headers=self.headers)
# MAGIC             return response.json()
# MAGIC         
# MAGIC         def _execute_query(self, message_id: str) -> Dict[str, Any]:
# MAGIC             url = f"{self.genie_space_url}/{self.convo_url}/{message_id}"
# MAGIC             while True:
# MAGIC                 response = requests.get(url, headers=self.headers)
# MAGIC                 data = response.json()
# MAGIC                 print(data["status"])
# MAGIC                 if data["status"] == "EXECUTING_QUERY":
# MAGIC                     query_result = self._get_query_result(message_id)
# MAGIC                     return query_result
# MAGIC                 if data['status'] == 'COMPLETED':
# MAGIC                         return data
# MAGIC                 time.sleep(5)
# MAGIC
# MAGIC         def ask(self, query: str) -> str:
# MAGIC             payload    = {"content": query}
# MAGIC             message_id = self._followup_conversation(payload)["id"]
# MAGIC             execute_query_response = self._execute_query(message_id)
# MAGIC             if 'attachments' in execute_query_response:
# MAGIC                 if 'text' in execute_query_response['attachments'][0]:
# MAGIC                     result = execute_query_response['attachments'][0]['text']['content']
# MAGIC             else:
# MAGIC                 result = {'columns' : execute_query_response['statement_response']['manifest']['schema']['columns'], 'data': execute_query_response['statement_response']['result']}
# MAGIC             
# MAGIC             return query, result
# MAGIC     
# MAGIC     client = DatabricksGenieTool()
# MAGIC     result = client.ask(question)
# MAGIC     
# MAGIC     return str(result)
# MAGIC
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC select brij_catalog.genie_agent_integration._genie_query("How has the market sentiment changed by week for my portfolio?") as col

# COMMAND ----------

# MAGIC %pip install databricks-sdk
# MAGIC %restart_python

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient(host  = f"https://YourCloudWorkspace.cloud.databricks.com",
                    token = 'test')
w.get_workspace_id()

# COMMAND ----------

w = WorkspaceClient()

# COMMAND ----------

w.tokens.list()

# COMMAND ----------

env:
  - name: "DATABRICKS_SECRETS"
    valueFrom: "dbrx_secret"
  - name: "DATABRICKS_HOST"
    value: "https://YourCloudWorkspace.cloud.databricks.com"
  - name: STREAMLIT_BROWSER_GATHER_USAGE_STATS
    value: "false"

# COMMAND ----------

w.current_user.me()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from brij_catalog.sce.sce_pdf_vsi_02 limit 5 -- Check if this table supports Lakehouse Federation

# COMMAND ----------

input_string = "D.20-10-005"
cleaned_string = input_string.replace('.', '').replace('-', '')
cleaned_string

# COMMAND ----------

"File Name: D.20-10-005 D2010005".split(" ")[2]

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from brij_catalog.sce.sce_pdf_vsi_02
# MAGIC -- SHOW TABLES IN brij_catalog.sce
