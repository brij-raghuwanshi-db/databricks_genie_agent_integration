# Databricks notebook source
# MAGIC %pip install --quiet -U databricks-agents mlflow-skinny mlflow mlflow[gateway] langchain langchain_core langchain_community databricks-vectorsearch databricks-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import requests, time, yaml, mlflow

from typing import Dict, Any
from langchain_core.tools import BaseTool
from langchain_community.chat_models import ChatDatabricks

# COMMAND ----------

# Make it more dynamic
# put it on Github
# get ideas
# once it is on github, then suggest to the user to install it.

class DatabricksGenieTool(BaseTool):

    name: str = "Databricks Genie Tool"
    description: str = "A tool to interact with Databricks Genie API."
    databricks_host: str = "https://e2-demo-field-eng.cloud.databricks.com"
    api_token: str = dbutils.secrets.get('brij_scope', 'brij_key')
    headers: Dict[str, str] = {"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"}
    base_url: str = f"{databricks_host}/api/2.0/genie/spaces"
    space_id: str = "01ef68cc62d715ebb1aee542871cd9da"
    conversation_id: str = "01ef75fe32f81046a77b2a04beaaeae7"
    genie_space_url: str = f"{base_url}/{space_id}"
    convo_url: str = f"conversations/{conversation_id}/messages"

    def _create_conversation(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.genie_space_url}/start-conversation"
        response = requests.post(url, headers=self.headers, json=payload)
        response.raise_for_status()
        return response.json()

    def _followup_conversation(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.genie_space_url}/{self.convo_url}"
        response = requests.post(url, headers=self.headers, json=payload)
        response.raise_for_status()
        return response.json()
    
    def _get_query_result(self, message_id: str) -> Dict[str, Any]:
        url = f"{self.genie_space_url}/{self.convo_url}/{message_id}/query-result"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()
    
    def _execute_query(self, message_id: str) -> Dict[str, Any]:
        url = f"{self.genie_space_url}/{self.convo_url}/{message_id}"
        while True:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json()
            print(data["status"])
            if data["status"] == "EXECUTING_QUERY":
                query_result = self._get_query_result(message_id)
                return query_result
            if data['status'] == 'COMPLETED':
                return data
            time.sleep(5)

    def _run(self, question: str) -> str:
        payload    = {"content": question}
        message_id = self._followup_conversation(payload)["id"]
        execute_query_response = self._execute_query(message_id)
        
        if 'attachments' in execute_query_response:
            if 'text' in execute_query_response['attachments'][0]:
                result = execute_query_response['attachments'][0]['text']['content']
        else:
            result = {'columns' : execute_query_response['statement_response']['manifest']['schema']['columns'], 'data': execute_query_response['statement_response']['result']}
        
        return str(result)

# COMMAND ----------

rag_chain_config = {
    "databricks_resources": {
        "llm_endpoint_name": "databricks-dbrx-instruct",
    },
    "input_example": {
        "messages": [
            {"role": "user", "content": "get s_loy_member table. blood pressure column is bp. tell top 2 records from table."}
        ]
    },
    "llm_config": {
        "llm_parameters": {"max_tokens": 1500, "temperature": 0.01},
        "llm_prompt_template": "You are a trusted assistant that helps answer questions based only on the provided information. If you do not know the answer to a question, you truthfully say you do not know. Here is some context which might or might not help you answer: {context}. Answer directly, do not repeat the question, do not start with something like: the answer to the question, do not add AI in front of your answer, do not say: here is the answer, do not mention the context or the question. Based on this context, answer this question: {question}",
        "llm_prompt_template_variables": ["context", "question"],
    }
}
try:
    with open('rag_chain_config.yaml', 'w') as f:
        yaml.dump(rag_chain_config, f)
except:
    print('pass to work on build job')

# COMMAND ----------

model_config = mlflow.models.ModelConfig(development_config="rag_chain_config.yaml")
databricks_resources = model_config.get("databricks_resources")
llm_config = model_config.get("llm_config")
input_example = model_config.get("input_example")

# COMMAND ----------

from operator import itemgetter

from langchain.tools import BaseTool
from langchain_community.chat_models import ChatDatabricks

from langchain_core.runnables import RunnablePassthrough, RunnableBranch, RunnableLambda, RunnableConfig
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser

api_tool = DatabricksGenieTool()

# Prompt Template for generation
prompt = ChatPromptTemplate.from_messages(
    [
        ("system", llm_config.get("llm_prompt_template")),
        ("user", "{question}"),
    ]
)

# Instantiate your model with the subclass
chat_model = ChatDatabricks(
    endpoint=databricks_resources.get("llm_endpoint_name"),
    extra_params=llm_config.get("llm_parameters"),
)

rag_chain = (
    {"context": api_tool, "question": RunnablePassthrough()}
    | prompt
    | chat_model
    | StrOutputParser()
)

# COMMAND ----------

rag_chain.invoke("How has the market sentiment changed by week for my portfolio?")
