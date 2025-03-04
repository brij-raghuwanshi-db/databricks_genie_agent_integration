# Databricks notebook source
# MAGIC %pip install --quiet -U databricks-agents mlflow-skinny mlflow mlflow[gateway] langchain langchain_core langchain_community databricks-vectorsearch databricks-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import requests, time, os, mlflow, yaml

from typing import Dict, Any
from operator import itemgetter

from langchain.tools import tool, BaseTool
from langchain_community.chat_models import ChatDatabricks

from langchain_core.runnables import RunnablePassthrough, RunnableBranch, RunnableLambda, RunnableConfig
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser

# COMMAND ----------

rag_chain_config = {
    "databricks_resources": {
        "llm_endpoint_name": "databricks-dbrx-instruct",
    },
    "input_example": {
        "messages": [
            {"role": "user", "content": "talk about the market changes by week for my portfolio"}
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

# MAGIC %%writefile chain.py
# MAGIC import requests, time, os, mlflow, json
# MAGIC
# MAGIC from typing import Dict, Any
# MAGIC from operator import itemgetter
# MAGIC from types import SimpleNamespace
# MAGIC
# MAGIC from langchain.tools import tool, BaseTool
# MAGIC
# MAGIC from langchain_community.chat_models import ChatDatabricks
# MAGIC
# MAGIC from langchain_core.runnables import RunnablePassthrough, RunnableBranch, RunnableLambda, RunnableConfig
# MAGIC from langchain_core.prompts import PromptTemplate, ChatPromptTemplate, MessagesPlaceholder
# MAGIC from langchain_core.output_parsers import StrOutputParser
# MAGIC from langchain_core.messages import HumanMessage, AIMessage
# MAGIC
# MAGIC mlflow.langchain.autolog()
# MAGIC
# MAGIC def dict_to_obj(d):
# MAGIC     if isinstance(d, dict):
# MAGIC         return SimpleNamespace(**{k: dict_to_obj(v) for k, v in d.items()})
# MAGIC     elif isinstance(d, list):
# MAGIC         return [dict_to_obj(v) for v in d]
# MAGIC     else:
# MAGIC         return d
# MAGIC
# MAGIC # Return the string contents of the most recent message from the user
# MAGIC def extract_user_query_string(chat_messages_array):
# MAGIC     return chat_messages_array[-1]["content"]
# MAGIC
# MAGIC # Return the chat history, which is is everything before the last question
# MAGIC def extract_chat_history(chat_messages_array):
# MAGIC     return chat_messages_array[:-1]
# MAGIC
# MAGIC # Enable verbose mode
# MAGIC verbose_config = RunnableConfig(verbose=True)
# MAGIC
# MAGIC model_config = mlflow.models.ModelConfig(development_config="rag_chain_config.yaml")
# MAGIC
# MAGIC databricks_resources = model_config.get("databricks_resources")
# MAGIC llm_config = model_config.get("llm_config")
# MAGIC input_example = model_config.get("input_example")
# MAGIC
# MAGIC ############### GENIE INTEGRATION ################
# MAGIC class DatabricksGenieTool(BaseTool):
# MAGIC
# MAGIC     name: str = "Databricks Genie Tool"
# MAGIC     description: str = "A tool to interact with Databricks Genie API."
# MAGIC     databricks_host: str = "https://YourCloudWorkspace.cloud.databricks.com"
# MAGIC     api_token: str = dbutils.secrets.get('brij_scope', 'brij_key')
# MAGIC     headers: Dict[str, str] = {"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"}
# MAGIC     base_url: str = f"{databricks_host}/api/2.0/genie/spaces"
# MAGIC     space_id: str = "01ef68cc62d715ebb1aee542871cd9da"
# MAGIC     conversation_id: str = "01ef75fe32f81046a77b2a04beaaeae7"
# MAGIC     genie_space_url: str = f"{base_url}/{space_id}"
# MAGIC     convo_url: str = f"conversations/{conversation_id}/messages"
# MAGIC
# MAGIC     def _create_conversation(self, payload: Dict[str, Any]) -> Dict[str, Any]:
# MAGIC         url = f"{self.genie_space_url}/start-conversation"
# MAGIC         response = requests.post(url, headers=self.headers, json=payload)
# MAGIC         response.raise_for_status()
# MAGIC         return response.json()
# MAGIC
# MAGIC     def _followup_conversation(self, payload: Dict[str, Any]) -> Dict[str, Any]:
# MAGIC         url = f"{self.genie_space_url}/{self.convo_url}"
# MAGIC         response = requests.post(url, headers=self.headers, json=payload)
# MAGIC         response.raise_for_status()
# MAGIC         return response.json()
# MAGIC     
# MAGIC     def _get_query_result(self, message_id: str) -> Dict[str, Any]:
# MAGIC         url = f"{self.genie_space_url}/{self.convo_url}/{message_id}/query-result"
# MAGIC         response = requests.get(url, headers=self.headers)
# MAGIC         response.raise_for_status()
# MAGIC         return response.json()
# MAGIC     
# MAGIC     def _execute_query(self, message_id: str) -> Dict[str, Any]:
# MAGIC         url = f"{self.genie_space_url}/{self.convo_url}/{message_id}"
# MAGIC         while True:
# MAGIC             response = requests.get(url, headers=self.headers)
# MAGIC             response.raise_for_status()
# MAGIC             data = response.json()
# MAGIC             print(data["status"])
# MAGIC             if data["status"] == "EXECUTING_QUERY":
# MAGIC                 query_result = self._get_query_result(message_id)
# MAGIC                 return query_result
# MAGIC             if data['status'] == 'COMPLETED':
# MAGIC                 return data
# MAGIC             time.sleep(5)
# MAGIC
# MAGIC     def _run(self, question: str) -> str:
# MAGIC         payload    = {"content": question}
# MAGIC         message_id = self._followup_conversation(payload)["id"]
# MAGIC         execute_query_response = self._execute_query(message_id)
# MAGIC         
# MAGIC         if 'attachments' in execute_query_response:
# MAGIC             if 'text' in execute_query_response['attachments'][0]:
# MAGIC                 result = execute_query_response['attachments'][0]['text']['content']
# MAGIC         else:
# MAGIC             result = {'columns' : execute_query_response['statement_response']['manifest']['schema']['columns'], 'data': execute_query_response['statement_response']['result']}
# MAGIC         
# MAGIC         return str(result)
# MAGIC
# MAGIC
# MAGIC @tool
# MAGIC def call_databricks_genie(question: str) -> str:
# MAGIC   """
# MAGIC     Calls the Databricks Genie tool with the provided query.
# MAGIC
# MAGIC     Args:
# MAGIC         query (str): The query to be sent to the Databricks Genie tool.
# MAGIC
# MAGIC     Returns:
# MAGIC         str: The response from the Databricks Genie tool.
# MAGIC     """
# MAGIC   client = DatabricksGenieTool()
# MAGIC   return client._run(question)
# MAGIC
# MAGIC ############# GENIE INTEGRATION END ##############
# MAGIC
# MAGIC def format_genie_context(docs):
# MAGIC     obj_docs = dict_to_obj(docs)
# MAGIC     pass_docs = obj_docs.genie_result
# MAGIC     chunk_contents = str(pass_docs)
# MAGIC     return "".join(chunk_contents)
# MAGIC
# MAGIC
# MAGIC # Prompt Template for generation
# MAGIC prompt = ChatPromptTemplate.from_messages(
# MAGIC     [
# MAGIC         ("system", llm_config.get("llm_prompt_template")),
# MAGIC         # Note: This chain does not compress the history, so very long converastions can overflow the context window.
# MAGIC         MessagesPlaceholder(variable_name="formatted_chat_history"),
# MAGIC         # User's most current question
# MAGIC         ("user", "{question}"),
# MAGIC     ]
# MAGIC )
# MAGIC
# MAGIC # Format the conversation history to fit into the prompt template above.
# MAGIC def format_chat_history_for_prompt(chat_messages_array):
# MAGIC     history = extract_chat_history(chat_messages_array)
# MAGIC     formatted_chat_history = []
# MAGIC     if len(history) > 0:
# MAGIC         for chat_message in history:
# MAGIC             if chat_message["role"] == "user":
# MAGIC                 formatted_chat_history.append(HumanMessage(content=chat_message["content"]))
# MAGIC             elif chat_message["role"] == "assistant":
# MAGIC                 formatted_chat_history.append(AIMessage(content=chat_message["content"]))
# MAGIC     return formatted_chat_history
# MAGIC
# MAGIC # Prompt Template for query rewriting to allow conversation history to work - this will translate a query such as "how does it work?" after a question such as "what is spark?" to "how does spark work?".
# MAGIC query_rewrite_template = """Based on the chat history below, we want you to generate a query for an external data source to retrieve relevant documents so that we can better answer the question. The query should be in natural language. The external data source uses similarity search to search for relevant documents in a vector space. So the query should be similar to the relevant documents semantically. Answer with only the query. Do not add explanation This is also going to query the genie api for which the space id, conversation id is supplied. It will generate query and get results. We need to use the relevant information in our results.
# MAGIC
# MAGIC Chat history: {chat_history}
# MAGIC
# MAGIC Question: {question}"""
# MAGIC
# MAGIC query_rewrite_prompt = PromptTemplate(
# MAGIC     template=query_rewrite_template,
# MAGIC     input_variables=["chat_history", "question"],
# MAGIC )
# MAGIC
# MAGIC # FM for generation
# MAGIC model = ChatDatabricks(
# MAGIC     endpoint=databricks_resources.get("llm_endpoint_name"),
# MAGIC     extra_params=llm_config.get("llm_parameters"),
# MAGIC     verbose=True
# MAGIC )
# MAGIC
# MAGIC # RAG Chain
# MAGIC chain = (
# MAGIC     {
# MAGIC         "question": itemgetter("messages") | RunnableLambda(extract_user_query_string), "chat_history": itemgetter("messages") | RunnableLambda(extract_chat_history), "formatted_chat_history": itemgetter("messages") | RunnableLambda(format_chat_history_for_prompt),
# MAGIC     }
# MAGIC     | RunnablePassthrough()
# MAGIC     | {
# MAGIC         "context": RunnableBranch(
# MAGIC             (
# MAGIC                 lambda x: len(x["chat_history"]) > 0,
# MAGIC                 query_rewrite_prompt | model | StrOutputParser(),
# MAGIC             ),
# MAGIC             itemgetter("question"),
# MAGIC             )
# MAGIC         | {'genie_result': call_databricks_genie}
# MAGIC         | RunnableLambda(format_genie_context), "formatted_chat_history": itemgetter("formatted_chat_history"), "question": itemgetter("question")
# MAGIC         }
# MAGIC     | prompt
# MAGIC     | model
# MAGIC     | StrOutputParser()
# MAGIC )
# MAGIC
# MAGIC mlflow.models.set_model(model=chain)

# COMMAND ----------

chain = (
    {
        "question": itemgetter("messages") | RunnableLambda(extract_user_query_string),
        "chat_history": itemgetter("messages") | RunnableLambda(extract_chat_history),
    }
    | RunnablePassthrough()
    | {"question": itemgetter("question")}
    | {RunnableLambda(query_as_vector), "question": itemgetter("question")}
    | prompt
    | model
    | StrOutputParser()
) 

# COMMAND ----------

with mlflow.start_run(run_name=f"genie_rag_v2"):
    logged_chain_info = mlflow.langchain.log_model(
        lc_model=os.path.join(os.getcwd(), 'chain.py'),
        model_config='rag_chain_config.yaml',
        artifact_path="chain",
        input_example=model_config.get("input_example"),
        example_no_conversion=True,
    )

chain = mlflow.langchain.load_model(logged_chain_info.model_uri)

# COMMAND ----------

new_question = "How has the market sentiment changed by week for my portfolio?"

# COMMAND ----------

new_message = {'messages': [{'content': new_question, 'role': 'user'}]}
chain.invoke(new_message)
