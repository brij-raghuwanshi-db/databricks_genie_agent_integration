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

catalog = "brij_catalog"
db = "rag_chatbot"
VECTOR_SEARCH_ENDPOINT_NAME = "brij_genie_integration"
vector_search_index = f"{catalog}.{db}.genie_integration_pdf_doc_vsi"
# brij_catalog.rag_chatbot.genie_integration_pdf_vsi
vector_search_index

# COMMAND ----------

rag_chain_config = {
    "databricks_resources": {
        "llm_endpoint_name": "databricks-dbrx-instruct",
        "vector_search_endpoint_name": VECTOR_SEARCH_ENDPOINT_NAME,
    },
    "input_example": {
        "messages": [
            {"role": "user", "content": "top 3 records from customer table."}
        ]
    },
    "llm_config": {
        "llm_parameters": {"max_tokens": 1500, "temperature": 0.01},
        "llm_prompt_template": "You are a trusted assistant that helps answer questions based only on the provided information. If you do not know the answer to a question, you truthfully say you do not know. Here is some context which might or might not help you answer: {context}. Answer directly, do not repeat the question, do not start with something like: the answer to the question, do not add AI in front of your answer, do not say: here is the answer, do not mention the context or the question. Based on this context, answer this question: {question}",
        "llm_prompt_template_variables": ["context", "question"],
    },
    "retriever_config": {
        "embedding_model": "databricks-gte-large-en",
        "chunk_template": "Passage: {chunk_text}\n",
        "data_pipeline_tag": "poc",
        "parameters": {"k": 5, "query_type": "hybrid"},
        "schema": {"chunk_text": "content", "document_uri": "url", "primary_key": "id"},
        "vector_search_index": f"{vector_search_index}",
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
# MAGIC from langchain_community.embeddings import DatabricksEmbeddings
# MAGIC from langchain_community.vectorstores import DatabricksVectorSearch
# MAGIC from databricks.vector_search.client import VectorSearchClient
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
# MAGIC # Load the chain's configuration
# MAGIC model_config = mlflow.models.ModelConfig(development_config="rag_chain_config.yaml")
# MAGIC
# MAGIC databricks_resources = model_config.get("databricks_resources")
# MAGIC llm_config = model_config.get("llm_config")
# MAGIC input_example = model_config.get("input_example")
# MAGIC retriever_config = model_config.get("retriever_config")
# MAGIC
# MAGIC
# MAGIC # Connect to the Vector Search Index
# MAGIC vs_client = VectorSearchClient(disable_notice=True)
# MAGIC vs_index = vs_client.get_index(
# MAGIC     endpoint_name=databricks_resources.get("vector_search_endpoint_name"),
# MAGIC     index_name=retriever_config.get("vector_search_index"),
# MAGIC )
# MAGIC vector_search_schema = retriever_config.get("schema")
# MAGIC
# MAGIC embedding_model = DatabricksEmbeddings(endpoint=retriever_config.get("embedding_model"))
# MAGIC
# MAGIC # Turn the Vector Search index into a LangChain retriever
# MAGIC vector_search_as_retriever = DatabricksVectorSearch(
# MAGIC     vs_index,
# MAGIC     text_column=vector_search_schema.get("chunk_text"),
# MAGIC     embedding=embedding_model, 
# MAGIC     columns=[
# MAGIC         vector_search_schema.get("primary_key"),
# MAGIC         vector_search_schema.get("chunk_text"),
# MAGIC         vector_search_schema.get("document_uri"),
# MAGIC     ],
# MAGIC ).as_retriever(search_kwargs=retriever_config.get("parameters"))
# MAGIC
# MAGIC # Enable the RAG Studio Review App to properly display retrieved chunks and evaluation suite to measure the retriever
# MAGIC mlflow.models.set_retriever_schema(
# MAGIC     primary_key=vector_search_schema.get("primary_key"),
# MAGIC     text_column=vector_search_schema.get("chunk_text"),
# MAGIC     doc_uri=vector_search_schema.get("document_uri")  # Review App uses `doc_uri` to display chunks from the same document in a single view
# MAGIC )
# MAGIC
# MAGIC ############### GENIE INTEGRATION ################
# MAGIC class DatabricksGenieTool(BaseTool):
# MAGIC
# MAGIC     name: str = "Databricks Genie Tool"
# MAGIC     description: str = "A tool to interact with Databricks Genie API."
# MAGIC     databricks_host: str = "https://e2-demo-field-eng.cloud.databricks.com"
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
# MAGIC # Method to format the docs returned by the retriever into the prompt
# MAGIC def format_context(docs):
# MAGIC     chunk_template = retriever_config.get("chunk_template")
# MAGIC     chunk_contents = [
# MAGIC         chunk_template.format(
# MAGIC             chunk_text=d.page_content,
# MAGIC             document_uri=d.metadata[vector_search_schema.get("document_uri")],
# MAGIC         )
# MAGIC         for d in docs
# MAGIC     ]
# MAGIC     return "".join(chunk_contents)
# MAGIC
# MAGIC def format_genie_context(docs):
# MAGIC     obj_docs = dict_to_obj(docs)
# MAGIC     
# MAGIC     g_pass_docs = obj_docs.genie_result
# MAGIC     g_chunk_contents = str(g_pass_docs)
# MAGIC
# MAGIC     v_pass_docs = obj_docs.vector_search_results
# MAGIC     v_chunk_contents = format_context(v_pass_docs)
# MAGIC
# MAGIC     return "".join([g_chunk_contents, v_chunk_contents])
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
# MAGIC         | {'genie_result': call_databricks_genie, 'vector_search_results': vector_search_as_retriever}
# MAGIC         | RunnableLambda(format_genie_context), "formatted_chat_history": itemgetter("formatted_chat_history"), "question": itemgetter("question")
# MAGIC         }
# MAGIC     | prompt
# MAGIC     | model
# MAGIC     | StrOutputParser()
# MAGIC )
# MAGIC
# MAGIC mlflow.models.set_model(model=chain)

# COMMAND ----------

with mlflow.start_run(run_name=f"genie_rag_v3"):
    logged_chain_info = mlflow.langchain.log_model(
        lc_model=os.path.join(os.getcwd(), 'chain.py'),
        model_config='rag_chain_config.yaml',
        artifact_path="chain",
        input_example=model_config.get("input_example"),
        example_no_conversion=True,
    )

chain = mlflow.langchain.load_model(logged_chain_info.model_uri)

# COMMAND ----------

new_question = "Give Details about customers living in Nancyland top 3. Also, tell about Delta live tables. If you don't have information on Delta Live tables; that's okay"

# COMMAND ----------

new_message = {'messages': [{'content': new_question, 'role': 'user'}]}
chain.invoke(new_message)
