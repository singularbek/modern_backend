import os
from typing import Annotated, TypedDict, Literal
from dotenv import load_dotenv

from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from langchain_core.tools import tool
from langchain_elasticsearch import ElasticsearchStore
from langchain_openai import ChatOpenAI, OpenAIEmbeddings

from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode

load_dotenv()

# ============================================================================
# 1. TOOL DEFINITION (Capabilities provided to the Agent)
# ============================================================================

# Initialize Vector Store tool pointing to existing Elasticsearch instance
embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
vector_store = ElasticsearchStore(
    index_name="oxa_catalog_context",
    es_url="http://localhost:9200",
    embedding=embeddings
)

@tool
def search_catalog_vectors(query: str) -> str:
    """Performs semantic vector search on stored product records."""
    results = vector_store.similarity_search(query, k=3)
    if not results:
        return "No relevant catalog records found."
    return "\n".join([f"- {doc.page_content}" for doc in results])

@tool
def calculate_discounted_price(original_price: float, discount_percent: float) -> str:
    """Calculates discounted price given an original price and a percentage."""
    final_price = original_price * (1 - (discount_percent / 100))
    return f"Calculated price: {final_price:.2f} ETB"

# Register available tools
tools = [search_catalog_vectors, calculate_discounted_price]


# ============================================================================
# 2. STATE DEFINITION
# ============================================================================

class AgentState(TypedDict):
    # `add_messages` appends new messages to current history rather than overwriting
    messages: Annotated[list[BaseMessage], add_messages]


# ============================================================================
# 3. AGENT EXECUTION NODES & CONDITIONAL LOGIC
# ============================================================================

# Bind tools to the LLM model
model = ChatOpenAI(model="gpt-4o", temperature=0).bind_tools(tools)

def call_model(state: AgentState):
    """Reasoning node: Calls the LLM with system prompt and message history."""
    system_prompt = SystemMessage(
        content="You are an autonomous domain assistant. Use tools when necessary "
                "to fetch vector data or run calculations before answering."
    )
    messages = [system_prompt] + state["messages"]
    response = model.invoke(messages)
    return {"messages": [response]}

def should_continue(state: AgentState) -> Literal["tools", "END"]:
    """Conditional edge: Checks if the LLM output requested a tool call."""
    last_message = state["messages"][-1]
    if hasattr(last_message, "tool_calls") and last_message.tool_calls:
        return "tools"
    return END


# ============================================================================
# 4. LANGGRAPH STATE MACHINE BUILD
# ============================================================================

workflow = StateGraph(AgentState)

# Add Nodes
workflow.add_node("agent", call_model)
workflow.add_node("tools", ToolNode(tools))

# Add Edges
workflow.add_edge(START, "agent")
workflow.add_conditional_edges("agent", should_continue, ["tools", END])
workflow.add_edge("tools", "agent") # Loop tool execution back to LLM for evaluation

# Compile Runnable Agent
agent_app = workflow.compile()


# ============================================================================
# 5. EXECUTION & DEMO
# ============================================================================

if __name__ == "__main__":
    user_query = "Find leather products in the catalog and tell me their price with a 15% discount."
    print(f"User Input: {user_query}\n")

    initial_state = {"messages": [HumanMessage(content=user_query)]}
    
    # Stream state updates through the agent loop
    for event in agent_app.stream(initial_state):
        for node_name, state_update in event.items():
            print(f"--- Node Executed: {node_name} ---")
            latest_msg = state_update["messages"][-1]
            print(f"Output: {latest_msg.content or latest_msg.tool_calls}\n")