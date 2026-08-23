# Distributed Event-Driven Microservices Architecture

A polyglot, event-driven microservices reference architecture demonstrating real-time data ingestion, high-performance inter-service communication, and hybrid vector search capabilities.

This project showcases how modern backend services built with Go, Node.js/TypeScript, and Python interact using gRPC, GraphQL, WebSockets, Apache Kafka, and an agentic RAG loop to deliver low-latency operations alongside asynchronous event processing and tool-using AI workflows.

## 🏗 System Architecture

```text
[ Client / Web UI ]
         │
 (GraphQL / HTTP)
         │
         v
┌───────────────────┐
│   API Gateway     │
│     (Kong)        │
└─────────┬─────────┘
          │
  (GraphQL / HTTP)
          │
          v
┌───────────────────────────────────────────────────────────────────┐
│ Node.js / TypeScript Backend-For-Frontend (BFF)                   │
│                                                                   │
│  • Exposes GraphQL endpoint for clients                           │
│  • Pushes real-time updates over WebSockets                       │
│  • Acts as a gRPC Client                                          │
└─────────────────────────────────┬─────────────────────────────────┘
                                  │
                           (gRPC / HTTP/2)
                                  │
                                  v
┌───────────────────────────────────────────────────────────────────┐
│ Go Core Domain Service                                             │
│                                                                   │
│  • Implements gRPC Server interface generated from .proto         │
│  • Handles transactional writes                                   │
│  • Acts as a Kafka Event Producer                                 │
└─────────────────────────────────┬─────────────────────────────────┘
                                  │
                         (Kafka Event Stream)
                                  │
                                  v
┌───────────────────────────────────────────────────────────────────┐
│ Apache Kafka Message Broker                                       │
│  • Managed via Strimzi Operator on Kubernetes                     │
│  • Keyed Partitioning for ordered stream processing               │
└─────────────────────────────────┬─────────────────────────────────┘
                                  │
                    (Consumer Group Subscription)
                                  │
                                  v
┌───────────────────────────────────────────────────────────────────┐
│ Python RAG & Vector Processing Service                            │
│                                                                   │
│  • Consumes async events from Kafka                               │
│  • Generates vector embeddings via Transformer models             │
│  • Indexes documents into Elasticsearch                            │
│  • Runs an agentic LangGraph loop with tool calling               │
└─────────────────────────────────┬─────────────────────────────────┘
                                  │
                           (Vector Storage)
                                  │
                                  v
                        ┌───────────────────┐
                        │   Elasticsearch   │
                        │ (Vector Search DB)│
                        └───────────────────┘
```

## 🔬 Key Architectural Concepts

### 1. Unified Contract-Driven Interface (gRPC & Protocol Buffers)

Internal inter-service communication between the BFF and core services relies on gRPC over HTTP/2.

Strict schemas defined in standard Protocol Buffer (`.proto`) files serve as single sources of truth for service contracts.

This avoids field-name serialization overhead over the network by encoding payloads into compact binary field tags.

### 2. Dual-Path Orchestration (BFF Pattern)

- **Synchronous path:** Client operations are received by the TypeScript BFF over GraphQL and forwarded instantly to the Go domain service over gRPC for immediate execution.
- **Asynchronous path:** Domain changes trigger events published to Apache Kafka, decoupling slow tasks such as indexing and embedding generation from client-facing response times.

### 3. Event-Driven Asynchrony (Kafka & Strimzi)

- State changes emit immutable, domain-neutral event payloads to Kafka.
- Partition keys ensure strict, ordered message consumption per entity.
- Topic infrastructure is managed declaratively using Strimzi Kafka Operators in Kubernetes (`KafkaTopic` CRDs).

### 4. Real-Time Push Notifications

The BFF maintains active WebSocket connections to client frontends.

Upon receiving a response from internal gRPC services, the BFF broadcasts live state updates back to open client sessions without requiring polling.

### 5. Semantic Vector Search & Ingestion (RAG)

A specialized Python worker service asynchronously ingests event streams.

Incoming textual data is encoded into high-dimensional vector representations using Sentence Transformers.

Vectors are indexed alongside structured metadata into Elasticsearch for hybrid similarity searches.

### 6. Agentic Retrieval and Tool-Use Loop

The Python service is not only a passive vector-ingestion worker. Its agentic query path uses LangGraph to coordinate an autonomous reasoning loop:

1. The agent receives a natural-language catalog request and maintains the conversation as typed state.
2. The language model decides whether it needs a capability, such as semantic catalog retrieval or discounted-price calculation.
3. LangGraph executes the selected tool and appends the result to the message history.
4. The result is returned to the model for another evaluation cycle, allowing multiple tool calls before the final response.
5. The loop ends only when the model has enough evidence to answer the user.

This makes the architecture agentic by design: the model dynamically selects actions, uses the vector store as memory, invokes deterministic business tools, and composes the final domain response. Kafka remains the asynchronous ingestion backbone, while the agentic loop turns indexed catalog knowledge into an interactive reasoning capability.

## 🛠 Tech Stack & Tools

| Component        | Technology                               | Purpose / Role                                                     |
| ---------------- | ---------------------------------------- | ------------------------------------------------------------------ |
| API Gateway      | Kong                                     | Route management, request proxying, API rate limiting              |
| BFF Layer        | Node.js, TypeScript, Express             | GraphQL engine, WebSockets provider, gRPC client                   |
| Core Service     | Go (Golang)                              | High-performance gRPC server, transactional core, Kafka producer   |
| Messaging        | Apache Kafka, Strimzi Operator           | Event bus, distributed commit log                                  |
| AI Worker        | Python, HuggingFace SentenceTransformers | Asynchronous event consumer, embedding generation                  |
| Agentic RAG      | LangGraph, LangChain, OpenAI             | Tool selection, iterative retrieval, reasoning, response synthesis |
| Search & Storage | Elasticsearch                            | Dense vector storage, k-NN similarity search                       |
| Protocol Schema  | Protocol Buffers (proto3), gRPC          | Service contract definition and binary serialization               |
| Containerization | Docker, Kubernetes                       | Multi-container orchestration and deployment                       |

## 📡 Protocols & Network Communication

| Source            | Target            | Protocol            | Transport | Serialization     |
| ----------------- | ----------------- | ------------------- | --------- | ----------------- |
| Client            | API Gateway       | HTTP/1.1            | TCP       | JSON              |
| API Gateway       | TypeScript BFF    | HTTP/1.1            | TCP       | JSON              |
| Client            | TypeScript BFF    | WebSockets          | TCP       | JSON              |
| TypeScript BFF    | Go Domain Service | gRPC                | HTTP/2    | Protobuf (Binary) |
| Go Domain Service | Kafka Broker      | Kafka Wire Protocol | TCP       | Binary / JSON     |
| Python Worker     | Kafka Broker      | Kafka Wire Protocol | TCP       | Binary / JSON     |
| Python Worker     | Elasticsearch     | HTTP                | TCP       | JSON              |

## 📂 Repository Structure

```text
.
├── proto/                     # Protocol Buffer definitions (.proto)
│   └── catalog.proto          # Shared gRPC service and message schemas
│
├── ts-bff/                    # TypeScript Backend-For-Frontend
│   ├── server.ts              # GraphQL, WebSockets, and gRPC client initialization
│   └── package.json
│
├── go-catalog/                # Core Domain Service in Go
│   ├── gen/                   # Code auto-generated by protoc
│   ├── main.go                # gRPC server implementation and Kafka producer
│   └── go.mod
│
├── python-rag/                # AI Vector Worker Service
│   ├── worker.py              # Kafka consumer, transformer embedder, and Elastic indexer
│   ├── agent.py               # Agentic LangGraph loop with vector search and tools
│   └── requirements.txt
│
├── k8s/                       # Infrastructure and Kubernetes manifests
│   └── kafka-topic.yaml        # Strimzi KafkaTopic custom resource definition
│
└── docker-compose.yml         # Local development environment setup
```

## 🚀 Execution & End-to-End Data Lifecycle

1. **Client request:** A client issues an HTTP POST GraphQL mutation to the API Gateway (Kong).
2. **Gateway forwarding:** Kong proxies the request to the TypeScript BFF.
3. **gRPC invocation:** The BFF uses dynamically loaded `.proto` definitions to send a compressed binary request over gRPC to the Go Core Service.
4. **Domain processing and event emission:** The Go service processes the core operation, constructs an event payload, and writes it to an Apache Kafka topic using keyed partition mapping.
5. **Immediate client acknowledgment:** The Go service responds over gRPC to the BFF, which immediately returns the status to the client over GraphQL while pushing a notification over WebSockets.
6. **Asynchronous processing:** The Python RAG Worker, consuming from the Kafka topic, captures the event, runs text through an embedding model, and stores the output in Elasticsearch as a `dense_vector`.
7. **Agentic retrieval:** A user query enters the Python agentic path, where LangGraph lets the model choose semantic vector search and deterministic calculation tools, loops over their results, and returns a grounded answer from the indexed catalog.
