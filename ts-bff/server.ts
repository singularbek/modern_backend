import express from "express";
import { createHandler } from "graphql-http/lib/use/express";
import { buildSchema } from "graphql";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { WebSocketServer, WebSocket } from "ws";

import { fileURLToPath } from "url";
import { dirname, join } from "path";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const PROTO_PATH = join(__dirname, "../proto/catalog.proto");

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});
const catalogProto: any = grpc.loadPackageDefinition(packageDefinition).catalog;
const grpcClient = new catalogProto.CatalogService(
  "127.0.0.1:50051",
  grpc.credentials.createInsecure(),
);

// 2. WebSocket Clients Management
const wsClients = new Set<WebSocket>();
const wss = new WebSocketServer({ port: 4001 });
wss.on("connection", (ws) => {
  wsClients.add(ws);
  console.log("📡 [TS BFF] Client connected via WebSocket");
  ws.on("close", () => wsClients.delete(ws));
});

function broadcast(data: any) {
  wsClients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify(data));
    }
  });
}

// 3. GraphQL Schema & Resolvers
const schema = buildSchema(`
  type Mutation {
    createProductDraft(merchantId: String!, title: String!, priceEtb: Float!, initialStock: Int!): DraftResult
  }
  type DraftResult {
    draftId: String
    status: String
  }
  type Query {
    health: String
  }
`);

const root = {
  health: () => "BFF Alive",
  createProductDraft: (args: any) => {
    return new Promise((resolve, reject) => {
      console.log(
        "📥 [TS BFF] GraphQL Mutation received. Forwarding to Go via gRPC...",
      );

      grpcClient.CreateDraft(args, (err: any, response: any) => {
        if (err) return reject(err);

        // Broadcast notification over WebSocket
        broadcast({
          event: "DRAFT_UPDATED",
          merchantId: args.merchantId,
          draftId: response.draft_id,
          title: args.title,
          status: "STAGED",
        });

        console.log(
          "✅ [TS BFF] gRPC response received & WebSocket broadcast sent",
        );
        resolve({
          draftId: response.draft_id,
          status: response.status,
        });
      });
    });
  },
};

const app = express();
app.use(express.json());
app.all("/graphql", createHandler({ schema, rootValue: root }));

app.listen(4000, () => {
  console.log(
    "⚡ [TS BFF] Running on http://localhost:4000/graphql | WS on ws://localhost:4001",
  );
});
