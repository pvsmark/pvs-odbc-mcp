import express from "express";
import cors from "cors";
import { randomUUID } from "node:crypto";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { z } from "zod";
import odbc from "odbc";
import dotenv from "dotenv";
import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const parentDir = path.dirname(__dirname);

function loadEnv(filePath) {
  let envConfig = {};

  if (fs.existsSync(filePath)) {
    envConfig = dotenv.parse(fs.readFileSync(filePath));
  }

  return { ...envConfig, ...process.env };
}

const myEnv = loadEnv(path.join(parentDir, ".env"));

const ODBC_DSN = myEnv.ODBC_DSN ?? "TestData";
const ODBC_USER = myEnv.ODBC_USER ?? "DBA";
const ODBC_PASSWORD = myEnv.ODBC_PASSWORD ?? "sql";
const API_KEY = myEnv.API_KEY ?? "none";
const PORT = Number(myEnv.PORT ?? process.env.PORT ?? 8000);

// Create a factory so each HTTP session gets its own MCP server instance.
function createServer() {
  const server = new McpServer({
    name: "MCP ODBC Server",
    version: "1.0.14",
  });

  function dataToMD(data) {
    if (!data || data.length === 0) return "No results found.";

    const columns = Object.keys(data[0]);
    let mdTable = `| ${columns.join(" | ")} |\n`;
    mdTable += `| ${columns.map(() => "---").join(" | ")} |\n`;

    for (const row of data) {
      mdTable += `| ${columns.map((col) => String(row[col] ?? "")).join(" | ")} |\n`;
    }

    return mdTable;
  }

  async function supportsCatalogs(connection) {
    try {
      const cats = await connection.tables("%", "", "", null);
      return Boolean(cats.length && cats[0].TABLE_QUALIFIER);
    } catch (_) {
      return false;
    }
  }

  server.tool(
    "virt_get_schemas",
    "Retrieve and return a list of all schema names from the connected Virtuoso database.",
    {
      user: z.string().optional(),
      password: z.string().optional(),
      dsn: z.string().optional(),
      format: z.string().optional(),
    },
    async ({ user = ODBC_USER, password = ODBC_PASSWORD, dsn = ODBC_DSN, format = "json" }) => {
      let connection;

      try {
        connection = await odbc.connect(`DSN=${dsn};UID=${user};PWD=${password}`);

        const catalogs = await connection.query(
          "SELECT DISTINCT name_part(KEY_TABLE,0) AS CATALOG_NAME FROM DB.DBA.SYS_KEYS where __any_grants(KEY_TABLE) and table_type (KEY_TABLE) = 'TABLE' and KEY_IS_MAIN = 1 and KEY_MIGRATE_TO is NULL"
        );

        let toolResult;

        if (format === "jsonl") toolResult = catalogs.map((row) => JSON.stringify(row)).join("\n");
        else if (format === "md") toolResult = dataToMD(catalogs);
        else toolResult = JSON.stringify(catalogs, null, 2);

        return { content: [{ type: "text", text: toolResult }] };
      } catch (error) {
        return {
          content: [{ type: "text", text: `Error: ${JSON.stringify(error, null, 2)}` }],
          isError: true,
        };
      } finally {
        if (connection) {
          try {
            await connection.close();
          } catch (_) {}
        }
      }
    }
  );

  server.tool(
    "get_schemas",
    "Retrieve and return a list of all schema names from the connected database.",
    {
      user: z.string().optional(),
      password: z.string().optional(),
      dsn: z.string().optional(),
      format: z.string().optional(),
    },
    async ({ user = ODBC_USER, password = ODBC_PASSWORD, dsn = ODBC_DSN, format = "json" }) => {
      let connection;

      try {
        connection = await odbc.connect(`DSN=${dsn};UID=${user};PWD=${password}`);

        const hasCatalogs = await supportsCatalogs(connection);

        const result = hasCatalogs
          ? await connection.tables("%", "", "", null)
          : await connection.tables(null, "%", "", null);

        let catName = "TABLE_CAT";

        if (result && result.length) {
          const row = result[0];

          if (hasCatalogs) {
            catName = "TABLE_CAT" in row ? "TABLE_CAT" : "TABLE_QUALIFIER";
          } else {
            catName = "TABLE_SCHEM" in row ? "TABLE_SCHEM" : "TABLE_OWNER";
          }
        }

        const catalogs = [...new Set(result.map((item) => item[catName]))].map((name) => ({
          CATALOG_NAME: name,
        }));

        let toolResult;

        if (format === "jsonl") toolResult = catalogs.map((row) => JSON.stringify(row)).join("\n");
        else if (format === "md") toolResult = dataToMD(catalogs);
        else toolResult = JSON.stringify(catalogs, null, 2);

        return { content: [{ type: "text", text: toolResult }] };
      } catch (error) {
        return {
          content: [{ type: "text", text: `Error: ${JSON.stringify(error, null, 2)}` }],
          isError: true,
        };
      } finally {
        if (connection) {
          try {
            await connection.close();
          } catch (_) {}
        }
      }
    }
  );

  server.tool(
    "get_tables",
    "Retrieve and return a list containing information about tables in specified schema, if empty uses connection default.",
    {
      schema: z.string().optional(),
      user: z.string().optional(),
      password: z.string().optional(),
      dsn: z.string().optional(),
      format: z.string().optional(),
    },
    async ({ schema = null, user = ODBC_USER, password = ODBC_PASSWORD, dsn = ODBC_DSN, format = "json" }) => {
      let connection;

      try {
        connection = await odbc.connect(`DSN=${dsn};UID=${user};PWD=${password}`);

        const hasCatalogs = await supportsCatalogs(connection);

        const data = hasCatalogs
          ? await connection.tables(schema, null, null, null)
          : await connection.tables(null, schema, null, null);

        let toolResult;

        if (format === "jsonl") toolResult = data.map((row) => JSON.stringify(row)).join("\n");
        else if (format === "md") toolResult = dataToMD(data);
        else toolResult = JSON.stringify(data, null, 2);

        return { content: [{ type: "text", text: toolResult }] };
      } catch (error) {
        return {
          content: [{ type: "text", text: `Error: ${JSON.stringify(error, null, 2)}` }],
          isError: true,
        };
      } finally {
        if (connection) {
          try {
            await connection.close();
          } catch (_) {}
        }
      }
    }
  );

  server.tool(
    "filter_table_names",
    "Retrieve and return a list containing information about tables whose names contain the substring q.",
    {
      q: z.string(),
      schema: z.string().optional(),
      user: z.string().optional(),
      password: z.string().optional(),
      dsn: z.string().optional(),
      format: z.string().optional(),
    },
    async ({ q, schema = null, user = ODBC_USER, password = ODBC_PASSWORD, dsn = ODBC_DSN, format = "json" }) => {
      let connection;

      try {
        connection = await odbc.connect(`DSN=${dsn};UID=${user};PWD=${password}`);

        const hasCatalogs = await supportsCatalogs(connection);
        const tablesInfo = [];
        const schemaPattern = schema || "%";

        const data = hasCatalogs
          ? await connection.tables(schemaPattern, null, "%", null)
          : await connection.tables(null, schemaPattern, "%", null);

        for (const row of data) {
          if (row.TABLE_NAME && row.TABLE_NAME.includes(q)) {
            tablesInfo.push(row);
          }
        }

        let toolResult;

        if (format === "jsonl") toolResult = tablesInfo.map((row) => JSON.stringify(row)).join("\n");
        else if (format === "md") toolResult = dataToMD(tablesInfo);
        else toolResult = JSON.stringify(tablesInfo, null, 2);

        return { content: [{ type: "text", text: toolResult }] };
      } catch (error) {
        return {
          content: [{ type: "text", text: `Error: ${JSON.stringify(error, null, 2)}` }],
          isError: true,
        };
      } finally {
        if (connection) {
          try {
            await connection.close();
          } catch (_) {}
        }
      }
    }
  );

  server.tool(
    "describe_table",
    `Retrieve and return a dictionary containing the definition of a table, including column names, data types, nullable, autoincrement, primary key, and foreign keys.`,
    {
      schema: z.string(),
      table: z.string(),
      user: z.string().optional(),
      password: z.string().optional(),
      dsn: z.string().optional(),
      format: z.string().optional(),
    },
    async ({ schema, table, user = ODBC_USER, password = ODBC_PASSWORD, dsn = ODBC_DSN, format = "json" }) => {
      let connection;

      try {
        connection = await odbc.connect(`DSN=${dsn};UID=${user};PWD=${password}`);

        const hasCatalogs = await supportsCatalogs(connection);

        const data = hasCatalogs
          ? await connection.columns(schema, null, table, null)
          : await connection.columns(null, schema, table, null);

        let toolResult;

        if (format === "jsonl") toolResult = data.map((row) => JSON.stringify(row)).join("\n");
        else if (format === "md") toolResult = dataToMD(data);
        else toolResult = JSON.stringify(data, null, 2);

        return { content: [{ type: "text", text: toolResult }] };
      } catch (error) {
        return {
          content: [{ type: "text", text: `Error: ${JSON.stringify(error, null, 2)}` }],
          isError: true,
        };
      } finally {
        if (connection) {
          try {
            await connection.close();
          } catch (_) {}
        }
      }
    }
  );

  async function query_database(query, user, password, dsn, format) {
    let connection;

    try {
      connection = await odbc.connect(`DSN=${dsn};UID=${user};PWD=${password}`);

      const data = await connection.query(query);

      let toolResult;

      if (format === "jsonl") toolResult = data.map((row) => JSON.stringify(row)).join("\n");
      else if (format === "md") toolResult = dataToMD(data);
      else toolResult = JSON.stringify(data, null, 2);

      return { content: [{ type: "text", text: toolResult }] };
    } catch (error) {
      return {
        content: [{ type: "text", text: `Error: ${JSON.stringify(error, null, 2)}` }],
        isError: true,
      };
    } finally {
      if (connection) {
        try {
          await connection.close();
        } catch (_) {}
      }
    }
  }

  server.tool(
    "query_database",
    "Execute a SQL query and return results in JSON, JSONL or MD format.",
    {
      query: z.string(),
      user: z.string().optional(),
      password: z.string().optional(),
      dsn: z.string().optional(),
      format: z.string().optional(),
    },
    async ({ query, user = ODBC_USER, password = ODBC_PASSWORD, dsn = ODBC_DSN, format = "json" }) => {
      return query_database(query, user, password, dsn, format);
    }
  );

  server.tool(
    "query_database_md",
    "Execute a SQL query and return results in MD format.",
    {
      query: z.string(),
      user: z.string().optional(),
      password: z.string().optional(),
      dsn: z.string().optional(),
    },
    async ({ query, user = ODBC_USER, password = ODBC_PASSWORD, dsn = ODBC_DSN }) => {
      return query_database(query, user, password, dsn, "md");
    }
  );

  server.tool(
    "query_database_jsonl",
    "Execute a SQL query and return results in JSONL format.",
    {
      query: z.string(),
      user: z.string().optional(),
      password: z.string().optional(),
      dsn: z.string().optional(),
    },
    async ({ query, user = ODBC_USER, password = ODBC_PASSWORD, dsn = ODBC_DSN }) => {
      return query_database(query, user, password, dsn, "jsonl");
    }
  );

  server.tool(
    "spasql_query",
    "Execute a SPASQL query and return results.",
    {
      query: z.string(),
      max_rows: z.number().optional(),
      timeout: z.number().optional(),
      format: z.string().optional(),
      user: z.string().optional(),
      password: z.string().optional(),
      dsn: z.string().optional(),
    },
    async ({
      query,
      max_rows = 20,
      timeout = 30000,
      format = "json",
      user = ODBC_USER,
      password = ODBC_PASSWORD,
      dsn = ODBC_DSN,
    }) => {
      let connection;

      try {
        connection = await odbc.connect(`DSN=${dsn};UID=${user};PWD=${password}`);

        const data = await connection.query(
          "select Demo.demo.execute_spasql_query(?,?,?,?) as result",
          [query, max_rows, timeout, format]
        );

        return { content: [{ type: "text", text: data[0].result }] };
      } catch (error) {
        return {
          content: [{ type: "text", text: `Error: ${JSON.stringify(error, null, 2)}` }],
          isError: true,
        };
      } finally {
        if (connection) {
          try {
            await connection.close();
          } catch (_) {}
        }
      }
    }
  );

  server.tool(
    "virtuoso_support_ai",
    "Tool to use the Virtuoso AI support function.",
    {
      prompt: z.string(),
      api_key: z.string().optional(),
      user: z.string().optional(),
      password: z.string().optional(),
      dsn: z.string().optional(),
    },
    async ({ prompt, api_key = API_KEY, user = ODBC_USER, password = ODBC_PASSWORD, dsn = ODBC_DSN }) => {
      let connection;

      try {
        connection = await odbc.connect(`DSN=${dsn};UID=${user};PWD=${password}`);

        const data = await connection.query(
          "select DEMO.DBA.OAI_VIRTUOSO_SUPPORT_AI(?,?) as result",
          [prompt, api_key]
        );

        return { content: [{ type: "text", text: data[0].result }] };
      } catch (error) {
        return {
          content: [{ type: "text", text: `Error: ${JSON.stringify(error, null, 2)}` }],
          isError: true,
        };
      } finally {
        if (connection) {
          try {
            await connection.close();
          } catch (_) {}
        }
      }
    }
  );

  server.tool(
    "sparql_list_entity_types",
    `This query retrieves all entity types in the RDF graph, along with their labels and comments if available.
It filters out blank nodes and ensures that only IRI types are returned.
The LIMIT clause is set to 100 to restrict the number of entity types returned.`,
    {
      graph_iri: z.string().optional(),
      user: z.string().optional(),
      password: z.string().optional(),
      dsn: z.string().optional(),
      format: z.string().optional(),
    },
    async ({ graph_iri = undefined, user = ODBC_USER, password = ODBC_PASSWORD, dsn = ODBC_DSN, format = "json" }) => {
      const filterGraph =
        typeof graph_iri === "string" && graph_iri.trim() !== ""
          ? `FILTER (?g = <${graph_iri}>)`
          : "";

      const query = `SELECT DISTINCT * FROM (
        SPARQL
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        SELECT ?o
        WHERE {
            GRAPH ?g {
                ?s a ?o .

                OPTIONAL {
                    ?s rdfs:label ?label .
                    FILTER (LANG(?label) = "en" || LANG(?label) = "")
                }

                OPTIONAL {
                    ?s rdfs:comment ?comment .
                    FILTER (LANG(?comment) = "en" || LANG(?comment) = "")
                }

                FILTER (isIRI(?o) && !isBlank(?o))
            }
            ${filterGraph}
        }
        LIMIT 100
    ) AS x`;

      return query_database(query, user, password, dsn, format);
    }
  );

  server.tool(
    "sparql_list_entity_types_detailed",
    `This query retrieves all entity types in the RDF graph, along with their labels and comments if available.
It filters out blank nodes and ensures that only IRI types are returned.
The LIMIT clause is set to 100 to restrict the number of entity types returned.`,
    {
      graph_iri: z.string().optional(),
      user: z.string().optional(),
      password: z.string().optional(),
      dsn: z.string().optional(),
      format: z.string().optional(),
    },
    async ({ graph_iri = undefined, user = ODBC_USER, password = ODBC_PASSWORD, dsn = ODBC_DSN, format = "json" }) => {
      const filterGraph =
        typeof graph_iri === "string" && graph_iri.trim() !== ""
          ? `FILTER (?g = <${graph_iri}>)`
          : "";

      const query = `
        SELECT * FROM (
            SPARQL
            PREFIX owl: <http://www.w3.org/2002/07/owl#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

            SELECT ?o, (SAMPLE(?label) AS ?label), (SAMPLE(?comment) AS ?comment)
            WHERE {
                GRAPH ?g {
                    ?s a ?o .
                    OPTIONAL {?o rdfs:label ?label . FILTER (LANG(?label) = "en" || LANG(?label) = "")}
                    OPTIONAL {?o rdfs:comment ?comment . FILTER (LANG(?comment) = "en" || LANG(?comment) = "")}
                    FILTER (isIRI(?o) && !isBlank(?o))
                }
               ${filterGraph}
            }
            GROUP BY ?o
            ORDER BY ?o
            LIMIT 20
        ) AS results
    `;

      return query_database(query, user, password, dsn, format);
    }
  );

  server.tool(
    "sparql_list_entity_types_samples",
    `This query retrieves samples of entities for each type in the RDF graph, along with their labels and counts.
It groups by entity type and orders the results by sample count in descending order.
Note: The LIMIT clause is set to 20 to restrict the number of entity types returned.`,
    {
      graph_iri: z.string().optional(),
      user: z.string().optional(),
      password: z.string().optional(),
      dsn: z.string().optional(),
      format: z.string().optional(),
    },
    async ({ graph_iri = undefined, user = ODBC_USER, password = ODBC_PASSWORD, dsn = ODBC_DSN, format = "json" }) => {
      const filterGraph =
        typeof graph_iri === "string" && graph_iri.trim() !== ""
          ? `FILTER (?g = <${graph_iri}>)`
          : "";

      const query = `
        SELECT * FROM (
            SPARQL
            PREFIX owl: <http://www.w3.org/2002/07/owl#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            SELECT (SAMPLE(?s) AS ?sample), ?slabel, (COUNT(*) AS ?sampleCount), (?o AS ?entityType), ?olabel
            WHERE {
                GRAPH ?g {
                    ?s a ?o .
                    OPTIONAL {?s rdfs:label ?slabel . FILTER (LANG(?slabel) = "en" || LANG(?slabel) = "")}
                    FILTER (isIRI(?s) && !isBlank(?s))
                    OPTIONAL {?o rdfs:label ?olabel . FILTER (LANG(?olabel) = "en" || LANG(?olabel) = "")}
                    FILTER (isIRI(?o) && !isBlank(?o))
                }
                ${filterGraph}
            }
            GROUP BY ?slabel ?o ?olabel
            ORDER BY DESC(?sampleCount) ?o ?slabel ?olabel
            LIMIT 20
        ) AS results
    `;

      return query_database(query, user, password, dsn, format);
    }
  );

  server.tool(
    "sparql_list_ontologies",
    "This query retrieves all ontologies in the RDF graph, along with their labels and comments if available.",
    {
      graph_iri: z.string().optional(),
      user: z.string().optional(),
      password: z.string().optional(),
      dsn: z.string().optional(),
      format: z.string().optional(),
    },
    async ({ graph_iri = undefined, user = ODBC_USER, password = ODBC_PASSWORD, dsn = ODBC_DSN, format = "json" }) => {
      const filterGraph =
        typeof graph_iri === "string" && graph_iri.trim() !== ""
          ? `FILTER (?g = <${graph_iri}>)`
          : "";

      const query = `
    SELECT * FROM (
        SPARQL
        DEFINE input:storage ""
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        SELECT ?s, ?label, ?comment
        WHERE {
            GRAPH ?g {
                ?s a owl:Ontology .
                OPTIONAL {
                    ?s rdfs:label ?label .
                    FILTER (LANG(?label) = "en" || LANG(?label) = "")
                }
                OPTIONAL {
                    ?s rdfs:comment ?comment .
                    FILTER (LANG(?comment) = "en" || LANG(?comment) = "")
                }
                FILTER (isIRI(?s) && !isBlank(?s))
            }
            ${filterGraph}
        }
        LIMIT 100
    ) AS x
    `;

      return query_database(query, user, password, dsn, format);
    }
  );

  server.tool(
    "chat_prompt_complete",
    "Tool to use the OPAL backend to complete chat prompt.",
    {
      model: z.string(),
      prompt: z.string(),
      assistant_config_id: z.string().optional(),
      function_names: z.string().optional(),
      temperature: z.number().optional(),
      top_p: z.number().optional(),
      max_tokens: z.number().optional(),
      api_key: z.string().optional(),
      user: z.string().optional(),
      password: z.string().optional(),
      dsn: z.string().optional(),
    },
    async ({
      model,
      prompt,
      assistant_config_id = null,
      function_names = null,
      temperature = 0.2,
      top_p = 0.5,
      max_tokens = null,
      api_key = API_KEY,
      user = ODBC_USER,
      password = ODBC_PASSWORD,
      dsn = ODBC_DSN,
    }) => {
      let connection;

      try {
        connection = await odbc.connect(`DSN=${dsn};UID=${user};PWD=${password}`);

        const data = await connection.query(
          "select OAI.DBA.chatPromptComplete(?,?,?,?,?,?,?,?) as result",
          [model, prompt, assistant_config_id, function_names, temperature, top_p, max_tokens, api_key]
        );

        return { content: [{ type: "text", text: data[0].result }] };
      } catch (error) {
        return {
          content: [{ type: "text", text: `Error: ${JSON.stringify(error, null, 2)}` }],
          isError: true,
        };
      } finally {
        if (connection) {
          try {
            await connection.close();
          } catch (_) {}
        }
      }
    }
  );

  return server;
}

const app = express();

const corsOptions = {
  // Testing mode: allow all browser origins.
  // For production, replace with your Sky domain.
  origin: true,
  methods: ["GET", "POST", "DELETE", "OPTIONS"],
  allowedHeaders: [
    "Content-Type",
    "Authorization",
    "MCP-Protocol-Version",
    "Mcp-Session-Id",
    "mcp-session-id",
    "ngrok-skip-browser-warning",
  ],
  exposedHeaders: ["Mcp-Session-Id", "mcp-session-id"],
  credentials: false,
  maxAge: 86400,
};

app.use(cors(corsOptions));

app.options("/mcp", cors(corsOptions), (_req, res) => {
  res.sendStatus(204);
});

app.use(express.json({ limit: "2mb" }));

const transports = new Map();

function getSessionId(req) {
  const sessionId = req.headers["mcp-session-id"];
  return Array.isArray(sessionId) ? sessionId[0] : sessionId;
}

function isInitializeRequest(body) {
  if (Array.isArray(body)) {
    return body.some((message) => message?.method === "initialize");
  }

  return body?.method === "initialize";
}

function getRequestId(body) {
  if (Array.isArray(body)) {
    return body[0]?.id ?? null;
  }

  return body?.id ?? null;
}

app.all("/mcp", async (req, res) => {
  try {
    res.setHeader("Access-Control-Expose-Headers", "Mcp-Session-Id, mcp-session-id");

    if (req.method === "OPTIONS") {
      res.sendStatus(204);
      return;
    }

    const sessionId = getSessionId(req);

    console.log("MCP incoming:", {
      httpMethod: req.method,
      mcpMethod: Array.isArray(req.body)
        ? req.body.map((message) => message?.method)
        : req.body?.method,
      sessionId,
      hasKnownSession: sessionId ? transports.has(sessionId) : false,
    });

    if (req.method === "POST") {
      let transport;

      if (sessionId && transports.has(sessionId)) {
        transport = transports.get(sessionId);
      } else if (isInitializeRequest(req.body)) {
        transport = new StreamableHTTPServerTransport({
          sessionIdGenerator: () => randomUUID(),
          onsessioninitialized: (newSessionId) => {
            console.log("MCP session initialized:", newSessionId);
            transports.set(newSessionId, transport);
          },
        });

        transport.onclose = () => {
          if (transport.sessionId) {
            console.log("MCP session closed:", transport.sessionId);
            transports.delete(transport.sessionId);
          }
        };

        const server = createServer();
        await server.connect(transport);
      } else {
        res.status(400).json({
          jsonrpc: "2.0",
          error: {
            code: -32000,
            message:
              "Missing or invalid MCP session ID. Send initialize first, then include Mcp-Session-Id on later requests.",
          },
          id: getRequestId(req.body),
        });
        return;
      }

      await transport.handleRequest(req, res, req.body);
      return;
    }

    if (req.method === "GET") {
      if (!sessionId || !transports.has(sessionId)) {
        res.status(400).json({
          jsonrpc: "2.0",
          error: {
            code: -32000,
            message: "Missing or invalid MCP session ID",
          },
          id: null,
        });
        return;
      }

      const transport = transports.get(sessionId);
      await transport.handleRequest(req, res);
      return;
    }

    if (req.method === "DELETE") {
      if (!sessionId || !transports.has(sessionId)) {
        res.status(400).json({
          jsonrpc: "2.0",
          error: {
            code: -32000,
            message: "Missing or invalid MCP session ID",
          },
          id: null,
        });
        return;
      }

      const transport = transports.get(sessionId);
      await transport.handleRequest(req, res);
      return;
    }

    res.setHeader("Allow", "GET, POST, DELETE, OPTIONS");
    res.status(405).json({ error: "Method not allowed" });
  } catch (err) {
    console.error("MCP HTTP error:", err);

    if (!res.headersSent) {
      res.status(500).json({
        jsonrpc: "2.0",
        error: {
          code: -32603,
          message: err?.message || "Internal server error",
        },
        id: null,
      });
    }
  }
});

app.get("/", (_req, res) => {
  res.json({
    ok: true,
    name: "MCP ODBC Server",
    endpoint: "/mcp",
  });
});

app.listen(PORT, () => {
  console.log(`MCP HTTP server listening on http://localhost:${PORT}/mcp`);
});