import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import { randomUUID } from "node:crypto";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { z } from "zod";
import odbc from "odbc";

dotenv.config();

/**
 * Environment
 *
 * Required option A:
 *   ODBC_DSN
 *   ODBC_USER
 *   ODBC_PASSWORD
 *
 * Or option B:
 *   ODBC_CONNECTION_STRING
 *
 * Optional:
 *   PORT
 *   MCP_SERVER_API_KEY
 *   CORS_ALLOWED_ORIGINS
 *   CORS_ALLOW_ALL
 *   DEFAULT_MAX_ROWS
 *   HARD_MAX_ROWS
 */

const PORT = Number(process.env.PORT ?? 8000);

const DEFAULT_MAX_ROWS = Number(process.env.DEFAULT_MAX_ROWS ?? 100);
const HARD_MAX_ROWS = Number(process.env.HARD_MAX_ROWS ?? 500);

const MCP_SERVER_API_KEY = process.env.MCP_SERVER_API_KEY?.trim() || "";

function requiredEnv(name) {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function buildConnectionString() {
  const directConnectionString = process.env.ODBC_CONNECTION_STRING?.trim();

  if (directConnectionString) {
    return directConnectionString;
  }

  const dsn = requiredEnv("ODBC_DSN");
  const user = requiredEnv("ODBC_USER");
  const password = requiredEnv("ODBC_PASSWORD");

  return `DSN=${dsn};UID=${user};PWD=${password}`;
}

const ODBC_CONNECTION_STRING = buildConnectionString();

const SECRET_VALUES = [
  process.env.ODBC_CONNECTION_STRING,
  process.env.ODBC_DSN,
  process.env.ODBC_USER,
  process.env.ODBC_PASSWORD,
  process.env.MCP_SERVER_API_KEY,
]
  .filter(Boolean)
  .map(String);

function redactSecrets(value) {
  let text = String(value ?? "");

  for (const secret of SECRET_VALUES) {
    if (!secret) continue;
    text = text.split(secret).join("[REDACTED]");
  }

  return text;
}

async function connectDb() {
  return odbc.connect(ODBC_CONNECTION_STRING);
}

function toPlainRows(data) {
  if (!data) return [];

  try {
    return Array.from(data).map((row) => ({ ...row }));
  } catch {
    return [];
  }
}

function escapeMdCell(value) {
  return String(value ?? "")
    .replace(/\|/g, "\\|")
    .replace(/\r?\n/g, " ");
}

function dataToMD(rows) {
  if (!rows || rows.length === 0) return "No results found.";

  const columns = Object.keys(rows[0]);

  let mdTable = `| ${columns.map(escapeMdCell).join(" | ")} |\n`;
  mdTable += `| ${columns.map(() => "---").join(" | ")} |\n`;

  for (const row of rows) {
    mdTable += `| ${columns.map((col) => escapeMdCell(row[col])).join(" | ")} |\n`;
  }

  return mdTable;
}

function formatData(rows, format = "json", metadata = null) {
  const safeRows = Array.isArray(rows) ? rows : [];

  if (format === "md") {
    const table = dataToMD(safeRows);

    if (!metadata) return table;

    const metaText = Object.entries(metadata)
      .map(([key, value]) => `- ${key}: ${value}`)
      .join("\n");

    return `${table}\n\n${metaText}`;
  }

  if (format === "jsonl") {
    if (safeRows.length === 0) return "No results found.";

    const rowLines = safeRows.map((row) => JSON.stringify(row));

    if (metadata) {
      rowLines.push(JSON.stringify({ _metadata: metadata }));
    }

    return rowLines.join("\n");
  }

  if (metadata) {
    return JSON.stringify(
      {
        metadata,
        rows: safeRows,
      },
      null,
      2
    );
  }

  return JSON.stringify(safeRows, null, 2);
}

function errorResult(error, fallbackMessage = "Unexpected server error") {
  const message = redactSecrets(error?.message || error || fallbackMessage);

  console.error("Tool error:", redactSecrets(error?.stack || message));

  return {
    content: [
      {
        type: "text",
        text: `Error: ${message}`,
      },
    ],
    isError: true,
  };
}

async function supportsCatalogs(connection) {
  try {
    const tables = await connection.tables("%", "", "", null);
    const first = tables?.[0];

    if (!first) return false;

    return Boolean(first.TABLE_CAT || first.TABLE_QUALIFIER);
  } catch {
    return false;
  }
}

function uniqueNonEmpty(values) {
  return [...new Set(values.filter((value) => value !== null && value !== undefined && String(value).trim() !== ""))];
}

function clampMaxRows(value) {
  const requested = Number(value ?? DEFAULT_MAX_ROWS);

  if (!Number.isFinite(requested) || requested <= 0) {
    return DEFAULT_MAX_ROWS;
  }

  return Math.min(Math.floor(requested), HARD_MAX_ROWS);
}

function stripSqlComments(query) {
  return String(query ?? "")
    .replace(/\/\*[\s\S]*?\*\//g, " ")
    .replace(/--.*$/gm, " ");
}

function validateReadonlyQuery(query) {
  const raw = String(query ?? "");
  const stripped = stripSqlComments(raw).trim();

  if (!stripped) {
    throw new Error("Query is required.");
  }

  const withoutSingleTrailingSemicolon = stripped.replace(/;\s*$/, "");

  if (withoutSingleTrailingSemicolon.includes(";")) {
    throw new Error("Multiple SQL statements are not allowed.");
  }

  if (!/^(select|with)\b/i.test(withoutSingleTrailingSemicolon)) {
    throw new Error("Only read-only SELECT queries are allowed.");
  }

  const blockedPattern =
    /\b(insert|update|delete|drop|alter|create|truncate|merge|exec|execute|grant|revoke|call|copy|load|backup|restore)\b/i;

  if (blockedPattern.test(withoutSingleTrailingSemicolon)) {
    throw new Error("This query contains a blocked SQL keyword. Only read-only SELECT queries are allowed.");
  }

  return withoutSingleTrailingSemicolon;
}

async function runMetadataQuery(callback) {
  let connection;

  try {
    connection = await connectDb();
    return await callback(connection);
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch {
        // Ignore close errors.
      }
    }
  }
}

function createServer() {
  const server = new McpServer({
    name: "ODBC Database MCP Server",
    version: "2.0.0",
  });

  server.tool(
    "list_schemas",
    "List available database schemas/catalogs from the configured ODBC connection.",
    {
      format: z.enum(["json", "jsonl", "md"]).optional(),
    },
    async ({ format = "json" }) => {
      try {
        const rows = await runMetadataQuery(async (connection) => {
          const hasCatalogs = await supportsCatalogs(connection);

          const result = hasCatalogs
            ? await connection.tables("%", "", "", null)
            : await connection.tables(null, "%", "", null);

          const data = toPlainRows(result);

          const schemaColumnCandidates = hasCatalogs
            ? ["TABLE_CAT", "TABLE_QUALIFIER"]
            : ["TABLE_SCHEM", "TABLE_OWNER"];

          let schemaColumn = schemaColumnCandidates.find((column) => data.some((row) => row[column]));

          if (!schemaColumn) {
            schemaColumn = schemaColumnCandidates[0];
          }

          return uniqueNonEmpty(data.map((row) => row[schemaColumn])).map((name) => ({
            schema: name,
          }));
        });

        return {
          content: [
            {
              type: "text",
              text: formatData(rows, format),
            },
          ],
        };
      } catch (error) {
        return errorResult(error);
      }
    }
  );

  server.tool(
    "list_tables",
    "List tables from the configured ODBC connection. Optionally filter by schema/catalog.",
    {
      schema: z.string().optional(),
      format: z.enum(["json", "jsonl", "md"]).optional(),
    },
    async ({ schema = "%", format = "json" }) => {
      try {
        const rows = await runMetadataQuery(async (connection) => {
          const hasCatalogs = await supportsCatalogs(connection);

          const result = hasCatalogs
            ? await connection.tables(schema || "%", null, "%", null)
            : await connection.tables(null, schema || "%", "%", null);

          return toPlainRows(result).map((row) => ({
            table_catalog: row.TABLE_CAT ?? row.TABLE_QUALIFIER ?? null,
            table_schema: row.TABLE_SCHEM ?? row.TABLE_OWNER ?? null,
            table_name: row.TABLE_NAME ?? null,
            table_type: row.TABLE_TYPE ?? null,
            remarks: row.REMARKS ?? null,
          }));
        });

        return {
          content: [
            {
              type: "text",
              text: formatData(rows, format),
            },
          ],
        };
      } catch (error) {
        return errorResult(error);
      }
    }
  );

  server.tool(
    "search_tables",
    "Search for tables whose names contain the given text.",
    {
      q: z.string(),
      schema: z.string().optional(),
      format: z.enum(["json", "jsonl", "md"]).optional(),
    },
    async ({ q, schema = "%", format = "json" }) => {
      try {
        const searchText = String(q ?? "").trim().toLowerCase();

        if (!searchText) {
          throw new Error("Search text is required.");
        }

        const rows = await runMetadataQuery(async (connection) => {
          const hasCatalogs = await supportsCatalogs(connection);

          const result = hasCatalogs
            ? await connection.tables(schema || "%", null, "%", null)
            : await connection.tables(null, schema || "%", "%", null);

          return toPlainRows(result)
            .filter((row) => String(row.TABLE_NAME ?? "").toLowerCase().includes(searchText))
            .map((row) => ({
              table_catalog: row.TABLE_CAT ?? row.TABLE_QUALIFIER ?? null,
              table_schema: row.TABLE_SCHEM ?? row.TABLE_OWNER ?? null,
              table_name: row.TABLE_NAME ?? null,
              table_type: row.TABLE_TYPE ?? null,
              remarks: row.REMARKS ?? null,
            }));
        });

        return {
          content: [
            {
              type: "text",
              text: formatData(rows, format),
            },
          ],
        };
      } catch (error) {
        return errorResult(error);
      }
    }
  );

  server.tool(
    "describe_table",
    "Describe a table by returning its column metadata.",
    {
      schema: z.string(),
      table: z.string(),
      format: z.enum(["json", "jsonl", "md"]).optional(),
    },
    async ({ schema, table, format = "json" }) => {
      try {
        const rows = await runMetadataQuery(async (connection) => {
          const hasCatalogs = await supportsCatalogs(connection);

          const result = hasCatalogs
            ? await connection.columns(schema, null, table, null)
            : await connection.columns(null, schema, table, null);

          return toPlainRows(result).map((row) => ({
            table_catalog: row.TABLE_CAT ?? row.TABLE_QUALIFIER ?? null,
            table_schema: row.TABLE_SCHEM ?? row.TABLE_OWNER ?? null,
            table_name: row.TABLE_NAME ?? null,
            column_name: row.COLUMN_NAME ?? null,
            data_type: row.TYPE_NAME ?? row.DATA_TYPE ?? null,
            column_size: row.COLUMN_SIZE ?? null,
            decimal_digits: row.DECIMAL_DIGITS ?? null,
            nullable: row.NULLABLE ?? null,
            default_value: row.COLUMN_DEF ?? null,
            ordinal_position: row.ORDINAL_POSITION ?? null,
            remarks: row.REMARKS ?? null,
          }));
        });

        return {
          content: [
            {
              type: "text",
              text: formatData(rows, format),
            },
          ],
        };
      } catch (error) {
        return errorResult(error);
      }
    }
  );

  server.tool(
    "run_readonly_query",
    "Run a read-only SELECT SQL query against the configured ODBC database. Credentials are never accepted as tool parameters.",
    {
      query: z.string(),
      max_rows: z.number().int().positive().optional(),
      format: z.enum(["json", "jsonl", "md"]).optional(),
    },
    async ({ query, max_rows = DEFAULT_MAX_ROWS, format = "json" }) => {
      let connection;

      try {
        const safeQuery = validateReadonlyQuery(query);
        const maxRows = clampMaxRows(max_rows);

        connection = await connectDb();

        const result = await connection.query(safeQuery);
        const allRows = toPlainRows(result);
        const rows = allRows.slice(0, maxRows);

        const metadata = {
          returned_rows: rows.length,
          max_rows: maxRows,
          truncated: allRows.length > rows.length,
        };

        return {
          content: [
            {
              type: "text",
              text: formatData(rows, format, metadata),
            },
          ],
        };
      } catch (error) {
        return errorResult(error);
      } finally {
        if (connection) {
          try {
            await connection.close();
          } catch {
            // Ignore close errors.
          }
        }
      }
    }
  );

  return server;
}

const app = express();

const allowedOrigins = (process.env.CORS_ALLOWED_ORIGINS ?? "")
  .split(",")
  .map((origin) => origin.trim())
  .filter(Boolean);

const corsAllowAll = String(process.env.CORS_ALLOW_ALL ?? "true").toLowerCase() === "true";

const corsOptions = {
  origin(origin, callback) {
    // Allow curl/Postman/server-to-server requests with no Origin header.
    if (!origin) {
      callback(null, true);
      return;
    }

    if (corsAllowAll) {
      callback(null, true);
      return;
    }

    if (allowedOrigins.includes(origin)) {
      callback(null, true);
      return;
    }

    callback(new Error(`CORS blocked origin: ${origin}`));
  },
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
app.use(express.json({ limit: "2mb" }));

app.use((err, _req, res, next) => {
  if (err instanceof SyntaxError) {
    res.status(400).json({
      jsonrpc: "2.0",
      error: {
        code: -32700,
        message: "Invalid JSON body",
      },
      id: null,
    });
    return;
  }

  next(err);
});

function requireMcpApiKey(req, res, next) {
  if (!MCP_SERVER_API_KEY) {
    next();
    return;
  }

  const authorization = String(req.headers.authorization ?? "");

  if (authorization === `Bearer ${MCP_SERVER_API_KEY}`) {
    next();
    return;
  }

  res.status(401).json({
    jsonrpc: "2.0",
    error: {
      code: -32001,
      message: "Unauthorized",
    },
    id: null,
  });
}

// Important for browser-based clients and Open WebUI preflight requests.
app.options("/mcp", cors(corsOptions), (_req, res) => {
  res.sendStatus(204);
});

const transports = new Map();

app.all("/mcp", requireMcpApiKey, async (req, res) => {
  try {
    if (req.method === "OPTIONS") {
      res.sendStatus(204);
      return;
    }

    const sessionId = req.headers["mcp-session-id"];

    if (req.method === "POST") {
      let transport;

      if (sessionId && transports.has(sessionId)) {
        transport = transports.get(sessionId);
      } else {
        transport = new StreamableHTTPServerTransport({
          sessionIdGenerator: () => randomUUID(),
          onsessioninitialized: (newSessionId) => {
            transports.set(newSessionId, transport);
          },
        });

        transport.onclose = () => {
          if (transport.sessionId) {
            transports.delete(transport.sessionId);
          }
        };

        const server = createServer();
        await server.connect(transport);
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

    res.status(405).json({
      jsonrpc: "2.0",
      error: {
        code: -32601,
        message: "Method not allowed",
      },
      id: null,
    });
  } catch (error) {
    console.error("MCP HTTP error:", redactSecrets(error?.stack || error));

    if (!res.headersSent) {
      res.status(500).json({
        jsonrpc: "2.0",
        error: {
          code: -32603,
          message: "Internal server error",
        },
        id: null,
      });
    }
  }
});

app.get("/", (_req, res) => {
  res.json({
    ok: true,
    name: "ODBC Database MCP Server",
    version: "2.0.0",
    endpoint: "/mcp",
    tools: [
      "list_schemas",
      "list_tables",
      "search_tables",
      "describe_table",
      "run_readonly_query",
    ],
  });
});

app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    service: "odbc-database-mcp-server",
  });
});

app.listen(PORT, () => {
  console.log(`ODBC Database MCP server listening on http://localhost:${PORT}/mcp`);
});