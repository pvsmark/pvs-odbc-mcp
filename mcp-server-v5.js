import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import { randomUUID, createHash } from "node:crypto";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { z } from "zod";
import odbc from "odbc";

dotenv.config();

process.on("uncaughtException", (error) => {
  console.error("Uncaught exception:", error);
});

process.on("unhandledRejection", (reason) => {
  console.error("Unhandled rejection:", reason);
});

process.on("exit", (code) => {
  console.error("Process exiting with code:", code);
});

/**
 * ODBC SQL Anywhere MCP Server
 *
 * Safer structure:
 * - credentials are loaded from environment variables only
 * - tools do not accept user/password/dsn parameters
 * - arbitrary SQL is read-only validated
 * - normal chat queries are row-limited by default
 * - all rows can only be returned when all_rows=true is explicitly passed
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
 *   METADATA_CACHE_TTL_MS
 *   QUERY_CACHE_TTL_MS
 *   ALLOWED_SCHEMAS
 *   BLOCKED_COLUMNS
 */

const PORT = Number(process.env.PORT ?? 8000);

const DEFAULT_MAX_ROWS = Number(process.env.DEFAULT_MAX_ROWS ?? 25);
const HARD_MAX_ROWS = Number(process.env.HARD_MAX_ROWS ?? 100);

const METADATA_CACHE_TTL_MS = Number(process.env.METADATA_CACHE_TTL_MS ?? 10 * 60 * 1000);
const QUERY_CACHE_TTL_MS = Number(process.env.QUERY_CACHE_TTL_MS ?? 60 * 1000);
const MCP_SERVER_API_KEY = process.env.MCP_SERVER_API_KEY?.trim() || "";

const FORMAT_VALUES = ["json", "json_compact", "json_pretty", "jsonl", "md"];

function parseCsvEnv(value) {
  return String(value ?? "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

const ALLOWED_SCHEMAS = parseCsvEnv(process.env.ALLOWED_SCHEMAS).map((value) => value.toLowerCase());

const BLOCKED_COLUMNS = new Set(
  parseCsvEnv(
    process.env.BLOCKED_COLUMNS ??
      "password,passwd,pwd,token,secret,api_key,apikey,authorization,access_token,refresh_token"
  ).map((value) => value.toLowerCase())
);

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

function sanitizeRows(rows) {
  return rows.map((row) => {
    const clean = {};

    for (const [key, value] of Object.entries(row)) {
      if (BLOCKED_COLUMNS.has(String(key).toLowerCase())) {
        clean[key] = "[REDACTED]";
      } else {
        clean[key] = value;
      }
    }

    return clean;
  });
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

function formatData(rows, format = "json_compact", metadata = null) {
  const safeRows = sanitizeRows(Array.isArray(rows) ? rows : []);

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

  if (format === "json_pretty") {
    if (metadata) {
      return JSON.stringify({ metadata, rows: safeRows }, null, 2);
    }

    return JSON.stringify(safeRows, null, 2);
  }

  // json and json_compact both use compact JSON for lower token usage.
  if (metadata) {
    return JSON.stringify({ metadata, rows: safeRows });
  }

  return JSON.stringify(safeRows);
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

function successText(text) {
  return {
    content: [
      {
        type: "text",
        text,
      },
    ],
  };
}

function jsonText(value, pretty = false) {
  return successText(JSON.stringify(value, null, pretty ? 2 : 0));
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
  return [
    ...new Set(
      values.filter(
        (value) =>
          value !== null &&
          value !== undefined &&
          String(value).trim() !== ""
      )
    ),
  ];
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

function normalizeSql(query) {
  return String(query ?? "").replace(/\s+/g, " ").trim();
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

function applySqlAnywhereLimit(query, maxRows) {
  const limit = Math.max(1, Math.floor(Number(maxRows)));
  return `SELECT TOP ${limit} * FROM (${query}) AS mcp_limited_result`;
}

function isSchemaAllowed(schema) {
  if (ALLOWED_SCHEMAS.length === 0) return true;
  return ALLOWED_SCHEMAS.includes(String(schema ?? "").toLowerCase());
}

function assertSchemaAllowed(schema) {
  if (!isSchemaAllowed(schema)) {
    throw new Error(`Schema is not allowed: ${schema}`);
  }
}

function quoteIdentifier(name) {
  const value = String(name ?? "").trim();

  if (!value) {
    throw new Error("SQL identifier is required.");
  }

  if (/[\0\r\n;]/.test(value)) {
    throw new Error("Invalid SQL identifier.");
  }

  return `"${value.replace(/"/g, '""')}"`;
}

function qualifiedTableName(schema, table) {
  assertSchemaAllowed(schema);
  return `${quoteIdentifier(schema)}.${quoteIdentifier(table)}`;
}

const cache = new Map();

function hashText(text) {
  return createHash("sha256").update(String(text)).digest("hex");
}

function makeCacheKey(toolName, args) {
  return `${toolName}:${hashText(JSON.stringify(args))}`;
}

function getCache(key) {
  const item = cache.get(key);

  if (!item) return null;

  if (Date.now() > item.expiresAt) {
    cache.delete(key);
    return null;
  }

  return item.value;
}

function setCache(key, value, ttlMs) {
  if (!ttlMs || ttlMs <= 0) return;

  cache.set(key, {
    value,
    expiresAt: Date.now() + ttlMs,
  });
}

function clearCacheByPrefix(prefix = "") {
  let removed = 0;

  for (const key of cache.keys()) {
    if (!prefix || key.startsWith(prefix)) {
      cache.delete(key);
      removed += 1;
    }
  }

  return removed;
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

async function getAllTables(connection, schema = "%") {
  const hasCatalogs = await supportsCatalogs(connection);

  const result = hasCatalogs
    ? await connection.tables(schema || "%", null, "%", null)
    : await connection.tables(null, schema || "%", "%", null);

  return toPlainRows(result)
    .map((row) => ({
      table_catalog: row.TABLE_CAT ?? row.TABLE_QUALIFIER ?? null,
      table_schema: row.TABLE_SCHEM ?? row.TABLE_OWNER ?? null,
      table_name: row.TABLE_NAME ?? null,
      table_type: row.TABLE_TYPE ?? null,
      remarks: row.REMARKS ?? null,
    }))
    .filter((row) => !row.table_schema || isSchemaAllowed(row.table_schema));
}

async function runReadonlyQueryRows(query, maxRows, allRows = false) {
  let connection;

  try {
    const safeQuery = validateReadonlyQuery(query);
    const limitedMaxRows = clampMaxRows(maxRows);

    const queryToRun = allRows
      ? safeQuery
      : applySqlAnywhereLimit(safeQuery, limitedMaxRows + 1);

    connection = await connectDb();

    const startedAt = Date.now();
    const result = await connection.query(queryToRun);
    const durationMs = Date.now() - startedAt;

    const fetchedRows = sanitizeRows(toPlainRows(result));
    const rows = allRows ? fetchedRows : fetchedRows.slice(0, limitedMaxRows);

    return {
      rows,
      metadata: {
        returned_rows: rows.length,
        max_rows: allRows ? null : limitedMaxRows,
        all_rows: Boolean(allRows),
        truncated: allRows ? false : fetchedRows.length > rows.length,
        limited_at_database: !allRows,
        duration_ms: durationMs,
      },
    };
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
    name: "ODBC SQL Anywhere MCP Server",
    version: "2.1.0",
  });

  server.tool(
    "ping_database",
    "Test whether the MCP server can connect to the configured ODBC database.",
    {},
    async () => {
      let connection;

      try {
        const startedAt = Date.now();
        connection = await connectDb();
        const durationMs = Date.now() - startedAt;

        return jsonText({
          ok: true,
          message: "Database connection successful.",
          duration_ms: durationMs,
        });
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

  server.tool(
    "clear_cache",
    "Clear in-memory metadata/query cache. Optionally provide a tool name prefix.",
    {
      prefix: z.string().optional(),
    },
    async ({ prefix = "" }) => {
      try {
        const removed = clearCacheByPrefix(prefix);
        return jsonText({ ok: true, removed });
      } catch (error) {
        return errorResult(error);
      }
    }
  );

  server.tool(
    "get_database_summary",
    "Return a compact summary of schemas, table/view counts, sample tables, and sample views.",
    {
      schema: z.string().optional(),
      format: z.enum(FORMAT_VALUES).optional(),
    },
    async ({ schema = "%", format = "json_compact" }) => {
      const cacheKey = makeCacheKey("get_database_summary", { schema, format });
      const cached = getCache(cacheKey);
      if (cached) return cached;

      try {
        const result = await runMetadataQuery(async (connection) => {
          const tables = await getAllTables(connection, schema);
          const schemas = uniqueNonEmpty(tables.map((row) => row.table_schema));
          const tableRows = tables.filter((row) => String(row.table_type ?? "").toUpperCase().includes("TABLE"));
          const viewRows = tables.filter((row) => String(row.table_type ?? "").toUpperCase().includes("VIEW"));

          return [
            {
              schemas: schemas.join(", "),
              table_count: tableRows.length,
              view_count: viewRows.length,
              total_objects: tables.length,
              sample_tables: tableRows
                .slice(0, 15)
                .map((row) => `${row.table_schema ?? ""}.${row.table_name}`)
                .join(", "),
              sample_views: viewRows
                .slice(0, 15)
                .map((row) => `${row.table_schema ?? ""}.${row.table_name}`)
                .join(", "),
            },
          ];
        });

        const response = successText(formatData(result, format));
        setCache(cacheKey, response, METADATA_CACHE_TTL_MS);
        return response;
      } catch (error) {
        return errorResult(error);
      }
    }
  );

  server.tool(
    "list_schemas",
    "List available database schemas/catalogs from the configured ODBC connection.",
    {
      format: z.enum(FORMAT_VALUES).optional(),
    },
    async ({ format = "json_compact" }) => {
      const cacheKey = makeCacheKey("list_schemas", { format });
      const cached = getCache(cacheKey);
      if (cached) return cached;

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

          return uniqueNonEmpty(data.map((row) => row[schemaColumn]))
            .filter((name) => isSchemaAllowed(name))
            .map((name) => ({ schema: name }));
        });

        const response = successText(formatData(rows, format));
        setCache(cacheKey, response, METADATA_CACHE_TTL_MS);
        return response;
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
      format: z.enum(FORMAT_VALUES).optional(),
    },
    async ({ schema = "%", format = "json_compact" }) => {
      const cacheKey = makeCacheKey("list_tables", { schema, format });
      const cached = getCache(cacheKey);
      if (cached) return cached;

      try {
        const rows = await runMetadataQuery(async (connection) => getAllTables(connection, schema));
        const tableRows = rows.filter((row) => String(row.table_type ?? "").toUpperCase().includes("TABLE"));

        const response = successText(formatData(tableRows, format));
        setCache(cacheKey, response, METADATA_CACHE_TTL_MS);
        return response;
      } catch (error) {
        return errorResult(error);
      }
    }
  );

  server.tool(
    "list_views",
    "List views from the configured ODBC connection. Optionally filter by schema/catalog.",
    {
      schema: z.string().optional(),
      format: z.enum(FORMAT_VALUES).optional(),
    },
    async ({ schema = "%", format = "json_compact" }) => {
      const cacheKey = makeCacheKey("list_views", { schema, format });
      const cached = getCache(cacheKey);
      if (cached) return cached;

      try {
        const rows = await runMetadataQuery(async (connection) => getAllTables(connection, schema));
        const viewRows = rows.filter((row) => String(row.table_type ?? "").toUpperCase().includes("VIEW"));

        const response = successText(formatData(viewRows, format));
        setCache(cacheKey, response, METADATA_CACHE_TTL_MS);
        return response;
      } catch (error) {
        return errorResult(error);
      }
    }
  );

  server.tool(
    "search_tables",
    "Search for tables or views whose names contain the given text.",
    {
      q: z.string(),
      schema: z.string().optional(),
      format: z.enum(FORMAT_VALUES).optional(),
    },
    async ({ q, schema = "%", format = "json_compact" }) => {
      const cacheKey = makeCacheKey("search_tables", { q, schema, format });
      const cached = getCache(cacheKey);
      if (cached) return cached;

      try {
        const searchText = String(q ?? "").trim().toLowerCase();

        if (!searchText) {
          throw new Error("Search text is required.");
        }

        const rows = await runMetadataQuery(async (connection) => {
          const tables = await getAllTables(connection, schema);

          return tables.filter((row) => String(row.table_name ?? "").toLowerCase().includes(searchText));
        });

        const response = successText(formatData(rows, format));
        setCache(cacheKey, response, METADATA_CACHE_TTL_MS);
        return response;
      } catch (error) {
        return errorResult(error);
      }
    }
  );

  server.tool(
    "search_columns",
    "Search for column names containing the given text across tables/views. Useful before writing SQL joins or filters.",
    {
      q: z.string(),
      schema: z.string().optional(),
      max_results: z.number().int().positive().optional(),
      format: z.enum(FORMAT_VALUES).optional(),
    },
    async ({ q, schema = "%", max_results = 100, format = "json_compact" }) => {
      const cacheKey = makeCacheKey("search_columns", { q, schema, max_results, format });
      const cached = getCache(cacheKey);
      if (cached) return cached;

      try {
        const searchText = String(q ?? "").trim().toLowerCase();

        if (!searchText) {
          throw new Error("Search text is required.");
        }

        const maxResults = Math.min(Math.floor(Number(max_results) || 100), 500);

        const rows = await runMetadataQuery(async (connection) => {
          const hasCatalogs = await supportsCatalogs(connection);
          const tables = await getAllTables(connection, schema);
          const matches = [];

          for (const tableRow of tables) {
            if (!tableRow.table_name) continue;
            if (!tableRow.table_schema && schema === "%") continue;

            const columnResult = hasCatalogs
              ? await connection.columns(tableRow.table_schema, null, tableRow.table_name, null)
              : await connection.columns(null, tableRow.table_schema, tableRow.table_name, null);

            for (const column of toPlainRows(columnResult)) {
              const columnName = String(column.COLUMN_NAME ?? "");

              if (!columnName.toLowerCase().includes(searchText)) continue;

              matches.push({
                table_schema: column.TABLE_SCHEM ?? column.TABLE_OWNER ?? tableRow.table_schema ?? null,
                table_name: column.TABLE_NAME ?? tableRow.table_name ?? null,
                column_name: columnName,
                data_type: column.TYPE_NAME ?? column.DATA_TYPE ?? null,
                column_size: column.COLUMN_SIZE ?? null,
                nullable: column.NULLABLE ?? null,
                ordinal_position: column.ORDINAL_POSITION ?? null,
              });

              if (matches.length >= maxResults) {
                return matches;
              }
            }
          }

          return matches;
        });

        const response = successText(
          formatData(rows, format, {
            returned_rows: rows.length,
            max_results: maxResults,
          })
        );

        setCache(cacheKey, response, METADATA_CACHE_TTL_MS);
        return response;
      } catch (error) {
        return errorResult(error);
      }
    }
  );

  server.tool(
    "describe_table_compact",
    "Describe a table using compact column metadata designed to reduce LLM context usage.",
    {
      schema: z.string(),
      table: z.string(),
      format: z.enum(FORMAT_VALUES).optional(),
    },
    async ({ schema, table, format = "json_compact" }) => {
      const cacheKey = makeCacheKey("describe_table_compact", { schema, table, format });
      const cached = getCache(cacheKey);
      if (cached) return cached;

      try {
        assertSchemaAllowed(schema);

        const rows = await runMetadataQuery(async (connection) => {
          const hasCatalogs = await supportsCatalogs(connection);

          const result = hasCatalogs
            ? await connection.columns(schema, null, table, null)
            : await connection.columns(null, schema, table, null);

          return toPlainRows(result).map((row) => ({
            column_name: row.COLUMN_NAME ?? null,
            data_type: row.TYPE_NAME ?? row.DATA_TYPE ?? null,
            column_size: row.COLUMN_SIZE ?? null,
            nullable: row.NULLABLE ?? null,
            ordinal_position: row.ORDINAL_POSITION ?? null,
          }));
        });

        const response = successText(
          formatData(rows, format, {
            schema,
            table,
            column_count: rows.length,
          })
        );

        setCache(cacheKey, response, METADATA_CACHE_TTL_MS);
        return response;
      } catch (error) {
        return errorResult(error);
      }
    }
  );

  server.tool(
    "describe_table",
    "Describe a table by returning detailed column metadata.",
    {
      schema: z.string(),
      table: z.string(),
      format: z.enum(FORMAT_VALUES).optional(),
    },
    async ({ schema, table, format = "json_compact" }) => {
      const cacheKey = makeCacheKey("describe_table", { schema, table, format });
      const cached = getCache(cacheKey);
      if (cached) return cached;

      try {
        assertSchemaAllowed(schema);

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

        const response = successText(formatData(rows, format));
        setCache(cacheKey, response, METADATA_CACHE_TTL_MS);
        return response;
      } catch (error) {
        return errorResult(error);
      }
    }
  );

  server.tool(
    "list_indexes",
    "List index/statistics metadata for a table when supported by the ODBC driver.",
    {
      schema: z.string(),
      table: z.string(),
      format: z.enum(FORMAT_VALUES).optional(),
    },
    async ({ schema, table, format = "json_compact" }) => {
      const cacheKey = makeCacheKey("list_indexes", { schema, table, format });
      const cached = getCache(cacheKey);
      if (cached) return cached;

      try {
        assertSchemaAllowed(schema);

        const rows = await runMetadataQuery(async (connection) => {
          if (typeof connection.statistics !== "function") {
            return [
              {
                message: "The current ODBC driver/package does not expose connection.statistics().",
              },
            ];
          }

          const hasCatalogs = await supportsCatalogs(connection);

          const result = hasCatalogs
            ? await connection.statistics(schema, null, table, false, false)
            : await connection.statistics(null, schema, table, false, false);

          return toPlainRows(result).map((row) => ({
            table_schema: row.TABLE_SCHEM ?? row.TABLE_OWNER ?? schema,
            table_name: row.TABLE_NAME ?? table,
            index_name: row.INDEX_NAME ?? null,
            column_name: row.COLUMN_NAME ?? null,
            non_unique: row.NON_UNIQUE ?? null,
            ordinal_position: row.ORDINAL_POSITION ?? null,
            type: row.TYPE ?? null,
            filter_condition: row.FILTER_CONDITION ?? null,
          }));
        });

        const response = successText(formatData(rows, format));
        setCache(cacheKey, response, METADATA_CACHE_TTL_MS);
        return response;
      } catch (error) {
        return errorResult(error);
      }
    }
  );

  server.tool(
    "find_relationships",
    "Return primary key and foreign key metadata for a table when supported by the ODBC driver.",
    {
      schema: z.string(),
      table: z.string(),
      format: z.enum(FORMAT_VALUES).optional(),
    },
    async ({ schema, table, format = "json_compact" }) => {
      const cacheKey = makeCacheKey("find_relationships", { schema, table, format });
      const cached = getCache(cacheKey);
      if (cached) return cached;

      try {
        assertSchemaAllowed(schema);

        const rows = await runMetadataQuery(async (connection) => {
          const hasCatalogs = await supportsCatalogs(connection);
          const relationshipRows = [];

          if (typeof connection.primaryKeys === "function") {
            try {
              const pkResult = hasCatalogs
                ? await connection.primaryKeys(schema, null, table)
                : await connection.primaryKeys(null, schema, table);

              for (const row of toPlainRows(pkResult)) {
                relationshipRows.push({
                  relationship_type: "primary_key",
                  table_schema: row.TABLE_SCHEM ?? row.TABLE_OWNER ?? schema,
                  table_name: row.TABLE_NAME ?? table,
                  column_name: row.COLUMN_NAME ?? null,
                  key_sequence: row.KEY_SEQ ?? null,
                  key_name: row.PK_NAME ?? null,
                });
              }
            } catch (error) {
              relationshipRows.push({
                relationship_type: "primary_key_error",
                message: redactSecrets(error.message),
              });
            }
          } else {
            relationshipRows.push({
              relationship_type: "primary_key_unavailable",
              message: "The current ODBC driver/package does not expose connection.primaryKeys().",
            });
          }

          if (typeof connection.foreignKeys === "function") {
            try {
              const fkResult = hasCatalogs
                ? await connection.foreignKeys(null, null, null, schema, null, table)
                : await connection.foreignKeys(null, null, null, null, schema, table);

              for (const row of toPlainRows(fkResult)) {
                relationshipRows.push({
                  relationship_type: "foreign_key",
                  fk_table_schema: row.FKTABLE_SCHEM ?? row.FKTABLE_OWNER ?? schema,
                  fk_table_name: row.FKTABLE_NAME ?? table,
                  fk_column_name: row.FKCOLUMN_NAME ?? null,
                  pk_table_schema: row.PKTABLE_SCHEM ?? row.PKTABLE_OWNER ?? null,
                  pk_table_name: row.PKTABLE_NAME ?? null,
                  pk_column_name: row.PKCOLUMN_NAME ?? null,
                  key_sequence: row.KEY_SEQ ?? null,
                  fk_name: row.FK_NAME ?? null,
                  pk_name: row.PK_NAME ?? null,
                });
              }
            } catch (error) {
              relationshipRows.push({
                relationship_type: "foreign_key_error",
                message: redactSecrets(error.message),
              });
            }
          } else {
            relationshipRows.push({
              relationship_type: "foreign_key_unavailable",
              message: "The current ODBC driver/package does not expose connection.foreignKeys().",
            });
          }

          return relationshipRows;
        });

        const response = successText(formatData(rows, format));
        setCache(cacheKey, response, METADATA_CACHE_TTL_MS);
        return response;
      } catch (error) {
        return errorResult(error);
      }
    }
  );

  server.tool(
    "get_table_sample",
    "Return rows from a table or view. By default row-limited; use all_rows=true to return all rows.",
    {
      schema: z.string(),
      table: z.string(),
      columns: z.array(z.string()).optional(),
      max_rows: z.number().int().positive().optional(),
      all_rows: z.boolean().optional(),
      format: z.enum(FORMAT_VALUES).optional(),
    },
    async ({
      schema,
      table,
      columns = [],
      max_rows = DEFAULT_MAX_ROWS,
      all_rows = false,
      format = "json_compact",
    }) => {
      try {
        const maxRows = clampMaxRows(max_rows);

        const selectedColumns =
          Array.isArray(columns) && columns.length > 0
            ? columns.map(quoteIdentifier).join(", ")
            : "*";

        const query = all_rows
          ? `SELECT ${selectedColumns} FROM ${qualifiedTableName(schema, table)}`
          : `SELECT TOP ${maxRows + 1} ${selectedColumns} FROM ${qualifiedTableName(schema, table)}`;

        let connection;

        try {
          connection = await connectDb();

          const startedAt = Date.now();
          const result = await connection.query(query);
          const durationMs = Date.now() - startedAt;

          const fetchedRows = sanitizeRows(toPlainRows(result));
          const rows = all_rows ? fetchedRows : fetchedRows.slice(0, maxRows);

          return successText(
            formatData(rows, format, {
              returned_rows: rows.length,
              max_rows: all_rows ? null : maxRows,
              all_rows: Boolean(all_rows),
              truncated: all_rows ? false : fetchedRows.length > rows.length,
              limited_at_database: !all_rows,
              duration_ms: durationMs,
            })
          );
        } finally {
          if (connection) {
            try {
              await connection.close();
            } catch {
              // Ignore close errors.
            }
          }
        }
      } catch (error) {
        return errorResult(error);
      }
    }
  );

  server.tool(
    "get_table_count",
    "Return COUNT(*) for a table or view.",
    {
      schema: z.string(),
      table: z.string(),
      format: z.enum(FORMAT_VALUES).optional(),
    },
    async ({ schema, table, format = "json_compact" }) => {
      const cacheKey = makeCacheKey("get_table_count", { schema, table, format });
      const cached = getCache(cacheKey);
      if (cached) return cached;

      try {
        const query = `SELECT COUNT(*) AS row_count FROM ${qualifiedTableName(schema, table)}`;

        let connection;

        try {
          connection = await connectDb();

          const startedAt = Date.now();
          const result = await connection.query(query);
          const durationMs = Date.now() - startedAt;

          const rows = sanitizeRows(toPlainRows(result));

          const response = successText(
            formatData(rows, format, {
              duration_ms: durationMs,
            })
          );

          setCache(cacheKey, response, QUERY_CACHE_TTL_MS);
          return response;
        } finally {
          if (connection) {
            try {
              await connection.close();
            } catch {
              // Ignore close errors.
            }
          }
        }
      } catch (error) {
        return errorResult(error);
      }
    }
  );

  server.tool(
    "get_distinct_values",
    "Return distinct values and counts for one column, ordered by frequency descending.",
    {
      schema: z.string(),
      table: z.string(),
      column: z.string(),
      max_rows: z.number().int().positive().optional(),
      format: z.enum(FORMAT_VALUES).optional(),
    },
    async ({ schema, table, column, max_rows = DEFAULT_MAX_ROWS, format = "json_compact" }) => {
      const cacheKey = makeCacheKey("get_distinct_values", {
        schema,
        table,
        column,
        max_rows,
        format,
      });

      const cached = getCache(cacheKey);
      if (cached) return cached;

      try {
        const maxRows = clampMaxRows(max_rows);
        const qColumn = quoteIdentifier(column);

        const query = `SELECT TOP ${maxRows} ${qColumn} AS value, COUNT(*) AS row_count FROM ${qualifiedTableName(
          schema,
          table
        )} GROUP BY ${qColumn} ORDER BY row_count DESC`;

        let connection;

        try {
          connection = await connectDb();

          const startedAt = Date.now();
          const result = await connection.query(query);
          const durationMs = Date.now() - startedAt;

          const rows = sanitizeRows(toPlainRows(result));

          const response = successText(
            formatData(rows, format, {
              returned_rows: rows.length,
              max_rows: maxRows,
              duration_ms: durationMs,
            })
          );

          setCache(cacheKey, response, QUERY_CACHE_TTL_MS);
          return response;
        } finally {
          if (connection) {
            try {
              await connection.close();
            } catch {
              // Ignore close errors.
            }
          }
        }
      } catch (error) {
        return errorResult(error);
      }
    }
  );

  server.tool(
    "run_readonly_query",
    "Run a read-only SELECT/WITH SQL query against the configured ODBC database. Results are row-limited by default. Use all_rows=true to return all matching rows.",
    {
      query: z.string(),
      max_rows: z.number().int().positive().optional(),
      all_rows: z.boolean().optional(),
      format: z.enum(FORMAT_VALUES).optional(),
    },
    async ({
      query,
      max_rows = DEFAULT_MAX_ROWS,
      all_rows = false,
      format = "json_compact",
    }) => {
      const useCache = !all_rows;

      const safeForKey = normalizeSql(query);
      const cacheKey = makeCacheKey("run_readonly_query", {
        query: safeForKey,
        max_rows,
        all_rows,
        format,
      });

      if (useCache) {
        const cached = getCache(cacheKey);
        if (cached) return cached;
      }

      try {
        const { rows, metadata } = await runReadonlyQueryRows(query, max_rows, all_rows);
        const response = successText(formatData(rows, format, metadata));

        if (useCache) {
          setCache(cacheKey, response, QUERY_CACHE_TTL_MS);
        }

        return response;
      } catch (error) {
        return errorResult(error);
      }
    }
  );

  server.tool(
    "query_database_md",
    "Convenience alias for run_readonly_query with Markdown output. Results are row-limited by default. Use all_rows=true to return all matching rows.",
    {
      query: z.string(),
      max_rows: z.number().int().positive().optional(),
      all_rows: z.boolean().optional(),
    },
    async ({ query, max_rows = DEFAULT_MAX_ROWS, all_rows = false }) => {
      try {
        const { rows, metadata } = await runReadonlyQueryRows(query, max_rows, all_rows);
        return successText(formatData(rows, "md", metadata));
      } catch (error) {
        return errorResult(error);
      }
    }
  );

  server.tool(
    "query_database_jsonl",
    "Convenience alias for run_readonly_query with JSONL output. Results are row-limited by default. Use all_rows=true to return all matching rows.",
    {
      query: z.string(),
      max_rows: z.number().int().positive().optional(),
      all_rows: z.boolean().optional(),
    },
    async ({ query, max_rows = DEFAULT_MAX_ROWS, all_rows = false }) => {
      try {
        const { rows, metadata } = await runReadonlyQueryRows(query, max_rows, all_rows);
        return successText(formatData(rows, "jsonl", metadata));
      } catch (error) {
        return errorResult(error);
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

const toolNames = [
  "ping_database",
  "clear_cache",
  "get_database_summary",
  "list_schemas",
  "list_tables",
  "list_views",
  "search_tables",
  "search_columns",
  "describe_table_compact",
  "describe_table",
  "list_indexes",
  "find_relationships",
  "get_table_sample",
  "get_table_count",
  "get_distinct_values",
  "run_readonly_query",
  "query_database_md",
  "query_database_jsonl",
];

app.get("/", (_req, res) => {
  res.json({
    ok: true,
    name: "ODBC SQL Anywhere MCP Server",
    version: "2.1.0",
    endpoint: "/mcp",
    tools: toolNames,
    defaults: {
      default_max_rows: DEFAULT_MAX_ROWS,
      hard_max_rows: HARD_MAX_ROWS,
      metadata_cache_ttl_ms: METADATA_CACHE_TTL_MS,
      query_cache_ttl_ms: QUERY_CACHE_TTL_MS,
    },
  });
});

app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    service: "odbc-sqlanywhere-mcp-server",
  });
});

app.listen(PORT, () => {
  console.log(`ODBC SQL Anywhere MCP server listening on http://localhost:${PORT}/mcp`);
});