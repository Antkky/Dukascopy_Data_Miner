import express from "express";
import mysql from "mysql2/promise";
import dotenv from "dotenv";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";

dotenv.config();

const app = express();
const PORT = 3000;
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

app.use(express.static(path.join(__dirname, "public")));
app.use(express.json());

const pool = mysql.createPool({
  host: process.env.HOST,
  user: process.env.USER,
  password: process.env.PWD,
  database: process.env.DB,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
});

const symbols = [
  "eurusd",
  "gbpusd",
  "audusd",
  "nzdusd",
  "usdcad",
  "usdchf",
  "usdjpy",
  "xagusd",
  "xauusd",
  "adausd",
  "aveusd",
  "batusd",
  "btcchf",
  "btceur",
  "btcgbp",
  "btcusd",
  "ethchf",
  "etheur",
  "ethgbp",
  "ethusd",
  "cmpusd",
  "dshusd",
  "enjusd",
  "eosusd",
  "lnkusd",
  "ltcchf",
  "ltceur",
  "ltcgbp",
  "ltcusd",
  "matusd",
  "mkrusd",
  "trxusd",
  "uniusd",
  "xlmchf",
  "xlmeur",
  "xlmgbp",
  "xlmusd",
];

app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

// API endpoint to get the latest checkpoint info
app.get("/api/checkpoint", async (req, res) => {
  try {
    const checkpointFile = "./checkpoint.json";
    const data = await fs.readFile(checkpointFile, "utf8");
    const checkpoint = JSON.parse(data);
    res.json(checkpoint);
  } catch (error) {
    console.error("Error reading checkpoint:", error);
    res.status(404).json({ error: "Checkpoint not found" });
  }
});

// API endpoint to get the list of symbols
app.get("/api/symbols", (req, res) => {
  res.json(symbols);
});

// API endpoint to get progress statistics
app.get("/api/progress", async (req, res) => {
  try {
    const connection = await pool.getConnection();

    try {
      const stats = [];

      // Get counts for each symbol table
      for (const symbol of symbols) {
        try {
          // Check if table exists first to avoid errors
          const [tables] = await connection.query(
            `SHOW TABLES LIKE '${symbol}'`
          );

          if (tables.length > 0) {
            const [rows] = await connection.query(
              `SELECT 
                COUNT(*) as total_records,
                MIN(Timestamp) as oldest_timestamp,
                MAX(Timestamp) as newest_timestamp
              FROM ${symbol}`
            );

            const record = rows[0];

            stats.push({
              symbol,
              total_records: record.total_records,
              oldest_date: record.oldest_timestamp
                ? new Date(record.oldest_timestamp).toISOString()
                : null,
              newest_date: record.newest_timestamp
                ? new Date(record.newest_timestamp).toISOString()
                : null,
              has_data: record.total_records > 0,
            });
          } else {
            stats.push({
              symbol,
              total_records: 0,
              oldest_date: null,
              newest_date: null,
              has_data: false,
            });
          }
        } catch (error) {
          console.error(`Error getting stats for ${symbol}:`, error);
          stats.push({
            symbol,
            total_records: 0,
            oldest_date: null,
            newest_date: null,
            has_data: false,
            error: error.message,
          });
        }
      }

      // Calculate overall statistics
      const tablesWithData = stats.filter((s) => s.has_data).length;
      const totalRecords = stats.reduce((sum, s) => sum + s.total_records, 0);

      let oldestDate = null;
      let newestDate = null;

      stats.forEach((s) => {
        if (
          s.oldest_date &&
          (!oldestDate || new Date(s.oldest_date) < new Date(oldestDate))
        ) {
          oldestDate = s.oldest_date;
        }
        if (
          s.newest_date &&
          (!newestDate || new Date(s.newest_date) > new Date(newestDate))
        ) {
          newestDate = s.newest_date;
        }
      });

      res.json({
        symbol_stats: stats,
        overall_stats: {
          total_symbols: symbols.length,
          tables_with_data: tablesWithData,
          completion_percentage: Math.round(
            (tablesWithData / symbols.length) * 100
          ),
          total_records: totalRecords,
          date_range: {
            from: oldestDate,
            to: newestDate,
          },
        },
      });
    } finally {
      connection.release();
    }
  } catch (error) {
    console.error("Error retrieving progress data:", error);
    res.status(500).json({ error: "Failed to retrieve progress data" });
  }
});

// API endpoint to get logs
app.get("/api/logs", async (req, res) => {
  try {
    const logDirectory = "./logs";
    const files = await fs.readdir(logDirectory);

    // Sort by modification time (newest first)
    const sortedFiles = await Promise.all(
      files.map(async (file) => {
        const stats = await fs.stat(path.join(logDirectory, file));
        return { file, mtime: stats.mtime };
      })
    );

    sortedFiles.sort((a, b) => b.mtime - a.mtime);

    // Get the most recent log file
    if (sortedFiles.length > 0) {
      const latestLog = sortedFiles[0].file;
      const logContent = await fs.readFile(
        path.join(logDirectory, latestLog),
        "utf8"
      );

      // Return the latest 100 lines for brevity
      const lines = logContent.split("\n").filter((line) => line.trim() !== "");
      const recentLines = lines.slice(Math.max(0, lines.length - 100));

      res.json({
        log_file: latestLog,
        total_lines: lines.length,
        recent_lines: recentLines,
      });
    } else {
      res.json({ log_file: null, recent_lines: [] });
    }
  } catch (error) {
    console.error("Error retrieving logs:", error);
    res.status(500).json({ error: "Failed to retrieve logs" });
  }
});

// Start the server
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
