import { getHistoricalRates } from "dukascopy-node";
import mysql from "mysql2/promise";
import dotenv from "dotenv";
import fs from "fs/promises";
import path from "path";

// Load environment variables
dotenv.config();

// Configuration
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

const startDate = "2020-01-01";
const endDate = "2025-04-25";
const batchSize = 20;
const pauseBetweenBatchesMs = 500;
const dbBatchSize = 1000; // Number of records to insert in a single query
const logDirectory = "./logs";
const checkpointFile = "./checkpoint.json";

// Create pool instead of single connection
const pool = mysql.createPool({
  host: process.env.HOST,
  user: process.env.USER,
  password: process.env.PWD,
  database: process.env.DB,
  port: Number(process.env.PORT) || 3306,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
});

/**
 * Ensures a table exists for a given symbol
 * @param {mysql.Pool} pool - The database pool
 * @param {string} symbol - The forex/crypto symbol
 */
async function ensureTableExists(pool, symbol) {
  try {
    const createTable = `
      CREATE TABLE IF NOT EXISTS ${symbol} (
        Timestamp BIGINT NOT NULL,
        BidPrice FLOAT NOT NULL,
        AskPrice FLOAT NOT NULL,
        BidVolume FLOAT NOT NULL,
        AskVolume FLOAT NOT NULL,
        PRIMARY KEY (Timestamp)
      )
    `;

    await pool.query(createTable);
    console.log(`Table for ${symbol} ensured`);
  } catch (error) {
    console.error(`Error creating table for ${symbol}:`, error);
    throw error;
  }
}

/**
 * Upload data to database in batches
 * @param {mysql.Pool} pool - The database pool
 * @param {Array} data - Data to upload
 * @param {string} symbol - The forex/crypto symbol
 */
async function uploadData(pool, data, symbol) {
  if (!data || data.length === 0) {
    console.log(`No data to upload for ${symbol}`);
    return;
  }

  const connection = await pool.getConnection();

  try {
    // Ensure table exists
    await ensureTableExists(pool, symbol);

    // Insert data in batches
    const insertQuery = `
      INSERT INTO ${symbol} (Timestamp, BidPrice, AskPrice, BidVolume, AskVolume)
      VALUES ?
      ON DUPLICATE KEY UPDATE 
      BidPrice = VALUES(BidPrice),
      AskPrice = VALUES(AskPrice),
      BidVolume = VALUES(BidVolume),
      AskVolume = VALUES(AskVolume)
    `;

    // Process in batches to avoid packet size limits
    for (let i = 0; i < data.length; i += dbBatchSize) {
      const batch = data.slice(i, i + dbBatchSize);
      const values = batch.map((item) => [
        item.timestamp,
        item.bidPrice,
        item.askPrice,
        item.bidVolume,
        item.askVolume,
      ]);

      await connection.query(insertQuery, [values]);
      console.log(
        `Uploaded batch ${i / dbBatchSize + 1} for ${symbol} (${
          batch.length
        } records)`
      );
    }

    console.log(`Successfully uploaded ${data.length} records for ${symbol}`);
  } catch (error) {
    console.error(`Error uploading data for ${symbol}:`, error);
    throw error;
  } finally {
    connection.release();
  }
}

/**
 * Fetch historical data for a specific symbol and date range
 * @param {string} symbol - The forex/crypto symbol
 * @param {Date} fromDate - Start date
 * @param {Date} toDate - End date
 * @returns {Array} - The historical data
 */
async function fetchHistoricalData(symbol, fromDate, toDate) {
  try {
    console.log(
      `Fetching data for ${symbol} from ${fromDate.toISOString()} to ${toDate.toISOString()}`
    );

    const data = await getHistoricalRates({
      instrument: symbol,
      dates: {
        from: fromDate,
        to: toDate,
      },
      timeframe: "tick",
      volumeUnits: "units",
      ignoreFlats: false,
      batchSize: batchSize,
      pauseBetweenBatchesMs: pauseBetweenBatchesMs,
      format: "json",
    });

    console.log(`Retrieved ${data.length} records for ${symbol}`);
    return data;
  } catch (error) {
    console.error(`Error fetching data for ${symbol}:`, error);
    return [];
  }
}

/**
 * Save checkpoint information
 * @param {Date} currentDate - Current processing date
 * @param {string} lastSymbol - Last processed symbol
 */
async function saveCheckpoint(currentDate, lastSymbol) {
  try {
    const checkpoint = {
      date: currentDate.toISOString(),
      lastSymbol: lastSymbol,
    };

    await fs.writeFile(checkpointFile, JSON.stringify(checkpoint, null, 2));
    console.log(
      `Checkpoint saved: ${currentDate.toISOString()}, last symbol: ${lastSymbol}`
    );
  } catch (error) {
    console.error(`Error saving checkpoint:`, error);
  }
}

/**
 * Load the latest checkpoint if available
 * @returns {Object|null} - The checkpoint information or null
 */
async function loadCheckpoint() {
  try {
    const data = await fs.readFile(checkpointFile, "utf8");
    const checkpoint = JSON.parse(data);
    console.log(
      `Checkpoint loaded: ${checkpoint.date}, last symbol: ${checkpoint.lastSymbol}`
    );
    return checkpoint;
  } catch (error) {
    // If file doesn't exist or is invalid, return null
    console.log("No valid checkpoint found, starting from the beginning");
    return null;
  }
}

/**
 * Setup logging directory
 */
async function setupLogging() {
  try {
    await fs.mkdir(logDirectory, { recursive: true });

    // Redirect console output to log file
    const logFile = path.join(
      logDirectory,
      `data_import_${new Date().toISOString().replace(/:/g, "-")}.log`
    );

    // Create a write stream for logging
    const fsPromises = await import("fs/promises");
    const fs = await import("fs");
    const logStream = fs.createWriteStream(logFile, { flags: "a" });

    // Keep references to original console methods
    const originalConsoleLog = console.log;
    const originalConsoleError = console.error;

    // Override console methods to write to both stdout and log file
    console.log = function () {
      const args = Array.from(arguments);
      const message = args
        .map((arg) => (typeof arg === "object" ? JSON.stringify(arg) : arg))
        .join(" ");

      logStream.write(`[LOG] ${new Date().toISOString()} - ${message}\n`);
      originalConsoleLog.apply(console, args);
    };

    console.error = function () {
      const args = Array.from(arguments);
      const message = args
        .map((arg) => (typeof arg === "object" ? JSON.stringify(arg) : arg))
        .join(" ");

      logStream.write(`[ERROR] ${new Date().toISOString()} - ${message}\n`);
      originalConsoleError.apply(console, args);
    };

    // Log environment variables for debugging (excluding sensitive information)
    console.log("Environment variables:");
    console.log(`  HOST: ${process.env.HOST}`);
    console.log(`  USER: ${process.env.USER}`);
    console.log(`  DB: ${process.env.DB}`);
    console.log(`  PWD exists: ${process.env.PWD !== undefined}`);

    console.log("Logging setup complete");
  } catch (error) {
    console.error("Failed to set up logging:", error);
  }
}

/**
 * Main function to run the data import process
 */
async function main() {
  try {
    await setupLogging();
    console.log("Starting data import process");

    // Load checkpoint if available
    const checkpoint = await loadCheckpoint();
    let current = checkpoint ? new Date(checkpoint.date) : new Date(startDate);
    const end = new Date(endDate);

    // Initial symbol index based on checkpoint
    let startSymbolIndex = 0;
    if (checkpoint && checkpoint.lastSymbol) {
      const symbolIndex = symbols.indexOf(checkpoint.lastSymbol);
      if (symbolIndex !== -1) {
        startSymbolIndex = symbolIndex + 1;
        if (startSymbolIndex >= symbols.length) {
          // If we've completed all symbols for this date, move to next date
          startSymbolIndex = 0;
          current.setDate(current.getDate() + 1);
        }
      }
    }

    // Process data for each day and each symbol
    while (current <= end) {
      console.log(`Processing date: ${current.toISOString().split("T")[0]}`);

      for (let i = startSymbolIndex; i < symbols.length; i++) {
        const symbol = symbols[i];
        try {
          // Set next day as end date
          let nextDay = new Date(current);
          nextDay.setDate(current.getDate() + 1);

          // Fetch data
          const data = await fetchHistoricalData(symbol, current, nextDay);

          // Upload data if any was retrieved
          if (data.length > 0) {
            await uploadData(pool, data, symbol);
          }

          // Save checkpoint after each symbol is processed
          await saveCheckpoint(current, symbol);
        } catch (error) {
          console.error(
            `Failed to process ${symbol} for ${current.toISOString()}:`,
            error
          );
          // Continue with next symbol despite errors
          continue;
        }
      }

      // Reset start symbol index for next day
      startSymbolIndex = 0;

      // Move to next day
      current.setDate(current.getDate() + 1);
      console.log(
        `Completed processing for date: ${current.toISOString().split("T")[0]}`
      );
    }

    console.log("Data import process completed successfully");
  } catch (error) {
    console.error("Fatal error in data import process:", error);
  } finally {
    // Close the pool before exiting
    await pool.end();
    console.log("Database connection pool closed");
  }
}

// Add debug logging for environment variables before starting
console.log("Environment variables check:");
console.log(`HOST: ${process.env.HOST || "not set"}`);
console.log(`USER: ${process.env.USER || "not set"}`);
console.log(`DB: ${process.env.DB || "not set"}`);
console.log(`PWD: ${process.env.PWD ? "set (not showing value)" : "not set"}`);

// Check if critical environment variables are missing
if (
  !process.env.HOST ||
  !process.env.USER ||
  !process.env.PWD ||
  !process.env.DB
) {
  console.error("ERROR: Missing required environment variables!");
  console.error(
    "Please make sure your .env file contains HOST, USER, PWD, and DB variables"
  );
  process.exit(1);
}

// Run the main function
main().catch((error) => {
  console.error("Unhandled error in main function:", error);
  process.exit(1);
});
