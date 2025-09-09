import bodyParser from "body-parser";
import express from "express";
import pg from "pg";

// Connect to the database using the DATABASE_URL environment
//   variable injected by Railway
const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL,
});

const app = express();
const port = process.env.PORT || 3333;

// Initialize database tables on startup
async function initializeDatabase() {
  try {
    // Create hypergrid_events table for structured flow data
    await pool.query(`
      CREATE TABLE IF NOT EXISTS hypergrid_events (
        id SERIAL PRIMARY KEY,
        tx_hash VARCHAR(255) UNIQUE NOT NULL,
        provider VARCHAR(255) NOT NULL,
        provider_node VARCHAR(255) NOT NULL,
        source_node VARCHAR(255) NOT NULL,
        arg_count INTEGER NOT NULL,
        price_usdc DECIMAL(10, 6) NOT NULL,
        transferred_usdc DECIMAL(10, 6) NOT NULL,
        status VARCHAR(20) NOT NULL CHECK (status IN ('Success', 'Failed')),
        started_at TIMESTAMP WITH TIME ZONE NOT NULL,
        completed_at TIMESTAMP WITH TIME ZONE,
        total_duration_ms INTEGER,
        successful_attempt INTEGER NOT NULL,
        total_attempts INTEGER NOT NULL,
        response_size_bytes INTEGER,
        error_type VARCHAR(50) CHECK (error_type IN ('ProviderNotFound', 'PaymentValidationFailed', 'AllRetriesFailed', 'ApiCallFailed') OR error_type IS NULL),
        error_message TEXT,
        validation_error TEXT,
        payment_validated BOOLEAN NOT NULL DEFAULT false,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    console.log("Database tables initialized successfully");
  } catch (error) {
    console.error("Failed to initialize database:", error);
    process.exit(1); // Exit if database setup fails
  }
}

app.use(bodyParser.json());
app.use(bodyParser.raw({ type: "application/vnd.custom-type" }));
app.use(bodyParser.text({ type: "text/html" }));

app.get("/", async (req, res) => {
  const { rows } = await pool.query("SELECT NOW()");
  res.send(`Hello, World! The time from the DB is ${rows[0].now}`);
});

// Validation helper function for provider call flow data
function validateProviderCallFlow(data: any): { isValid: boolean; errors: string[] } {
  const errors: string[] = [];
  
  if (!data.tx_hash || typeof data.tx_hash !== 'string') {
    errors.push('tx_hash is required and must be a string');
  }
  
  if (!data.provider || typeof data.provider !== 'string') {
    errors.push('provider is required and must be a string');
  }
  
  if (!data.provider_node || typeof data.provider_node !== 'string') {
    errors.push('provider_node is required and must be a string');
  }
  
  if (!data.source_node || typeof data.source_node !== 'string') {
    errors.push('source_node is required and must be a string');
  }
  
  if (typeof data.arg_count !== 'number' || !Number.isInteger(data.arg_count)) {
    errors.push('arg_count is required and must be an integer');
  }
  
  if (typeof data.price_usdc !== 'number') {
    errors.push('price_usdc is required and must be a number');
  }
  
  if (typeof data.transferred_usdc !== 'number') {
    errors.push('transferred_usdc is required and must be a number');
  }
  
  if (!data.status || !['Success', 'Failed'].includes(data.status)) {
    errors.push('status is required and must be one of: Success, Failed');
  }
  
  if (!data.started_at || typeof data.started_at !== 'string') {
    errors.push('started_at is required and must be a valid RFC3339 timestamp string');
  }
  
  if (typeof data.successful_attempt !== 'number' || !Number.isInteger(data.successful_attempt)) {
    errors.push('successful_attempt is required and must be an integer');
  }
  
  if (typeof data.total_attempts !== 'number' || !Number.isInteger(data.total_attempts)) {
    errors.push('total_attempts is required and must be an integer');
  }
  
  if (typeof data.payment_validated !== 'boolean') {
    errors.push('payment_validated is required and must be a boolean');
  }
  
  // Optional fields validation
  if (data.error_type !== null && data.error_type !== undefined && 
      !['ProviderNotFound', 'PaymentValidationFailed', 'AllRetriesFailed', 'ApiCallFailed'].includes(data.error_type)) {
    errors.push('error_type must be one of: ProviderNotFound, PaymentValidationFailed, AllRetriesFailed, ApiCallFailed, or null');
  }
  
  return {
    isValid: errors.length === 0,
    errors
  };
}

// IP-restricted endpoint to receive data from authorized services
app.post("/api/data", async (req, res) => {
  try {
    // IP Whitelist Authentication
    const clientIp = req.ip || 
                     req.connection.remoteAddress || 
                     req.socket.remoteAddress ||
                     (req.headers['x-forwarded-for'] as string)?.split(',')[0]?.trim();
    
    const allowedIps = (process.env.ALLOWED_IPS || '').split(',').map(ip => ip.trim()).filter(Boolean);
    
    if (allowedIps.length === 0) {
      return res.status(500).json({
        error: "Server misconfiguration",
        message: "No allowed IPs configured"
      });
    }
    
    if (!clientIp || !allowedIps.includes(clientIp)) {
      console.log(`Rejected request from IP: ${clientIp}. Allowed IPs: ${allowedIps.join(', ')}`);
      return res.status(403).json({
        error: "Forbidden",
        message: "IP address not authorized"
      });
    }

    console.log("Received provider call flow data:", JSON.stringify(req.body, null, 2));
    
    // Basic validation - ensure we have some data
    if (!req.body || Object.keys(req.body).length === 0) {
      return res.status(400).json({ 
        error: "No data provided",
        message: "Request body must contain data to store"
      });
    }

    // Validate provider call flow structure
    const validation = validateProviderCallFlow(req.body);
    if (!validation.isValid) {
      return res.status(400).json({
        error: "Invalid data format",
        message: "Provider call flow data validation failed",
        details: validation.errors
      });
    }

    const flow = req.body;

    // Insert or update flow record in hypergrid_events table
    const upsertQuery = `
      INSERT INTO hypergrid_events (
        tx_hash, provider, provider_node, source_node, arg_count,
        price_usdc, transferred_usdc, status, started_at, completed_at,
        total_duration_ms, successful_attempt, total_attempts, 
        response_size_bytes, error_type, error_message, validation_error,
        payment_validated
      ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
      ) ON CONFLICT (tx_hash) DO UPDATE SET
        provider = EXCLUDED.provider,
        provider_node = EXCLUDED.provider_node,
        source_node = EXCLUDED.source_node,
        status = EXCLUDED.status,
        completed_at = EXCLUDED.completed_at,
        total_duration_ms = EXCLUDED.total_duration_ms,
        successful_attempt = EXCLUDED.successful_attempt,
        total_attempts = EXCLUDED.total_attempts,
        response_size_bytes = EXCLUDED.response_size_bytes,
        error_type = EXCLUDED.error_type,
        error_message = EXCLUDED.error_message,
        validation_error = EXCLUDED.validation_error,
        payment_validated = EXCLUDED.payment_validated,
        updated_at = NOW()
      RETURNING id, tx_hash, created_at, updated_at
    `;
    
    const { rows } = await pool.query(upsertQuery, [
      flow.tx_hash,
      flow.provider,
      flow.provider_node,
      flow.source_node,
      flow.arg_count,
      flow.price_usdc,
      flow.transferred_usdc,
      flow.status,
      flow.started_at,
      flow.completed_at || null,
      flow.total_duration_ms || null,
      flow.successful_attempt,
      flow.total_attempts,
      flow.response_size_bytes || null,
      flow.error_type || null,
      flow.error_message || null,
      flow.validation_error || null,
      flow.payment_validated
    ]);

    console.log(`Provider call flow stored/updated successfully for tx_hash: ${flow.tx_hash}`);
    
    res.status(200).json({
      success: true,
      tx_hash: flow.tx_hash
    });

  } catch (error) {
    console.error("Database error:", error);
    res.status(500).json({
      error: "Database persistence failed"
    });
  }
});

// Health check endpoint
app.get("/health", async (req, res) => {
  try {
    const { rows } = await pool.query("SELECT NOW()");
    res.json({
      status: "healthy",
      database: "connected",
      timestamp: rows[0].now
    });
  } catch (error) {
    console.error("Health check failed:", error);
    res.status(503).json({
      status: "unhealthy",
      database: "disconnected",
      error: error instanceof Error ? error.message : "Unknown error"
    });
  }
});

app.listen(port, async () => {
  console.log(`Example app listening at http://localhost:${port}`);
  console.log(`Data ingestion endpoint available at: http://localhost:${port}/api/data`);
  
  // Initialize database tables
  await initializeDatabase();
});
