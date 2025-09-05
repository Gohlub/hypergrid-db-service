import bodyParser from "body-parser";
import express from "express";
import pg from "pg";

// Connect to the database using the DATABASE_URL environment
//   variable injected by Railway
const pool = new pg.Pool();

const app = express();
const port = process.env.PORT || 3333;

app.use(bodyParser.json());
app.use(bodyParser.raw({ type: "application/vnd.custom-type" }));
app.use(bodyParser.text({ type: "text/html" }));

app.get("/", async (req, res) => {
  const { rows } = await pool.query("SELECT NOW()");
  res.send(`Hello, World! The time from the DB is ${rows[0].now}`);
});

// Public endpoint to receive data from external services
app.post("/api/data", async (req, res) => {
  try {
    console.log("Received data:", JSON.stringify(req.body, null, 2));
    
    // Basic validation - ensure we have some data
    if (!req.body || Object.keys(req.body).length === 0) {
      return res.status(400).json({ 
        error: "No data provided",
        message: "Request body must contain data to store"
      });
    }

    // Create a simple data table if it doesn't exist
    await pool.query(`
      CREATE TABLE IF NOT EXISTS incoming_data (
        id SERIAL PRIMARY KEY,
        data JSONB NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        source_ip INET,
        user_agent TEXT
      )
    `);

    // Insert the received data into the database
    const insertQuery = `
      INSERT INTO incoming_data (data, source_ip, user_agent)
      VALUES ($1, $2, $3)
      RETURNING id, created_at
    `;
    
    const sourceIp = req.ip || req.connection.remoteAddress || req.socket.remoteAddress;
    const userAgent = req.get('User-Agent') || 'Unknown';
    
    const { rows } = await pool.query(insertQuery, [
      JSON.stringify(req.body),
      sourceIp,
      userAgent
    ]);

    console.log(`Data stored successfully with ID: ${rows[0].id}`);
    
    res.status(201).json({
      success: true,
      message: "Data received and stored successfully",
      id: rows[0].id,
      created_at: rows[0].created_at
    });

  } catch (error) {
    console.error("Error processing data:", error);
    res.status(500).json({
      error: "Internal server error",
      message: "Failed to process and store data"
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

app.listen(port, () => {
  console.log(`Example app listening at http://localhost:${port}`);
  console.log(`Data ingestion endpoint available at: http://localhost:${port}/api/data`);
});
