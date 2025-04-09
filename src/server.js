const express = require('express');
const cors = require('cors');
const { FTPHandler } = require('./ftpHandler');

const app = express();
const port = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(express.json());

// Routes
app.get('/api/csv/first', async (req, res) => {
    const ftpHandler = new FTPHandler();
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 2000;
        const start = (page - 1) * limit;
        const end = start + limit;

        await ftpHandler.connect();
        const allRecords = await ftpHandler.getFirstFileRecords();
        await ftpHandler.disconnect();

        const total = allRecords.length;
        const pageRecords = allRecords.slice(start, end);

       
          res.json({
            success: true,
            total,
            page,
            limit,
            records: pageRecords
        });
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: err.message });
    }
});
app.get('/api/csv/latest', async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 2000;
        const start = (page - 1) * limit;
        const end = start + limit;
 
        const ftpHandler = new FTPHandler();
        await ftpHandler.connect();
        
        const csvData = await ftpHandler.getLatestFileRecords();
        await ftpHandler.disconnect();

        const total = csvData.length;
        const pageRecords = csvData.slice(start, end);

        if (!csvData) {
            return res.status(404).json({ error: 'No CSV file found' });
        }

        res.json({
            success: true,
            total,
            page,
            limit,
            records: pageRecords
        });
    } catch (error) {
        console.error('Error:', error);
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/csv/files', async (req, res) => {
    try {
        const ftpHandler = new FTPHandler();
        await ftpHandler.connect();
        
        const files = await ftpHandler.listCSVFiles();
        await ftpHandler.disconnect();

        res.json({
            success: true,
            files
        });
    } catch (error) {
        console.error('Error:', error);
        res.status(500).json({ error: error.message });
    }
});

// Start server
app.listen(port, () => {
    console.log(`Server is running on port ${port}`);
});