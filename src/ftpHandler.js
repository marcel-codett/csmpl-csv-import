const ftp = require('basic-ftp');
const { parse } = require('csv-parse/sync');
const fs = require('fs');
const path = require('path');

class FTPHandler {
    constructor() {
        this.client = new ftp.Client();
        this.client.ftp.verbose = false;
        this.config = {
            host: 'selfservice.radixpension.com',
            user: 'zoho',
            password: process.env.FTP_PASSWORD,
        };
    }

    async connect() {
        await this.client.access(this.config);
        console.log('Connected to FTP server');
    }

    async disconnect() {
        this.client.close();
        console.log('Disconnected from FTP server');
    }

    async listCSVFiles() {
        const files = await this.client.list();
        return files.filter(file => file.name.endsWith('.csv')).sort((a, b) =>
            a.name.localeCompare(b.name)
        );
    }

   
    async downloadAndParseCSV(fileName) {
        const downloadDir = path.join(__dirname, '../downloads');
        if (!fs.existsSync(downloadDir)) fs.mkdirSync(downloadDir, { recursive: true });

        const localPath = path.join(downloadDir, fileName);
        await this.client.downloadTo(localPath, fileName);

        const fileContent = fs.readFileSync(localPath, 'utf-8');

        // Split lines and clean up
        const cleanedLines = fileContent
            .split('\n')
            .filter(line => line.trim() !== '' && !line.toLowerCase().includes('rows affected'));

        const parsedRows = [];

        for (const [index, line] of cleanedLines.entries()) {
            const columns = line.split(',').map(col => col.trim());
            const colCount = columns.length;

            let headers;
            if (colCount === 8) {
                headers = ['penId', 'firstName', 'middleName', 'lastName', 'phone', 'email', 'referenceId', 'status'];
            } else if (colCount === 6) {
                headers = ['penId', 'fullName', 'phone', 'requestType', 'email', 'status'];
            } else if (colCount === 5) {
                headers = ['penId', 'fullName', 'phone', 'requestType', 'status'];
            } 
            else if (colCount === 7) {
                headers = ['penId', 'fullName', 'phone', 'requestType', 'email', 'referenceId', 'status'];
            } 
            else {
                console.warn(`Skipping row ${index + 1}: unexpected column count (${colCount})`);
                continue;
            }

            try {
                const record = parse(line, {
                    columns: headers,
                    skip_empty_lines: true,
                    trim: true
                })[0];

                const formatted = {
                    penId: record.penId || '',
                    fullName: record.fullName || `${record.firstName || ''} ${record.middleName || ''} ${record.lastName || ''}`.trim(),
                    firstName: record.firstName || `${record.fullName.split(' ')[0]}`|| '',
                    middleName: record.middleName || `${record.fullName.split(' ')[1]}`|| '',
                    lastName: record.lastName || record.fullName.split(' ').length == 2 ? `${record.fullName.split(' ')[1]}` : `${record.fullName.split(' ')[2]}`|| '',
                    phone: record.phone || '',
                    requestType: record.requestType || '',
                    email: record.email || '',
                    status: record.status || '',
                    referenceId: record.referenceId || `REF${Date.now()}-${index}`
                };

                parsedRows.push(formatted);
            } catch (error) {
                console.warn(`Failed to parse row ${index + 1}:`, error.message);
            }
        }

        return parsedRows;
    }


    async getFirstFileRecords() {
        const files = await this.listCSVFiles();
        if (files.length === 0) throw new Error('No CSV files found');
        const firstFile = files[0];
        return this.downloadAndParseCSV(firstFile.name);
    }

    async getLatestFileRecords() {
        const files = await this.listCSVFiles();
        if (files.length === 0) throw new Error('No CSV files found');
        const latestFile = files[files.length - 1];
        return this.downloadAndParseCSV(latestFile.name);
    }
}

module.exports = { FTPHandler };
