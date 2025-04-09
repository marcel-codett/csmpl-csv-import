const ftp = require("basic-ftp");
const { parse } = require("csv-parse/sync");
const fs = require("fs");
const path = require("path");

class FTPHandler {
  constructor() {
    this.client = new ftp.Client();
    this.client.ftp.verbose = false;
    this.config = {
      host: "selfservice.radixpension.com",
      user: "zoho",
      password: process.env.FTP_PASSWORD,
    };
  }

  async connect() {
    await this.client.access(this.config);
    console.log("Connected to FTP server");
  }

  async disconnect() {
    this.client.close();
    console.log("Disconnected from FTP server");
  }

  async listCSVFiles() {
    const files = await this.client.list();
    return files
      .filter((file) => file.name.endsWith(".csv"))
      .sort((a, b) => a.name.localeCompare(b.name));
  }

  async downloadAndParseCSV(fileName) {
    const downloadDir = path.join(__dirname, "../downloads");
    if (!fs.existsSync(downloadDir))
      fs.mkdirSync(downloadDir, { recursive: true });

    const localPath = path.join(downloadDir, fileName);
    await this.client.downloadTo(localPath, fileName);

    const fileContent = fs.readFileSync(localPath, "utf-8");

    // Split lines and clean up
    const cleanedLines = fileContent
      .split("\n")
      .filter(
        (line) =>
          line.trim() !== "" && !line.toLowerCase().includes("rows affected")
      );

    const parsedRows = [];

    for (const [index, line] of cleanedLines.entries()) {
      const columns = line.split(",").map((col) => col.trim());
      const colCount = columns.length;

      let headers;
      if (colCount === 8) {
        headers = [
          "penId",
          "firstName",
          "middleName",
          "lastName",
          "phone",
          "email",
          "referenceId",
          "status",
        ];
      } else if (colCount === 6) {
        headers = [
          "penId",
          "fullName",
          "phone",
          "requestType",
          "email",
          "status",
        ];
      } else if (colCount === 5) {
        headers = ["penId", "fullName", "phone", "requestType", "status"];
      } else if (colCount === 7) {
        headers = [
          "penId",
          "fullName",
          "phone",
          "requestType",
          "email",
          "referenceId",
          "status",
        ];
      } else {
        console.warn(
          `Skipping row ${index + 1}: unexpected column count (${colCount})`
        );
        continue;
      }

      try {
        const record = parse(line, {
          columns: headers,
          skip_empty_lines: true,
          trim: true,
        })[0];

    // Safely extract first, middle, and last names
        let firstName = '';
        let middleName = '';
        let lastName = '';

        if (record.fullName) {
            const nameParts = record.fullName.trim().split(' ');
            firstName = nameParts[0] || '';
            if (nameParts.length === 2) {
                lastName = nameParts[1];
            } else if (nameParts.length >= 3) {
                middleName = nameParts.slice(1, -1).join(' ');
                lastName = nameParts[nameParts.length - 1];
            }
        } else {
            firstName = record.firstName || '';
            middleName = record.middleName || '';
            lastName = record.lastName || '';
        }

        const formatted = {
            penId: record.penId || '',
            fullName: record.fullName || `${firstName} ${middleName} ${lastName}`.trim(),
            firstName,
            middleName,
            lastName,
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
    if (files.length === 0) throw new Error("No CSV files found");
    const firstFile = files[0];
    return this.downloadAndParseCSV(firstFile.name);
  }

  async getLatestFileRecords() {
    const files = await this.listCSVFiles();
    if (files.length === 0) throw new Error("No CSV files found");
    const latestFile = files[files.length - 1];
    return this.downloadAndParseCSV(latestFile.name);
  }
}

module.exports = { FTPHandler };
