# Bidirectional ClickHouse & Flat File Ingestion Tool

This project is a web-based tool that enables bidirectional data ingestion between a **ClickHouse database** and **CSV flat files**. It supports JWT-based authentication for ClickHouse, lets users choose specific columns for ingestion, and displays the total number of records processed after each operation.

---

## 🔧 Features

- ✅ Ingest data from **ClickHouse to Flat File (CSV)**
- ✅ Ingest data from **Flat File (CSV) to ClickHouse**
- ✅ JWT-based authentication for ClickHouse
- ✅ UI-based configuration of source details and column selection
- ✅ Record count display after successful ingestion
- ✅ Basic error handling and user feedback

---

## 🛠 Tech Stack

- **Backend:** Java 21, Spring Boot
- **Frontend:** HTML, CSS, JavaScript (Vanilla)
- **Database:** ClickHouse (via Docker)
- **Authentication:** JWT Token (for ClickHouse)

---

## 🚀 Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/your-username/Ingestion-too.git
cd Ingestion-tool

2. Setup ClickHouse via Docker
bash
Copy code
docker run -d --name clickhouse-server --ulimit nofile=262144:262144 -p 8123:8123 -p 9000:9000 clickhouse/clickhouse-server
Load example datasets (uk_price_paid, ontime) into ClickHouse manually or using the provided scripts if any.

3. Run the Spring Boot backend
cd Ingestion-tool 
mvn  spring-boot:run -DskipTests
4. Open the Frontend
Open zeotap-frontend/index.html in your web browser directly.

📂 Project Modules
ClickHouse to Flat File
Connect using provided credentials and JWT token.
Select table and columns.
Download filtered CSV.
Flat File to ClickHouse
Upload a CSV file.
Map selected columns.
Insert into ClickHouse as a new table.

📌 Test Cases Covered
ClickHouse (single table) → Flat File (selected columns)
Flat File (CSV upload) → ClickHouse table (selected columns)
Error cases like invalid JWT, unreachable DB, etc.
(Optional) Preview and progress bar features are not implemented.

📄 AI Tool Usage:
AI tools were used to assist with:

Spring Boot backend scaffolding
HTML/JS frontend logic
Docker setup for ClickHouse

All prompt logs are maintained in prompts.txt.

🧾 Deliverables
✅ Full source code (backend + frontend)
✅ README.md (this file)
✅ prompts.txt (AI prompts used)

📎 License
Licensed under the Apache License 2.0
MIT



