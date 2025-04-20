const apiUrl = "http://localhost:8080/api";

function getConfigData() {
    return {
        host: document.getElementById("host").value,
        port: parseInt(document.getElementById("port").value),
        database: document.getElementById("database").value,
        username: document.getElementById("username").value,
        password: document.getElementById("password").value,
        useHttps: document.getElementById("useHttps").checked
    };
}

function testConnection() {
    const config = getConfigData();

    fetch(`${apiUrl}/test-connection`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(config)
    })
    .then(res => res.json())
    .then(data => {
        document.getElementById("output").innerText = "Connection success: " + data;
    })
    .catch(err => {
        document.getElementById("output").innerText = "Error: " + err;
    });
}

function getTables() {
    const config = getConfigData();

    fetch(`${apiUrl}/tables`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(config)
    })
    .then(res => res.json())
    .then(data => {
        document.getElementById("output").innerText = "Tables: " + data.join(", ");
    })
    .catch(err => {
        document.getElementById("output").innerText = "Error: " + err;
    });
}
let selectedColumns = [];  // Array to store selected columns

// Function to get columns and open the modal
function getColumns() {
    const config = getConfigData();
    const tableName = document.getElementById("tableName").value.trim();

    if (!tableName) {
        alert("Please enter a valid table name.");
        return;
    }

    const request = { config, tableName };
    showLoading(true);

    fetch(`${apiUrl}/columns`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(request)
    })
    .then(res => res.json())
    .then(columns => {
        const list = document.getElementById("columnsList");
        list.innerHTML = '';  // Clear existing columns list

        columns.forEach(col => {
            const div = document.createElement("div");
            div.innerText = col;
            div.onclick = () => toggleColumnSelection(col);  // Select/deselect column on click
            list.appendChild(div);
        });

        // Show the modal
        document.getElementById("columnsModal").style.display = "block";
    })
    .catch(err => {
        console.error("Error:", err);
        document.getElementById("columnsList").innerText = "Error: " + err;
    })
    .finally(() => showLoading(false));
}

// Function to toggle column selection
function toggleColumnSelection(column) {
    if (selectedColumns.includes(column)) {
        selectedColumns = selectedColumns.filter(col => col !== column);  // Remove column if already selected
    } else {
        selectedColumns.push(column);  // Add column to selection
    }
    console.log("Selected Columns:", selectedColumns);
}

// Function to close the modal
function closeModal() {
    document.getElementById("columnsModal").style.display = "none";  // Hide the modal
}

// Function to preview CSV data
function previewCSV() {
    const config = getConfigData();
    const tableName = document.getElementById("tableName").value.trim();

    if (!tableName || selectedColumns.length === 0) {
        alert("Table name and columns are required.");
        return;
    }

    fetch("http://localhost:8080/api/preview", {
        method: "POST",
        headers: {
            "Content-Type": "application/json"
        },
        body: JSON.stringify({
            config: config,
            tableName: tableName,
            columns: selectedColumns
        })
    })
    .then(res => res.json())
    .then(data => {
        const previewDiv = document.getElementById("previewOutput");
        previewDiv.innerHTML = "<h3>Preview:</h3>";

        if (!data || !Array.isArray(data) || data.length === 0) {
            previewDiv.innerHTML += "<p>No data found.</p>";
            return;
        }

        let table = "<table border='1'><tr>";
        data[0].forEach(col => table += `<th>${col}</th>`);
        table += "</tr>";

        for (let i = 1; i < data.length; i++) {
            table += "<tr>";
            data[i].forEach(cell => table += `<td>${cell}</td>`);
            table += "</tr>";
        }

        table += "</table>";
        previewDiv.innerHTML += table;
    })
    .catch(err => {
        console.error(err);
        alert("Error previewing data.");
    });
}

function uploadCSV() {
    const config = getConfigData();
    const tableName = document.getElementById("tableName").value;
    const delimiter = document.getElementById("delimiter").value || ",";

    const fileInput = document.getElementById("csvFile");
    if (!fileInput.files.length) {
        alert("Please select a CSV file to upload.");
        return;
    }

    const reader = new FileReader();
    reader.onload = function () {
        const content = reader.result;

        // Save the CSV content to a temp file using backend logic (simulate path for now)
        const requestPayload = {
            config,
            tableName,
            delimiter,
            outputFilePath: "data.csv", // Simulated path; your backend reads directly
            csvContent: content
        };

        fetch(`${apiUrl}/csv-to-clickhouse`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(requestPayload)
        })
        .then(res => res.json())
        .then(data => {
            document.getElementById("output").innerText = `Rows inserted: ${data}`;
        })
        .catch(err => {
            document.getElementById("output").innerText = "Error: " + err;
        });
    };
    reader.readAsText(fileInput.files[0]);
}

function downloadCSV() {
    const tableName = document.getElementById("tableName").value.trim();

    if (!tableName) {
        alert("Please enter a table name to download.");
        return;
    }

    const config = {
        host: document.getElementById("host").value,
        port: document.getElementById("port").value,
        database: document.getElementById("database").value,
        username: document.getElementById("username").value,
        password: document.getElementById("password").value,
        useHttps: document.getElementById("useHttps").checked
    };

    // ✅ Get selected columns from the select box
    const selectedOptions = document.getElementById("selectedColumns").selectedOptions;
    const selectedColumns = Array.from(selectedOptions).map(option => option.value);

    fetch("http://localhost:8080/api/clickhouse/ingest-to-file", {
        method: "POST",
        headers: {
            "Content-Type": "application/json"
        },
        body: JSON.stringify({
            config: config,
            tableName: tableName,
            columns: selectedColumns, // ✅ Send selected columns
            delimiter: ","
        })
    })
    .then(response => {
        if (!response.ok) throw new Error("Network response was not ok");
        return response.blob();
    })
    .then(blob => {
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = `${tableName}.csv`;
        document.body.appendChild(a);
        a.click();
        a.remove();
    })
    .catch(error => {
        console.error("Error:", error);
        alert("Failed to download file: " + error.message);
    });
}

function getColumns() {
    const config = getConfigData();
    const tableName = document.getElementById("tableName").value.trim();
    console.log("Table Name:", tableName);  // Debugging line

    if (!tableName) {
        alert("Please enter a valid table name.");
        return;
    }

    const request = { config, tableName };
    showLoading(true);

    // Fetch the columns from the backend
    fetch(`${apiUrl}/columns`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(request)
    })
    .then(res => res.json())
    .then(columns => {
        console.log("Columns fetched:", columns);  // Check if columns are returned
        
        const list = document.getElementById("columnsList");
        list.innerHTML = `<b>Columns:</b> ${columns.join(", ")}`;

        const selectBox = document.getElementById("selectedColumns");
        selectBox.innerHTML = ""; // Clear any existing options

        columns.forEach(col => {
            const option = document.createElement("option");
            option.value = col;
            option.text = col;
            selectBox.appendChild(option);
        });
    })
    .catch(err => {
        console.error("Error:", err);  // Log error for debugging
        document.getElementById("columnsList").innerText = "Error: " + err;
    })
    .finally(() => showLoading(false));
}


// Handle column selection
function getSelectedColumns() {
    const selectedOptions = document.getElementById("selectedColumns").selectedOptions;
    const selectedColumns = Array.from(selectedOptions).map(option => option.value);
    return selectedColumns;
}

// Modify downloadCSV to handle selected columns
function downloadCSV() {
    // Get table name and validate it
    const tableName = document.getElementById("tableName").value.trim();
    if (!tableName) {
        alert("Please enter a table name to download.");
        return;
    }

    // Get the selected columns from the dropdown
    const selectedColumns = getSelectedColumns(); 
    if (selectedColumns.length === 0) {
        alert("Please select at least one column.");
        return;
    }

    // Define the config object
    const config = getConfigData();

    // Send the request to the backend to download the CSV with selected columns
    fetch("http://localhost:8080/api/clickhouse/ingest-to-file", {
        method: "POST",
        headers: {
            "Content-Type": "application/json"
        },
        body: JSON.stringify({
            config: config,
            tableName: tableName,
            columns: selectedColumns, // Send only selected columns
            delimiter: "," // Optional: specify the delimiter
        })
    })
    .then(response => {
        if (!response.ok) throw new Error("Network response was not ok");
        return response.blob();  // Key part: the response is treated as a file (blob)
    })
    .then(blob => {
        // Create a temporary download link for the CSV file
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = `${tableName}.csv`;  // Use the table name as the file name
        document.body.appendChild(a);
        a.click();
        a.remove();
    })
    .catch(error => {
        console.error("Error:", error);
        alert("Failed to download file: " + error.message);
    });
}


function showLoading(isLoading) {
    const loadingElement = document.getElementById("loadingIndicator");
    
    if (isLoading) {
        loadingElement.style.display = "block"; // Show the loading indicator
    } else {
        loadingElement.style.display = "none"; // Hide the loading indicator
    }
}

function previewCSV() {
    const config = getConfigData();
    const tableName = document.getElementById("tableName").value.trim();
    const selectedColumns = getSelectedColumns();
    const joinQuery = document.getElementById("joinQuery").value.trim();

    if (!tableName) {
        alert("Table name is required.");
        return;
    }

    fetch("http://localhost:8080/api/preview", {
        method: "POST",
        headers: {
            "Content-Type": "application/json"
        },
        body: JSON.stringify({
            config: config,
            tableName: tableName,
            columns: selectedColumns,
            joinQuery: joinQuery
        })
    })
    .then(res => res.json())
    .then(data => {
        const previewDiv = document.getElementById("previewOutput");
        previewDiv.innerHTML = "<h3>Preview:</h3>";

        if (!data || !Array.isArray(data) || data.length === 0) {
            previewDiv.innerHTML += "<p>No data found.</p>";
            return;
        }

        let table = "<table border='1'><tr>";
        data[0].forEach(col => table += `<th>${col}</th>`);
        table += "</tr>";

        for (let i = 1; i < data.length; i++) {
            table += "<tr>";
            data[i].forEach(cell => table += `<td>${cell}</td>`);
            table += "</tr>";
        }

        table += "</table>";
        previewDiv.innerHTML += table;
    })
    .catch(err => {
        console.error(err);
        alert("Error previewing data.");
    });
}


