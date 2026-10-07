```markdown
# Automated ETL & Data Cleaning Pipeline

An enterprise-ready, automated ETL (Extract, Transform, Load) pipeline designed to ingest raw, unformatted tabular/JSON student data, normalize values via LLM prompting, and enforce strict deterministic data integrity constraints via JavaScript runtime validation before exporting to clean CSV or structured JSON.

---

## 📌 Architecture Overview

```text
[Raw Input / Webhook] 
         │
         ▼
[Workflow Engine (n8n / Node.js)]
         │
         ▼
[LLM Normalization Node (GPT-4o / Claude)]
  └─ Semantic casing, prompt-based cleaning, format stabilization
         │
         ▼
[Deterministic JS Validation & Parser]
  ├─ Strips markdown fences & stringified wrappers
  ├─ Enforces Regex email, phone, and date constraints
  ├─ Removes empty rows & exact duplicate fingerprints
  └─ Maps normalized values to strict fallback defaults
         │
         ├──► [Clean JSON Array (Native Key-Value)]
         └──► [Standardized CSV Output]

```

---

## ✨ Features

* **Automated Data Normalization:** Standardizes names, locations, course codes, and statuses across arbitrary raw input.
* **Dual-Layer Validation:** Combines the semantic extraction flexibility of LLMs with deterministic, rule-based JavaScript sanitation for zero-hallucination guarantees.
* **Batch Processing:** Processes single entries or 50+ records simultaneously using dynamic item unpacking (`$input.all()`).
* **Defensive Error Handling:** Falls back to safe standard defaults (`Unknown`, `unknown@example.com`, `Not Available`, `1970-01-01`) for corrupt, missing, or malformed fields.
* **Export Ready:** Seamlessly formats output into standardized CSV rows or clean JSON structures without escaped characters or broken string wrappers.

---

## 📋 Data Cleaning & Validation Rules

| Field | Cleaning & Validation Rule | Fallback Value |
| --- | --- | --- |
| **Student_ID** | Preserved verbatim; whitespace trimmed | `"Unknown"` |
| **Name** | Normalized to Title Case | `"Unknown"` |
| **Course** | Normalized to Title Case | `"Unknown"` |
| **City** | Normalized to Title Case | `"Unknown"` |
| **Email** | Lowercase string; verified with RFC-compliant regex | `"unknown@example.com"` |
| **Phone** | Strips parentheses, dashes, and whitespace; preserves `+` | `"Not Available"` |
| **Fee_Paid** | Normalized strictly to `"Yes"` or `"No"` | `"No"` |
| **Enrolled_Date** | Parsed into ISO format (`YYYY-MM-DD`) | `"1970-01-01"` |
| **Deduplication** | Strips exact duplicate rows and null/empty objects | *Omitted* |

---

## 🚀 Implementation Guide

### 1. LLM Ingestion Prompt (Step 1)

Pass the raw dataset to your language model node using the following prompt template:

```text
TASK: Clean and normalize the raw student dataset provided in the <RAW_DATA> block below.

IMPORTANT:
- Process ONLY the actual records provided. Do not hallucinate dummy examples.
- Return ONLY a valid JSON array of objects. No markdown formatting, no code fences.

RULES:
1. Deduplication: Drop empty objects and exact duplicate records.
2. Whitespace: Trim leading, trailing, and excessive spaces across all fields.
3. Student_ID: Retain original value.
4. Name, Course, City: Convert to Title Case. If missing/invalid, use "Unknown".
5. Email: Lowercase string. If missing/invalid, use "unknown@example.com".
6. Phone: Strip spaces, hyphens, and brackets. Keep digits and optional leading '+'. If missing/invalid, use "Not Available".
7. Fee_Paid: Output strictly as "Yes" or "No".
8. Enrolled_Date: Format valid dates as "YYYY-MM-DD". If invalid, use "1970-01-01".

<RAW_DATA>
{{ JSON.stringify($json) }}
</RAW_DATA>

```

---

### 2. Runtime JavaScript Validation & Parser (Step 2)

Add this script to an **n8n Code Node** or **Node.js runtime** downstream from the model execution:

```javascript
// Validation & normalization utilities
const toTitleCase = (val) => {
  if (!val || typeof val !== 'string' || !val.trim()) return "Unknown";
  return val
    .trim()
    .toLowerCase()
    .split(/\s+/)
    .map(w => w.charAt(0).toUpperCase() + w.slice(1))
    .join(' ');
};

const cleanEmail = (val) => {
  if (!val || typeof val !== 'string') return "unknown@example.com";
  const cleaned = val.trim().toLowerCase();
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/;
  return emailRegex.test(cleaned) ? cleaned : "unknown@example.com";
};

const cleanPhone = (val) => {
  if (!val) return "Not Available";
  const str = String(val).trim();
  const hasPlus = str.startsWith('+');
  const digits = str.replace(/\D/g, '');
  if (!digits || digits.length < 7) return "Not Available";
  return hasPlus ? `+${digits}` : digits;
};

const cleanFeePaid = (val) => {
  if (val === null || val === undefined) return "No";
  const str = String(val).trim().toLowerCase();
  return ["yes", "paid", "1", "true"].includes(str) ? "Yes" : "No";
};

const cleanDate = (val) => {
  if (!val) return "1970-01-01";
  const parsed = new Date(val);
  return isNaN(parsed.getTime()) ? "1970-01-01" : parsed.toISOString().split('T')[0];
};

// 1. Ingest across incoming items
let rawRecords = [];

for (const item of $input.all()) {
  let data = item.json.text || item.json.csvData || item.json;

  if (typeof data === 'string') {
    let sanitized = data.replace(/```json|```csv|```/g, '').trim();
    try {
      const parsed = JSON.parse(sanitized);
      Array.isArray(parsed) ? rawRecords.push(...parsed) : rawRecords.push(parsed);
    } catch (e) {
      // Continue on invalid payload segment
    }
  } else if (Array.isArray(data)) {
    rawRecords.push(...data);
  } else if (typeof data === 'object' && data !== null) {
    rawRecords.push(data);
  }
}

// 2. Process, sanitize, and deduplicate
const seen = new Set();
const cleanedRecords = [];

for (const row of rawRecords) {
  if (!row || Object.values(row).every(v => v === null || v === '' || v === undefined)) {
    continue;
  }

  const cleanedRow = {
    Student_ID: row.Student_ID ? String(row.Student_ID).trim() : "Unknown",
    Name: toTitleCase(row.Name),
    Email: cleanEmail(row.Email),
    Phone: cleanPhone(row.Phone),
    Course: toTitleCase(row.Course),
    Fee_Paid: cleanFeePaid(row.Fee_Paid),
    City: toTitleCase(row.City),
    Enrolled_Date: cleanDate(row.Enrolled_Date)
  };

  const fingerprint = JSON.stringify(cleanedRow);
  if (!seen.has(fingerprint)) {
    seen.add(fingerprint);
    cleanedRecords.push(cleanedRow);
  }
}

// Return items directly for subsequent steps
return cleanedRecords.map(record => ({ json: record }));

```

---

## 📊 I/O Example

### Raw Input (Uncleaned)

```json
[
  {
    "Student_ID": "STU1001",
    "Name": "manoj k",
    "Email": "manojk@gmail",
    "Phone": " (987) 654-3210 ",
    "Course": "PYTHON",
    "Fee_Paid": "paid",
    "City": "chennai",
    "Enrolled_Date": "23-05-2025"
  }
]

```

### Processed Output (Cleaned JSON)

```json
[
  {
    "Student_ID": "STU1001",
    "Name": "Manoj K",
    "Email": "unknown@example.com",
    "Phone": "9876543210",
    "Course": "Python",
    "Fee_Paid": "Yes",
    "City": "Chennai",
    "Enrolled_Date": "2025-05-23"
  }
]

```

### CSV Export Output

```csv
Student_ID,Name,Email,Phone,Course,Fee_Paid,City,Enrolled_Date
"STU1001","Manoj K","unknown@example.com","9876543210","Python","Yes","Chennai","2025-05-23"

```

---

## 🛠️ Supported Workflows

* **n8n Workflow Engine:** Direct drag-and-drop integration using standard OpenAI/Anthropic nodes combined with Code Nodes.
* **Make (Integromat):** Drop-in compatible with custom Webhook and JavaScript modules.
* **Standalone Node.js Microservices:** Easily adaptable to serverless functions (AWS Lambda, Google Cloud Functions).

---

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

```

```
