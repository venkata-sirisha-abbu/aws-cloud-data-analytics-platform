import { S3Client, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import csv from "csv-parser";
import XLSX from "xlsx";
import { Parser } from "json2csv";
import OpenAI from "openai";
import { Readable } from "stream";

const s3Client = new S3Client({ region: process.env.AWS_REGION });
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

const INPUT_BUCKET = process.env.INPUT_BUCKET;
const OUTPUT_BUCKET = process.env.OUTPUT_BUCKET;

// ---------- Helpers ----------
async function parseCsvFromS3(body) {
  const rows = [];
  return new Promise((resolve, reject) => {
    Readable.from(body.toString())
      .pipe(csv())
      .on("data", (data) => rows.push(data))
      .on("end", () => resolve(rows))
      .on("error", reject);
  });
}

async function parseXlsxFromS3(body) {
  const workbook = XLSX.read(body, { type: "buffer" });
  const sheetName = workbook.SheetNames[0];
  return XLSX.utils.sheet_to_json(workbook.Sheets[sheetName]);
}

function buildProfile(rows) {
  const numericCols = {};
  let rowCount = rows.length;

  if (rowCount === 0) return { rowCount: 0, numericCols };

  const sample = rows[0];
  const cols = Object.keys(sample);

  cols.forEach((col) => {
    const nums = rows.map((r) => parseFloat(r[col])).filter((n) => !isNaN(n));
    if (nums.length > 0) {
      const sum = nums.reduce((a, b) => a + b, 0);
      const avg = sum / nums.length;
      const min = Math.min(...nums);
      const max = Math.max(...nums);
      numericCols[col] = { count: nums.length, sum, avg, min, max };
    }
  });

  return { rowCount, numericCols };
}

async function generateAiSummary(rows, profile) {
  const prompt = `
You are a data analyst. Summarize this dataset in plain English.
Dataset has ${profile.rowCount} rows.
Numeric columns and their stats:
${JSON.stringify(profile.numericCols, null, 2)}
Provide a concise readable analysis with trends, anomalies, and key findings.
  `;
  const res = await openai.chat.completions.create({
    model: "gpt-4o-mini",
    messages: [{ role: "user", content: prompt }],
  });
  return res.choices[0].message.content.trim();
}

async function generateAiJsonSummary(profile) {
  const prompt = `
Summarize this dataset as JSON for BI dashboards.
Include insights, trends, anomalies, recommendations.
Dataset stats:
${JSON.stringify(profile, null, 2)}
Output JSON only.
  `;
  const res = await openai.chat.completions.create({
    model: "gpt-4o-mini",
    messages: [{ role: "user", content: prompt }],
    response_format: { type: "json_object" },
  });
  return res.choices[0].message.content.trim();
}

// ---------- Lambda Handler ----------
export const handler = async (event) => {
  console.log("🚀 Event received:", JSON.stringify(event));

  const record = event.Records[0];
  const key = decodeURIComponent(record.s3.object.key.replace(/\+/g, " "));
  const bucket = record.s3.bucket.name;
  console.log(`📂 File received: ${key} from ${bucket}`);

  try {
    // 1. Read file
    const obj = await s3Client.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
    const body = await obj.Body.transformToByteArray();

    let rows;
    if (key.endsWith(".csv")) {
      rows = await parseCsvFromS3(Buffer.from(body));
    } else if (key.endsWith(".xlsx")) {
      rows = await parseXlsxFromS3(Buffer.from(body));
    } else {
      throw new Error("Unsupported file type");
    }

    console.log(`✅ Parsed ${rows.length} rows`);

    // 2. Profile dataset
    const profile = buildProfile(rows);
    console.log("📊 Profile:", JSON.stringify(profile));

    // 3. AI summaries
    const textSummary = await generateAiSummary(rows, profile);
    const jsonSummary = await generateAiJsonSummary(profile);

    // 4. Power BI CSV summary
    console.log("📊 Building Power BI CSV summary...");
    const csvRows = Object.entries(profile.numericCols).map(([col, stats]) => ({
      Column: col,
      Count: stats.count,
      Sum: stats.sum,
      Avg: stats.avg,
      Min: stats.min,
      Max: stats.max,
    }));

    // Add row count as special row
    csvRows.unshift({
      Column: "__ROWCOUNT__",
      Count: profile.rowCount,
      Sum: "",
      Avg: "",
      Min: "",
      Max: "",
    });

    const csvParser = new Parser({ fields: ["Column", "Count", "Sum", "Avg", "Min", "Max"] });
    const csvSummary = csvParser.parse(csvRows);

    // 5. Save outputs to OUTPUT_BUCKET
    const baseName = key.split("/").pop().split(".")[0];
    const outputs = [
      { ext: "txt", body: textSummary },
      { ext: "json", body: jsonSummary },
      { ext: "csv", body: csvSummary },
    ];

    for (const out of outputs) {
      const outKey = `summaries/${baseName}.${out.ext}`;
      await s3Client.send(
        new PutObjectCommand({
          Bucket: OUTPUT_BUCKET,
          Key: outKey,
          Body: out.body,
        })
      );
      console.log(`💾 Saved ${outKey} to ${OUTPUT_BUCKET}`);
    }

    return { statusCode: 200, body: "Summaries generated successfully" };
  } catch (err) {
    console.error("❌ Error:", err);
    throw err;
  }
};
