import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { DynamoDBClient, PutItemCommand } from "@aws-sdk/client-dynamodb";
import { Readable } from "stream";

const s3 = new S3Client();
const dynamo = new DynamoDBClient();

export const handler = async (event) => {
  console.log("Event received:", JSON.stringify(event));

  try {
    // 1. Get bucket and key from S3 event
    const record = event.Records[0];
    const bucket = record.s3.bucket.name;
    const key = decodeURIComponent(record.s3.object.key.replace(/\+/g, " "));

    console.log(`Processing file: ${key} from bucket: ${bucket}`);

    // 2. Get file content from S3
    const getObjectCommand = new GetObjectCommand({ Bucket: bucket, Key: key });
    const response = await s3.send(getObjectCommand);

    // Convert stream to text
    const streamToString = (stream) =>
      new Promise((resolve, reject) => {
        const chunks = [];
        stream.on("data", (chunk) => chunks.push(chunk));
        stream.on("error", reject);
        stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf-8")));
      });

    const fileContent = await streamToString(response.Body);

    // 3. Process text: count lines, words, characters
    const lineCount = fileContent.split(/\r?\n/).length;
    const wordCount = fileContent.trim().split(/\s+/).length;
    const charCount = fileContent.length;
    const preview = fileContent.slice(0, 100);

    // 4. Store results in DynamoDB
    const putItemCommand = new PutItemCommand({
      TableName: "FileProcessingResults",
      Item: {
        fileName: { S: key },
        lineCount: { N: lineCount.toString() },
        wordCount: { N: wordCount.toString() },
        charCount: { N: charCount.toString() },
        preview: { S: preview },
        processedAt: { S: new Date().toISOString() },
      },
    });

    await dynamo.send(putItemCommand);
    console.log("Data saved to DynamoDB successfully.");

    return {
      statusCode: 200,
      body: JSON.stringify({
        message: "File processed successfully",
        fileName: key,
        lineCount,
        wordCount,
        charCount,
      }),
    };
  } catch (error) {
    console.error("Error processing file:", error);
    throw error;
  }
};
