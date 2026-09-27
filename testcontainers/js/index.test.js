import { test } from "node:test";
import assert from "node:assert/strict";
import { S3Client, PutObjectCommand, GetObjectCommand } from "@aws-sdk/client-s3";
import { GarbageBinContainer } from "./index.js";

test("put and get", async () => {
  const gb = await new GarbageBinContainer(process.env.GB_TEST_IMAGE ?? "garbagebin:latest")
    .withInitBuckets("tc-bucket")
    .withCredentials("gbaccess", "gbsecret123")
    .withEnvironment({ S3_STRICT_AUTH: "true" }) // proves the credentials reach the server
    .start();
  try {
    const s3 = new S3Client({
      endpoint: gb.endpoint(),
      region: "us-east-1",
      forcePathStyle: true,
      credentials: { accessKeyId: gb.accessKey(), secretAccessKey: gb.secretKey() },
    });
    await s3.send(new PutObjectCommand({ Bucket: "tc-bucket", Key: "hello.txt", Body: "hello" }));
    const out = await s3.send(new GetObjectCommand({ Bucket: "tc-bucket", Key: "hello.txt" }));
    assert.equal(await out.Body.transformToString(), "hello");
  } finally {
    await gb.stop();
  }
});
