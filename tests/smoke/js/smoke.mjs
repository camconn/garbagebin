// aws-sdk-js v3 smoke test. Env: S3_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY.
import { randomBytes } from "node:crypto";
import {
  S3Client,
  CreateBucketCommand,
  PutObjectCommand,
  HeadObjectCommand,
  GetObjectCommand,
  ListObjectsV2Command,
  CopyObjectCommand,
  DeleteObjectsCommand,
  DeleteBucketCommand,
  HeadBucketCommand,
} from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

function check(cond, msg) {
  if (!cond) throw new Error(msg);
}

const s3 = new S3Client({
  endpoint: process.env.S3_ENDPOINT || "http://localhost:9000",
  region: "us-east-1",
  forcePathStyle: true,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || "test",
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || "test",
  },
});
const Bucket = `gb-smoke-js-${randomBytes(6).toString("hex")}`;
const small = Buffer.from("hello, garbage bin\n");
const big = randomBytes(20 * 1024 * 1024);
const bytesOf = async (resp) => Buffer.from(await resp.Body.transformToByteArray());

async function main() {
  await s3.send(new CreateBucketCommand({ Bucket }));

  await s3.send(new PutObjectCommand({ Bucket, Key: "dir/a/small.txt", Body: small }));

  // lib-storage switches to multipart above its part size.
  await new Upload({ client: s3, params: { Bucket, Key: "dir/b/big.bin", Body: big } }).done();
  const head = await s3.send(new HeadObjectCommand({ Bucket, Key: "dir/b/big.bin" }));
  check(head.ETag.includes("-"), `big object ETag is not multipart: ${head.ETag}`);
  check(head.ContentLength === big.length, `big object size ${head.ContentLength}`);
  const got = await bytesOf(await s3.send(new GetObjectCommand({ Bucket, Key: "dir/b/big.bin" })));
  check(got.equals(big), "big object body mismatch");

  const list = await s3.send(new ListObjectsV2Command({ Bucket, Prefix: "dir/", Delimiter: "/" }));
  const prefixes = (list.CommonPrefixes ?? []).map((p) => p.Prefix).sort();
  check(JSON.stringify(prefixes) === '["dir/a/","dir/b/"]', `CommonPrefixes: ${prefixes}`);
  check(!list.Contents?.length, `unexpected Contents: ${JSON.stringify(list.Contents)}`);

  const range = await s3.send(new GetObjectCommand({ Bucket, Key: "dir/b/big.bin", Range: "bytes=100-199" }));
  check(range.ContentRange === `bytes 100-199/${big.length}`, `Content-Range: ${range.ContentRange}`);
  check((await bytesOf(range)).equals(big.subarray(100, 200)), "range get bytes mismatch");

  await s3.send(new CopyObjectCommand({ Bucket, Key: "copy/small.txt", CopySource: `${Bucket}/dir/a/small.txt` }));
  const copied = await bytesOf(await s3.send(new GetObjectCommand({ Bucket, Key: "copy/small.txt" })));
  check(copied.equals(small), "copied object mismatch");

  const url = await getSignedUrl(s3, new GetObjectCommand({ Bucket, Key: "copy/small.txt" }), { expiresIn: 300 });
  const resp = await fetch(url);
  const body = Buffer.from(await resp.arrayBuffer());
  check(resp.status === 200, `presigned get status ${resp.status}: ${body}`);
  check(body.equals(small), "presigned get mismatch");

  const keys = ["dir/a/small.txt", "dir/b/big.bin", "copy/small.txt"];
  const del = await s3.send(
    new DeleteObjectsCommand({ Bucket, Delete: { Objects: keys.map((Key) => ({ Key })) } }),
  );
  check(!del.Errors?.length && del.Deleted?.length === 3, `DeleteObjects: ${JSON.stringify(del)}`);
  const left = await s3.send(new ListObjectsV2Command({ Bucket }));
  check(left.KeyCount === 0, "bucket not empty after batch delete");

  await s3.send(new DeleteBucketCommand({ Bucket }));
  const gone = await s3.send(new HeadBucketCommand({ Bucket })).then(
    () => false,
    (e) => e.$metadata?.httpStatusCode === 404,
  );
  check(gone, "bucket still exists after delete");
}

try {
  await main();
  console.log("PASS js");
} catch (e) {
  console.error(`FAIL js: ${e.name}: ${e.message}`);
  process.exit(1);
}
