// aws-sdk-java v2 smoke test. Env: S3_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY.

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

public class Smoke {
    static void check(boolean cond, String msg) {
        if (!cond) throw new AssertionError(msg);
    }

    static String env(String k, String def) {
        String v = System.getenv(k);
        return v == null || v.isEmpty() ? def : v;
    }

    public static void main(String[] args) {
        try {
            run();
        } catch (Throwable e) {
            System.err.println("FAIL java: " + e);
            System.exit(1);
        }
        System.out.println("PASS java");
    }

    static void run() throws Exception {
        var endpoint = URI.create(env("S3_ENDPOINT", "http://localhost:9000"));
        var creds = StaticCredentialsProvider.create(AwsBasicCredentials.create(
                env("AWS_ACCESS_KEY_ID", "test"), env("AWS_SECRET_ACCESS_KEY", "test")));
        var region = Region.US_EAST_1;
        var rnd = new SecureRandom();
        var suffix = new byte[6];
        rnd.nextBytes(suffix);
        var bucket = "gb-smoke-java-" + HexFormat.of().formatHex(suffix);
        var small = "hello, garbage bin\n".getBytes();
        var big = new byte[20 * 1024 * 1024];
        rnd.nextBytes(big);

        try (var s3 = S3Client.builder().endpointOverride(endpoint).region(region)
                        .credentialsProvider(creds).forcePathStyle(true).build();
                // The multipart-enabled async client switches to multipart above its threshold.
                var s3async = S3AsyncClient.builder().endpointOverride(endpoint).region(region)
                        .credentialsProvider(creds).forcePathStyle(true).multipartEnabled(true).build();
                var presigner = S3Presigner.builder().endpointOverride(endpoint).region(region)
                        .credentialsProvider(creds)
                        .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                        .build()) {

            s3.createBucket(b -> b.bucket(bucket));

            s3.putObject(b -> b.bucket(bucket).key("dir/a/small.txt"), RequestBody.fromBytes(small));

            s3async.putObject(b -> b.bucket(bucket).key("dir/b/big.bin"), AsyncRequestBody.fromBytes(big)).join();
            var head = s3.headObject(b -> b.bucket(bucket).key("dir/b/big.bin"));
            check(head.eTag().contains("-"), "big object ETag is not multipart: " + head.eTag());
            check(head.contentLength() == big.length, "big object size " + head.contentLength());
            var got = s3.getObjectAsBytes(b -> b.bucket(bucket).key("dir/b/big.bin")).asByteArray();
            check(Arrays.equals(got, big), "big object body mismatch");

            var list = s3.listObjectsV2(b -> b.bucket(bucket).prefix("dir/").delimiter("/"));
            var prefixes = list.commonPrefixes().stream().map(CommonPrefix::prefix).sorted().toList();
            check(prefixes.equals(List.of("dir/a/", "dir/b/")), "CommonPrefixes: " + prefixes);
            check(list.contents().isEmpty(), "unexpected Contents: " + list.contents());

            var range = s3.getObjectAsBytes(b -> b.bucket(bucket).key("dir/b/big.bin").range("bytes=100-199"));
            check(("bytes 100-199/" + big.length).equals(range.response().contentRange()),
                    "Content-Range: " + range.response().contentRange());
            check(Arrays.equals(range.asByteArray(), Arrays.copyOfRange(big, 100, 200)), "range get bytes mismatch");

            s3.copyObject(b -> b.sourceBucket(bucket).sourceKey("dir/a/small.txt")
                    .destinationBucket(bucket).destinationKey("copy/small.txt"));
            var copied = s3.getObjectAsBytes(b -> b.bucket(bucket).key("copy/small.txt")).asByteArray();
            check(Arrays.equals(copied, small), "copied object mismatch");

            var url = presigner.presignGetObject(p -> p.signatureDuration(Duration.ofMinutes(5))
                    .getObjectRequest(g -> g.bucket(bucket).key("copy/small.txt"))).url();
            try (var http = HttpClient.newHttpClient()) {
                var resp = http.send(HttpRequest.newBuilder(url.toURI()).build(),
                        HttpResponse.BodyHandlers.ofByteArray());
                check(resp.statusCode() == 200, "presigned get status " + resp.statusCode());
                check(Arrays.equals(resp.body(), small), "presigned get mismatch");
            }

            var ids = List.of("dir/a/small.txt", "dir/b/big.bin", "copy/small.txt").stream()
                    .map(k -> ObjectIdentifier.builder().key(k).build()).toList();
            var del = s3.deleteObjects(b -> b.bucket(bucket).delete(d -> d.objects(ids)));
            check(del.errors().isEmpty() && del.deleted().size() == 3,
                    "DeleteObjects: deleted " + del.deleted() + ", errors " + del.errors());
            check(s3.listObjectsV2(b -> b.bucket(bucket)).keyCount() == 0, "bucket not empty after batch delete");

            s3.deleteBucket(b -> b.bucket(bucket));
            try {
                s3.headBucket(b -> b.bucket(bucket));
                check(false, "bucket still exists after delete");
            } catch (NoSuchBucketException expected) {
                // Deleted.
            }
        }
    }
}
