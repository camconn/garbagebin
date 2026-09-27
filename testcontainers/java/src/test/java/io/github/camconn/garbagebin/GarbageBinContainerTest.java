package io.github.camconn.garbagebin;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

class GarbageBinContainerTest {
    @Test
    void putGet() {
        try (var gb = new GarbageBinContainer(System.getenv("GB_TEST_IMAGE"))
                .withInitBuckets("tc-bucket")
                .withCredentials("gbaccess", "gbsecret123")
                .withEnv("S3_STRICT_AUTH", "true")) { // proves the credentials reach the server
            gb.start();
            try (var s3 = S3Client.builder()
                    .endpointOverride(URI.create(gb.getEndpoint()))
                    .region(Region.US_EAST_1)
                    .forcePathStyle(true)
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(gb.getAccessKey(), gb.getSecretKey())))
                    .build()) {
                s3.putObject(b -> b.bucket("tc-bucket").key("hello.txt"), RequestBody.fromString("hello"));
                assertEquals("hello", s3.getObjectAsBytes(b -> b.bucket("tc-bucket").key("hello.txt")).asUtf8String());
            }
        }
    }
}
