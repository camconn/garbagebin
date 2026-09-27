package io.github.camconn.garbagebin;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/** Testcontainers module for the Garbage Bin S3 server. */
public class GarbageBinContainer extends GenericContainer<GarbageBinContainer> {
    public static final String DEFAULT_IMAGE = "garbagebin:latest";
    private static final int PORT = 9000;

    private String accessKey = "test";
    private String secretKey = "test";

    public GarbageBinContainer() {
        this(DEFAULT_IMAGE);
    }

    public GarbageBinContainer(String image) {
        this(DockerImageName.parse(image));
    }

    public GarbageBinContainer(DockerImageName image) {
        super(image);
        withExposedPorts(PORT);
        withEnv("S3_BACKEND", "memory");
        withCredentials(accessKey, secretKey);
        waitingFor(Wait.forHttp("/health").forPort(PORT).forStatusCode(200));
    }

    public GarbageBinContainer withBackend(String backend) {
        return withEnv("S3_BACKEND", backend);
    }

    public GarbageBinContainer withInitBuckets(String... buckets) {
        return withEnv("S3_INIT_BUCKETS", String.join(",", buckets));
    }

    public GarbageBinContainer withCredentials(String accessKey, String secretKey) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        return withEnv("S3_ACCESS_KEY", accessKey).withEnv("S3_SECRET_KEY", secretKey);
    }

    public String getEndpoint() {
        return "http://" + getHost() + ":" + getMappedPort(PORT);
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }
}
