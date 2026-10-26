import { GenericContainer, Wait } from "testcontainers";

const PORT = 9000;

/** Testcontainers module for the Garbage Bin S3 server. */
export class GarbageBinContainer extends GenericContainer {
  #accessKey = "test";
  #secretKey = "test";

  constructor(image = "camconn/garbagebin:latest") {
    super(image);
    this.withExposedPorts(PORT)
      .withEnvironment({ S3_BACKEND: "memory", S3_ACCESS_KEY: "test", S3_SECRET_KEY: "test" })
      .withWaitStrategy(Wait.forHttp("/health", PORT).forStatusCode(200));
  }

  /** @param {"memory" | "fs"} backend */
  withBackend(backend) {
    return this.withEnvironment({ S3_BACKEND: backend });
  }

  /** @param {...string} buckets */
  withInitBuckets(...buckets) {
    return this.withEnvironment({ S3_INIT_BUCKETS: buckets.join(",") });
  }

  withCredentials(accessKey, secretKey) {
    this.#accessKey = accessKey;
    this.#secretKey = secretKey;
    return this.withEnvironment({ S3_ACCESS_KEY: accessKey, S3_SECRET_KEY: secretKey });
  }

  /** @returns {Promise<StartedGarbageBinContainer>} */
  async start() {
    return new StartedGarbageBinContainer(await super.start(), this.#accessKey, this.#secretKey);
  }
}

export class StartedGarbageBinContainer {
  #accessKey;
  #secretKey;

  constructor(container, accessKey, secretKey) {
    this.container = container;
    this.#accessKey = accessKey;
    this.#secretKey = secretKey;
  }

  endpoint() {
    return `http://${this.container.getHost()}:${this.container.getMappedPort(PORT)}`;
  }

  accessKey() {
    return this.#accessKey;
  }

  secretKey() {
    return this.#secretKey;
  }

  stop() {
    return this.container.stop();
  }
}
