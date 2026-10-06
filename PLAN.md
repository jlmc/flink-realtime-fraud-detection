Claro. Abaixo está o **prompt final consolidado**, já com Maven multi-module, plugins de validação, Flink 2.x, Kafka, PostgreSQL, Flyway, MinIO/S3 para state/checkpoints, Docker Compose, testes de falhas e requisitos de robustez.

```text
You are a Senior Software Architect / Principal Engineer.

Your task is to DESIGN, IMPLEMENT, BUILD and VALIDATE a complete production-oriented Proof of Concept for real-time transaction fraud/risk analysis using Apache Flink.

Do not only provide an architecture proposal. You must actually create the project, implement the code, infrastructure, tests and documentation, build everything successfully and validate the main failure/recovery scenarios.

Do not make assumptions silently.

When an architectural decision depends on the selected Flink version or another external dependency, verify the current official documentation/source first.

Clearly distinguish:
- Facts
- Assumptions
- Architectural decisions
- Trade-offs
- Known limitations
- Risks

Prefer simple, production-oriented solutions over unnecessary complexity.

==================================================
1. OBJECTIVE
==================================================

Build a Java-based real-time transaction risk/fraud analysis platform.

The system receives transaction events from Kafka, processes them using Apache Flink, evaluates configurable validation/risk rules, persists relevant results into PostgreSQL, and publishes risk events back to Kafka.

The POC must demonstrate:

- Stateful stream processing
- Event-time processing
- Watermarks
- Out-of-order events
- Late events
- Deduplication
- Managed Flink state
- Checkpoints
- Savepoints
- Durable state storage using S3
- Backpressure
- Failure recovery
- Restart strategies
- Poison/invalid messages
- Idempotent PostgreSQL persistence
- Kafka recovery
- Pluggable validation rules
- Independent validation-rule JARs
- Observability
- Integration testing
- Reproducible local infrastructure

==================================================
2. TECHNOLOGY REQUIREMENTS
==================================================

Use:

- Java
- Maven
- Apache Flink latest stable 2.x release
- Apache Kafka
- PostgreSQL
- Flyway
- S3-compatible object storage for Flink checkpoints/savepoints
- MinIO for local development
- Docker Compose / Podman Compose compatible infrastructure

Do NOT use Spring unless there is a strong, explicitly justified architectural reason.

Prefer plain Java and native Flink APIs.

Before implementation:

1. Determine the latest stable Apache Flink 2.x release.
2. Verify its official Java compatibility.
3. Verify the current recommended state/checkpoint APIs.
4. Verify the recommended Kafka connector/API.
5. Verify S3 checkpoint/savepoint configuration for that Flink version.

Do not use deprecated Flink APIs when a supported Flink 2.x alternative exists.

If Java 25 is officially supported by the selected Flink version, use Java 25.

Otherwise use the newest officially supported Java version and document the reason.

==================================================
3. MULTI-MODULE MAVEN PROJECT
==================================================

The repository MUST be a Maven multi-module project.

Root structure:

transaction-fraud-poc/
├── pom.xml
├── docker-compose.yml
├── docker/
├── db/
│   └── migration/
├── validation-api/
│   ├── pom.xml
│   └── src/
├── validation-rule-minimum-amount/
│   ├── pom.xml
│   └── src/
├── validation-rule-supported-currency/
│   ├── pom.xml
│   └── src/
├── validation-rule-schema/
│   ├── pom.xml
│   └── src/
├── flink-job/
│   ├── pom.xml
│   └── src/
└── integration-tests/
    ├── pom.xml
    └── src/

The root pom.xml must:

- Be the Maven parent
- Define the modules
- Centralize dependency versions
- Centralize plugin versions
- Configure Java version
- Configure Flink version
- Configure testing dependencies
- Configure compiler/build plugins
- Provide a reproducible Maven build

The following MUST work:

mvn clean verify

==================================================
4. MODULE RESPONSIBILITIES
==================================================

validation-api
---------------

Contains only the public plugin contracts/domain-independent API.

It MUST NOT depend on Flink.

Example:

public interface TransactionValidationRule {
    ValidationResult validate(
        Transaction transaction,
        ValidationContext context
    );
}

The API should contain appropriate domain types such as:

- Transaction
- ValidationResult
- ValidationContext
- RiskResult
- RiskContext

Keep this module small and stable.

--------------------------------------------------

validation-rule-minimum-amount
------------------------------

Independent Maven module.

Produces its own JAR.

Example rule:

Reject transactions with amount <= 0.

It depends only on:

validation-api

It MUST NOT depend on:

- flink-job
- Flink runtime
- Flink operators
- RuntimeContext
- Flink state APIs

--------------------------------------------------

validation-rule-supported-currency
-----------------------------------

Independent Maven module/JAR.

Example:

Accept:

EUR
USD
GBP

Reject unsupported currencies.

It depends only on validation-api.

--------------------------------------------------

validation-rule-schema
----------------------

Independent Maven module/JAR.

Validates required transaction fields.

Examples:

- transactionId is present
- customerId is present
- merchantId is present
- amount is valid
- currency is present
- timestamp is valid

It depends only on validation-api.

--------------------------------------------------

flink-job
---------

Contains the actual Flink streaming pipeline.

It depends on:

- validation-api
- Flink
- Kafka connector
- PostgreSQL driver
- required S3 filesystem support

It MUST NOT directly compile against concrete validation-rule modules.

Business validation rules must be discovered dynamically.

--------------------------------------------------

integration-tests
-----------------

Contains end-to-end/integration tests.

Test:

- Kafka -> Flink -> PostgreSQL
- Kafka -> Flink -> Kafka
- invalid messages
- duplicate messages
- stateful rules
- checkpoints
- recovery
- PostgreSQL failures
- out-of-order events
- late events
- plugin discovery

==================================================
5. PLUGIN ARCHITECTURE
==================================================

Validation rules MUST follow a plugin architecture.

The Flink core pipeline must NOT hard-code concrete validation rule implementations.

Use Java ServiceLoader or another simple, well-justified plugin mechanism.

Preferred implementation:

META-INF/services/

For example:

META-INF/services/com.example.validation.TransactionValidationRule

containing:

com.example.validation.rules.MinimumAmountRule

The Flink application discovers the rules during startup.

Do NOT implement runtime hot-loading/unloading in this POC.

Plugins are packaged/deployed with the job.

Changing a plugin requires rebuilding/redeploying the Flink job.

This is intentional because it provides predictable job-code/checkpoint compatibility.

IMPORTANT:

Verify that ServiceLoader/classloading actually works with the selected Flink 2.x deployment model.

Document:

- Where plugin JARs are packaged
- How TaskManagers see them
- How dependencies are handled
- Whether shading is necessary
- Potential dependency conflicts
- Plugin API compatibility/versioning

Each rule must be independently unit-testable without starting Flink.

==================================================
6. STATEFUL RISK RULES
==================================================

Separate stateless validation from stateful risk analysis.

Stateless validation rules belong to the plugin system.

Flink owns:

- keyed state
- timers
- windows
- event time
- watermarks
- state cleanup
- late events

Stateful risk rules should receive a domain-level context.

Example:

public record RiskContext(
    int transactionsLastMinute,
    BigDecimal amountLastTenMinutes,
    String previousCountry
) {}

Example:

public interface TransactionRiskRule {
    RiskResult evaluate(
        Transaction transaction,
        RiskContext context
    );
}

Do not allow plugins to manipulate Flink state directly.

==================================================
7. INPUT TRANSACTION
==================================================

Kafka transaction event:

{
  "transactionId": "tx-123",
  "customerId": "customer-42",
  "merchantId": "merchant-10",
  "amount": 950.00,
  "currency": "EUR",
  "country": "PT",
  "timestamp": "2026-10-06T13:10:01Z"
}

Use an explicit event timestamp.

Do not assume Kafka arrival time is event time.

==================================================
8. KAFKA TOPICS
==================================================

Create:

transaction.events

transaction.risk.events

transaction.invalid.events

transaction.events:

Input transactions.

transaction.risk.events:

Risk/fraud results.

transaction.invalid.events:

Invalid/poison events that cannot be processed.

Topic creation MUST be idempotent.

Use a kafka-init service.

Partition count must be configurable.

Document the chosen default.

==================================================
9. VALIDATION PIPELINE
==================================================

The Flink pipeline should conceptually be:

Kafka
  |
  v
Deserialize
  |
  v
Validation plugins
  |
  +---- invalid ---> transaction.invalid.events
  |
  v
Deduplication
  |
  v
Assign event timestamp
  |
  v
Watermarks
  |
  v
keyBy(customerId)
  |
  v
Managed keyed state
  |
  v
Risk rules
  |
  +---- PostgreSQL
  |
  +---- transaction.risk.events

Use proper Flink operators for each responsibility.

Keep the pipeline modular and testable.

==================================================
10. RISK RULES
==================================================

Implement at least:

1. Transaction velocity

Trigger risk when a customer has more than N transactions within 1 minute.

2. Spending velocity

Trigger risk when a customer spends more than X within 10 minutes.

3. Country change

Detect suspicious country changes.

Example:

PT -> US -> PT within a short time window.

Do not assume geographic distance calculations unless explicitly implemented.

4. Amount anomaly

Detect a transaction significantly above the customer's recent transaction behavior.

Keep the algorithm simple and explain the limitation.

Risk rules must produce structured results.

Example:

{
  "transactionId": "tx-123",
  "customerId": "customer-42",
  "riskScore": 85,
  "riskLevel": "HIGH",
  "reasons": [
    "HIGH_TRANSACTION_VELOCITY",
    "UNUSUAL_AMOUNT"
  ],
  "timestamp": "..."
}

==================================================
11. EVENT TIME
==================================================

Use Flink event-time processing.

Implement:

- timestamps
- watermarks
- bounded out-of-orderness
- allowed lateness where appropriate

Do not rely on Kafka ordering as a substitute for event-time processing.

Document:

- maximum expected out-of-order delay
- watermark strategy
- late-event behavior
- state cleanup behavior

Create tests for:

- ordered events
- out-of-order events
- late events

==================================================
12. DEDUPLICATION
==================================================

Kafka may deliver duplicate events.

The system must tolerate duplicate transaction events.

Use transactionId as the logical idempotency key.

Implement appropriate Flink-side deduplication.

Also implement PostgreSQL idempotency as defense in depth.

The system must not create duplicate transaction/risk records when the same transaction is delivered multiple times.

Explain exactly where deduplication occurs.

==================================================
13. POSTGRESQL
==================================================

Persist relevant information in PostgreSQL.

At minimum consider:

transactions
risk_scores
fraud_alerts

Use appropriate primary/unique keys.

transactionId should be unique where appropriate.

PostgreSQL writes MUST be idempotent.

Use mechanisms such as:

INSERT ... ON CONFLICT

or another justified strategy.

Do not claim end-to-end exactly-once merely because Flink checkpoints are enabled.

Explicitly explain the guarantee boundary:

Kafka -> Flink
Flink state
Flink -> PostgreSQL
Flink -> Kafka

If PostgreSQL is an external side effect, explain why checkpointing alone does not provide exactly-once PostgreSQL semantics.

==================================================
14. FLYWAY
==================================================

Database schema must be managed with Flyway.

Create:

db/
└── migration/
    ├── V1__create_transactions.sql
    ├── V2__create_risk_scores.sql
    └── V3__create_fraud_alerts.sql

The exact schema can be adjusted based on the implementation.

The Flink job MUST NOT create tables.

Create a dedicated Docker Compose service:

postgres-init-schema

It executes Flyway migrations.

The migration operation must be idempotent.

==================================================
15. DOCKER COMPOSE INFRASTRUCTURE
==================================================

Provide a complete docker-compose.yml.

Required services:

postgres
postgres-init-schema

kafka
kafka-init
kafka-ui

minio
minio-init

flink-jobmanager
flink-taskmanager

All services must share the appropriate Docker network.

Use healthchecks.

Do not rely solely on depends_on ordering.

Services must tolerate startup timing/transient dependency failures where appropriate.

==================================================
16. POSTGRES SERVICE
==================================================

PostgreSQL must have:

- persistent volume
- configurable credentials
- configurable database
- healthcheck
- exposed local port
- Docker network connectivity

Example defaults:

database: fraud
user: fraud
password: fraud
port: 5432

Credentials must be configurable through environment variables.

==================================================
17. POSTGRES INIT SCHEMA SERVICE
==================================================

Add:

postgres-init-schema

Use Flyway.

It must:

1. Wait for PostgreSQL readiness.
2. Run migrations.
3. Exit successfully.
4. Be safe to run multiple times.

Flink must not start processing application data before the schema is available.

==================================================
18. KAFKA
==================================================

Use Kafka in KRaft mode unless there is a strong compatibility reason not to.

Provide:

- persistent volume where appropriate
- internal Docker listener
- host-accessible listener
- healthcheck/readiness
- configurable settings

Document:

Docker bootstrap server

and:

Host bootstrap server

==================================================
19. KAFKA INIT
==================================================

Add:

kafka-init

It creates/verifies:

transaction.events
transaction.risk.events
transaction.invalid.events

Topic creation must be idempotent.

It should exit successfully after initialization.

==================================================
20. KAFKA UI
==================================================

Add Kafka UI.

It must allow inspection of:

- topics
- partitions
- messages
- consumer groups
- offsets
- consumer lag

Configure it automatically against Kafka.

Document its local URL.

==================================================
21. MINIO / S3
==================================================

Flink checkpoints and savepoints MUST use S3-compatible object storage.

Do NOT use:

file:///tmp/...

for the actual checkpoint configuration.

Use MinIO locally.

Required services:

minio
minio-init

MinIO must provide:

- persistent volume
- S3 API
- healthcheck
- configurable access key
- configurable secret key

Create bucket:

flink-state

Use:

s3://flink-state/checkpoints/

and:

s3://flink-state/savepoints/

MinIO-specific behavior must not leak into application business logic.

Configuration must be externalized:

S3 endpoint
S3 access key
S3 secret key
S3 bucket
checkpoint path
savepoint path

The same application configuration should support AWS S3 in production.

==================================================
22. MINIO INIT
==================================================

minio-init must:

1. Wait for MinIO.
2. Create flink-state bucket if necessary.
3. Configure required access.
4. Exit successfully.

It must be idempotent.

==================================================
23. FLINK JOBMANAGER
==================================================

Add Flink JobManager.

Requirements:

- exact selected Flink version
- Web UI
- explicit checkpoint configuration
- durable checkpoint storage
- durable savepoint storage
- explicit restart strategy
- appropriate state backend/configuration for selected Flink 2.x APIs

Expose Flink Web UI locally.

Document the URL.

==================================================
24. FLINK TASKMANAGER
==================================================

Add at least one TaskManager.

Configure:

- task slots
- memory
- resources
- JobManager connection

The infrastructure must allow scaling:

docker compose up --scale flink-taskmanager=3

or the equivalent Podman Compose command.

Document how parallelism affects processing.

==================================================
25. CHECKPOINTS
==================================================

Enable checkpoints explicitly.

Configure:

- checkpoint interval
- checkpoint timeout
- minimum pause between checkpoints
- tolerable checkpoint failures
- externalized checkpoint retention where supported
- S3-backed storage

Do not choose arbitrary values without explanation.

The values should be appropriate for a local POC but clearly documented as examples.

Demonstrate recovery from:

- TaskManager failure
- JobManager failure
- Flink job restart

The POC must demonstrate that managed state can be recovered.

==================================================
26. SAVEPOINTS
==================================================

Document how to:

- create a savepoint
- stop a job
- restart from a savepoint

Use the S3-backed savepoint location.

Explain the relationship between:

checkpoint
savepoint
Flink state
Kafka offsets

==================================================
27. BACKPRESSURE
==================================================

The system must explicitly demonstrate backpressure.

Consider this scenario:

Kafka:
50,000 events/sec

Flink:
50,000 events/sec

PostgreSQL:
5,000 writes/sec

The system must NOT attempt to hide the problem using unbounded memory queues.

Expected behavior:

- downstream PostgreSQL becomes the bottleneck
- Flink backpressure propagates upstream
- Kafka consumer lag increases
- system remains stable
- process does not OOM because of unbounded buffering

Use bounded concurrency/batching where appropriate.

Do not claim that backpressure is "eliminated".

Explain how to observe it in Flink UI/metrics.

==================================================
28. FAILURE HANDLING
==================================================

Implement and test:

A. Transient PostgreSQL failure

Retry appropriately.

B. PostgreSQL prolonged outage

System must apply backpressure rather than silently losing data.

C. Invalid transaction

Do not crash the entire Flink job.

Send to:

transaction.invalid.events

D. Poison message

Prevent restart loops caused by permanently invalid data.

E. Flink TaskManager crash

Recover using checkpoint/state.

F. JobManager restart

Recover job.

G. Duplicate Kafka event

Do not create duplicate logical results.

H. Out-of-order event

Correctly process according to event time.

I. Late event

Apply documented late-event policy.

J. Kafka broker restart

Job must recover.

K. Checkpoint failure

Job should use the configured failure/restart strategy.

==================================================
29. RESTART STRATEGY
==================================================

Configure a deliberate Flink restart strategy.

Prefer a bounded retry strategy with delay/backoff rather than immediate infinite restarts.

Differentiate:

- transient infrastructure failures
- permanent invalid data

Invalid business data must not cause restart loops.

Document the chosen strategy.

==================================================
30. OBSERVABILITY
==================================================

Expose/track at least:

- Kafka records consumed
- Kafka consumer lag
- records processed
- records rejected
- DLQ/invalid events
- duplicate events
- processing latency
- checkpoint duration
- checkpoint failures
- checkpoint size
- state size where available
- PostgreSQL latency
- PostgreSQL failures
- PostgreSQL retries
- Flink restart count
- backpressure

Prometheus/Grafana may be included if it does not add disproportionate complexity.

If not included, document how the most important metrics can be observed through Flink/Kafka/PostgreSQL tooling.

==================================================
31. TESTING STRATEGY
==================================================

Provide:

Unit tests
Integration tests
Failure/recovery tests where practical

Unit tests must cover:

- validation rules
- risk rules
- serialization/deserialization
- deduplication logic
- risk calculations

Integration tests must cover:

Kafka -> Flink -> PostgreSQL

Kafka -> Flink -> Kafka

Duplicate event

Invalid event

Out-of-order event

Late event

Plugin discovery

Multiple plugins

PostgreSQL idempotency

Checkpoint/restart recovery where practical

Do not rely exclusively on mocks.

Use real Kafka/PostgreSQL/MinIO for important integration scenarios.

Testcontainers may be used if justified, but the Docker Compose environment must still exist as the main local development environment.

==================================================
32. PLUGIN TEST
==================================================

There must be a test proving that:

1. Flink starts.
2. The validation plugin JAR is available.
3. The rule is discovered using ServiceLoader.
4. The rule executes.
5. No direct reference from flink-job to the concrete rule class exists.

The test should demonstrate that adding another validation-rule module does not require modifying the Flink core pipeline.

==================================================
33. DATABASE IDEMPOTENCY TEST
==================================================

Send the same transaction event multiple times.

Verify:

- one logical transaction
- no duplicated risk record
- no duplicated fraud alert

Document which layer provides each guarantee.

==================================================
34. FAILURE RECOVERY TEST
==================================================

Provide a reproducible documented test:

1. Start infrastructure.
2. Start Flink.
3. Send transactions.
4. Verify state/results.
5. Trigger a checkpoint.
6. Kill/restart a TaskManager or Flink job.
7. Restart the job.
8. Send more events.
9. Verify state continuity.
10. Verify no unexpected duplicate PostgreSQL records.

The README must contain the exact commands.

==================================================
35. PROJECT STRUCTURE
==================================================

Use clean architecture principles where useful.

Suggested:

validation-api/
  src/main/java/...

validation-rule-minimum-amount/
  src/main/java/...
  src/main/resources/META-INF/services/...

validation-rule-supported-currency/
  ...

validation-rule-schema/
  ...

flink-job/
  src/main/java/
    .../pipeline/
    .../state/
    .../risk/
    .../persistence/
    .../serialization/
    .../plugins/
    .../configuration/

integration-tests/
  ...

Do not over-engineer.

==================================================
36. CONFIGURATION
==================================================

Externalize:

Kafka bootstrap servers
Kafka topics
consumer group
parallelism
checkpoint interval
checkpoint timeout
S3 endpoint
S3 credentials
S3 bucket
PostgreSQL URL
PostgreSQL username
PostgreSQL password
risk thresholds
watermark delay
allowed lateness

Provide sensible local defaults.

Never hard-code production credentials.

==================================================
37. SECURITY
==================================================

For the local POC, simple credentials are acceptable.

Clearly distinguish local development credentials from production configuration.

Do not commit real credentials.

==================================================
38. DOCKER COMPOSE STARTUP
==================================================

The intended developer experience should be approximately:

docker compose up -d

Then:

mvn clean verify

Then deploy/start the Flink job.

Document the exact commands required.

The README must include:

- prerequisites
- build
- infrastructure startup
- topic creation
- database migration
- Flink startup
- job deployment
- producing sample transactions
- consuming risk events
- inspecting PostgreSQL
- Kafka UI
- Flink UI
- MinIO UI
- running tests
- failure/recovery tests
- stopping/cleaning the environment

Provide a complete "Quick Start" section.

==================================================
39. SAMPLE DATA
==================================================

Provide a simple mechanism to produce test transactions.

For example:

scripts/send-transactions.sh

or a Java/Python utility if justified.

Include scenarios:

1. Normal transaction
2. High velocity
3. High spending
4. Country change
5. Unusual amount
6. Invalid transaction
7. Duplicate transaction
8. Out-of-order transaction
9. Late transaction

==================================================
40. DOCUMENTATION
==================================================

README must explain:

Architecture

Data flow

Module structure

Plugin architecture

Kafka topics

Database schema

Flink state

Checkpoint strategy

Savepoint strategy

S3/MinIO configuration

Event-time processing

Watermarks

Late events

Deduplication

Backpressure

Failure recovery

PostgreSQL consistency

Kafka delivery semantics

Plugin classloading

How to add a new validation plugin

How to add a new risk rule

How to run tests

How to reproduce failures

Production considerations

==================================================
41. IMPORTANT EXACTLY-ONCE REQUIREMENT
==================================================

Do NOT use marketing language such as:

"the system is exactly-once"

unless this is actually demonstrated and technically correct.

Explicitly describe:

Kafka -> Flink guarantee

Flink state guarantee

Flink -> Kafka guarantee

Flink -> PostgreSQL guarantee

Explain the difference between:

processing guarantees
delivery guarantees
idempotency
transactional sinks
external side effects

PostgreSQL idempotency must be part of the design.

==================================================
42. ARCHITECTURAL DECISION: STATE OWNERSHIP
==================================================

Flink owns streaming state.

PostgreSQL owns durable business data.

Kafka owns event transport.

S3/MinIO owns durable Flink checkpoint/savepoint storage.

Do not use PostgreSQL as a replacement for Flink managed state.

Do not use Kafka as a replacement for Flink managed state.

==================================================
43. ARCHITECTURAL DECISION: PLUGIN OWNERSHIP
==================================================

Validation plugins must own business validation logic.

Flink owns:

- orchestration
- event time
- windows
- state
- timers
- recovery

Plugins should receive domain information rather than manipulating Flink internals.

==================================================
44. BUILD REQUIREMENT
==================================================

After implementation you MUST actually run:

mvn clean verify

Fix all compilation/test failures.

Do not stop at generating source code.

Verify that every Maven module builds.

Verify every validation rule produces its own JAR.

Verify ServiceLoader metadata exists.

Verify the Flink job artifact is produced.

==================================================
45. INFRASTRUCTURE VALIDATION
==================================================

Start the complete Docker Compose environment.

Verify:

PostgreSQL healthy

Flyway migrations applied

Kafka healthy

Kafka topics created

Kafka UI accessible

MinIO healthy

flink-state bucket exists

Flink JobManager healthy

Flink TaskManager connected

Flink checkpoint storage configured correctly

==================================================
46. FINAL VALIDATION
==================================================

Before considering the task complete, execute an end-to-end scenario:

1. Start infrastructure.
2. Initialize PostgreSQL schema.
3. Initialize Kafka topics.
4. Initialize MinIO.
5. Start Flink.
6. Verify plugin discovery.
7. Produce normal transactions.
8. Produce fraudulent/risky transactions.
9. Produce duplicates.
10. Produce invalid events.
11. Produce out-of-order events.
12. Verify risk events.
13. Verify PostgreSQL records.
14. Verify invalid events.
15. Verify checkpoints.
16. Restart Flink/TaskManager.
17. Verify state recovery.
18. Verify idempotency.
19. Verify Kafka lag/backpressure behavior where possible.

==================================================
47. FINAL RESPONSE
==================================================

When implementation is complete, provide:

1. Final architecture summary.
2. Project structure.
3. Key architectural decisions.
4. Flink version selected and why.
5. Java version selected and why.
6. Maven module explanation.
7. Plugin architecture explanation.
8. Docker Compose services.
9. State/checkpoint architecture.
10. Failure/recovery guarantees.
11. Exactly-once/idempotency boundaries.
12. Commands to build and run.
13. Tests executed and their results.
14. Known limitations.
15. Production recommendations.

Most importantly:

DO NOT merely describe what should be implemented.

IMPLEMENT IT.

BUILD IT.

RUN THE TESTS.

VALIDATE THE INFRASTRUCTURE.

FIX FAILURES.

Only then consider the POC complete.
```
