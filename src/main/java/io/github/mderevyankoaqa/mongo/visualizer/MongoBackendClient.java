package io.github.mderevyankoaqa.mongo.visualizer;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.jmeter.assertions.AssertionResult;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.visualizers.backend.AbstractBackendListenerClient;
import org.apache.jmeter.visualizers.backend.BackendListenerContext;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * Backend listener for JMeter that sends sample results in bulk to MongoDB.
 * <p>
 * Supports batching, transaction controller grouping, and detailed assertion reporting.
 */
public class MongoBackendClient extends AbstractBackendListenerClient {

    // MongoDB configuration parameters
    public static final String MONGO_URI = "mongo_uri";
    public static final String MONGO_COLLECTION_NAME = "mongo_collection_name";
    public static final String MONGO_TIME_FIELD = "mongo_time_field";
    public static final String TRANSACTION_PREFIX = "transaction.controller.prefix";
    public static final String BATCH_SIZE = "batch.size";
    public static final String SAVE_RESPONSE_BODY = "save.response.body";
    public static final String ENVIRONMENT = "environment";
    public static final String REPORT_ID = "report_id";

    private MongoClient mongoClient;
    private MongoCollection<Document> collection;
    private String mongoTimeField;
    private String transactionControllerPrefix;
    private String environment;
    private int batchSize;
    private String saveResponseBodyMode;

    private String runId;
    private String reportId;

    private final List<Document> batch = new ArrayList<>();
    private static final Logger log = LoggerFactory.getLogger(MongoBackendClient.class);

    @Override
    public Arguments getDefaultParameters() {
        Arguments arguments = new Arguments();
        arguments.addArgument(MONGO_URI, "mongodb://user:pass@localhost:27017/perf_db");
        arguments.addArgument(MONGO_COLLECTION_NAME, "api");
        arguments.addArgument(MONGO_TIME_FIELD, "date");
        arguments.addArgument(TRANSACTION_PREFIX, "TC");
        arguments.addArgument(BATCH_SIZE, "10");
        arguments.addArgument(SAVE_RESPONSE_BODY, "onError"); // options: onError, always, off
        arguments.addArgument(ENVIRONMENT, "env");
        arguments.addArgument(REPORT_ID, ""); // Default is empty string
        return arguments;
    }

    @Override
    public void setupTest(BackendListenerContext context) throws Exception {
        String mongoUri = context.getParameter(MONGO_URI);
        String collectionName = context.getParameter(MONGO_COLLECTION_NAME);
        if (collectionName == null || collectionName.isEmpty()) {
            throw new IllegalArgumentException("MONGO_COLLECTION_NAME must not be empty");
        }

        this.mongoTimeField = context.getParameter(MONGO_TIME_FIELD, "date");
        this.transactionControllerPrefix = context.getParameter(TRANSACTION_PREFIX, "TC");
        this.batchSize = context.getIntParameter(BATCH_SIZE, 1000);
        this.saveResponseBodyMode = context.getParameter(SAVE_RESPONSE_BODY, "onError");
        this.environment = context.getParameter(ENVIRONMENT, "env");

        // Read report_id from user input (defaults to "" if empty)
        this.reportId = context.getParameter(REPORT_ID, "");

        // Always generate a unique run_id for this execution
        this.runId = UUID.randomUUID().toString();

        // Initialize MongoDB client
        ConnectionString connectionString = new ConnectionString(mongoUri);
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .build();
        mongoClient = MongoClients.create(settings);

        // Determine database
        String databaseName = connectionString.getDatabase();
        if (databaseName == null || databaseName.isEmpty()) {
            databaseName = "test";
            log.warn("No database specified in Mongo URI, using default 'test'");
        }

        MongoDatabase database = mongoClient.getDatabase(databaseName);
        collection = database.getCollection(collectionName);

        log.info("MongoBackendClient initialized: database='{}', collection='{}', run_id={}, report_id='{}'",
                databaseName, collectionName, runId, reportId);
    }

    @Override
    public void handleSampleResults(List<SampleResult> sampleResults, BackendListenerContext context) {
        for (SampleResult sample : sampleResults) {
            processSampleRecursively(sample, null);
        }
    }

    private void processSampleRecursively(SampleResult result, String parentLabel) {
        String label = result.getSampleLabel();
        boolean success = result.isSuccessful();
        long responseTime = result.getTime();
        String responseCode = result.getResponseCode();
        String responseMessage = result.getResponseMessage();

        Date timestamp = new Date(result.getTimeStamp());

        String responseBody = null;
        if ("always".equalsIgnoreCase(saveResponseBodyMode) ||
                ("onError".equalsIgnoreCase(saveResponseBodyMode) && !success)) {
            responseBody = result.getResponseDataAsString();
        }
        if (responseBody != null && responseBody.length() > 2048) {
            responseBody = responseBody.substring(0, 2048) + "...";
        }

        StringBuilder assertionMessages = new StringBuilder();
        for (AssertionResult ar : result.getAssertionResults()) {
            if (ar.isFailure() || ar.isError()) {
                assertionMessages.append(ar.getName()).append(": ").append(ar.getFailureMessage()).append("; ");
            }
        }

        int startedThreads = JMeterContextService.getTotalThreads();
        int activeThreads = JMeterContextService.getNumberOfThreads();
        int finishedThreads = startedThreads - activeThreads;
        boolean isTransactionController = label.startsWith(transactionControllerPrefix);

        // Create the Main Document with Flattened Fields (No Metadata Object)
        Document doc = new Document()
                // --- Metadata (Now at Root) ---
                .append("report_id", reportId)
                .append("environment", environment)
                .append("run_id", runId)
                .append("test_name", "") // Placeholder for consistency

                // --- Metrics ---
                .append("threadName", result.getThreadName())
                .append("label", label)
                .append("parentLabel", parentLabel)
                .append("isTransactionController", isTransactionController)
                .append("success", success)
                .append("responseTime", responseTime)
                .append("responseCode", responseCode)
                .append("responseMessage", responseMessage)
                .append("responseBody", responseBody)
                .append("assertions", assertionMessages.toString())
                .append(mongoTimeField, timestamp)
                .append("activeThreads", activeThreads)
                .append("startedThreads", startedThreads)
                .append("finishedThreads", finishedThreads);

        synchronized (batch) {
            batch.add(doc);
            if (batch.size() >= batchSize) {
                flushBatch();
            }
        }

        for (SampleResult child : result.getSubResults()) {
            processSampleRecursively(child, label);
        }
    }

    private void flushBatch() {
        if (batch.isEmpty()) return;

        try {
            collection.insertMany(new ArrayList<>(batch));
            // Log the run_id with every batch insert
            log.info("Run ID: {} | Inserted {} documents into MongoDB", runId, batch.size());
        } catch (Exception e) {
            log.error("Run ID: {} | Error inserting batch to MongoDB: {}", runId, e.getMessage(), e);
        } finally {
            batch.clear();
        }
    }

    @Override
    public void teardownTest(BackendListenerContext context) throws Exception {
        try {
            flushBatch();
        } catch (Exception e) {
            log.error("Error flushing remaining batch on teardown: {}", e.getMessage(), e);
        } finally {
            if (mongoClient != null) {
                mongoClient.close();
            }
        }
    }
}