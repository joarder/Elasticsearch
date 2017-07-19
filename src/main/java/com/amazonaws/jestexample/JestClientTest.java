package com.amazonaws.jestexample;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.google.common.base.Supplier;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.JestResultHandler;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import vc.inreach.aws.request.AWSSigner;
import vc.inreach.aws.request.AWSSigningRequestInterceptor;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

/**
 * Created by joarderk on 7/18/17.
 */
public class JestClientTest {

    private static final String SERVICE = "es";
    private static final String REGION = "eu-west-1";
    private static final String HOST = "search-es-51-gt25azdfbksmzdlr2ffruwiudy.eu-west-1.es.amazonaws.com";
    private static final String ENDPOINT_ROOT = "https://" + HOST;
    private static final String PATH = "/";
    private static final String ENDPOINT = ENDPOINT_ROOT + PATH;

    private static final String NOTES_TYPE_NAME = "notes";
    private static final String DIARY_INDEX_NAME = "diary";

    private static AWSCredentialsProvider credentialsProvider;

    // Initialization
    private static void init() {
        // Ensure the JVM will refresh the cached IP values of AWS resources (e.g. service endpoints).
        java.security.Security.setProperty("networkaddress.cache.ttl", "60");

        /*
         * The ProfileCredentialsProvider will return your [profileName]
         * credential profile by reading from the credentials file located at
         * (~/.aws/credentials).
         */
        //credentialsProvider = new ProfileCredentialsProvider("awses");
        credentialsProvider = new DefaultAWSCredentialsProviderChain();

        try {
            credentialsProvider.getCredentials();

        } catch (Exception e) {
            throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
                    + "Please make sure that your credentials file is at the correct "
                    + "location (~/.aws/credentials), and is in valid format.", e);
        }
    }

    // Create an index
    private static void createIndex(final JestClient jestClient) throws Exception {
        System.out.println(">> Creating index \""+DIARY_INDEX_NAME+"\" ...");

        Settings.Builder settings = Settings.builder();
        settings.put("number_of_shards", 3);
        settings.put("number_of_replicas", 1);

        jestClient.execute(new CreateIndex.Builder(DIARY_INDEX_NAME)
                .settings(settings.build().getAsMap()).build());
    }

    // Index some documents into Elasticsearch
    private static void indexData(final JestClient jestClient) throws Exception {
        // Blocking index
        final Note note1 = new Note("User1", "Note1: do u see this - "
                + System.currentTimeMillis());
        Index index = new Index.Builder(note1).index(DIARY_INDEX_NAME).type(NOTES_TYPE_NAME).build();

        System.out.println(">> Inserting a single document ...\n" + note1);
        jestClient.execute(index);

        // Asynch index
        final Note note2 = new Note("User2", "Note2: do u see this - "
                + System.currentTimeMillis());
        index = new Index.Builder(note2).index(DIARY_INDEX_NAME).type(NOTES_TYPE_NAME).build();

        System.out.println(">> Inserting a single document asynchronously ...\n" + note2);
        jestClient.executeAsync(index, new JestResultHandler<JestResult>() {
            public void failed(Exception ex) { }

            public void completed(JestResult result) {
                note2.setId((String) result.getValue("_id"));
            }
        });

        // bulk index
        final Note note3 = new Note("User3", "Note3: do u see this - "
                + System.currentTimeMillis());
        final Note note4 = new Note("User4", "Note4: do u see this - "
                + System.currentTimeMillis());

        Bulk bulk = new Bulk.Builder()
                .addAction(new Index.Builder(note3).index(DIARY_INDEX_NAME)
                                .type(NOTES_TYPE_NAME).build())
                .addAction(new Index.Builder(note4).index(DIARY_INDEX_NAME)
                                .type(NOTES_TYPE_NAME).build()).build();

        System.out.println(">> Inserting two documents using bulk API ...\n" + note3 +"\n"+ note4);
        JestResult result = jestClient.execute(bulk);

        Thread.sleep(2000);

        System.out.println(result.toString());
    }

    // Query or Search within an index
    private static void queryIndex(final JestClient jestClient) throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termQuery("note", "see"));

        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex(DIARY_INDEX_NAME).addType(NOTES_TYPE_NAME).build();

        System.out.println(">> Querying index \""+DIARY_INDEX_NAME+"\" for 'note' and 'see' ...");
        System.out.println(searchSourceBuilder.toString());

        JestResult result = jestClient.execute(search);
        List<Note> notes = result.getSourceAsObjectList(Note.class);

        for (Note note : notes)
            System.out.println(note);
    }

    // Delete an index
    private static void deleteIndex(final JestClient jestClient) throws Exception {
        System.out.println(">> Deleting index \""+DIARY_INDEX_NAME+"\" ...");

        DeleteIndex deleteIndex = new DeleteIndex.Builder(DIARY_INDEX_NAME).build();
        jestClient.execute(deleteIndex);
    }

    // Main function - entry point
    // Here we used the AWSSigner package available at https://github.com/inreachventures/aws-signing-request-interceptor
    // for signing the Elasticsearch requests using AWS credentials
    public static void main(String[] args) {

        try {
            init();

            HttpClientConfig clientConfig = new HttpClientConfig.Builder(ENDPOINT).multiThreaded(true).build();

            final Supplier<LocalDateTime> clock = () -> LocalDateTime.now(ZoneOffset.UTC);
            final AWSSigner awsSigner = new AWSSigner(credentialsProvider, REGION, SERVICE, clock);

            final AWSSigningRequestInterceptor requestInterceptor = new AWSSigningRequestInterceptor(awsSigner);

            final JestClientFactory factory = new JestClientFactory() {
                @Override
                protected HttpClientBuilder configureHttpClient(HttpClientBuilder builder) {
                    builder.addInterceptorLast(requestInterceptor);
                    return builder;
                }
                @Override
                protected HttpAsyncClientBuilder configureHttpClient(HttpAsyncClientBuilder builder) {
                    builder.addInterceptorLast(requestInterceptor);
                    return builder;
                }
            };

            factory.setHttpClientConfig(clientConfig);
            JestClient jestClient = factory.getObject();

            try {
                // Create, index, query, and delete
                createIndex(jestClient);
                indexData(jestClient);
                queryIndex(jestClient);
                deleteIndex(jestClient);

            } finally {
                // Shutdown client
                jestClient.shutdownClient();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}