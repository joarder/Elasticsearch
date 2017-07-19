package com.amazonaws.javaexample;

import com.amazonaws.*;
import com.amazonaws.auth.*;
import com.amazonaws.http.AmazonHttpClient;
import com.amazonaws.http.ExecutionContext;
import com.amazonaws.http.HttpMethodName;
import com.amazonaws.http.HttpResponseHandler;
import java.io.*;
import java.net.URI;

/**
 * Created by joarderk on 7/18/17.
 */

public class JavaClientTest {

    private static final String SERVICE_NAME = "es";
    private static final String REGION = "eu-west-1";
    private static final String HOST = "search-es-51-gt25azdfbksmzdlr2ffruwiudy.eu-west-1.es.amazonaws.com";
    private static final String ENDPOINT_ROOT = "https://" + HOST;
    private static final String PATH = "/_stats";
    private static final String ENDPOINT = ENDPOINT_ROOT + PATH;
    private static final String pretty="true";

    // Set up the request
    private static Request<?> generateRequest() {
        Request<?> request = new DefaultRequest<Void>(SERVICE_NAME);
        request.setContent(new ByteArrayInputStream("".getBytes()));
        request.setEndpoint(URI.create(ENDPOINT));
        request.setHttpMethod(HttpMethodName.GET);
        request.addParameter("pretty",pretty);
        return request;
    }

    // Perform Signature Version 4 signing
    private static void performSigningSteps(Request<?> requestToSign) {
        AWS4Signer signer = new AWS4Signer();
        signer.setServiceName(SERVICE_NAME);
        signer.setRegionName(REGION);

        // Get credentials
        AWSCredentials creds = new DefaultAWSCredentialsProviderChain().getCredentials();

        // Sign request with supplied creds
        signer.sign(requestToSign, creds);
    }

    // Send the request to the server
    private static void sendRequest(Request<?> request) {
        ExecutionContext context = new ExecutionContext(true);

        ClientConfiguration clientConfiguration = new ClientConfiguration();
        AmazonHttpClient client = new AmazonHttpClient(clientConfiguration);

        MyHttpResponseHandler<Void> responseHandler = new MyHttpResponseHandler<Void>();
        MyErrorHandler errorHandler = new MyErrorHandler();

        Response<Void> response =
                client.execute(request, responseHandler, errorHandler, context);

        // Need to use client.requestExecutionBuilder().execute()
    }

    public static void main(String[] args) {
        // Generate the request
        Request<?> request = generateRequest();

        // Perform Signature Version 4 signing
        performSigningSteps(request);

        // Send the request to the server
        sendRequest(request);
    }

    public static String convertStreamToString(InputStream is) throws IOException {
        // To convert the InputStream to String we use the
        // Reader.read(char[] buffer) method. We iterate until the
        // Reader return -1 which means there's no more data to
        // read. We use the StringWriter class to produce the string.
        if (is != null) {
            Writer writer = new StringWriter();

            char[] buffer = new char[1024];
            try {
                Reader reader = new BufferedReader(
                        new InputStreamReader(is, "UTF-8"));
                int n;
                while ((n = reader.read(buffer)) != -1) {
                    writer.write(buffer, 0, n);
                }
            } finally {
                is.close();
            }
            return writer.toString();
        }
        return "";
    }

    public static class MyHttpResponseHandler<T> implements HttpResponseHandler<AmazonWebServiceResponse<T>> {

        @Override
        public AmazonWebServiceResponse<T> handle(
                com.amazonaws.http.HttpResponse response) throws Exception {

            InputStream responseStream = response.getContent();
            String responseString = convertStreamToString(responseStream);
            System.out.println(responseString);

            AmazonWebServiceResponse<T> awsResponse = new AmazonWebServiceResponse<T>();
            return awsResponse;
        }

        @Override
        public boolean needsConnectionLeftOpen() {
            return false;
        }
    }

    public static class MyErrorHandler implements HttpResponseHandler<AmazonServiceException> {

        @Override
        public AmazonServiceException handle(
                com.amazonaws.http.HttpResponse response) throws Exception {
            System.out.println("In exception handler!");

            AmazonServiceException ase = new AmazonServiceException("Fake service exception.");
            ase.setStatusCode(response.getStatusCode());
            ase.setErrorCode(response.getStatusText());
            return ase;
        }

        @Override
        public boolean needsConnectionLeftOpen() {
            return false;
        }
    }
}