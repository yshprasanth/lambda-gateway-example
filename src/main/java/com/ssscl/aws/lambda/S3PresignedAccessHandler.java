package com.ssscl.aws.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.time.Duration;

public class S3PresignedAccessHandler {

    private JSONParser parser = new JSONParser();
    private S3Presigner s3Presigner = S3Presigner.builder()
            .region(Region.US_WEST_1)
            .build();

    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        JSONObject responseJson = new JSONObject();

        try {
            JSONObject event = (JSONObject) parser.parse(reader);

            String bucketName = (String) event.get("bucket");
            String fileName = (String) event.get("file");

            JSONObject responseBody = new JSONObject();
            responseBody.put("url", getPresignedUrl(s3Presigner, bucketName, fileName, context));

            responseJson.put("statusCode", 200);
            responseJson.put("body", responseBody.toString());

        } catch (ParseException pex) {
            responseJson.put("statusCode", 400);
            responseJson.put("exception", pex);
        }

        OutputStreamWriter writer = new OutputStreamWriter(outputStream, "UTF-8");
        writer.write(responseJson.toString());
        writer.close();
    }

    public static String getPresignedUrl(S3Presigner presigner, String bucketName, String keyName, Context context) {
        try {
            GetObjectRequest getObjectRequest =
                    GetObjectRequest.builder()
                            .bucket(bucketName)
                            .key(keyName)
                            .build();

            GetObjectPresignRequest getObjectPresignRequest =
                    GetObjectPresignRequest.builder()
                            .signatureDuration(Duration.ofMinutes(10))
                            .getObjectRequest(getObjectRequest)
                            .build();

            // Generate the presigned request
            PresignedGetObjectRequest presignedGetObjectRequest =
                    presigner.presignGetObject(getObjectPresignRequest);

            // Log the presigned URL
            context.getLogger().log("Presigned URL: " + presignedGetObjectRequest.url());

            return presignedGetObjectRequest.url().toExternalForm();
        } catch (S3Exception e) {
            e.getStackTrace();
        }
        return null;
    }
}
