package com.dev.cristian.ejemplos.s3;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3EncryptionClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.sun.xml.internal.ws.api.addressing.WSEndpointReference;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Hello world!
 *
 */
public class App {
    public static void main( String[] args ) {

        S3Consumidor s3Consumidor = new S3Consumidor();

        s3Consumidor.subirArchivo();
//        s3Consumidor.getArchivo();

    }
}
