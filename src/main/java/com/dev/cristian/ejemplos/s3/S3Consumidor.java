package com.dev.cristian.ejemplos.s3;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.*;

public class S3Consumidor {

    private AmazonS3 s3Client;

    public S3Consumidor() {
        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(
                Constantes.Credenciales.ACCESS_KEY,
                Constantes.Credenciales.SECRET_KEY
        );

        s3Client = AmazonS3ClientBuilder
                .standard()
                .withRegion(Regions.US_EAST_2)
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .build();

    }

    public void subirArchivo() {

        File archivoSubir = new File("archivo.txt");

        PutObjectRequest putObjectRequest = new PutObjectRequest(
                Constantes.Bucket.NOMBRE, // Nombre del bucket
                "archivo_de_prueba_desde_java.txt", // Nombre del archivo que se le pondrá en el bucket
                archivoSubir
        );

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType("plain/text");

        putObjectRequest.setMetadata(metadata);

        System.out.println("Empezamos a subir el archivo");

        try {

            s3Client.putObject(putObjectRequest);

            System.out.println("Archivo subido correctamente");

        } catch (SdkClientException sce) {
            System.out.println("Error al subir el objeto");
            sce.printStackTrace();
        }
    }

    public void getArchivo() {
        // Obtenemos el objeto del bucket
        S3Object s3Object = s3Client.getObject(
                Constantes.Bucket.NOMBRE,
                "archivo_de_prueba_desde_java.txt"
        );

        // Ahora creamos un archivo con el stream obtenido de amazon S3 para hacer una copia local del archivo
        BufferedInputStream reader = new BufferedInputStream(
                s3Object.getObjectContent()
        );

        File archivoLocal = new File("archivo_de_bucket.txt");

        try {
            BufferedOutputStream writer = new BufferedOutputStream(new FileOutputStream(archivoLocal));

            int leido = -1;

            while ( ( leido = reader.read() ) != -1 ) {
                writer.write(leido);
            }

            writer.flush();

            writer.close();
            reader.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
