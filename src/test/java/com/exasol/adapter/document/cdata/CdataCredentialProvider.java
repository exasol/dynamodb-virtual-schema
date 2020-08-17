package com.exasol.adapter.document.cdata;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

/**
 * This class reads the credential data required for the CData connector from a config file in the users home directory.
 */
public class CdataCredentialProvider {
    private static final String AWS_ACCESS_KEY = "aws_access_key";
    private static final String AWS_SECRET_KEY = "aws_secret_key";
    private static final String RTK = "rtk";

    private final JsonObject settings;

    public CdataCredentialProvider() throws IOException {
        try (final InputStream inputStream = new FileInputStream(
                new File(System.getProperty("user.home") + "/cdata_credentials.json"));
                final JsonReader reader = Json.createReader(inputStream)) {
            this.settings = reader.readObject();
        }
    }

    public String getAwsAccessKey() {
        return this.settings.getString(AWS_ACCESS_KEY);
    }

    public String getAwsSecretKey() {
        return this.settings.getString(AWS_SECRET_KEY);
    }

    public String getRtk() {
        return this.settings.getString(RTK);
    }
}
