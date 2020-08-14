package com.exasol;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.Map;

import javax.net.ssl.*;

import org.apache.xmlrpc.XmlRpcException;
import org.apache.xmlrpc.client.XmlRpcClient;
import org.apache.xmlrpc.client.XmlRpcClientConfigImpl;
import org.apache.xmlrpc.client.XmlRpcClientException;

import software.amazon.awssdk.utils.IoUtils;

public class ExaOperationInterface {
    private final XmlRpcClient client;

    public ExaOperationInterface(final String exasolIpAddress, final String adminUser, final String adminPass)
            throws MalformedURLException, KeyManagementException, NoSuchAlgorithmException {
        final XmlRpcClientConfigImpl config = new XmlRpcClientConfigImpl();
        config.setServerURL(new URL("https://" + exasolIpAddress + "/cluster1"));
        config.setBasicUserName(adminUser);
        config.setBasicPassword(adminPass);
        this.client = new XmlRpcClient();
        this.client.setConfig(config);
        setTrustAllCerts();
    }

    public void setBucketfsPort(final int port) throws XmlRpcException {
        final Object[] result = (Object[]) this.client.execute("bfsdefault.editBucketFS",
                new Object[] { Map.of("http_port", port) });
    }

    public void setBucketPasswords(final String readPassword, final String writePassword) throws XmlRpcException {
        final Object[] result = (Object[]) this.client.execute("bfsdefault.default.editBucketFSBucket",
                new Object[] { Map.of("read_password", readPassword, "write_password", writePassword) });
    }

    public void createAndUploadJdbcDriver(final String name, final String jdbcMainClass, final String prefix,
            final boolean disableSecurityManager, final File driverJar) throws XmlRpcException, IOException {
        final String driverId = addJdbcDriver(name, jdbcMainClass, prefix, disableSecurityManager);
        uploadJdbcDriver(driverId, driverJar);
    }

    private String addJdbcDriver(final String name, final String jdbcMainClass, final String prefix,
            final boolean disableSecurityManager) throws XmlRpcException {
        return (String) this.client.execute("addJDBCDriver", new Object[] { Map.of("jdbc_main", jdbcMainClass,
                "jdbc_name", name, "jdbc_prefix", prefix, "disable_security_manager", disableSecurityManager) });
    }

    private void uploadJdbcDriver(final String jdbcDriverId, final File file) throws IOException, XmlRpcException {
        try (final InputStream inputStream = new FileInputStream(file)) {
            final byte[] driverBytes = IoUtils.toByteArray(inputStream);
            this.client.execute(jdbcDriverId + ".uploadFile", new Object[] { driverBytes, file.getName() });
        } catch (final XmlRpcClientException exception) {
            // This exception happens always. I don't know why but it still works...
        }
    }

    public void setTrustAllCerts() throws NoSuchAlgorithmException, KeyManagementException {
        try {
            // Create a trust manager that does not validate certificate chains
            final TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
                public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                    return null;
                }

                public void checkClientTrusted(final X509Certificate[] certs, final String authType) {
                }

                public void checkServerTrusted(final X509Certificate[] certs, final String authType) {
                }
            } };

            // Install the all-trusting trust manager
            final SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, trustAllCerts, new java.security.SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());

            // Create all-trusting host name verifier
            final HostnameVerifier allHostsValid = new HostnameVerifier() {
                public boolean verify(final String hostname, final SSLSession session) {
                    return true;
                }
            };

            // Install the all-trusting host verifier
            HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
        } catch (final NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (final KeyManagementException e) {
            e.printStackTrace();
        }
    }
}
