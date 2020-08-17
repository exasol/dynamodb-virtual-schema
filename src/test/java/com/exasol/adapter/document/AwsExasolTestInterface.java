package com.exasol.adapter.document;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.xmlrpc.XmlRpcException;

import com.exasol.ExaOperationInterface;
import com.exasol.TerraformInterface;

/**
 * Exasol test interface for exasol clusters running on AWS. Start the cluster by running {@code terraform apply} in
 * {@code /cloudSetup}
 */
public class AwsExasolTestInterface extends AbstractExasolTestInterface {
    private static final int BUCKETFS_PORT = 2580;
    private final String exasolIp;
    private final ExaOperationInterface exaOperationInterface;
    protected static final String READ_PASSWORD = "readpw";
    protected static final String WRITE_PASSWORD = "writepw";

    public AwsExasolTestInterface()
            throws IOException, NoSuchAlgorithmException, KeyManagementException, XmlRpcException {
        final TerraformInterface terraformInterface = new TerraformInterface();
        this.exasolIp = terraformInterface.getExasolDataNodeIp();
        this.exaOperationInterface = new ExaOperationInterface(terraformInterface.getExasolManagementNodeIp(), "admin",
                terraformInterface.getExasolAdminUserPass());
        this.exaOperationInterface.setBucketPasswords(READ_PASSWORD, WRITE_PASSWORD);
        this.exaOperationInterface.setBucketfsPort(BUCKETFS_PORT);
    }

    public static Connection getConnection(final String exasolIpAddress, final String sysUserPw)
            throws SQLException, IOException {
        return DriverManager.getConnection("jdbc:exa:" + exasolIpAddress + ":8563;schema=SYS", "sys", sysUserPw);
    }

    @Override
    public void teardown() {

    }

    @Override
    public String getTestHostIpAddress() {
        return null;
    }

    @Override
    protected String getContainerUrl() {
        return this.exasolIp;
    }

    @Override
    protected int getBucketFsPort() {
        return BUCKETFS_PORT;
    }

    @Override
    protected String getBucketFsReadPassword() {
        return READ_PASSWORD;
    }

    @Override
    protected String getBucketFsWritePassword() {
        return WRITE_PASSWORD;
    }

    @Override
    public Connection getConnection() throws SQLException, IOException {
        return getConnection(this.exasolIp, new TerraformInterface().getExasolSysUserPass());
    }

    public ExaOperationInterface getExaOperationInterface() {
        return this.exaOperationInterface;
    }
}
