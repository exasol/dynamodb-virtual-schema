package com.exasol;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

/**
 * This offers an interface for reading terraform outputs from java code.
 */
public class TerraformInterface {

    public String getExasolManagementNodeIp() throws IOException {
        return getOutput("exasol_ip");
    }

    public String getExasolDataNodeIp() throws IOException {
        return getOutput("exasol_datanode_ip");
    }

    public String getExasolAdminUserPass() throws IOException {
        return getOutput("exasol_admin_pw");
    }

    public String getExasolSysUserPass() throws IOException {
        return getOutput("exasol_sys_pw");
    }

    private String getOutput(final String outputName) throws IOException {
        final Runtime runtime = Runtime.getRuntime();
        final String[] command = { "terraform", "output", "-state=./cloudSetup/terraform.tfstate", outputName };
        final Process terraformProcess = runtime.exec(command);
        final BufferedReader stdInput = new BufferedReader(new InputStreamReader(terraformProcess.getInputStream()));
        final String output = stdInput.readLine();
        if (output == null) {
            final StringBuilder errorMessage = new StringBuilder("Could not read terraform output:\n");
            final BufferedReader stdError = new BufferedReader(
                    new InputStreamReader(terraformProcess.getErrorStream()));
            errorMessage.append(stdError.lines().collect(Collectors.joining("\n")));
            throw new IllegalStateException(errorMessage.toString());
        }
        return output;
    }
}
