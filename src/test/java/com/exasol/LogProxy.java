package com.exasol;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

import org.testcontainers.shaded.org.apache.commons.io.IOUtils;

/**
 * This class opens a tcp socket and dumps everything to stdout. It is used for printing logs from the udf.
 */
public class LogProxy implements Runnable {
    private final static int PORT = 3001;
    private static LogProxy instance;
    private final ServerSocket server;

    public LogProxy() throws IOException {
        this.server = new ServerSocket(PORT);
    }

    public static void startIfNotRunning() throws IOException {
        if (instance == null) {
            instance = new LogProxy();
            new Thread(instance).start();
        }
    }

    @Override
    public void run() {
        try {
            while (true) {
                new Thread(new Logger(this.server.accept())).start();
            }
        } catch (final IOException exception) {
            exception.printStackTrace();
        }
    }

    private static class Logger implements Runnable {
        private final Socket socket;

        private Logger(final Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try {
                System.out.println("connected logger");
                final BufferedReader reader = new BufferedReader(new InputStreamReader(this.socket.getInputStream()));
                IOUtils.copy(this.socket.getInputStream(), System.out);
            } catch (final IOException exception) {
                exception.printStackTrace();
            }
        }
    }
}
