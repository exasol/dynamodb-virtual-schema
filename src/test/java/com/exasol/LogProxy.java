package com.exasol;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

import org.testcontainers.shaded.org.apache.commons.io.IOUtils;

/**
 * This class opens a TCP socket and dumps everything to {@code STDOUT}. It is used for printing logs from the UDF.
 */
public final class LogProxy implements Runnable {
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

    @Override
    protected void finalize() throws Throwable {
        try {
            this.server.close();
        } finally {
            super.finalize();
        }
    }

    private static class Logger implements Runnable {
        private final Socket socket;

        private Logger(final Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try (final BufferedReader reader = new BufferedReader(
                    new InputStreamReader(this.socket.getInputStream()))) {
                IOUtils.copy(this.socket.getInputStream(), System.out);
            } catch (final IOException exception) {
                exception.printStackTrace();
            }
        }
    }
}
