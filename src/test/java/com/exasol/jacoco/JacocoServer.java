package com.exasol.jacoco;

/*
 * *****************************************************************************
 * Modified example from {@link https://raw.githubusercontent.com/jacoco/jacoco/master/org.jacoco.examples/src/org/jacoco/examples/ExecutionDataServer.java}
 *
 * Original license:
 *
 * Copyright (c) 2009, 2020 Mountainminds GmbH & Co. KG and Contributors
 * This program and the accompanying materials are made available under
 * the terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors:
 *    Marc R. Hoffmann - initial API and implementation
 *
 *******************************************************************************/

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;

import org.jacoco.core.data.*;
import org.jacoco.core.runtime.RemoteControlReader;
import org.jacoco.core.runtime.RemoteControlWriter;

/**
 * This class starts a socket server to collect the code coverage data from the jacoco agent in the udfs. The collected
 * data is dumped to a local file.
 */
public final class JacocoServer implements Runnable {

    private static final String DESTFILE = Path.of("target", "jacoco-udf.exec").toString();
    private static final int PORT = 3002;
    private static JacocoServer instance;
    private final ExecutionDataWriter fileWriter;

    private JacocoServer() throws IOException {
        this.fileWriter = new ExecutionDataWriter(new FileOutputStream(DESTFILE));
    }

    public static void startIfNotRunning() throws IOException {
        if (instance == null) {
            instance = new JacocoServer();
            new Thread(instance).start();
        }
    }

    @Override
    public void run() {
        try (final ServerSocket server = new ServerSocket(PORT)) {
            while (true) {
                final Handler handler = new Handler(server.accept(), this.fileWriter);
                new Thread(handler).start();
            }
        } catch (final IOException e) {
            throw new RuntimeException("Could not start jacoco server");
        }
    }

    private static class Handler implements Runnable, ISessionInfoVisitor, IExecutionDataVisitor {

        private final Socket socket;

        private final RemoteControlReader reader;

        private final ExecutionDataWriter fileWriter;

        Handler(final Socket socket, final ExecutionDataWriter fileWriter) throws IOException {
            this.socket = socket;
            this.fileWriter = fileWriter;

            // Just send a valid header:
            new RemoteControlWriter(socket.getOutputStream());

            this.reader = new RemoteControlReader(socket.getInputStream());
            this.reader.setSessionInfoVisitor(this);
            this.reader.setExecutionDataVisitor(this);
        }

        public void run() {
            try {
                while (this.reader.read()) {
                }
                this.socket.close();
                synchronized (this.fileWriter) {
                    this.fileWriter.flush();
                }
            } catch (final IOException e) {
                e.printStackTrace();
            }
        }

        public void visitSessionInfo(final SessionInfo info) {
            System.out.printf("Retrieving execution Data for session: %s%n", info.getId());
            synchronized (this.fileWriter) {
                this.fileWriter.visitSessionInfo(info);
            }
        }

        public void visitClassExecution(final ExecutionData data) {
            synchronized (this.fileWriter) {
                this.fileWriter.visitClassExecution(data);
            }
        }
    }
}