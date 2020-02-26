/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.transactional.runtimes;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.nifi.transactional.bootstrap.RunnableFlow;
import org.apache.nifi.transactional.core.TransactionalFlow;
import org.apache.nifi.transactional.core.TransactionalFlowFile;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Program {

    public static final String RUN_FROM_REGISTRY = "RunFromRegistry";

    public static void launch(final String[] args, final ClassLoader systemClassLoader, final File narWorkingDirectory) throws Exception {

        if (args.length == 0) {
            printUsage();
            System.exit(1);
        } else if (args[0].equals(RUN_FROM_REGISTRY) && (args[1].equalsIgnoreCase("Once") || args[1].equalsIgnoreCase("Continuous")) && args.length >= 4) {
            runLocal(args, systemClassLoader, narWorkingDirectory);
        } else {
            System.out.println("Invalid input: " + String.join(",", args));
            printUsage();
            System.exit(1);
        }
    }

    private static void runLocal(final String[] args, final ClassLoader systemClassLoader, final File narWorkingDirectory) throws Exception {
        final boolean once = args[1].equalsIgnoreCase("Once");

        final String json;
        if (args[2].equals("--file")) {
            json = new String(Files.readAllBytes(Paths.get(args[3])));
        } else if (args[2].equals("--json")) {
            json = args[3];
        }  else if (args[2].equals("--yarnjson")) {
            json = args[3].replace(';',',');
        } else {
            System.out.println("Invalid input: " + String.join(",", args));
            printUsage();
            System.exit(1);
            return;
        }
        JsonObject jsonObject = new JsonParser().parse(json).getAsJsonObject();
        System.out.println("Running from json:");
        System.out.println(jsonObject.toString());
        final RunnableFlow flow = TransactionalFlow.createAndEnqueueFromJSON(jsonObject, systemClassLoader, narWorkingDirectory);

        // Run Flow
        final Queue<TransactionalFlowFile> outputFlowFiles = new LinkedList<>();
        final boolean successful;
        if (once) {
            successful = flow.runOnce(outputFlowFiles);
        } else {
            successful = flow.run(outputFlowFiles);  //Run forever
        }

        if (successful) {
            System.out.println("Flow Succeeded");
            outputFlowFiles.forEach(f -> System.out.println(f.toString()));
        } else {
            System.out.println("Flow Failed");
            outputFlowFiles.forEach(f -> System.out.println(f.toString()));
            System.exit(1);
        }
    }


    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("       " + RUN_FROM_REGISTRY + " [Once|Continuous] --json <JSON>");
        System.out.println("       " + RUN_FROM_REGISTRY + " [Once|Continuous] --file <File Name>");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("    1) " + RUN_FROM_REGISTRY + " Once --json \"{\\\"registryUrl\\\":\\\"http://172.26.198.107:61080\\\",\\\"bucketId\\\":\\\"5eec8794-01b3-4cd7-8536-0167c8b4ce8c\\\",\\\"flowId\\\": \\\"c5fa1d4f-b453-4bf5-8ff3-352352c418f3\\\"}\"");
        System.out.println();
        System.out.println("Notes:");
        System.out.println("    1) The configuration file must be in JSON format. ");
        System.out.println("    2) When providing configurations via JSON, the following attributes must be provided: " + TransactionalFlow.REGISTRY + ", " + TransactionalFlow.BUCKETID
            + ", " + TransactionalFlow.FLOWID + ".");
        System.out.println("          All other attributes will be passed to the flow using the variable registry interface");
        System.out.println();
    }
}
