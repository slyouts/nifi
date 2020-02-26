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
package org.apache.nifi.transactional.core;

import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.util.NiFiProperties;

import java.io.File;

public class TransactionalProcessorInitializationContext implements ProcessorInitializationContext {
    private static final NodeTypeProvider STANDALONE = new NodeTypeProvider() {
            public boolean isClustered() {
                return false;
            }

            public boolean isPrimary() {
                // want data flows with primary processors to load, but not run the primary. Need to create a separate data flow that contains the
                // primary processors
                return false;
            }
        };
    private final ComponentLog logger;
    private final String processorId;
    private final ControllerServiceLookup controllerServiceLookup;
    private final NiFiProperties nifiProperties;

    public TransactionalProcessorInitializationContext(final String id, final Processor processor, final ProcessContext context, final NiFiProperties nifiProperties) {
        this.nifiProperties = nifiProperties;
        processorId = id;
        logger = new SLF4JComponentLog(processor);
        this.controllerServiceLookup = context.getControllerServiceLookup();
    }

    public TransactionalProcessorInitializationContext(final String id, final Processor processor, final ControllerServiceLookup controllerServiceLookup, final NiFiProperties nifiProperties) {
        this.nifiProperties = nifiProperties;
        processorId = id;
        logger = new SLF4JComponentLog(processor);
        this.controllerServiceLookup = controllerServiceLookup;
    }

    public String getIdentifier() {
        return processorId;
    }

    public ComponentLog getLogger() {
        return logger;
    }

    public ControllerServiceLookup getControllerServiceLookup() {
        return controllerServiceLookup;
    }

    public NodeTypeProvider getNodeTypeProvider() {
        return STANDALONE;
    }

    public String getKerberosServicePrincipal() {
        return nifiProperties.getKerberosServicePrincipal();
    }

    public File getKerberosServiceKeytab() {
        return new File(nifiProperties.getKerberosServiceKeytabLocation());
    }

    public File getKerberosConfigurationFile() {
        return nifiProperties.getKerberosConfigurationFile();
    }
}
