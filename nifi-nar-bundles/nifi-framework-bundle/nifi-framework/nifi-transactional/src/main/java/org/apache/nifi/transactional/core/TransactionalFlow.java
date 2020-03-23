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

import static java.util.Objects.requireNonNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import javax.net.ssl.SSLContext;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.shaded.com.google.common.base.Functions;
import org.apache.nifi.annotation.lifecycle.OnAdded;
import org.apache.nifi.annotation.lifecycle.OnConfigurationRestored;
import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.components.validation.StandardValidationTrigger;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.connectable.Size;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.MissingBundleException;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.StandardFlowSynchronizer;
import org.apache.nifi.controller.Template;
import org.apache.nifi.controller.TemplateUtils;
import org.apache.nifi.controller.exception.ComponentLifeCycleException;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.kerberos.KerberosConfig;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.queue.LoadBalanceCompression;
import org.apache.nifi.controller.queue.LoadBalanceStrategy;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.metrics.RingBufferEventRepository;
import org.apache.nifi.controller.scheduling.StandardProcessScheduler;
import org.apache.nifi.controller.serialization.FlowEncodingVersion;
import org.apache.nifi.controller.serialization.FlowFromDOMFactory;
import org.apache.nifi.controller.serialization.FlowSerializationException;
import org.apache.nifi.controller.service.ControllerServiceLoader;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.state.manager.StandardStateManagerProvider;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.events.VolatileBulletinRepository;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroupPortDescriptor;
import org.apache.nifi.groups.StandardProcessGroup;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.logging.LogRepository;
import org.apache.nifi.logging.LogRepositoryFactory;
import org.apache.nifi.transactional.util.RegistryUtil;
import org.apache.nifi.util.BundleUtils;
import org.apache.nifi.util.DomUtils;
import org.apache.nifi.util.LoggingXmlParserErrorHandler;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.ReflectionUtils;
import org.apache.nifi.util.file.FileUtils;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.ConnectableDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.FunnelDTO;
import org.apache.nifi.web.api.dto.LabelDTO;
import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.dto.ParameterDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.ReportingTaskDTO;
import org.apache.nifi.web.api.dto.TemplateDTO;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.apache.nifi.web.api.entity.ParameterEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterContextManager;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.parameter.StandardParameterContextManager;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.registry.ComponentVariableRegistry;
import org.apache.nifi.registry.VariableDescriptor;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.flow.ConnectableComponent;
import org.apache.nifi.registry.flow.FlowRegistry;
import org.apache.nifi.registry.flow.StandardVersionControlInformation;
import org.apache.nifi.registry.flow.VersionedConnection;
import org.apache.nifi.registry.flow.VersionedControllerService;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedFlowState;
import org.apache.nifi.registry.flow.VersionedPort;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.registry.flow.VersionedProcessor;
import org.apache.nifi.registry.flow.VersionedRemoteGroupPort;
import org.apache.nifi.registry.flow.VersionedRemoteProcessGroup;
import org.apache.nifi.registry.variable.FileBasedVariableRegistry;
import org.apache.nifi.registry.variable.MutableVariableRegistry;
import org.apache.nifi.registry.variable.StandardComponentVariableRegistry;
import org.apache.nifi.remote.PublicPort;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.scheduling.ExecutionNode;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.stateless.core.SLF4JComponentLog;
import org.apache.nifi.stateless.core.StatelessControllerServiceLookup;
import org.apache.nifi.stateless.core.StatelessProcessContext;
import org.apache.nifi.stateless.core.StatelessStateManager;
import org.apache.nifi.transactional.bootstrap.ExtensionDiscovery;
import org.apache.nifi.transactional.bootstrap.RunnableFlow;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class TransactionalFlow implements RunnableFlow {

    public static final String REGISTRY = "registryUrl";
    public static final String BUCKETID = "bucketId";
    public static final String FLOWID = "flowId";
    public static final String FLOWVERSION = "flowVersion";
    public static final String FAILUREPORTS = "nifi.failurePortIds";
    public static final String FLOWFILES = "flowFiles";
    public static final String CONTENT = "nifi_content";
    public static final String PARAMETERS = "parameters";
    public static final String PARAMETER_SENSITIVE = "sensitive";
    public static final String PARAMETER_VALUE = "value";

    public static final String SSL = "ssl";
    public static final String KEYSTORE = "keystore";
    public static final String KEYSTORE_PASS = "keystorePass";
    public static final String KEY_PASS = "keyPass";
    public static final String KEYSTORE_TYPE = "keystoreType";
    public static final String TRUSTSTORE = "truststore";
    public static final String TRUSTSTORE_PASS = "truststorePass";
    public static final String TRUSTSTORE_TYPE = "truststoreType";
    public static final URL FLOW_XSD_RESOURCE = StringEncryptor.class.getResource("/FlowConfiguration.xsd");

    static final ParameterContext EMPTY_PC = new TransactionalParameterContext("0", "controller", Collections.emptyMap());

    private static final Logger logger = LoggerFactory.getLogger(TransactionalFlow.class);

    private TransactionalComponent sourceComponent = null;

    private StateManagerProvider stateManagerProvider;

    public TransactionalFlow(TransactionalFlowManager flowManager, ExtensionManager extensionManager, SSLContext sslContext,
            Map<String, ParameterContext> paramaterContexts, StateManagerProvider stateManagerProvider, VariableRegistry baseVarRegistry,
            StringEncryptor encryptor) {

        final Map<String, VersionedProcessor> processors = findProcessorsRecursive(flow).stream()
                .collect(Collectors.toMap(VersionedProcessor::getIdentifier, proc -> proc));

        final Map<String, VersionedRemoteProcessGroup> rpgs = new HashMap<>();
        final Map<String, VersionedRemoteGroupPort> remotePorts = new HashMap<>();
        findRemoteGroupRecursive(flow, rpgs, remotePorts);

        final Set<VersionedConnection> connections = findConnectionsRecursive(flow);
        final Set<VersionedPort> inputPorts = flow.getInputPorts();

        if (inputPorts.size() > 1) {
            throw new IllegalArgumentException("Only one input port per flow is allowed");
        }

        final TransactionalControllerServiceLookup serviceLookup = new TransactionalControllerServiceLookup(parameterContext);

        final Set<VersionedControllerService> controllerServices = flow.getControllerServices();
        for (final VersionedControllerService versionedControllerService : controllerServices) {
            final StateManager stateManager = new StatelessStateManager();

            final ControllerService service = componentFactory.createControllerService(versionedControllerService, variableRegistry, serviceLookup,
                    stateManager, parameterContext);
            serviceLookup.addControllerService(service, versionedControllerService.getName());
            serviceLookup.setControllerServiceAnnotationData(service, versionedControllerService.getAnnotationData());

            final SLF4JComponentLog logger = new SLF4JComponentLog(service);
            final TransactionalProcessContext processContext = new TransactionalProcessContext(service, serviceLookup,
                    versionedControllerService.getName(), logger, stateManager, variableRegistry, parameterContext, encryptor);

            final Map<String, String> versionedPropertyValues = versionedControllerService.getProperties();
            for (final Map.Entry<String, String> entry : versionedPropertyValues.entrySet()) {
                final String propertyName = entry.getKey();
                final String propertyValue = entry.getValue();
                final PropertyDescriptor descriptor = service.getPropertyDescriptor(propertyName);

                serviceLookup.setControllerServiceProperty(service, descriptor, processContext, variableRegistry, propertyValue);
            }

            for (final PropertyDescriptor descriptor : service.getPropertyDescriptors()) {
                final String versionedPropertyValue = versionedPropertyValues.get(descriptor.getName());
                if (versionedPropertyValue == null && descriptor.getDefaultValue() != null) {
                    serviceLookup.setControllerServiceProperty(service, descriptor, processContext, variableRegistry, descriptor.getDefaultValue());
                }
            }
        }

        serviceLookup.enableControllerServices(variableRegistry);

        final Map<String, TransactionalComponent> componentMap = new HashMap<>();
        for (final VersionedConnection connection : connections) {
            boolean isInputPortConnection = false;

            final ConnectableComponent source = connection.getSource();
            final ConnectableComponent destination = connection.getDestination();

            TransactionalComponent sourceComponent = null;
            if (componentMap.containsKey(source.getId())) {
                sourceComponent = componentMap.get(source.getId());
            } else {
                switch (source.getType()) {
                case PROCESSOR:
                    final VersionedProcessor processor = processors.get(source.getId());

                    if (processor == null) {
                        throw new IllegalArgumentException("Unknown input processor. " + source.getId());
                    } else {
                        sourceComponent = componentFactory.createProcessor(processor, serviceLookup, variableRegistry, null, parameterContext);
                        componentMap.put(source.getId(), sourceComponent);
                    }
                    break;
                case REMOTE_INPUT_PORT:
                    throw new IllegalArgumentException("Unsupported source type: " + source.getType());
                case REMOTE_OUTPUT_PORT:
                    final VersionedRemoteGroupPort remotePort = remotePorts.get(source.getId());
                    final VersionedRemoteProcessGroup rpg = rpgs.get(remotePort.getRemoteGroupId());

                    sourceComponent = new StatelessRemoteOutputPort(rpg, remotePort, sslContext);
                    componentMap.put(source.getId(), sourceComponent);
                    break;
                case OUTPUT_PORT:
                case FUNNEL:
                    sourceComponent = new StatelessPassThroughComponent();
                    componentMap.put(source.getId(), sourceComponent);
                    break;
                case INPUT_PORT:
                    if (flow.getIdentifier().equals(connection.getGroupIdentifier())) {
                        isInputPortConnection = true;
                    } else {
                        sourceComponent = new StatelessPassThroughComponent();
                        componentMap.put(source.getId(), sourceComponent);
                    }

                    break;
                }
            }

            TransactionalComponent destinationComponent = null;
            switch (destination.getType()) {
            case PROCESSOR:
                if (componentMap.containsKey(destination.getId())) {
                    destinationComponent = componentMap.get(destination.getId());
                } else {
                    final VersionedProcessor processor = processors.get(destination.getId());
                    if (processor == null) {
                        return;
                    }

                    destinationComponent = componentFactory.createProcessor(processor, serviceLookup, variableRegistry, null, parameterContext);
                    destinationComponent.addParent(sourceComponent);
                    componentMap.put(destination.getId(), destinationComponent);
                }

                break;
            case REMOTE_INPUT_PORT:
                if (componentMap.containsKey(destination.getId())) {
                    destinationComponent = componentMap.get(destination.getId());
                } else {
                    final VersionedRemoteGroupPort remotePort = remotePorts.get(destination.getId());
                    final VersionedRemoteProcessGroup rpg = rpgs.get(remotePort.getRemoteGroupId());

                    destinationComponent = new StatelessRemoteInputPort(rpg, remotePort, sslContext);
                    destinationComponent.addParent(sourceComponent);
                    componentMap.put(destination.getId(), destinationComponent);
                }

                break;
            case REMOTE_OUTPUT_PORT:
                throw new IllegalArgumentException("Unsupported destination type: " + destination.getType());
            case OUTPUT_PORT:
                if (isInputPortConnection) {
                    throw new IllegalArgumentException("Input ports can not be mapped directly to output ports...");
                }

                // If Output Port is top-level port, treat it differently than if it's an inner group.
                if (flow.getIdentifier().equals(connection.getGroupIdentifier())) {
                    // Link source and destination
                    for (final String selectedRelationship : connection.getSelectedRelationships()) {
                        final Relationship relationship = new Relationship.Builder().name(selectedRelationship).build();
                        final boolean failurePort = failureOutputPorts.contains(destination.getId());
                        sourceComponent.addOutputPort(relationship, failurePort);
                    }

                    break;
                }

                // Intentionally let the flow drop-through, and treat the same as an output port or funnel.
            case INPUT_PORT:
            case FUNNEL:
                if (componentMap.containsKey(destination.getId())) {
                    destinationComponent = componentMap.get(destination.getId());
                } else {
                    destinationComponent = new StatelessPassThroughComponent();
                    componentMap.put(destination.getId(), destinationComponent);
                }

                break;
            }

            if (destinationComponent != null) {
                destinationComponent.addIncomingConnection(connection.getIdentifier());

                if (isInputPortConnection) {
                    this.sourceComponent = destinationComponent;
                } else {
                    destinationComponent.addParent(sourceComponent);

                    // Link source and destination
                    for (final String relationship : connection.getSelectedRelationships()) {
                        sourceComponent.addChild(destinationComponent, new Relationship.Builder().name(relationship).build());
                    }
                }

            }
        }

        roots = componentMap.values().stream().filter(statelessComponent -> statelessComponent.getParents().isEmpty()).collect(Collectors.toList());
    }

    private Set<VersionedProcessor> findProcessorsRecursive(final VersionedProcessGroup group) {
        final Set<VersionedProcessor> processors = new HashSet<>();
        findProcessorsRecursive(group, processors);
        return processors;
    }

    private void findProcessorsRecursive(final VersionedProcessGroup group, final Set<VersionedProcessor> processors) {
        processors.addAll(group.getProcessors());
        group.getProcessGroups().forEach(child -> findProcessorsRecursive(child, processors));
    }

    private Set<VersionedConnection> findConnectionsRecursive(final VersionedProcessGroup group) {
        final Set<VersionedConnection> connections = new HashSet<>();
        findConnectionsRecursive(group, connections);
        return connections;
    }

    private void findConnectionsRecursive(final VersionedProcessGroup group, final Set<VersionedConnection> connections) {
        connections.addAll(group.getConnections());
        group.getProcessGroups().forEach(child -> findConnectionsRecursive(child, connections));
    }

    private void findRemoteGroupRecursive(final VersionedProcessGroup group, final Map<String, VersionedRemoteProcessGroup> rpgs,
            final Map<String, VersionedRemoteGroupPort> ports) {
        for (final VersionedRemoteProcessGroup rpg : group.getRemoteProcessGroups()) {
            rpgs.put(rpg.getIdentifier(), rpg);

            rpg.getInputPorts().forEach(port -> ports.put(port.getIdentifier(), port));
            rpg.getOutputPorts().forEach(port -> ports.put(port.getIdentifier(), port));
        }
    }

    public boolean run(final Queue<TransactionalFlowFile> output) {
        while (!this.stopRequested) {
            for (final TransactionalComponent pw : roots) {
                final boolean successful = pw.runRecursive(output);
                if (!successful) {
                    return false;
                }
            }
        }

        return true;
    }

    public boolean runOnce(Queue<TransactionalFlowFile> output) {
        for (final TransactionalComponent pw : roots) {
            final boolean successful = pw.runRecursive(output);
            if (!successful) {
                return false;
            }
        }

        return true;
    }

    public void shutdown() {
        this.stopRequested = true;
        this.roots.forEach(TransactionalComponent::shutdown);
    }

    private static void updateProcessor(final ProcessorNode procNode, final ProcessorDTO processorDTO, final ProcessGroup processGroup,
            FlowManager flowManager) {

        procNode.pauseValidationTrigger();
        try {
            final ProcessorConfigDTO config = processorDTO.getConfig();
            procNode.setProcessGroup(processGroup);
            procNode.setLossTolerant(config.isLossTolerant());
            procNode.setPenalizationPeriod(config.getPenaltyDuration());
            procNode.setYieldPeriod(config.getYieldDuration());
            procNode.setBulletinLevel(LogLevel.valueOf(config.getBulletinLevel()));
            updateNonFingerprintedProcessorSettings(procNode, processorDTO);

            if (config.getSchedulingStrategy() != null) {
                procNode.setSchedulingStrategy(SchedulingStrategy.valueOf(config.getSchedulingStrategy()));
            }

            // if (config.getExecutionNode() != null) {
            // procNode.setExecutionNode(ExecutionNode.valueOf(config.getExecutionNode()));
            // }

            // must set scheduling strategy before these two
            procNode.setMaxConcurrentTasks(config.getConcurrentlySchedulableTaskCount());
            procNode.setScheduldingPeriod(config.getSchedulingPeriod());
            if (config.getRunDurationMillis() != null) {
                procNode.setRunDuration(config.getRunDurationMillis(), TimeUnit.MILLISECONDS);
            }

            procNode.setAnnotationData(config.getAnnotationData());

            if (config.getAutoTerminatedRelationships() != null) {
                final Set<Relationship> relationships = new HashSet<>();
                for (final String rel : config.getAutoTerminatedRelationships()) {
                    relationships.add(procNode.getRelationship(rel));
                }
                procNode.setAutoTerminatedRelationships(relationships);
            }

            procNode.setProperties(config.getProperties());

            procNode.performValidation(); // ensure that processor has been validated
            String groupId = processGroup.getIdentifier();
            String processorId = procNode.getIdentifier();
            final ProcessGroup group = flowManager.getGroup(groupId);
            if (group == null) {
                throw new IllegalStateException("No Group with ID " + groupId + " exists");
            }

            final ProcessorNode node = group.getProcessor(processorId);
            if (node == null) {
                throw new IllegalStateException("Cannot find ProcessorNode with ID " + processorId + " within ProcessGroup with ID " + groupId);
            }

            group.startProcessor(node, true);
        } finally {
            procNode.resumeValidationTrigger();
        }
    }

    private static void updateNonFingerprintedProcessorSettings(final ProcessorNode procNode, final ProcessorDTO processorDTO) {
        procNode.setName(processorDTO.getName());
        procNode.setStyle(processorDTO.getStyle());
        procNode.setComments(processorDTO.getConfig().getComments());
    }

    public static SSLContext getSSLContext() {
        String keystore = System.getenv(KEYSTORE);
        String keystorePass = System.getenv(KEYSTORE_PASS);
        String keystoreType = System.getenv(KEYSTORE_TYPE);
        String truststore = System.getenv(TRUSTSTORE);
        String truststorePass = System.getenv(TRUSTSTORE_PASS);
        String truststoreType = System.getenv(TRUSTSTORE_TYPE);

        if (StringUtils.isNoneBlank(keystore, keystorePass, keystoreType, truststore, truststorePass, truststoreType)) {

            String keyPass = System.getenv(KEY_PASS);
            if (StringUtils.isBlank(keyPass)) {
                keyPass = keystorePass;
            }

            try {
                return SslContextFactory.createSslContext(keystore, keystorePass.toCharArray(), keyPass.toCharArray(), keystoreType, truststore,
                        truststorePass.toCharArray(), truststoreType, SslContextFactory.ClientAuth.REQUIRED, "TLS");
            } catch (final Exception e) {
                throw new RuntimeException("Failed to create Keystore", e);
            }
        }

        return null;
    }

    static void createControllerService(final Element cse, final StringEncryptor encryptor, final FlowEncodingVersion encodingVersion,
            final TransactionalProcessGroup processGroup, final ControllerServiceProvider serviceProvider, final List<ControllerServiceDTO> dtos,
            final TransactionalFlowManager flowManager) {

        ControllerServiceDTO csDTO = FlowFromDOMFactory.getControllerService(cse, encryptor, encodingVersion);
        dtos.add(csDTO);
        BundleDTO bundleDTO = csDTO.getBundle();
        String type = csDTO.getType();
        String id = csDTO.getId();
        final BundleCoordinate coordinate = new BundleCoordinate(bundleDTO.getGroup(), bundleDTO.getArtifact(), bundleDTO.getVersion());
        ControllerServiceNode serviceNode = flowManager.createControllerService(type, id, coordinate, Collections.emptySet(), true, true);

        serviceNode.setComments(csDTO.getComments());
        serviceNode.setName(csDTO.getName());
        if (Objects.nonNull(processGroup)) {
            serviceNode.setVersionedComponentId(csDTO.getVersionedComponentId());
            processGroup.addControllerService(serviceNode);
        } else {
            flowManager.addRootControllerService(serviceNode);
        }
        serviceNode.pauseValidationTrigger();
    }

    public static TransactionalFlow createContextAndFlow(final ExtensionManager extensionManager, final NiFiProperties properties)
            throws InitializationException, IOException, ProcessorInstantiationException, NiFiRegistryException {

        // Get flow config file location from props
        File flowConfigFile = properties.getFlowConfigurationFile();
        final String algorithm = properties.getProperty(NiFiProperties.SENSITIVE_PROPS_ALGORITHM);
        final String provider = properties.getProperty(NiFiProperties.SENSITIVE_PROPS_PROVIDER);
        final String password = properties.getProperty(NiFiProperties.SENSITIVE_PROPS_KEY);
        final StringEncryptor encryptor = StringEncryptor.createEncryptor(algorithm, provider, password);
        final SSLContext sslContext = getSSLContext();
        String principal = properties.getKerberosServicePrincipal();
        File keytabLocation = new File(properties.getKerberosServiceKeytabLocation());
        File kerberosConfigurationFile = properties.getKerberosConfigurationFile();
        final KerberosConfig kerberosConfig = new KerberosConfig(principal, keytabLocation, kerberosConfigurationFile);

        // this requires the existence of a file which contains the state of all variables
        VariableRegistry baseVarRegistry = new FileBasedVariableRegistry(properties.getVariableRegistryPropertiesPaths());

        // load the flow file
        try {
            byte[] proposedFlow = readFlowFromDisk(properties);
            if (proposedFlow == null || proposedFlow.length == 0) {
                return null;
            }

            final Document configuration = parseFlowBytes(proposedFlow);
            checkBundleCompatibility(configuration, extensionManager);
            final Element rootElement = configuration.getDocumentElement();
            final FlowEncodingVersion encodingVersion = FlowEncodingVersion.parse(rootElement);

            logger.trace("Setting controller thread counts");
            final Integer maxThreadCount = getInteger(rootElement, "maxThreadCount");
            final Integer maxTimerThreadCount = getInteger(rootElement, "maxTimerDrivenThreadCount");

            FlowEngine validationThreadPool = new FlowEngine(5, "Validate Components", true);
            AtomicBoolean initialized = new AtomicBoolean(false);
            StandardValidationTrigger validationTrigger = new StandardValidationTrigger(validationThreadPool, () -> initialized.get());

            final StateManagerProvider stateManagerProvider = TransactionalStateManagerProvider.create(properties, baseVarRegistry, extensionManager,
                    ParameterLookup.EMPTY);
            stateManagerProvider.enableClusterProvider();
            FlowEngine timerDrivenEngineRef = new FlowEngine(maxTimerThreadCount, "Timer-Driven Process", true);

            TransactionalProcessScheduler scheduler = new TransactionalProcessScheduler(timerDrivenEngineRef, encryptor, stateManagerProvider,
                    extensionManager, properties);

            BulletinRepository bulletinRepo = new VolatileBulletinRepository();
            TransactionalControllerServiceProvider serviceProvider = new TransactionalControllerServiceProvider(extensionManager, scheduler, bulletinRepo);
            FlowFileEventRepository flowFileEventRepository = new RingBufferEventRepository(5);
            TransactionalReloadComponent reloadComponent = new TransactionalReloadComponent(extensionManager, baseVarRegistry, serviceProvider,
                    stateManagerProvider, validationTrigger, encryptor);
            ParameterContextManager parameterContextManager = new StandardParameterContextManager();
            TransactionalFlowManager flowManager = new TransactionalFlowManager(properties, sslContext, extensionManager, scheduler,
                    stateManagerProvider, baseVarRegistry, flowFileEventRepository, parameterContextManager, serviceProvider, validationTrigger,
                    reloadComponent, kerberosConfig);

            final Element parameterContextsElement = DomUtils.getChild(rootElement, "parameterContexts");
            Map<String, ParameterContext> paramaterContexts = new HashMap<>();
            if (parameterContextsElement != null) {
                final List<Element> contextElements = DomUtils.getChildElementsByTagName(parameterContextsElement, "parameterContext");
                for (final Element contextElement : contextElements) {
                    Map<String, Parameter> parameters;
                    String paramContextID = DomUtils.getChildText(contextElement, "id");
                    String paramContextName = DomUtils.getChildText(contextElement, "name");
                    List<Element> params = DomUtils.getChildElementsByTagName(contextElement, "parameter");
                    for (Element param : params) {
                        String value = DomUtils.getChildText(param, "value");
                        String name = DomUtils.getChildText(param, "name");
                        String description = DomUtils.getChildText(param, "description");
                        boolean sensitive = Boolean.parseBoolean(DomUtils.getChildText(param, "sensitive"));
                        ParameterDescriptor descriptor = new ParameterDescriptor.Builder().name(name).description(description).sensitive(sensitive)
                                .build();
                        Parameter result = new Parameter(descriptor, value);
                        parameters.put(name, result);
                    }

                    final ParameterContext pc = flowManager.createParameterContext(paramContextID, paramContextName, parameters);
                    paramaterContexts.put(paramContextID, pc);
                }
            }
            // get the root group XML element
            logger.trace("Adding root process group");
            final Element rootGroupElement = (Element) rootElement.getElementsByTagName("rootGroup").item(0);

            // get the process groups...this recurses down through the child ProcessGroups
            final TransactionalProcessGroup rootGroup = addProcessGroup(flowManager, null, rootGroupElement, encryptor, encodingVersion,
                    serviceProvider, extensionManager);

            // need to build the controller level controller services...these are the ones used by ReportingTasks
            final Element controllerServicesElement = DomUtils.getChild(rootElement, "controllerServices");
            if (controllerServicesElement != null) {
                final List<ControllerServiceDTO> dtos = new ArrayList<>();
                try {
                    List<Element> controllerServices = DomUtils.getChildElementsByTagName(controllerServicesElement, "controllerService");
                    controllerServices.forEach(cse -> {
                        createControllerService(cse, encryptor, encodingVersion, null, serviceProvider, dtos, flowManager);
                    });

                    // configure controller services. We do this after creating all of them in case 1 service
                    // references another service.
                    dtos.forEach(csDTO -> {
                        final String serviceId = csDTO.getId();
                        ControllerServiceNode csn = serviceProvider.getControllerServiceNode(serviceId);
                        csn.setAnnotationData(csDTO.getAnnotationData());
                        csn.setProperties(csDTO.getProperties()); // this will cause a reload and expanding of classLoader if need be
                    });
                } finally {
                    dtos.forEach(csDTO -> {
                        ControllerServiceNode csn = serviceProvider.getControllerServiceNode(csDTO.getId());
                        csn.resumeValidationTrigger();
                    });
                }
            }

            final Element reportingTasksElement = DomUtils.getChild(rootElement, "reportingTasks");
            final List<ReportingTaskDTO> taskElements;
            if (reportingTasksElement != null) {
                DomUtils.getChildElementsByTagName(reportingTasksElement, "reportingTask")
                        .forEach(rte -> taskElements.add(FlowFromDOMFactory.getReportingTask(rte, encryptor, encodingVersion)));
                taskElements.forEach(rtDTO -> {
                    StateManager stateManager = stateManagerProvider.getStateManager(rtDTO.getId());
                    String type = rtDTO.getType();
                    BundleDTO bundleDTO = rtDTO.getBundle();
                    BundleCoordinate bundleCoordinate = new BundleCoordinate(bundleDTO.getGroup(), bundleDTO.getArtifact(), bundleDTO.getVersion());
                    ReportingTaskNode reportingTask = flowManager.createReportingTask(type, bundleCoordinate, true);
                    reportingTask.setName(rtDTO.getName());
                    reportingTask.setComments(rtDTO.getComments());
                    reportingTask.setSchedulingPeriod(rtDTO.getSchedulingPeriod());
                    reportingTask.setSchedulingStrategy(SchedulingStrategy.valueOf(rtDTO.getSchedulingStrategy()));
                    reportingTask.setAnnotationData(rtDTO.getAnnotationData());
                    reportingTask.setProperties(rtDTO.getProperties());
                });
            }
            Collection<ControllerServiceNode> services = flowManager.getRootControllerServices();
            services.forEach(ControllerServiceNode::performValidation); // validate services before attempting to enable them
            serviceProvider.enableControllerServices(services);

            final TransactionalFlow flow = new TransactionalFlow(flowManager, extensionManager, sslContext, paramaterContexts, stateManagerProvider,
                    baseVarRegistry, encryptor);
        } catch (Exception e) {
            throw new FlowSerializationException(e);
        }

        // create ParameterContexts
        // create Variables

        int flowVersion = -1;
        if (args.has(FLOWVERSION)) {
            flowVersion = args.getAsJsonPrimitive(FLOWVERSION).getAsInt();
        }

        final List<String> failurePorts = new ArrayList<>();
        if (args.has(FAILUREPORTS)) {
            args.getAsJsonArray(FAILUREPORTS).forEach(port -> failurePorts.add(port.getAsString()));
        }

        final VersionedFlowSnapshot snapshot = new RegistryUtil(registryurl, sslContext).getFlowByID(bucketID, flowID, flowVersion);

        final Map<VariableDescriptor, String> inputVariables = new HashMap<>();
        final VersionedProcessGroup versionedGroup = snapshot.getFlowContents();
        if (versionedGroup != null) {
            for (final Map.Entry<String, String> entry : versionedGroup.getVariables().entrySet()) {
                final String variableName = entry.getKey();
                final String variableValue = entry.getValue();
                inputVariables.put(new VariableDescriptor(variableName), variableValue);
            }
        }

        final Map<String, Parameter> parameters = new LinkedHashMap<>();
        final Set<String> parameterNames = new HashSet<>();
        if (args.has(PARAMETERS)) {
            final JsonElement parametersElement = args.get(PARAMETERS);
            final JsonObject parametersObject = parametersElement.getAsJsonObject();

            for (final Map.Entry<String, JsonElement> entry : parametersObject.entrySet()) {
                final String parameterName = entry.getKey();
                final JsonElement valueElement = entry.getValue();

                if (parameterNames.contains(parameterName)) {
                    throw new IllegalStateException("Cannot parse configuration because Parameter '" + parameterName + "' has been defined twice");
                }

                parameterNames.add(parameterName);

                if (valueElement.isJsonObject()) {
                    final JsonObject valueObject = valueElement.getAsJsonObject();

                    final boolean sensitive;
                    if (valueObject.has(PARAMETER_SENSITIVE)) {
                        sensitive = valueObject.get(PARAMETER_SENSITIVE).getAsBoolean();
                    } else {
                        sensitive = false;
                    }

                    if (valueObject.has(PARAMETER_VALUE)) {
                        final String value = valueObject.get(PARAMETER_VALUE).getAsString();
                        final ParameterDescriptor descriptor = new ParameterDescriptor.Builder().name(parameterName).sensitive(sensitive).build();
                        final Parameter parameter = new Parameter(descriptor, value);
                        parameters.add(parameter);
                    } else {
                        throw new IllegalStateException(
                                "Cannot parse configuration because Parameter '" + parameterName + "' does not have a value associated with it");
                    }
                } else {
                    final String parameterValue = entry.getValue().getAsString();
                    final ParameterDescriptor descriptor = new ParameterDescriptor.Builder().name(parameterName).build();
                    final Parameter parameter = new Parameter(descriptor, parameterValue);
                    parameters.add(parameter);
                }
            }
        }

        final ParameterContext parameterContext = new TransactionalParameterContext(parameters);
        final ExtensionManager extensionManager = ExtensionDiscovery.discover(narWorkingDir, systemClassLoader);
        final TransactionalFlow flow = new TransactionalFlow(snapshot.getFlowContents(), extensionManager, () -> inputVariables, failurePorts,
                sslContext, parameterContext);
        flow.enqueueFromJSON(args);
        return flow;
    }

    private static Set<URL> getAdditionalClasspathResources(final List<PropertyDescriptor> propertyDescriptors, final String componentId,
            final Map<String, String> properties, final ParameterLookup parameterLookup, final VariableRegistry variableRegistry,
            final ComponentLog logger) {
        final Set<String> modulePaths = new LinkedHashSet<>();
        for (final PropertyDescriptor descriptor : propertyDescriptors) {
            if (descriptor.isDynamicClasspathModifier()) {
                final String value = properties.get(descriptor.getName());
                if (!StringUtils.isEmpty(value)) {
                    final StandardPropertyValue propertyValue = new StandardPropertyValue(value, null, parameterLookup, variableRegistry);
                    modulePaths.add(propertyValue.evaluateAttributeExpressions().getValue());
                }
            }
        }

        final Set<URL> additionalUrls = new LinkedHashSet<>();
        try {
            final URL[] urls = ClassLoaderUtils.getURLsForClasspath(modulePaths, null, true);
            if (urls != null) {
                additionalUrls.addAll(Arrays.asList(urls));
            }
        } catch (MalformedURLException mfe) {
            logger.error("Error processing classpath resources for " + componentId + ": " + mfe.getMessage(), mfe);
        }

        return additionalUrls;
    }

    private static void checkBundleCompatibility(final Document configuration, ExtensionManager extensionManager) {
        final NodeList bundleNodes = configuration.getElementsByTagName("bundle");
        for (int i = 0; i < bundleNodes.getLength(); i++) {
            final Node bundleNode = bundleNodes.item(i);
            if (bundleNode instanceof Element) {
                final Element bundleElement = (Element) bundleNode;

                final Node componentNode = bundleElement.getParentNode();
                if (componentNode instanceof Element) {
                    final Element componentElement = (Element) componentNode;
                    if (!withinTemplate(componentElement)) {
                        final String componentType = DomUtils.getChildText(componentElement, "class");
                        try {
                            BundleUtils.getBundle(extensionManager, componentType, FlowFromDOMFactory.getBundle(bundleElement));
                        } catch (IllegalStateException e) {
                            throw new MissingBundleException(e.getMessage(), e);
                        }
                    }
                }
            }
        }
    }

    private static boolean withinTemplate(final Element element) {
        if ("template".equals(element.getTagName())) {
            return true;
        } else {
            final Node parentNode = element.getParentNode();
            if (parentNode instanceof Element) {
                return withinTemplate((Element) parentNode);
            } else {
                return false;
            }
        }
    }

    private static TransactionalProcessGroup addProcessGroup(final TransactionalFlowManager flowManager, final ProcessGroup parentGroup,
            final Element processGroupElement, final StringEncryptor encryptor, final FlowEncodingVersion encodingVersion,
            final ControllerServiceProvider serviceProvider, final ExtensionManager extensionManager) {

        // get the parent group ID
        final String parentId = (parentGroup == null) ? null : parentGroup.getIdentifier();

        // add the process group
        final ProcessGroupDTO processGroupDTO = FlowFromDOMFactory.getProcessGroup(parentId, processGroupElement, encryptor, encodingVersion);
        final TransactionalProcessGroup processGroup = flowManager.createProcessGroup(processGroupDTO.getId());
        processGroup.setComments(processGroupDTO.getComments());
        processGroup.setVersionedComponentId(processGroupDTO.getVersionedComponentId());
        processGroup.setName(processGroupDTO.getName());
        processGroup.setParent(parentGroup);
        if (parentGroup == null) {
            flowManager.setRootGroup(processGroup);
        } else {
            parentGroup.addProcessGroup(processGroup);
        }

        final String parameterContextId = getString(processGroupElement, "parameterContextId");
        if (parameterContextId != null) {
            final ParameterContext parameterContext = flowManager.getParameterContextManager().getParameterContext(parameterContextId);
            processGroup.setParameterContext(parameterContext);
        }

        // Set the variables for the variable registry
        final Map<String, String> variables = new HashMap<>();
        final List<Element> variableElements = getChildrenByTagName(processGroupElement, "variable");
        for (final Element variableElement : variableElements) {
            final String variableName = variableElement.getAttribute("name");
            final String variableValue = variableElement.getAttribute("value");
            if (variableName == null || variableValue == null) {
                continue;
            }

            variables.put(variableName, variableValue);
        }

        processGroup.setVariables(variables);

        final VersionControlInformationDTO versionControlInfoDto = processGroupDTO.getVersionControlInformation();
        if (versionControlInfoDto != null) {
            final String registryName = versionControlInfoDto.getRegistryId();
            versionControlInfoDto.setState(VersionedFlowState.UP_TO_DATE.name());
            versionControlInfoDto.setStateExplanation("Process Group is loaded from latest flow.xml");
            final StandardVersionControlInformation versionControlInformation = StandardVersionControlInformation.Builder
                    .fromDto(versionControlInfoDto).registryName(registryName).build();

            // pass empty map for the version control mapping because the VersionedComponentId has already been set on the components
            processGroup.setVersionControlInformation(versionControlInformation, Collections.emptyMap());
        }

        // Add Controller Services
        final List<Element> serviceNodeList = getChildrenByTagName(processGroupElement, "controllerService");
        if (!serviceNodeList.isEmpty()) {
            List<ControllerServiceDTO> dtos = new ArrayList<>();
            List<ControllerServiceNode> services = new ArrayList<>();
            try {
                for (Element serviceNode : serviceNodeList) {
                    createControllerService(serviceNode, encryptor, encodingVersion, processGroup, serviceProvider, dtos, flowManager);
                }

                // configure controller services. We do this after creating all of them in case 1 service
                // references another service.
                dtos.forEach(dto -> {
                    final String serviceId = dto.getId();
                    ControllerServiceNode csn = serviceProvider.getControllerServiceNode(serviceId);
                    services.add(csn);
                    csn.setAnnotationData(dto.getAnnotationData());
                    csn.setProperties(dto.getProperties()); // this will cause a reload and expanding of classLoader if need be
                });
            } finally {
                services.forEach(ControllerServiceNode::resumeValidationTrigger);
            }
            services.forEach(ControllerServiceNode::performValidation); // validate services before attempting to enable them
            serviceProvider.enableControllerServices(services);
        }

        // add processors
        final List<Element> processorNodeList = getChildrenByTagName(processGroupElement, "processor");
        for (final Element processorElement : processorNodeList) {
            final ProcessorDTO processorDTO = FlowFromDOMFactory.getProcessor(processorElement, encryptor, encodingVersion);

            BundleCoordinate coordinate;
            try {
                coordinate = BundleUtils.getCompatibleBundle(extensionManager, processorDTO.getType(), processorDTO.getBundle());
            } catch (final IllegalStateException e) {
                final BundleDTO bundleDTO = processorDTO.getBundle();
                if (bundleDTO == null) {
                    coordinate = BundleCoordinate.UNKNOWN_COORDINATE;
                } else {
                    coordinate = new BundleCoordinate(bundleDTO.getGroup(), bundleDTO.getArtifact(), bundleDTO.getVersion());
                }
            }

            final ProcessorNode procNode = flowManager.createProcessor(processorDTO.getType(), processorDTO.getId(), coordinate, false);
            procNode.setVersionedComponentId(processorDTO.getVersionedComponentId());
            processGroup.addProcessor(procNode);
            updateProcessor(procNode, processorDTO, processGroup, flowManager);
        }

        // add input ports
        final List<Element> inputPortNodeList = getChildrenByTagName(processGroupElement, "inputPort");
        for (final Element inputPortElement : inputPortNodeList) {
            final PortDTO portDTO = FlowFromDOMFactory.getPort(inputPortElement);

            final Port port;
            if (processGroup.isRootGroup() || Boolean.TRUE.equals(portDTO.getAllowRemoteAccess())) {
                port = flowManager.createPublicInputPort(portDTO.getId(), portDTO.getName());
            } else {
                port = flowManager.createLocalInputPort(portDTO.getId(), portDTO.getName());
            }

            port.setVersionedComponentId(portDTO.getVersionedComponentId());
            port.setComments(portDTO.getComments());
            port.setProcessGroup(processGroup);

            final Set<String> userControls = portDTO.getUserAccessControl();
            if (userControls != null && !userControls.isEmpty()) {
                if (!(port instanceof PublicPort)) {
                    throw new IllegalStateException(
                            "Attempting to add User Access Controls to " + port.getIdentifier() + ", but it is not a RootGroupPort");
                }
                ((PublicPort) port).setUserAccessControl(userControls);
            }
            final Set<String> groupControls = portDTO.getGroupAccessControl();
            if (groupControls != null && !groupControls.isEmpty()) {
                if (!(port instanceof PublicPort)) {
                    throw new IllegalStateException(
                            "Attempting to add Group Access Controls to " + port.getIdentifier() + ", but it is not a RootGroupPort");
                }
                ((PublicPort) port).setGroupAccessControl(groupControls);
            }

            processGroup.addInputPort(port);
            if (portDTO.getConcurrentlySchedulableTaskCount() != null) {
                port.setMaxConcurrentTasks(portDTO.getConcurrentlySchedulableTaskCount());
            }

            final ProcessGroup group = requireNonNull(port).getProcessGroup();
            group.startInputPort(port);
        }

        // add output ports
        final List<Element> outputPortNodeList = getChildrenByTagName(processGroupElement, "outputPort");
        for (final Element outputPortElement : outputPortNodeList) {
            final PortDTO portDTO = FlowFromDOMFactory.getPort(outputPortElement);

            final Port port;
            if (processGroup.isRootGroup() || Boolean.TRUE.equals(portDTO.getAllowRemoteAccess())) {
                port = flowManager.createPublicOutputPort(portDTO.getId(), portDTO.getName());
            } else {
                port = flowManager.createLocalOutputPort(portDTO.getId(), portDTO.getName());
            }

            port.setVersionedComponentId(portDTO.getVersionedComponentId());
            port.setComments(portDTO.getComments());
            port.setProcessGroup(processGroup);

            final Set<String> userControls = portDTO.getUserAccessControl();
            if (userControls != null && !userControls.isEmpty()) {
                if (!(port instanceof PublicPort)) {
                    throw new IllegalStateException(
                            "Attempting to add User Access Controls to " + port.getIdentifier() + ", but it is not a RootGroupPort");
                }
                ((PublicPort) port).setUserAccessControl(userControls);
            }
            final Set<String> groupControls = portDTO.getGroupAccessControl();
            if (groupControls != null && !groupControls.isEmpty()) {
                if (!(port instanceof PublicPort)) {
                    throw new IllegalStateException(
                            "Attempting to add Group Access Controls to " + port.getIdentifier() + ", but it is not a RootGroupPort");
                }
                ((PublicPort) port).setGroupAccessControl(groupControls);
            }

            processGroup.addOutputPort(port);
            if (portDTO.getConcurrentlySchedulableTaskCount() != null) {
                port.setMaxConcurrentTasks(portDTO.getConcurrentlySchedulableTaskCount());
            }

            final ProcessGroup group = requireNonNull(port).getProcessGroup();
            group.startOutputPort(port);
        }

        // add funnels
        final List<Element> funnelNodeList = getChildrenByTagName(processGroupElement, "funnel");
        for (final Element funnelElement : funnelNodeList) {
            final FunnelDTO funnelDTO = FlowFromDOMFactory.getFunnel(funnelElement);
            final Funnel funnel = flowManager.createFunnel(funnelDTO.getId());
            funnel.setVersionedComponentId(funnelDTO.getVersionedComponentId());

            // Since this is called during startup, we want to add the funnel without enabling it
            // and then tell the controller to enable it. This way, if the controller is not fully
            // initialized, the starting of the funnel is delayed until the controller is ready.
            processGroup.addFunnel(funnel, false);
            final ProcessGroup group = requireNonNull(funnel).getProcessGroup();
            group.startFunnel((Funnel) funnel);
        }

        // add nested process groups (recursively)
        final List<Element> nestedProcessGroupNodeList = getChildrenByTagName(processGroupElement, "processGroup");
        for (final Element nestedProcessGroupElement : nestedProcessGroupNodeList) {
            addProcessGroup(flowManager, processGroup, nestedProcessGroupElement, encryptor, encodingVersion, serviceProvider, extensionManager);
        }

        // add remote process group
        final List<Element> remoteProcessGroupNodeList = getChildrenByTagName(processGroupElement, "remoteProcessGroup");
        for (final Element remoteProcessGroupElement : remoteProcessGroupNodeList) {
            final RemoteProcessGroupDTO remoteGroupDto = FlowFromDOMFactory.getRemoteProcessGroup(remoteProcessGroupElement, encryptor);
            final RemoteProcessGroup remoteGroup = flowManager.createRemoteProcessGroup(remoteGroupDto.getId(), remoteGroupDto.getTargetUris());
            remoteGroup.setVersionedComponentId(remoteGroupDto.getVersionedComponentId());
            remoteGroup.setComments(remoteGroupDto.getComments());
            final String name = remoteGroupDto.getName();
            if (name != null && !name.trim().isEmpty()) {
                remoteGroup.setName(name);
            }
            remoteGroup.setProcessGroup(processGroup);
            remoteGroup.setCommunicationsTimeout(remoteGroupDto.getCommunicationsTimeout());

            if (remoteGroupDto.getYieldDuration() != null) {
                remoteGroup.setYieldDuration(remoteGroupDto.getYieldDuration());
            }

            final String transportProtocol = remoteGroupDto.getTransportProtocol();
            if (transportProtocol != null && !transportProtocol.trim().isEmpty()) {
                remoteGroup.setTransportProtocol(SiteToSiteTransportProtocol.valueOf(transportProtocol.toUpperCase()));
            }

            if (remoteGroupDto.getProxyHost() != null) {
                remoteGroup.setProxyHost(remoteGroupDto.getProxyHost());
            }

            if (remoteGroupDto.getProxyPort() != null) {
                remoteGroup.setProxyPort(remoteGroupDto.getProxyPort());
            }

            if (remoteGroupDto.getProxyUser() != null) {
                remoteGroup.setProxyUser(remoteGroupDto.getProxyUser());
            }

            if (remoteGroupDto.getProxyPassword() != null) {
                remoteGroup.setProxyPassword(remoteGroupDto.getProxyPassword());
            }

            if (StringUtils.isBlank(remoteGroupDto.getLocalNetworkInterface())) {
                remoteGroup.setNetworkInterface(null);
            } else {
                remoteGroup.setNetworkInterface(remoteGroupDto.getLocalNetworkInterface());
            }

            final Set<RemoteProcessGroupPortDescriptor> inputPorts = new HashSet<>();
            for (final Element portElement : getChildrenByTagName(remoteProcessGroupElement, "inputPort")) {
                inputPorts.add(FlowFromDOMFactory.getRemoteProcessGroupPort(portElement));
            }
            remoteGroup.setInputPorts(inputPorts, false);

            final Set<RemoteProcessGroupPortDescriptor> outputPorts = new HashSet<>();
            for (final Element portElement : getChildrenByTagName(remoteProcessGroupElement, "outputPort")) {
                outputPorts.add(FlowFromDOMFactory.getRemoteProcessGroupPort(portElement));
            }
            remoteGroup.setOutputPorts(outputPorts, false);
            processGroup.addRemoteProcessGroup(remoteGroup);

            for (final RemoteProcessGroupPortDescriptor remoteGroupPortDTO : outputPorts) {
                final RemoteGroupPort port = remoteGroup.getOutputPort(remoteGroupPortDTO.getId());
                port.getRemoteProcessGroup().startTransmitting(port);
            }
            for (final RemoteProcessGroupPortDescriptor remoteGroupPortDTO : inputPorts) {
                final RemoteGroupPort port = remoteGroup.getInputPort(remoteGroupPortDTO.getId());
                port.getRemoteProcessGroup().startTransmitting(port);
            }
        }

        // add connections
        final List<Element> connectionNodeList = getChildrenByTagName(processGroupElement, "connection");
        for (final Element connectionElement : connectionNodeList) {
            final ConnectionDTO dto = FlowFromDOMFactory.getConnection(connectionElement);

            final Connectable source;
            final ConnectableDTO sourceDto = dto.getSource();
            if (ConnectableType.REMOTE_OUTPUT_PORT.name().equals(sourceDto.getType())) {
                final RemoteProcessGroup remoteGroup = processGroup.getRemoteProcessGroup(sourceDto.getGroupId());
                source = remoteGroup.getOutputPort(sourceDto.getId());
            } else {
                final ProcessGroup sourceGroup = flowManager.getGroup(sourceDto.getGroupId());
                if (sourceGroup == null) {
                    throw new RuntimeException("Found Invalid ProcessGroup ID for Source: " + dto.getSource().getGroupId());
                }

                source = sourceGroup.getConnectable(sourceDto.getId());
            }
            if (source == null) {
                throw new RuntimeException("Found Invalid Connectable ID for Source: " + dto.getSource().getId());
            }

            final Connectable destination;
            final ConnectableDTO destinationDto = dto.getDestination();
            if (ConnectableType.REMOTE_INPUT_PORT.name().equals(destinationDto.getType())) {
                final RemoteProcessGroup remoteGroup = processGroup.getRemoteProcessGroup(destinationDto.getGroupId());
                destination = remoteGroup.getInputPort(destinationDto.getId());
            } else {
                final ProcessGroup destinationGroup = flowManager.getGroup(destinationDto.getGroupId());
                if (destinationGroup == null) {
                    throw new RuntimeException("Found Invalid ProcessGroup ID for Destination: " + dto.getDestination().getGroupId());
                }

                destination = destinationGroup.getConnectable(destinationDto.getId());
            }
            if (destination == null) {
                throw new RuntimeException("Found Invalid Connectable ID for Destination: " + dto.getDestination().getId());
            }

            final Connection connection = flowManager.createConnection(dto.getId(), dto.getName(), source, destination,
                    dto.getSelectedRelationships());
            connection.setVersionedComponentId(dto.getVersionedComponentId());
            connection.setProcessGroup(processGroup);

            final List<Position> bendPoints = new ArrayList<>();
            for (final PositionDTO bend : dto.getBends()) {
                bendPoints.add(new Position(bend.getX(), bend.getY()));
            }
            connection.setBendPoints(bendPoints);

            final Long zIndex = dto.getzIndex();
            if (zIndex != null) {
                connection.setZIndex(zIndex);
            }

            if (dto.getLabelIndex() != null) {
                connection.setLabelIndex(dto.getLabelIndex());
            }

            List<FlowFilePrioritizer> newPrioritizers = null;
            final List<String> prioritizers = dto.getPrioritizers();
            if (prioritizers != null) {
                final List<String> newPrioritizersClasses = new ArrayList<>(prioritizers);
                newPrioritizers = new ArrayList<>();
                for (final String className : newPrioritizersClasses) {
                    try {
                        newPrioritizers.add(flowManager.createPrioritizer(className));
                    } catch (final ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                        throw new IllegalArgumentException("Unable to set prioritizer " + className + ": " + e);
                    }
                }
            }
            if (newPrioritizers != null) {
                connection.getFlowFileQueue().setPriorities(newPrioritizers);
            }

            if (dto.getBackPressureObjectThreshold() != null) {
                connection.getFlowFileQueue().setBackPressureObjectThreshold(dto.getBackPressureObjectThreshold());
            }
            if (dto.getBackPressureDataSizeThreshold() != null) {
                connection.getFlowFileQueue().setBackPressureDataSizeThreshold(dto.getBackPressureDataSizeThreshold());
            }
            if (dto.getFlowFileExpiration() != null) {
                connection.getFlowFileQueue().setFlowFileExpiration(dto.getFlowFileExpiration());
            }

            if (dto.getLoadBalanceStrategy() != null) {
                connection.getFlowFileQueue().setLoadBalanceStrategy(LoadBalanceStrategy.valueOf(dto.getLoadBalanceStrategy()),
                        dto.getLoadBalancePartitionAttribute());
            }

            if (dto.getLoadBalanceCompression() != null) {
                connection.getFlowFileQueue().setLoadBalanceCompression(LoadBalanceCompression.valueOf(dto.getLoadBalanceCompression()));
            }

            processGroup.addConnection(connection);
        }

        return processGroup;
    }

    static private byte[] readFlowFromDisk(NiFiProperties nifiProperties) throws IOException {
        final Path flowPath = nifiProperties.getFlowConfigurationFile().toPath();
        if (!Files.exists(flowPath) || Files.size(flowPath) == 0) {
            return new byte[0];
        }

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (final InputStream in = Files.newInputStream(flowPath, StandardOpenOption.READ); final InputStream gzipIn = new GZIPInputStream(in)) {
            FileUtils.copy(gzipIn, baos);
        }

        return baos.toByteArray();
    }

    private static void parseFlow(byte[] existingFlow) {
        final Document document = parseFlowBytes(existingFlow);
        final Element rootElement = document.getDocumentElement();
        final FlowEncodingVersion encodingVersion = FlowEncodingVersion.parse(rootElement);

        logger.trace("Setting controller thread counts");
        final Integer maxThreadCount = getInteger(rootElement, "maxThreadCount");
        if (maxThreadCount == null) {
            controller.setMaxTimerDrivenThreadCount(getInt(rootElement, "maxTimerDrivenThreadCount"));
            controller.setMaxEventDrivenThreadCount(getInt(rootElement, "maxEventDrivenThreadCount"));
        } else {
            controller.setMaxTimerDrivenThreadCount(maxThreadCount * 2 / 3);
            controller.setMaxEventDrivenThreadCount(maxThreadCount / 3);
        }

        final Element reportingTasksElement = DomUtils.getChild(rootElement, "reportingTasks");
        final List<Element> taskElements;
        if (reportingTasksElement == null) {
            taskElements = Collections.emptyList();
        } else {
            taskElements = DomUtils.getChildElementsByTagName(reportingTasksElement, "reportingTask");
        }

        final Element controllerServicesElement = DomUtils.getChild(rootElement, "controllerServices");
        final List<Element> unrootedControllerServiceElements;
        if (controllerServicesElement == null) {
            unrootedControllerServiceElements = Collections.emptyList();
        } else {
            unrootedControllerServiceElements = DomUtils.getChildElementsByTagName(controllerServicesElement, "controllerService");
        }

        final boolean registriesPresent;
        final Element registriesElement = DomUtils.getChild(rootElement, "registries");
        if (registriesElement == null) {
            registriesPresent = false;
        } else {
            final List<Element> flowRegistryElems = DomUtils.getChildElementsByTagName(registriesElement, "flowRegistry");
            registriesPresent = !flowRegistryElems.isEmpty();
        }

        final boolean parametersPresent;
        final Element parameterContextsElement = DomUtils.getChild(rootElement, "parameterContexts");
        if (parameterContextsElement == null) {
            parametersPresent = false;
        } else {
            final List<Element> contextList = DomUtils.getChildElementsByTagName(parameterContextsElement, "parameterContext");
            parametersPresent = !contextList.isEmpty();
        }

        logger.trace("Parsing process group from DOM");
        final Element rootGroupElement = (Element) rootElement.getElementsByTagName("rootGroup").item(0);
        final ProcessGroupDTO rootGroupDto = FlowFromDOMFactory.getProcessGroup(null, rootGroupElement, encryptor, encodingVersion);
        existingFlowEmpty = taskElements.isEmpty() && unrootedControllerServiceElements.isEmpty() && isEmpty(rootGroupDto) && !registriesPresent
                && !parametersPresent;
        logger.debug("Existing Flow Empty = {}", existingFlowEmpty);

    }

    private static Document parseFlowBytes(final byte[] flow) throws FlowSerializationException {
        // create document by parsing proposed flow bytes
        try {
            // create validating document builder
            final SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            final Schema schema = schemaFactory.newSchema(FLOW_XSD_RESOURCE);
            final DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
            docFactory.setNamespaceAware(true);
            docFactory.setSchema(schema);

            final DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
            docBuilder.setErrorHandler(new LoggingXmlParserErrorHandler("Flow Configuration", logger));

            // parse flow
            return (flow == null || flow.length == 0) ? null : docBuilder.parse(new ByteArrayInputStream(flow));
        } catch (final SAXException | ParserConfigurationException | IOException ex) {
            throw new FlowSerializationException(ex);
        }
    }

    private static int getInt(final Element element, final String childElementName) {
        return Integer.parseInt(getString(element, childElementName));
    }

    private static Integer getInteger(final Element element, final String childElementName) {
        final String value = getString(element, childElementName);
        return (value == null || value.trim().equals("") ? null : Integer.parseInt(value));
    }

    private static String getString(final Element element, final String childElementName) {
        final List<Element> nodeList = getChildrenByTagName(element, childElementName);
        if (nodeList == null || nodeList.isEmpty()) {
            return "";
        }
        final Element childElement = nodeList.get(0);
        return childElement.getTextContent();
    }

    private static List<Element> getChildrenByTagName(final Element element, final String tagName) {
        final List<Element> matches = new ArrayList<>();
        final NodeList nodeList = element.getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++) {
            final Node node = nodeList.item(i);
            if (!(node instanceof Element)) {
                continue;
            }

            final Element child = (Element) nodeList.item(i);
            if (child.getNodeName().equals(tagName)) {
                matches.add(child);
            }
        }

        return matches;
    }

}
