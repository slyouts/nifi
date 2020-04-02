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

import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.net.ssl.SSLContext;

import org.apache.nifi.annotation.lifecycle.OnAdded;
import org.apache.nifi.annotation.lifecycle.OnConfigurationRestored;
import org.apache.nifi.annotation.lifecycle.OnRemoved;
import org.apache.nifi.authorization.AuthorizationRequest;
import org.apache.nifi.authorization.AuthorizationResult;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.authorization.AuthorizerInitializationContext;
import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.authorization.exception.AuthorizerDestructionException;
import org.apache.nifi.authorization.util.IdentityMappingUtil;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.LocalPort;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.FlowSnippet;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.StandardFlowSnippet;
import org.apache.nifi.controller.StandardFunnel;
import org.apache.nifi.controller.StandardProcessorNode;
import org.apache.nifi.controller.exception.ComponentLifeCycleException;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.kerberos.KerberosConfig;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.label.StandardLabel;
import org.apache.nifi.controller.reporting.ReportingTaskInstantiationException;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.events.VolatileBulletinRepository;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.logging.ControllerServiceLogObserver;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.logging.LogRepository;
import org.apache.nifi.logging.LogRepositoryFactory;
import org.apache.nifi.logging.ProcessorLogObserver;
import org.apache.nifi.logging.ReportingTaskLogObserver;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterContextManager;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.registry.variable.MutableVariableRegistry;
import org.apache.nifi.remote.PublicPort;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.remote.StandardPublicPort;
import org.apache.nifi.remote.StandardRemoteProcessGroup;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.ReflectionUtils;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionalFlowManager implements FlowManager {
    String ROOT_GROUP_ID_ALIAS = "root";
    String DEFAULT_ROOT_GROUP_NAME = "NiFi Flow";
    private static final Logger logger = LoggerFactory.getLogger(TransactionalFlowManager.class);

    private final NiFiProperties nifiProperties;
    private final TransactionalProcessScheduler processScheduler;
    private final Authorizer authorizer;
    private final SSLContext sslContext;
    private final FlowFileEventRepository flowFileEventRepository;
    private final ParameterContextManager parameterContextManager;
    private final StateManagerProvider stateManagementProvider;
    private final ExtensionManager extensionManager;
    private final VariableRegistry variableRegistry;
    private final BulletinRepository bulletinRepository;
    private final ControllerServiceProvider serviceProvider;
    private final ValidationTrigger validationTrigger;
    private final ReloadComponent reloadComponent;
    private final KerberosConfig kerberosConfig;

    private final boolean isSiteToSiteSecure;

    private volatile TransactionalProcessGroup rootGroup;
    private final ConcurrentMap<String, ProcessGroup> allProcessGroups = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ProcessorNode> allProcessors = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ReportingTaskNode> allReportingTasks = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ControllerServiceNode> rootControllerServices = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Connection> allConnections = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Port> allInputPorts = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Port> allOutputPorts = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Funnel> allFunnels = new ConcurrentHashMap<>();
    private TransactionalEventAccess eventAccess;

    public TransactionalFlowManager(final NiFiProperties nifiProperties, final SSLContext sslContext, final ExtensionManager extensionManager,
            final TransactionalProcessScheduler processScheduler, final StateManagerProvider stateManagementProvider,
            final VariableRegistry variableRegistry, final FlowFileEventRepository flowFileEventRepository,
            final ParameterContextManager parameterContextManager, final TransactionalControllerServiceProvider serviceProvider,
            final ValidationTrigger validationTrigger, final TransactionalReloadComponent reloadComponent, KerberosConfig kerberosConfig) {
        this.nifiProperties = nifiProperties;
        this.extensionManager = extensionManager;
        this.processScheduler = processScheduler;
        this.authorizer = getDefaultAuthorizer();
        this.sslContext = sslContext;
        this.flowFileEventRepository = flowFileEventRepository;
        this.parameterContextManager = parameterContextManager;
        this.stateManagementProvider = stateManagementProvider;
        this.variableRegistry = variableRegistry;
        this.serviceProvider = serviceProvider;
        this.bulletinRepository = new VolatileBulletinRepository();
        this.validationTrigger = validationTrigger;
        this.reloadComponent = reloadComponent;
        this.isSiteToSiteSecure = Boolean.TRUE.equals(nifiProperties.isSiteToSiteSecure());
        this.kerberosConfig = kerberosConfig;
        serviceProvider.setFlowManager(this);
        reloadComponent.setFlowManager(this);
    }

    private Authorizer getDefaultAuthorizer() {
        return new Authorizer() {

            @Override
            public void preDestruction() throws AuthorizerDestructionException {

            }

            @Override
            public void onConfigured(AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {

            }

            @Override
            public void initialize(AuthorizerInitializationContext initializationContext) throws AuthorizerCreationException {

            }

            @Override
            public AuthorizationResult authorize(AuthorizationRequest request) throws AuthorizationAccessException {
                return AuthorizationResult.approved();
            }
        };
    }

    public Port createPublicInputPort(String id, String name) {
        id = requireNonNull(id).intern();
        name = requireNonNull(name).intern();
        verifyPortIdDoesNotExist(id);
        return new StandardPublicPort(id, name, TransferDirection.RECEIVE, ConnectableType.INPUT_PORT, authorizer, bulletinRepository,
                processScheduler, isSiteToSiteSecure, nifiProperties.getBoredYieldDuration(),
                IdentityMappingUtil.getIdentityMappings(nifiProperties));
    }

    public Port createPublicOutputPort(String id, String name) {
        id = requireNonNull(id).intern();
        name = requireNonNull(name).intern();
        verifyPortIdDoesNotExist(id);
        return new StandardPublicPort(id, name, TransferDirection.SEND, ConnectableType.OUTPUT_PORT, authorizer, bulletinRepository, processScheduler,
                isSiteToSiteSecure, nifiProperties.getBoredYieldDuration(), IdentityMappingUtil.getIdentityMappings(nifiProperties));
    }

    /**
     * Gets all remotely accessible ports in any process group.
     *
     * @return input ports
     */
    public Set<Port> getPublicInputPorts() {
        return getPublicPorts(ProcessGroup::getInputPorts);
    }

    /**
     * Gets all remotely accessible ports in any process group.
     *
     * @return output ports
     */
    public Set<Port> getPublicOutputPorts() {
        return getPublicPorts(ProcessGroup::getOutputPorts);
    }

    private Set<Port> getPublicPorts(final Function<ProcessGroup, Set<Port>> getPorts) {
        final Set<Port> publicPorts = new HashSet<>();
        TransactionalProcessGroup rootGroup = getRootGroup();
        getPublicPorts(publicPorts, rootGroup, getPorts);
        return publicPorts;
    }

    private void getPublicPorts(final Set<Port> publicPorts, final ProcessGroup group, final Function<ProcessGroup, Set<Port>> getPorts) {
        for (final Port port : getPorts.apply(group)) {
            if (port instanceof PublicPort) {
                publicPorts.add(port);
            }
        }
        group.getProcessGroups().forEach(childGroup -> getPublicPorts(publicPorts, childGroup, getPorts));
    }

    public Optional<Port> getPublicInputPort(String name) {
        return findPort(name, getPublicInputPorts());
    }

    public Optional<Port> getPublicOutputPort(String name) {
        return findPort(name, getPublicOutputPorts());
    }

    private Optional<Port> findPort(final String portName, final Set<Port> ports) {
        if (ports != null) {
            for (final Port port : ports) {
                if (portName.equals(port.getName())) {
                    return Optional.of(port);
                }
            }
        }
        return Optional.empty();
    }

    public RemoteProcessGroup createRemoteProcessGroup(final String id, final String uris) {
        return new StandardRemoteProcessGroup(requireNonNull(id), uris, null, processScheduler, bulletinRepository, sslContext, nifiProperties,
                stateManagementProvider.getStateManager(id));
    }

    public void setRootGroup(final TransactionalProcessGroup rootGroup) {
        this.rootGroup = rootGroup;
        allProcessGroups.put(ROOT_GROUP_ID_ALIAS, rootGroup);
        allProcessGroups.put(rootGroup.getIdentifier(), rootGroup);
    }

    public TransactionalProcessGroup getRootGroup() {
        return rootGroup;
    }

    public String getRootGroupId() {
        return rootGroup.getIdentifier();
    }

    public boolean areGroupsSame(final String id1, final String id2) {
        if (id1 == null || id2 == null) {
            return false;
        } else if (id1.equals(id2)) {
            return true;
        } else {
            final String comparable1 = id1.equals(ROOT_GROUP_ID_ALIAS) ? getRootGroupId() : id1;
            final String comparable2 = id2.equals(ROOT_GROUP_ID_ALIAS) ? getRootGroupId() : id2;
            return comparable1.equals(comparable2);
        }
    }

    private void verifyPortIdDoesNotExist(final String id) {
        final TransactionalProcessGroup rootGroup = getRootGroup();
        Port port = rootGroup.findOutputPort(id);
        if (port != null) {
            throw new IllegalStateException("An Input Port already exists with ID " + id);
        }
        port = rootGroup.findInputPort(id);
        if (port != null) {
            throw new IllegalStateException("An Input Port already exists with ID " + id);
        }
    }

    public Label createLabel(final String id, final String text) {
        return new StandardLabel(requireNonNull(id).intern(), text);
    }

    public Funnel createFunnel(final String id) {
        return new StandardFunnel(id.intern(), nifiProperties);
    }

    public Port createLocalInputPort(String id, String name) {
        id = requireNonNull(id).intern();
        name = requireNonNull(name).intern();
        verifyPortIdDoesNotExist(id);
        return new LocalPort(id, name, ConnectableType.INPUT_PORT, processScheduler, nifiProperties);
    }

    public Port createLocalOutputPort(String id, String name) {
        id = requireNonNull(id).intern();
        name = requireNonNull(name).intern();
        verifyPortIdDoesNotExist(id);
        return new LocalPort(id, name, ConnectableType.OUTPUT_PORT, processScheduler, nifiProperties);
    }

    public TransactionalProcessGroup createProcessGroup(final String id) {
        final MutableVariableRegistry mutableVariableRegistry = new MutableVariableRegistry(variableRegistry);

        final TransactionalProcessGroup group = new TransactionalProcessGroup(requireNonNull(id), mutableVariableRegistry, this, processScheduler);
        allProcessGroups.put(group.getIdentifier(), group);

        return group;
    }

    public void instantiateSnippet(final TransactionalProcessGroup group, final FlowSnippetDTO dto) throws ProcessorInstantiationException {
        requireNonNull(group);
        requireNonNull(dto);

        final FlowSnippet snippet = new StandardFlowSnippet(dto, extensionManager);
        snippet.validate(group);
        snippet.instantiate(this, group);

        group.findAllRemoteProcessGroups().forEach(RemoteProcessGroup::initialize);
    }

    public FlowFilePrioritizer createPrioritizer(final String type) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        FlowFilePrioritizer prioritizer;

        final ClassLoader ctxClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            final List<Bundle> prioritizerBundles = extensionManager.getBundles(type);
            if (prioritizerBundles.size() == 0) {
                throw new IllegalStateException(String.format("The specified class '%s' is not known to this nifi.", type));
            }
            if (prioritizerBundles.size() > 1) {
                throw new IllegalStateException(String.format("Multiple bundles found for the specified class '%s', only one is allowed.", type));
            }

            final Bundle bundle = prioritizerBundles.get(0);
            final ClassLoader detectedClassLoaderForType = bundle.getClassLoader();
            final Class<?> rawClass = Class.forName(type, true, detectedClassLoaderForType);

            Thread.currentThread().setContextClassLoader(detectedClassLoaderForType);
            final Class<? extends FlowFilePrioritizer> prioritizerClass = rawClass.asSubclass(FlowFilePrioritizer.class);
            final Object processorObj = prioritizerClass.newInstance();
            prioritizer = prioritizerClass.cast(processorObj);

            return prioritizer;
        } finally {
            if (ctxClassLoader != null) {
                Thread.currentThread().setContextClassLoader(ctxClassLoader);
            }
        }
    }

    public ProcessGroup getGroup(final String id) {
        return allProcessGroups.get(requireNonNull(id));
    }

    public void onProcessGroupAdded(final ProcessGroup group) {
        allProcessGroups.put(group.getIdentifier(), group);
    }

    public void onProcessGroupRemoved(final ProcessGroup group) {
        allProcessGroups.remove(group.getIdentifier());
    }

    public ProcessorNode createProcessor(final String type, final String id, final BundleCoordinate coordinate) {
        return createProcessor(type, id, coordinate, true);
    }

    public ProcessorNode createProcessor(final String type, String id, final BundleCoordinate coordinate, final boolean firstTimeAdded) {
        return createProcessor(type, id, coordinate, Collections.emptySet(), firstTimeAdded, true);
    }

    public ProcessorNode createProcessor(final String type, String id, final BundleCoordinate coordinate, final Set<URL> additionalUrls,
            final boolean firstTimeAdded, final boolean registerLogObserver) {

        // make sure the first reference to LogRepository happens outside of a NarCloseable so that we use the framework's ClassLoader
        final LogRepository logRepository = LogRepositoryFactory.getRepository(id);

        ProcessorNode procNode;
        try {
            procNode = new ExtensionBuilder().identifier(id).type(type).bundleCoordinate(coordinate).extensionManager(extensionManager)
                    .controllerServiceProvider(serviceProvider).processScheduler(processScheduler).validationTrigger(validationTrigger)
                    .reloadComponent(reloadComponent).variableRegistry(variableRegistry).addClasspathUrls(additionalUrls)
                    .kerberosConfig(kerberosConfig).stateManagerProvider(stateManagementProvider).buildProcessor();

            LogRepositoryFactory.getRepository(procNode.getIdentifier()).setLogger(procNode.getLogger());
            if (registerLogObserver) {
                logRepository.addObserver(StandardProcessorNode.BULLETIN_OBSERVER_ID, procNode.getBulletinLevel(),
                        new ProcessorLogObserver(bulletinRepository, procNode));
            }

            if (firstTimeAdded) {
                try (final NarCloseable x = NarCloseable.withComponentNarLoader(extensionManager, procNode.getProcessor().getClass(),
                        procNode.getProcessor().getIdentifier())) {
                    ReflectionUtils.invokeMethodsWithAnnotation(OnAdded.class, procNode.getProcessor());
                    ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnConfigurationRestored.class, procNode.getProcessor());
                } catch (final Exception e) {
                    if (registerLogObserver) {
                        logRepository.removeObserver(StandardProcessorNode.BULLETIN_OBSERVER_ID);
                    }
                    throw new ComponentLifeCycleException("Failed to invoke @OnAdded methods of " + procNode.getProcessor(), e);
                }
            }

            return procNode;
        } catch (ProcessorInstantiationException e1) {
            throw new IllegalStateException("Can not instantiate processor " + type);
        }
    }

    public void onProcessorAdded(final ProcessorNode procNode) {
        allProcessors.put(procNode.getIdentifier(), procNode);
    }

    public void onProcessorRemoved(final ProcessorNode procNode) {
        String identifier = procNode.getIdentifier();
        flowFileEventRepository.purgeTransferEvents(identifier);
        allProcessors.remove(identifier);
    }

    public Connectable findConnectable(final String id) {
        final ProcessorNode procNode = getProcessorNode(id);
        if (procNode != null) {
            return procNode;
        }

        final Port inPort = getInputPort(id);
        if (inPort != null) {
            return inPort;
        }

        final Port outPort = getOutputPort(id);
        if (outPort != null) {
            return outPort;
        }

        final Funnel funnel = getFunnel(id);
        if (funnel != null) {
            return funnel;
        }

        final RemoteGroupPort remoteGroupPort = getRootGroup().findRemoteGroupPort(id);
        if (remoteGroupPort != null) {
            return remoteGroupPort;
        }

        return null;
    }

    public ProcessorNode getProcessorNode(final String id) {
        return allProcessors.get(id);
    }

    public void onConnectionAdded(final Connection connection) {
        allConnections.put(connection.getIdentifier(), connection);

        // if (flowController.isInitialized()) {
        // connection.getFlowFileQueue().startLoadBalancing();
        // }
    }

    public void onConnectionRemoved(final Connection connection) {
        String identifier = connection.getIdentifier();
        flowFileEventRepository.purgeTransferEvents(identifier);
        allConnections.remove(identifier);
    }

    public Connection getConnection(final String id) {
        return allConnections.get(id);
    }

    public Connection createConnection(final String id, final String name, final Connectable source, final Connectable destination,
            final Collection<String> relationshipNames) {
        List<Relationship> rels = relationshipNames.stream().map((rel) -> {
            Relationship relationship = new Relationship.Builder().name(rel).build();
            return relationship;
        }).collect(Collectors.toList());
        TransactionalConnection connection = new TransactionalConnection.Builder().destination(destination).id(id).name(name).relationships(rels)
                .source(source).build();
        return connection;
    }

    public Set<Connection> findAllConnections() {
        return new HashSet<>(allConnections.values());
    }

    public void onInputPortAdded(final Port inputPort) {
        allInputPorts.put(inputPort.getIdentifier(), inputPort);
    }

    public void onInputPortRemoved(final Port inputPort) {
        String identifier = inputPort.getIdentifier();
        flowFileEventRepository.purgeTransferEvents(identifier);
        allInputPorts.remove(identifier);
    }

    public Port getInputPort(final String id) {
        return allInputPorts.get(id);
    }

    public void onOutputPortAdded(final Port outputPort) {
        allOutputPorts.put(outputPort.getIdentifier(), outputPort);
    }

    public void onOutputPortRemoved(final Port outputPort) {
        String identifier = outputPort.getIdentifier();
        flowFileEventRepository.purgeTransferEvents(identifier);
        allOutputPorts.remove(identifier);
    }

    public Port getOutputPort(final String id) {
        return allOutputPorts.get(id);
    }

    public void onFunnelAdded(final Funnel funnel) {
        allFunnels.put(funnel.getIdentifier(), funnel);
    }

    public void onFunnelRemoved(final Funnel funnel) {
        String identifier = funnel.getIdentifier();
        flowFileEventRepository.purgeTransferEvents(identifier);
        allFunnels.remove(identifier);
    }

    public Funnel getFunnel(final String id) {
        return allFunnels.get(id);
    }

    public ReportingTaskNode createReportingTask(final String type, final BundleCoordinate bundleCoordinate) {
        return createReportingTask(type, bundleCoordinate, true);
    }

    public ReportingTaskNode createReportingTask(final String type, final BundleCoordinate bundleCoordinate, final boolean firstTimeAdded) {
        return createReportingTask(type, UUID.randomUUID().toString(), bundleCoordinate, firstTimeAdded);
    }

    public ReportingTaskNode createReportingTask(final String type, final String id, final BundleCoordinate bundleCoordinate,
            final boolean firstTimeAdded) {
        return createReportingTask(type, id, bundleCoordinate, Collections.emptySet(), firstTimeAdded, true);
    }

    public ReportingTaskNode createReportingTask(final String type, final String id, final BundleCoordinate bundleCoordinate,
            final Set<URL> additionalUrls, final boolean firstTimeAdded, final boolean register) {

        if (type == null || id == null || bundleCoordinate == null) {
            throw new NullPointerException();
        }

        // make sure the first reference to LogRepository happens outside of a NarCloseable so that we use the framework's ClassLoader
        final LogRepository logRepository = LogRepositoryFactory.getRepository(id);

        ReportingTaskNode taskNode;
        try {
            taskNode = new ExtensionBuilder().identifier(id).type(type).bundleCoordinate(bundleCoordinate).extensionManager(extensionManager)
                    .controllerServiceProvider(serviceProvider).processScheduler(processScheduler).validationTrigger(validationTrigger)
                    .reloadComponent(reloadComponent).variableRegistry(variableRegistry).addClasspathUrls(additionalUrls)
                    .kerberosConfig(kerberosConfig).extensionManager(extensionManager).stateManagerProvider(stateManagementProvider)
                    .buildReportingTask(this, bulletinRepository, eventAccess);

            LogRepositoryFactory.getRepository(taskNode.getIdentifier()).setLogger(taskNode.getLogger());

            if (firstTimeAdded) {
                final Class<?> taskClass = taskNode.getReportingTask().getClass();
                final String identifier = taskNode.getReportingTask().getIdentifier();

                try (final NarCloseable x = NarCloseable.withComponentNarLoader(extensionManager, taskClass, identifier)) {
                    ReflectionUtils.invokeMethodsWithAnnotation(OnAdded.class, taskNode.getReportingTask());

                    ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnConfigurationRestored.class, taskNode.getReportingTask());
                } catch (final Exception e) {
                    throw new ComponentLifeCycleException("Failed to invoke On-Added Lifecycle methods of " + taskNode.getReportingTask(), e);
                }
            }

            if (register) {
                allReportingTasks.put(id, taskNode);

                // Register log observer to provide bulletins when reporting task logs anything at WARN level or above
                logRepository.addObserver(StandardProcessorNode.BULLETIN_OBSERVER_ID, LogLevel.WARN,
                        new ReportingTaskLogObserver(bulletinRepository, taskNode));
            }

            return taskNode;
        } catch (ReportingTaskInstantiationException e1) {
            throw new IllegalStateException("Can not instantiate reporting task " + type);
        }
    }

    public ReportingTaskNode getReportingTaskNode(final String taskId) {
        return allReportingTasks.get(taskId);
    }

    public void removeReportingTask(final ReportingTaskNode reportingTaskNode) {
        final ReportingTaskNode existing = allReportingTasks.get(reportingTaskNode.getIdentifier());
        if (existing == null || existing != reportingTaskNode) {
            throw new IllegalStateException("Reporting Task " + reportingTaskNode + " does not exist in this Flow");
        }

        reportingTaskNode.verifyCanDelete();

        final Class<?> taskClass = reportingTaskNode.getReportingTask().getClass();
        try (final NarCloseable x = NarCloseable.withComponentNarLoader(extensionManager, taskClass,
                reportingTaskNode.getReportingTask().getIdentifier())) {
            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnRemoved.class, reportingTaskNode.getReportingTask(),
                    reportingTaskNode.getConfigurationContext());
        }

        for (final Map.Entry<PropertyDescriptor, String> entry : reportingTaskNode.getEffectivePropertyValues().entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.getControllerServiceDefinition() != null) {
                final String value = entry.getValue() == null ? descriptor.getDefaultValue() : entry.getValue();
                if (value != null) {
                    final ControllerServiceNode serviceNode = serviceProvider.getControllerServiceNode(value);
                    if (serviceNode != null) {
                        serviceNode.removeReference(reportingTaskNode, descriptor);
                    }
                }
            }
        }

        allReportingTasks.remove(reportingTaskNode.getIdentifier());
        LogRepositoryFactory.removeRepository(reportingTaskNode.getIdentifier());
        processScheduler.onReportingTaskRemoved(reportingTaskNode);

        extensionManager.removeInstanceClassLoader(reportingTaskNode.getIdentifier());
    }

    public Set<ReportingTaskNode> getAllReportingTasks() {
        return new HashSet<>(allReportingTasks.values());
    }

    public Set<ControllerServiceNode> getRootControllerServices() {
        return new HashSet<>(rootControllerServices.values());
    }

    public void addRootControllerService(final ControllerServiceNode serviceNode) {
        final ControllerServiceNode existing = rootControllerServices.putIfAbsent(serviceNode.getIdentifier(), serviceNode);
        if (existing != null) {
            throw new IllegalStateException("Controller Service with ID " + serviceNode.getIdentifier() + " already exists at the Controller level");
        }
    }

    public ControllerServiceNode getRootControllerService(final String serviceIdentifier) {
        return rootControllerServices.get(serviceIdentifier);
    }

    public void removeRootControllerService(final ControllerServiceNode service) {
        throw new UnsupportedOperationException("removeRootControllerService not supported");
    }

    @Override
    public ControllerServiceNode createControllerService(final String type, final String id, final BundleCoordinate bundleCoordinate,
            final Set<URL> additionalUrls, final boolean firstTimeAdded, final boolean registerLogObserver) {

        ControllerServiceNode serviceNode;
        try {
            serviceNode = new ExtensionBuilder().identifier(id).type(type).bundleCoordinate(bundleCoordinate)
                    .controllerServiceProvider(serviceProvider).processScheduler(processScheduler).validationTrigger(validationTrigger)
                    .reloadComponent(reloadComponent).variableRegistry(variableRegistry).addClasspathUrls(additionalUrls)
                    .kerberosConfig(kerberosConfig).stateManagerProvider(stateManagementProvider).extensionManager(extensionManager)
                    .buildControllerService();

            final LogRepository logRepository = LogRepositoryFactory.getRepository(id);
            LogRepositoryFactory.getRepository(serviceNode.getIdentifier()).setLogger(serviceNode.getLogger());
            if (registerLogObserver) {
                // Register log observer to provide bulletins when reporting task logs anything at WARN level or above
                logRepository.addObserver(StandardProcessorNode.BULLETIN_OBSERVER_ID, LogLevel.WARN,
                        new ControllerServiceLogObserver(bulletinRepository, serviceNode));
            }

            if (firstTimeAdded) {
                final ControllerService service = serviceNode.getControllerServiceImplementation();

                try (final NarCloseable nc = NarCloseable.withComponentNarLoader(extensionManager, service.getClass(), service.getIdentifier())) {
                    ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnConfigurationRestored.class, service);
                }

                final ControllerService serviceImpl = serviceNode.getControllerServiceImplementation();
                try (final NarCloseable x = NarCloseable.withComponentNarLoader(extensionManager, serviceImpl.getClass(),
                        serviceImpl.getIdentifier())) {
                    ReflectionUtils.invokeMethodsWithAnnotation(OnAdded.class, serviceImpl);
                } catch (final Exception e) {
                    throw new ComponentLifeCycleException("Failed to invoke On-Added Lifecycle methods of " + serviceImpl, e);
                }
            }

            serviceProvider.onControllerServiceAdded(serviceNode);

            return serviceNode;
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException | InitializationException e1) {
            throw new IllegalStateException("Can not instantiate controller service " + type);
        }
    }

    public Set<ControllerServiceNode> getAllControllerServices() {
        final Set<ControllerServiceNode> allServiceNodes = new HashSet<>();
        allServiceNodes.addAll(serviceProvider.getNonRootControllerServices());
        allServiceNodes.addAll(rootControllerServices.values());
        return allServiceNodes;
    }

    public ControllerServiceNode getControllerServiceNode(final String id) {
        return serviceProvider.getControllerServiceNode(id);
    }

    public ParameterContextManager getParameterContextManager() {
        return parameterContextManager;
    }

    public ParameterContext createParameterContext(final String id, final String name, final Map<String, Parameter> parameters) {
        final boolean namingConflict = parameterContextManager.getParameterContexts().stream()
                .anyMatch(paramContext -> paramContext.getName().equals(name));

        if (namingConflict) {
            throw new IllegalStateException(
                    "Cannot create Parameter Context with name '" + name + "' because a Parameter Context already exists with that name");
        }

        final ParameterContext parameterContext = new TransactionalParameterContext(id, name, parameters);
        parameterContextManager.addParameterContext(parameterContext);
        return parameterContext;
    }

    @Override
    public void instantiateSnippet(ProcessGroup group, FlowSnippetDTO dto) throws ProcessorInstantiationException {

    }

    public void setEventAccess(TransactionalEventAccess eventAccess) {
        this.eventAccess = eventAccess;
    }

}
