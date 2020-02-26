package org.apache.nifi.transactional.core;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.attribute.expression.language.Query;
import org.apache.nifi.attribute.expression.language.VariableImpact;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.connectable.Positionable;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.PropertyConfiguration;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.Snippet;
import org.apache.nifi.controller.Template;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.ProcessGroupCounts;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterReference;
import org.apache.nifi.registry.ComponentVariableRegistry;
import org.apache.nifi.registry.VariableDescriptor;
import org.apache.nifi.registry.flow.FlowRegistryClient;
import org.apache.nifi.registry.flow.VersionControlInformation;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.variable.MutableVariableRegistry;
import org.apache.nifi.remote.PublicPort;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionalProcessGroup implements ProcessGroup {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionalProcessGroup.class);

    ProcessGroup parent;
    private final Map<String, ProcessGroup> processGroups = new HashMap<>();
    final String id;
    String name;
    final MutableVariableRegistry variableRegistry;
    ParameterContext parameterContext;
    private final Map<String, Port> inputPorts = new HashMap<>();
    private final Map<String, Port> outputPorts = new HashMap<>();
    private final Map<String, ControllerServiceNode> controllerServices = new HashMap<>();
    private final Map<String, Connection> connections = new HashMap<>();
    private final Map<String, Funnel> funnels = new HashMap<>();
    private final Map<String, RemoteProcessGroup> remoteGroups = new HashMap<>();

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();
    private final FlowManager flowManager;
    private final Map<String, ProcessorNode> processors = new HashMap<>();

    private final TransactionalProcessScheduler scheduler;

    private String comments;

    private String versionedComponentId;

    public TransactionalProcessGroup(TransactionalProcessGroup parent, ProcessGroupDTO element, ComponentVariableRegistry variableRegistry,
            FlowManager flowManager, TransactionalProcessScheduler scheduler) {
        this.parent = parent;
        this.id = element.getId();
        this.name = element.getName();
        this.variableRegistry = new MutableVariableRegistry(variableRegistry);
        this.flowManager = flowManager;
        this.scheduler = scheduler;
        Map<VariableDescriptor, String> variableMap = this.variableRegistry.getVariableMap();
        element.getVariables().forEach((key, value) -> {
            variableMap.put(new VariableDescriptor(key), value);
        });
        FlowSnippetDTO contents = element.getContents();
        contents.getConnections();
        contents.getControllerServices();
        contents.getFunnels();
        contents.getInputPorts();
        contents.getLabels();
        contents.getOutputPorts();
        contents.getProcessors();
        contents.getRemoteProcessGroups();
        contents.getProcessGroups();
    }

    public TransactionalProcessGroup(ProcessGroupDTO element, ComponentVariableRegistry variableRegistry, FlowManager flowManager,
            TransactionalProcessScheduler scheduler) {
        this(null, element, variableRegistry, flowManager, scheduler);
    }

    public TransactionalProcessGroup(String id, ComponentVariableRegistry variableRegistry, FlowManager flowManager,
            TransactionalProcessScheduler scheduler) {
        this.name = "";
        this.variableRegistry = new MutableVariableRegistry(variableRegistry);
        this.id = id;
        this.parent = null;
        this.flowManager = flowManager;
        this.scheduler = scheduler;
    }

    public ProcessGroup getParent() {
        return parent;
    }

    public String getName() {
        return name;
    }

    public MutableVariableRegistry getVariableRegistry() {
        return variableRegistry;
    }

    public Set<Port> getInputPorts() {
        readLock.lock();
        try {
            return new HashSet<>(inputPorts.values());
        } finally {
            readLock.unlock();
        }
    }

    public Set<Port> getOutputPorts() {
        readLock.lock();
        try {
            return new HashSet<>(outputPorts.values());
        } finally {
            readLock.unlock();
        }
    }

    public Set<ProcessGroup> getProcessGroups() {
        readLock.lock();
        try {
            return new HashSet<>(processGroups.values());
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TransactionalProcessGroup other = (TransactionalProcessGroup) obj;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        return true;
    }

    @Override
    public ProcessGroup findProcessGroup(String groupId) {
        if (requireNonNull(id).equals(getIdentifier())) {
            return this;
        }

        final ProcessGroup group = flowManager.getGroup(id);
        if (group == null) {
            return null;
        }

        // We found a Process Group in the Controller, but we only want to return it if
        // the Process Group is this or is a child of this.
        if (isOwner(group.getParent())) {
            return group;
        }

        return null;
    }

    @Override
    public String getProcessGroupIdentifier() {
        return id;
    }

    @Override
    public Authorizable getParentAuthorizable() {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");
    }

    @Override
    public Resource getResource() {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");
    }

    @Override
    public Position getPosition() {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");
    }

    @Override
    public void setPosition(Position position) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");
    }

    @Override
    public Optional<String> getVersionedComponentId() {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");
    }

    @Override
    public void setVersionedComponentId(String versionedComponentId) {
        this.versionedComponentId = versionedComponentId;
    }

    @Override
    public void setParent(ProcessGroup group) {
        this.parent = group;
    }

    @Override
    public String getIdentifier() {
        return id;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getComments() {
        return comments;
    }

    @Override
    public void setComments(String comments) {
        this.comments = comments;
    }

    @Override
    public void startProcessing() {
        readLock.lock();
        try {
            findAllProcessors().stream().filter(START_PROCESSORS_FILTER).forEach(node -> {
                try {
                    node.getProcessGroup().startProcessor(node, true);
                } catch (final Throwable t) {
                    LOG.error("Unable to start processor {} due to {}", new Object[] { node.getIdentifier(), t });
                }
            });

            findAllInputPorts().stream().filter(START_PORTS_FILTER).forEach(port -> port.getProcessGroup().startInputPort(port));

            findAllOutputPorts().stream().filter(START_PORTS_FILTER).forEach(port -> {
                port.getProcessGroup().startOutputPort(port);
            });
        } finally {
            readLock.unlock();
        }

    }

    @Override
    public void stopProcessing() {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");
    }

    @Override
    public void enableProcessor(ProcessorNode processor) {
        readLock.lock();
        try {
            if (!processors.containsKey(processor.getIdentifier())) {
                throw new IllegalStateException("No Processor with ID " + processor.getIdentifier() + " belongs to this Process Group");
            }

            final ScheduledState state = processor.getScheduledState();
            if (state == ScheduledState.STOPPED) {
                return;
            } else if (state == ScheduledState.RUNNING) {
                throw new IllegalStateException("Processor is currently running");
            }

            scheduler.enableProcessor(processor);
        } finally {
            readLock.unlock();
        }

    }

    @Override
    public void enableInputPort(Port port) {
        readLock.lock();
        try {
            if (!inputPorts.containsKey(port.getIdentifier())) {
                throw new IllegalStateException("No Input Port with ID " + port.getIdentifier() + " belongs to this Process Group");
            }

            final ScheduledState state = port.getScheduledState();
            if (state == ScheduledState.STOPPED) {
                return;
            } else if (state == ScheduledState.RUNNING) {
                throw new IllegalStateException("InputPort is currently running");
            }

            scheduler.enablePort(port);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void enableOutputPort(Port port) {
        readLock.lock();
        try {
            if (!outputPorts.containsKey(port.getIdentifier())) {
                throw new IllegalStateException("No Output Port with ID " + port.getIdentifier() + " belongs to this Process Group");
            }

            final ScheduledState state = port.getScheduledState();
            if (state == ScheduledState.STOPPED) {
                return;
            } else if (state == ScheduledState.RUNNING) {
                throw new IllegalStateException("OutputPort is currently running");
            }

            scheduler.enablePort(port);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public CompletableFuture<Void> startProcessor(ProcessorNode processor, boolean failIfStopping) {
        readLock.lock();
        try {
            if (getProcessor(processor.getIdentifier()) == null) {
                throw new IllegalStateException("Processor is not a member of this Process Group");
            }

            final ScheduledState state = processor.getScheduledState();
            if (state == ScheduledState.DISABLED) {
                throw new IllegalStateException("Processor is disabled");
            } else if (state == ScheduledState.RUNNING) {
                return CompletableFuture.completedFuture(null);
            }
            processor.reloadAdditionalResourcesIfNecessary();

            return scheduler.startProcessor(processor, failIfStopping);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void startInputPort(Port port) {
        readLock.lock();
        try {
            if (getInputPort(port.getIdentifier()) == null) {
                throw new IllegalStateException("Port " + port.getIdentifier() + " is not a member of this Process Group");
            }

            final ScheduledState state = port.getScheduledState();
            if (state == ScheduledState.DISABLED) {
                throw new IllegalStateException("InputPort " + port.getIdentifier() + " is disabled");
            } else if (state == ScheduledState.RUNNING) {
                return;
            }

            scheduler.startPort(port);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void startOutputPort(Port port) {
        readLock.lock();
        try {
            if (getOutputPort(port.getIdentifier()) == null) {
                throw new IllegalStateException("Port is not a member of this Process Group");
            }

            final ScheduledState state = port.getScheduledState();
            if (state == ScheduledState.DISABLED) {
                throw new IllegalStateException("OutputPort is disabled");
            } else if (state == ScheduledState.RUNNING) {
                return;
            }

            scheduler.startPort(port);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void startFunnel(Funnel funnel) {
        readLock.lock();
        try {
            if (getFunnel(funnel.getIdentifier()) == null) {
                throw new IllegalStateException("Funnel is not a member of this Process Group");
            }

            final ScheduledState state = funnel.getScheduledState();
            if (state == ScheduledState.RUNNING) {
                return;
            }
            scheduler.startFunnel(funnel);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public CompletableFuture<Void> stopProcessor(ProcessorNode processor) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public void terminateProcessor(ProcessorNode processor) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public void stopInputPort(Port port) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public void stopOutputPort(Port port) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public void disableProcessor(ProcessorNode processor) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public void disableInputPort(Port port) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public void disableOutputPort(Port port) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public void shutdown() {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public boolean isRootGroup() {
        return parent == null;
    }

    @Override
    public void addInputPort(Port port) {
        if (isRootGroup()) {
            if (!(port instanceof PublicPort)) {
                throw new IllegalArgumentException("Cannot add Input Port of type " + port.getClass().getName() + " to the Root Group");
            }
        }

        writeLock.lock();
        try {
            // Unique port check within the same group.
            verifyPortUniqueness(port, inputPorts, name -> getInputPortByName(name));

            port.setProcessGroup(this);
            inputPorts.put(requireNonNull(port).getIdentifier(), port);
            flowManager.onInputPortAdded(port);

            LOG.info("Input Port {} added to {}", port, this);
        } finally {
            writeLock.unlock();
        }

    }

    @Override
    public void removeInputPort(Port port) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public Port getInputPort(String id) {
        readLock.lock();
        try {
            return inputPorts.get(Objects.requireNonNull(id));
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void addOutputPort(Port port) {
        if (isRootGroup()) {
            if (!(port instanceof PublicPort)) {
                throw new IllegalArgumentException("Cannot add Output Port " + port.getClass().getName() + " to the Root Group");
            }
        }

        writeLock.lock();
        try {
            // Unique port check within the same group.
            verifyPortUniqueness(port, outputPorts, this::getOutputPortByName);

            port.setProcessGroup(this);
            outputPorts.put(port.getIdentifier(), port);
            flowManager.onOutputPortAdded(port);
            onComponentModified();

            LOG.info("Output Port {} added to {}", port, this);
        } finally {
            writeLock.unlock();
        }

    }

    @Override
    public void removeOutputPort(Port port) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public Port getOutputPort(String id) {
        readLock.lock();
        try {
            return outputPorts.get(Objects.requireNonNull(id));
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void addProcessGroup(ProcessGroup group) {
        if (StringUtils.isEmpty(group.getName())) {
            throw new IllegalArgumentException("Process Group's name must be specified");
        }

        writeLock.lock();
        try {
            group.setParent(this);
            group.getVariableRegistry().setParent(getVariableRegistry());

            processGroups.put(Objects.requireNonNull(group).getIdentifier(), group);
            flowManager.onProcessGroupAdded(group);

            group.findAllControllerServices().forEach(this::updateControllerServiceReferences);
            group.findAllProcessors().forEach(this::updateControllerServiceReferences);

            onComponentModified();

            LOG.info("{} added to {}", group, this);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public ProcessGroup getProcessGroup(String id) {
        readLock.lock();
        try {
            return processGroups.get(id);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void removeProcessGroup(ProcessGroup group) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public void addProcessor(ProcessorNode processor) {
        writeLock.lock();
        try {
            final String processorId = requireNonNull(processor).getIdentifier();
            final ProcessorNode existingProcessor = processors.get(processorId);
            if (existingProcessor != null) {
                throw new IllegalStateException("A processor is already registered to this ProcessGroup with ID " + processorId);
            }

            processor.setProcessGroup(this);
            processor.getVariableRegistry().setParent(getVariableRegistry());
            processors.put(processorId, processor);
            flowManager.onProcessorAdded(processor);
            updateControllerServiceReferences(processor);
            onComponentModified();

            LOG.info("{} added to {}", processor, this);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void removeProcessor(ProcessorNode processor) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public Collection<ProcessorNode> getProcessors() {
        readLock.lock();
        try {
            return new ArrayList<>(processors.values());
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public ProcessorNode getProcessor(String id) {
        readLock.lock();
        try {
            return processors.get(Objects.requireNonNull(id));
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Connectable getConnectable(String id) {
        readLock.lock();
        try {
            final ProcessorNode node = processors.get(id);
            if (node != null) {
                return node;
            }

            final Port inputPort = inputPorts.get(id);
            if (inputPort != null) {
                return inputPort;
            }

            final Port outputPort = outputPorts.get(id);
            if (outputPort != null) {
                return outputPort;
            }

            final Funnel funnel = funnels.get(id);
            if (funnel != null) {
                return funnel;
            }

            return null;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void addConnection(Connection connection) {
        writeLock.lock();
        try {
            final String id = requireNonNull(connection).getIdentifier();
            final Connection existingConnection = connections.get(id);
            if (existingConnection != null) {
                throw new IllegalStateException("Connection already exists with ID " + id);
            }

            final Connectable source = connection.getSource();
            final Connectable destination = connection.getDestination();
            final ProcessGroup sourceGroup = source.getProcessGroup();
            final ProcessGroup destinationGroup = destination.getProcessGroup();

            // validate the connection is validate wrt to the source & destination groups
            if (isInputPort(source)) { // if source is an input port, its destination must be in the same group unless it's an input port
                if (isInputPort(destination)) { // if destination is input port, it must be in a child group.
                    if (!processGroups.containsKey(destinationGroup.getIdentifier())) {
                        throw new IllegalStateException(
                                "Cannot add Connection to Process Group because destination is an Input Port that does not belong to a child Process Group");
                    }
                } else if (sourceGroup != this || destinationGroup != this) {
                    throw new IllegalStateException(
                            "Cannot add Connection to Process Group because source and destination are not both in this Process Group");
                }
            } else if (isOutputPort(source)) {
                // if source is an output port, its group must be a child of this group, and its destination must be in this
                // group (processor/output port) or a child group (input port)
                if (!processGroups.containsKey(sourceGroup.getIdentifier())) {
                    throw new IllegalStateException(
                            "Cannot add Connection to Process Group because source is an Output Port that does not belong to a child Process Group");
                }

                if (isInputPort(destination)) {
                    if (!processGroups.containsKey(destinationGroup.getIdentifier())) {
                        throw new IllegalStateException(
                                "Cannot add Connection to Process Group because its destination is an Input Port that does not belong to a child Process Group");
                    }
                } else if (destinationGroup != this) {
                    throw new IllegalStateException(
                            "Cannot add Connection to Process Group because its destination does not belong to this Process Group");
                }
            } else { // source is not a port
                if (sourceGroup != this) {
                    throw new IllegalStateException(
                            "Cannot add Connection to Process Group because the source does not belong to this Process Group");
                }

                if (isOutputPort(destination)) {
                    if (destinationGroup != this) {
                        throw new IllegalStateException(
                                "Cannot add Connection to Process Group because its destination is an Output Port but does not belong to this Process Group");
                    }
                } else if (isInputPort(destination)) {
                    if (!processGroups.containsKey(destinationGroup.getIdentifier())) {
                        throw new IllegalStateException("Cannot add Connection to Process Group because its destination is an Input "
                                + "Port but the Input Port does not belong to a child Process Group");
                    }
                } else if (destinationGroup != this) {
                    throw new IllegalStateException("Cannot add Connection between " + source.getIdentifier() + " and " + destination.getIdentifier()
                            + " because they are in different Process Groups and neither is an Input Port or Output Port");
                }
            }

            connection.setProcessGroup(this);
            source.addConnection(connection);
            if (source != destination) { // don't call addConnection twice if it's a self-looping connection.
                destination.addConnection(connection);
            }
            connections.put(connection.getIdentifier(), connection);
            flowManager.onConnectionAdded(connection);
            onComponentModified();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void removeConnection(Connection connection) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public void inheritConnection(Connection connection) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public Connection getConnection(final String id) {
        readLock.lock();
        try {
            return connections.get(Objects.requireNonNull(id));
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Set<Connection> getConnections() {
        readLock.lock();
        try {
            return new HashSet<>(connections.values());
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Connection findConnection(final String id) {
        final Connection connection = flowManager.getConnection(id);
        if (connection == null) {
            return null;
        }

        // We found a Connection in the Controller, but we only want to return it if
        // the Process Group is this or is a child of this.
        if (isOwner(connection.getProcessGroup())) {
            return connection;
        }

        return null;
    }

    @Override
    public List<Connection> findAllConnections() {
        return findAllConnections(this);
    }

    private List<Connection> findAllConnections(final ProcessGroup group) {
        final List<Connection> connections = new ArrayList<>(group.getConnections());
        for (final ProcessGroup childGroup : group.getProcessGroups()) {
            connections.addAll(findAllConnections(childGroup));
        }
        return connections;
    }

    @Override
    public Funnel findFunnel(String id) {
        final Funnel funnel = flowManager.getFunnel(id);
        if (funnel == null) {
            return funnel;
        }

        if (isOwner(funnel.getProcessGroup())) {
            return funnel;
        }

        return null;
    }

    @Override
    public ControllerServiceNode findControllerService(String id, boolean includeDescendants, boolean includeAncestors) {
        ControllerServiceNode serviceNode;
        if (includeDescendants) {
            serviceNode = findDescendantControllerService(id, this);
        } else {
            serviceNode = getControllerService(id);
        }

        if (serviceNode == null && includeAncestors) {
            serviceNode = findAncestorControllerService(id, getParent());
        }

        return serviceNode;
    }

    private ControllerServiceNode findAncestorControllerService(final String id, final ProcessGroup start) {
        if (start == null) {
            return null;
        }

        final ControllerServiceNode serviceNode = start.getControllerService(id);
        if (serviceNode != null) {
            return serviceNode;
        }

        final ProcessGroup parent = start.getParent();
        return findAncestorControllerService(id, parent);
    }

    private ControllerServiceNode findDescendantControllerService(final String id, final ProcessGroup start) {
        ControllerServiceNode service = start.getControllerService(id);
        if (service != null) {
            return service;
        }

        for (final ProcessGroup group : start.getProcessGroups()) {
            service = findDescendantControllerService(id, group);
            if (service != null) {
                return service;
            }
        }

        return null;
    }

    @Override
    public Set<ControllerServiceNode> findAllControllerServices() {
        return findAllControllerServices(this);
    }

    public Set<ControllerServiceNode> findAllControllerServices(ProcessGroup start) {
        final Set<ControllerServiceNode> services = start.getControllerServices(false);
        for (final ProcessGroup group : start.getProcessGroups()) {
            services.addAll(findAllControllerServices(group));
        }

        return services;
    }

    @Override
    public void addRemoteProcessGroup(RemoteProcessGroup remoteGroup) {
        writeLock.lock();
        try {
            if (remoteGroups.containsKey(requireNonNull(remoteGroup).getIdentifier())) {
                throw new IllegalStateException("RemoteProcessGroup already exists with ID " + remoteGroup.getIdentifier());
            }

            remoteGroup.setProcessGroup(this);
            remoteGroups.put(Objects.requireNonNull(remoteGroup).getIdentifier(), remoteGroup);
            onComponentModified();

            LOG.info("{} added to {}", remoteGroup, this);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void removeRemoteProcessGroup(RemoteProcessGroup remoteGroup) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public RemoteProcessGroup getRemoteProcessGroup(String id) {
        readLock.lock();
        try {
            return remoteGroups.get(Objects.requireNonNull(id));
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Set<RemoteProcessGroup> getRemoteProcessGroups() {
        readLock.lock();
        try {
            return new HashSet<>(remoteGroups.values());
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void addLabel(Label label) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");
    }

    @Override
    public void removeLabel(Label label) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public Set<Label> getLabels() {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public Label getLabel(String id) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public List<ProcessGroup> findAllProcessGroups() {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public List<ProcessGroup> findAllProcessGroups(Predicate<ProcessGroup> filter) {
        final List<ProcessGroup> matching = new ArrayList<>();
        if (filter.test(this)) {
            matching.add(this);
        }

        for (final ProcessGroup group : getProcessGroups()) {
            matching.addAll(group.findAllProcessGroups(filter));
        }

        return matching;
    }

    @Override
    public RemoteProcessGroup findRemoteProcessGroup(String id) {
        return findRemoteProcessGroup(requireNonNull(id), this);
    }

    private RemoteProcessGroup findRemoteProcessGroup(final String id, final ProcessGroup start) {
        RemoteProcessGroup remoteGroup = start.getRemoteProcessGroup(id);
        if (remoteGroup != null) {
            return remoteGroup;
        }

        for (final ProcessGroup group : start.getProcessGroups()) {
            remoteGroup = findRemoteProcessGroup(id, group);
            if (remoteGroup != null) {
                return remoteGroup;
            }
        }

        return null;
    }

    @Override
    public List<RemoteProcessGroup> findAllRemoteProcessGroups() {
        return findAllRemoteProcessGroups(this);
    }

    private List<RemoteProcessGroup> findAllRemoteProcessGroups(final ProcessGroup start) {
        final List<RemoteProcessGroup> remoteGroups = new ArrayList<>(start.getRemoteProcessGroups());
        for (final ProcessGroup childGroup : start.getProcessGroups()) {
            remoteGroups.addAll(findAllRemoteProcessGroups(childGroup));
        }
        return remoteGroups;
    }

    @Override
    public ProcessorNode findProcessor(String id) {
        final ProcessorNode node = flowManager.getProcessorNode(id);
        if (node == null) {
            return null;
        }

        // We found a Processor in the Controller, but we only want to return it if
        // the Process Group is this or is a child of this.
        if (isOwner(node.getProcessGroup())) {
            return node;
        }

        return null;
    }

    @Override
    public List<ProcessorNode> findAllProcessors() {
        return findAllProcessors(this);
    }

    private List<ProcessorNode> findAllProcessors(final ProcessGroup start) {
        final List<ProcessorNode> allNodes = new ArrayList<>(start.getProcessors());
        for (final ProcessGroup group : start.getProcessGroups()) {
            allNodes.addAll(findAllProcessors(group));
        }
        return allNodes;
    }

    @Override
    public Label findLabel(String id) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public List<Label> findAllLabels() {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public Port findInputPort(final String id) {
        final Port port = flowManager.getInputPort(id);
        if (port == null) {
            return null;
        }

        if (isOwner(port.getProcessGroup())) {
            return port;
        }

        return null;
    }

    @Override
    public List<Port> findAllInputPorts() {
        return findAllInputPorts(this);
    }

    private List<Port> findAllInputPorts(final ProcessGroup start) {
        final List<Port> allOutputPorts = new ArrayList<>(start.getInputPorts());
        for (final ProcessGroup group : start.getProcessGroups()) {
            allOutputPorts.addAll(findAllInputPorts(group));
        }
        return allOutputPorts;
    }

    @Override
    public Port getInputPortByName(String name) {
        return getPortByName(name, this, new InputPortRetriever());
    }

    @Override
    public Port findOutputPort(final String id) {
        final Port port = flowManager.getOutputPort(id);
        if (port == null) {
            return null;
        }

        if (isOwner(port.getProcessGroup())) {
            return port;
        }

        return null;
    }

    @Override
    public List<Port> findAllOutputPorts() {
        return findAllOutputPorts(this);
    }

    private List<Port> findAllOutputPorts(final ProcessGroup start) {
        final List<Port> allOutputPorts = new ArrayList<>(start.getOutputPorts());
        for (final ProcessGroup group : start.getProcessGroups()) {
            allOutputPorts.addAll(findAllOutputPorts(group));
        }
        return allOutputPorts;
    }

    @Override
    public Port getOutputPortByName(String name) {
        return getPortByName(name, this, new OutputPortRetriever());
    }

    @Override
    public void addFunnel(final Funnel funnel) {
        addFunnel(funnel, true);
    }

    @Override
    public void addFunnel(final Funnel funnel, final boolean autoStart) {
        writeLock.lock();
        try {
            final Funnel existing = funnels.get(requireNonNull(funnel).getIdentifier());
            if (existing != null) {
                throw new IllegalStateException("A funnel already exists in this ProcessGroup with ID " + funnel.getIdentifier());
            }

            funnel.setProcessGroup(this);
            funnels.put(funnel.getIdentifier(), funnel);
            flowManager.onFunnelAdded(funnel);

            if (autoStart) {
                startFunnel(funnel);
            }

            onComponentModified();
            LOG.info("{} added to {}", funnel, this);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Set<Funnel> getFunnels() {
        readLock.lock();
        try {
            return new HashSet<>(funnels.values());
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Funnel getFunnel(final String id) {
        readLock.lock();
        try {
            return funnels.get(id);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void removeFunnel(Funnel funnel) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public List<Funnel> findAllFunnels() {
        return findAllFunnels(this);
    }

    private List<Funnel> findAllFunnels(final ProcessGroup start) {
        final List<Funnel> allFunnels = new ArrayList<>(start.getFunnels());
        for (final ProcessGroup group : start.getProcessGroups()) {
            allFunnels.addAll(findAllFunnels(group));
        }
        return allFunnels;
    }

    @Override
    public void addControllerService(ControllerServiceNode service) {
        writeLock.lock();
        try {
            final String id = requireNonNull(service).getIdentifier();
            final ControllerServiceNode existingService = controllerServices.get(id);
            if (existingService != null) {
                throw new IllegalStateException("A Controller Service is already registered to this ProcessGroup with ID " + id);
            }

            service.setProcessGroup(this);
            service.getVariableRegistry().setParent(getVariableRegistry());
            this.controllerServices.put(service.getIdentifier(), service);
            LOG.info("{} added to {}", service, this);
            updateControllerServiceReferences(service);
            onComponentModified();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public ControllerServiceNode getControllerService(String id) {
        readLock.lock();
        try {
            return controllerServices.get(requireNonNull(id));
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Set<ControllerServiceNode> getControllerServices(boolean recursive) {
        final Set<ControllerServiceNode> services = new HashSet<>();

        readLock.lock();
        try {
            services.addAll(controllerServices.values());
        } finally {
            readLock.unlock();
        }

        if (recursive) {
            final ProcessGroup parentGroup = parent;
            if (parentGroup != null) {
                services.addAll(parentGroup.getControllerServices(true));
            }
        }

        return services;
    }

    @Override
    public void removeControllerService(ControllerServiceNode service) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public boolean isEmpty() {
        readLock.lock();
        try {
            return inputPorts.isEmpty() && outputPorts.isEmpty() && connections.isEmpty() && processGroups.isEmpty() && processors.isEmpty()
                    && remoteGroups.isEmpty();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void remove(Snippet snippet) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public RemoteGroupPort findRemoteGroupPort(String identifier) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public Set<Positionable> findAllPositionables() {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");
    }

    @Override
    public void move(Snippet snippet, ProcessGroup destination) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public void updateFlow(VersionedFlowSnapshot proposedSnapshot, String componentIdSeed, boolean verifyNotDirty, boolean updateSettings,
            boolean updateDescendantVersionedFlows) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public void verifyCanAddTemplate(String name) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public void verifyCanDelete() {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public void verifyCanDelete(boolean ignorePortConnections) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public void verifyCanStart(Connectable connectable) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public void verifyCanStart() {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public void verifyCanStop(Connectable connectable) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public void verifyCanStop() {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public void verifyCanDelete(Snippet snippet) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public void verifyCanMove(Snippet snippet, ProcessGroup newProcessGroup) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public void verifyCanUpdateVariables(Map<String, String> updatedVariables) {
        if (updatedVariables == null || updatedVariables.isEmpty()) {
            return;
        }

        readLock.lock();
        try {
            final Set<String> updatedVariableNames = getUpdatedVariables(updatedVariables);
            if (updatedVariableNames.isEmpty()) {
                return;
            }

            // Determine any Processors that references the variable
            for (final ProcessorNode processor : getProcessors()) {
                if (!processor.isRunning()) {
                    continue;
                }

                for (final VariableImpact impact : getVariableImpact(processor)) {
                    for (final String variableName : updatedVariableNames) {
                        if (impact.isImpacted(variableName)) {
                            throw new IllegalStateException("Cannot update variable '" + variableName + "' because it is referenced by " + processor
                                    + ", which is currently running");
                        }
                    }
                }
            }

            // Determine any Controller Service that references the variable.
            for (final ControllerServiceNode service : getControllerServices(false)) {
                if (!service.isActive()) {
                    continue;
                }

                for (final VariableImpact impact : getVariableImpact(service)) {
                    for (final String variableName : updatedVariableNames) {
                        if (impact.isImpacted(variableName)) {
                            throw new IllegalStateException("Cannot update variable '" + variableName + "' because it is referenced by " + service
                                    + ", which is currently running");
                        }
                    }
                }
            }

            // For any child Process Group that does not override the variable, also include its references.
            // If a child group has a value for the same variable, though, then that means that the child group
            // is overriding the variable and its components are actually referencing a different variable.
            for (final ProcessGroup childGroup : getProcessGroups()) {
                for (final String variableName : updatedVariableNames) {
                    final ComponentVariableRegistry childRegistry = childGroup.getVariableRegistry();
                    final VariableDescriptor descriptor = childRegistry.getVariableKey(variableName);
                    final boolean overridden = childRegistry.getVariableMap().containsKey(descriptor);
                    if (!overridden) {
                        final Set<ComponentNode> affectedComponents = childGroup.getComponentsAffectedByVariable(variableName);

                        for (final ComponentNode affectedComponent : affectedComponents) {
                            if (affectedComponent instanceof ProcessorNode) {
                                final ProcessorNode affectedProcessor = (ProcessorNode) affectedComponent;
                                if (affectedProcessor.isRunning()) {
                                    throw new IllegalStateException("Cannot update variable '" + variableName + "' because it is referenced by "
                                            + affectedComponent + ", which is currently running.");
                                }
                            } else if (affectedComponent instanceof ControllerServiceNode) {
                                final ControllerServiceNode affectedService = (ControllerServiceNode) affectedComponent;
                                if (affectedService.isActive()) {
                                    throw new IllegalStateException("Cannot update variable '" + variableName + "' because it is referenced by "
                                            + affectedComponent + ", which is currently active.");
                                }
                            } else if (affectedComponent instanceof ReportingTaskNode) {
                                final ReportingTaskNode affectedReportingTask = (ReportingTaskNode) affectedComponent;
                                if (affectedReportingTask.isRunning()) {
                                    throw new IllegalStateException("Cannot update variable '" + variableName + "' because it is referenced by "
                                            + affectedComponent + ", which is currently running.");
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void verifyCanUpdate(VersionedFlowSnapshot updatedFlow, boolean verifyConnectionRemoval, boolean verifyNotDirty) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public void verifyCanRevertLocalModifications() {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public void verifyCanShowLocalModifications() {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public void verifyCanSaveToFlowRegistry(String registryId, String bucketId, String flowId, String saveAction) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public void addTemplate(Template template) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public void removeTemplate(Template template) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public Template getTemplate(String id) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public Template findTemplate(String id) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");
    }

    @Override
    public Set<Template> getTemplates() {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");
    }

    @Override
    public Set<Template> findAllTemplates() {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");
    }

    @Override
    public void setVariables(Map<String, String> variables) {
        writeLock.lock();
        try {
            verifyCanUpdateVariables(variables);

            if (variables == null) {
                return;
            }

            final Map<VariableDescriptor, String> variableMap = new HashMap<>();
            // cannot use Collectors.toMap because value may be null
            variables.forEach((key, value) -> variableMap.put(new VariableDescriptor(key), value));

            variableRegistry.setVariables(variableMap);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Set<ComponentNode> getComponentsAffectedByVariable(String variableName) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");
    }

    @Override
    public VersionControlInformation getVersionControlInformation() {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");
    }

    @Override
    public void setVersionControlInformation(VersionControlInformation versionControlInformation, Map<String, String> versionedComponentIds) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public void disconnectVersionControl(boolean removeVersionedComponentIds) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public void synchronizeWithFlowRegistry(FlowRegistryClient flowRegistry) {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public void onComponentModified() {
        throw new UnsupportedOperationException("Not supported in Transactional NiFi");

    }

    @Override
    public void setParameterContext(ParameterContext parameterContext) {
        verifyCanSetParameterContext(parameterContext);
        this.parameterContext = parameterContext;

        getProcessors().forEach(ProcessorNode::resetValidationState);
        getControllerServices(false).forEach(ControllerServiceNode::resetValidationState);
    }

    @Override
    public ParameterContext getParameterContext() {
        return parameterContext;
    }

    @Override
    public void verifyCanSetParameterContext(final ParameterContext parameterContext) {
        readLock.lock();
        try {
            if (Objects.equals(parameterContext, getParameterContext())) {
                return;
            }

            for (final ProcessorNode processor : processors.values()) {
                final boolean referencingParam = processor.isReferencingParameter();
                if (!referencingParam) {
                    continue;
                }

                if (processor.isRunning()) {
                    throw new IllegalStateException("Cannot change Parameter Context for " + this + " because " + processor
                            + " is referencing at least one Parameter and is running");
                }

                verifyParameterSensitivityIsValid(processor, parameterContext);
            }

            for (final ControllerServiceNode service : controllerServices.values()) {
                final boolean referencingParam = service.isReferencingParameter();
                if (!referencingParam) {
                    continue;
                }

                if (service.getState() != ControllerServiceState.DISABLED) {
                    throw new IllegalStateException("Cannot change Parameter Context for " + this + " because " + service
                            + " is referencing at least one Parameter and is not disabled");
                }

                verifyParameterSensitivityIsValid(service, parameterContext);
            }
        } finally {
            readLock.unlock();
        }
    }

    private void verifyParameterSensitivityIsValid(final ComponentNode component, final ParameterContext parameterContext) {
        if (parameterContext == null) {
            return;
        }

        final Map<PropertyDescriptor, PropertyConfiguration> properties = component.getProperties();
        for (final Map.Entry<PropertyDescriptor, PropertyConfiguration> entry : properties.entrySet()) {
            final PropertyConfiguration configuration = entry.getValue();
            if (configuration == null) {
                continue;
            }

            for (final ParameterReference reference : configuration.getParameterReferences()) {
                final String paramName = reference.getParameterName();
                final Optional<Parameter> parameter = parameterContext.getParameter(paramName);

                if (parameter.isPresent()) {
                    final PropertyDescriptor propertyDescriptor = entry.getKey();
                    if (parameter.get().getDescriptor().isSensitive() && !propertyDescriptor.isSensitive()) {
                        throw new IllegalStateException("Cannot change Parameter Context for " + this + " because " + component
                                + " is referencing Parameter '" + paramName + "' from the '" + propertyDescriptor.getDisplayName()
                                + "' property and the Parameter is sensitive. Sensitive Parameters may only be referenced "
                                + "by sensitive properties.");
                    }

                    if (!parameter.get().getDescriptor().isSensitive() && propertyDescriptor.isSensitive()) {
                        throw new IllegalStateException(
                                "Cannot change Parameter Context for " + this + " because " + component + " is referencing Parameter '" + paramName
                                        + "' from a sensitive property and the Parameter is not sensitive. Sensitive properties may only reference "
                                        + "by Sensitive Parameters.");
                    }
                }
            }
        }
    }

    @Override
    public void onParameterContextUpdated() {
        readLock.lock();
        try {
            for (final ProcessorNode processorNode : getProcessors()) {
                if (processorNode.isReferencingParameter() && processorNode.getScheduledState() != ScheduledState.RUNNING) {
                    processorNode.resetValidationState();
                }
            }

            for (final ControllerServiceNode serviceNode : getControllerServices(false)) {
                if (serviceNode.isReferencingParameter() && serviceNode.getState() == ControllerServiceState.DISABLING
                        || serviceNode.getState() == ControllerServiceState.DISABLED) {
                    serviceNode.resetValidationState();
                }
            }
        } finally {
            readLock.unlock();
        }

    }

    @Override
    public ProcessGroupCounts getCounts() {
        int localInputPortCount = 0;
        int localOutputPortCount = 0;
        int publicInputPortCount = 0;
        int publicOutputPortCount = 0;

        int running = 0;
        int stopped = 0;
        int invalid = 0;
        int disabled = 0;
        int activeRemotePorts = 0;
        int inactiveRemotePorts = 0;

        int upToDate = 0;
        int locallyModified = 0;
        int stale = 0;
        int locallyModifiedAndStale = 0;
        int syncFailure = 0;

        readLock.lock();
        try {
            for (final ProcessorNode procNode : processors.values()) {
                if (ScheduledState.DISABLED.equals(procNode.getScheduledState())) {
                    disabled++;
                } else if (procNode.isRunning()) {
                    running++;
                } else if (procNode.getValidationStatus() == ValidationStatus.INVALID) {
                    invalid++;
                } else {
                    stopped++;
                }
            }

            for (final Port port : inputPorts.values()) {
                if (port instanceof PublicPort) {
                    publicInputPortCount++;
                } else {
                    localInputPortCount++;
                }
                if (ScheduledState.DISABLED.equals(port.getScheduledState())) {
                    disabled++;
                } else if (port.isRunning()) {
                    running++;
                } else if (!port.isValid()) {
                    invalid++;
                } else {
                    stopped++;
                }
            }

            for (final Port port : outputPorts.values()) {
                if (port instanceof PublicPort) {
                    publicOutputPortCount++;
                } else {
                    localOutputPortCount++;
                }
                if (ScheduledState.DISABLED.equals(port.getScheduledState())) {
                    disabled++;
                } else if (port.isRunning()) {
                    running++;
                } else if (!port.isValid()) {
                    invalid++;
                } else {
                    stopped++;
                }
            }

            for (final ProcessGroup childGroup : processGroups.values()) {
                final ProcessGroupCounts childCounts = childGroup.getCounts();
                running += childCounts.getRunningCount();
                stopped += childCounts.getStoppedCount();
                invalid += childCounts.getInvalidCount();
                disabled += childCounts.getDisabledCount();

                // update the vci counts for this child group
                final VersionControlInformation vci = childGroup.getVersionControlInformation();
                if (vci != null) {
                    switch (vci.getStatus().getState()) {
                    case LOCALLY_MODIFIED:
                        locallyModified++;
                        break;
                    case LOCALLY_MODIFIED_AND_STALE:
                        locallyModifiedAndStale++;
                        break;
                    case STALE:
                        stale++;
                        break;
                    case SYNC_FAILURE:
                        syncFailure++;
                        break;
                    case UP_TO_DATE:
                        upToDate++;
                        break;
                    }
                }

                // update the vci counts for all nested groups within the child
                upToDate += childCounts.getUpToDateCount();
                locallyModified += childCounts.getLocallyModifiedCount();
                stale += childCounts.getStaleCount();
                locallyModifiedAndStale += childCounts.getLocallyModifiedAndStaleCount();
                syncFailure += childCounts.getSyncFailureCount();
            }

            for (final RemoteProcessGroup remoteGroup : findAllRemoteProcessGroups()) {
                // Count only input ports that have incoming connections
                for (final Port port : remoteGroup.getInputPorts()) {
                    if (port.hasIncomingConnection()) {
                        if (port.isRunning()) {
                            activeRemotePorts++;
                        } else {
                            inactiveRemotePorts++;
                        }
                    }
                }

                // Count only output ports that have outgoing connections
                for (final Port port : remoteGroup.getOutputPorts()) {
                    if (!port.getConnections().isEmpty()) {
                        if (port.isRunning()) {
                            activeRemotePorts++;
                        } else {
                            inactiveRemotePorts++;
                        }
                    }
                }

                final String authIssue = remoteGroup.getAuthorizationIssue();
                if (authIssue != null) {
                    invalid++;
                }
            }
        } finally {
            readLock.unlock();
        }

        return new ProcessGroupCounts(localInputPortCount, localOutputPortCount, publicInputPortCount, publicOutputPortCount, running, stopped,
                invalid, disabled, activeRemotePorts, inactiveRemotePorts, upToDate, locallyModified, stale, locallyModifiedAndStale, syncFailure);
    }

    private void verifyPortUniqueness(final Port port, final Map<String, Port> portIdMap, final Function<String, Port> getPortByName) {

        if (portIdMap.containsKey(requireNonNull(port).getIdentifier())) {
            throw new IllegalStateException("A port with the same id already exists.");
        }

        if (getPortByName.apply(port.getName()) != null) {
            throw new IllegalStateException("A port with the same name already exists.");
        }
    }

    private interface PortRetriever {

        Port getPort(ProcessGroup group, String id);

        Set<Port> getPorts(ProcessGroup group);
    }

    private static class InputPortRetriever implements PortRetriever {

        @Override
        public Set<Port> getPorts(final ProcessGroup group) {
            return group.getInputPorts();
        }

        @Override
        public Port getPort(final ProcessGroup group, final String id) {
            return group.getInputPort(id);
        }
    }

    private static class OutputPortRetriever implements PortRetriever {

        @Override
        public Set<Port> getPorts(final ProcessGroup group) {
            return group.getOutputPorts();
        }

        @Override
        public Port getPort(final ProcessGroup group, final String id) {
            return group.getOutputPort(id);
        }
    }

    private Port getPortByName(final String name, final ProcessGroup group, final PortRetriever retriever) {
        for (final Port port : retriever.getPorts(group)) {
            if (port.getName().equals(name)) {
                return port;
            }
        }

        return null;
    }

    private boolean isOwner(ProcessGroup owner) {
        while (owner != this && owner != null) {
            owner = owner.getParent();
        }

        return owner == this;

    }

    /**
     * Looks for any property that is configured on the given component that references a Controller Service. If any exists, and that Controller Service is not accessible from this Process Group, then
     * the given component will be removed from the service's referencing components.
     *
     * @param component
     *            the component whose invalid references should be removed
     */
    private void updateControllerServiceReferences(final ComponentNode component) {
        for (final Map.Entry<PropertyDescriptor, String> entry : component.getEffectivePropertyValues().entrySet()) {
            final String serviceId = entry.getValue();
            if (serviceId == null) {
                continue;
            }

            final PropertyDescriptor propertyDescriptor = entry.getKey();
            final Class<? extends ControllerService> serviceClass = propertyDescriptor.getControllerServiceDefinition();

            if (serviceClass != null) {
                final boolean validReference = isValidServiceReference(serviceId, serviceClass);
                final ControllerServiceNode serviceNode = flowManager.getControllerServiceNode(serviceId);
                if (serviceNode != null) {
                    if (validReference) {
                        serviceNode.addReference(component, propertyDescriptor);
                    } else {
                        serviceNode.removeReference(component, propertyDescriptor);
                    }
                }
            }
        }
    }

    private boolean isValidServiceReference(final String serviceId, final Class<? extends ControllerService> serviceClass) {
        Set<ControllerServiceNode> allServices = flowManager.getAllControllerServices();
        boolean isValid = allServices.stream().filter((cs) -> serviceClass.isAssignableFrom(cs.getProxiedControllerService().getClass()))
                .anyMatch((cs) -> cs.getIdentifier().equals(serviceId));
        return isValid;
    }

    private boolean isInputPort(final Connectable connectable) {
        if (connectable.getConnectableType() != ConnectableType.INPUT_PORT) {
            return false;
        }
        return findInputPort(connectable.getIdentifier()) != null;
    }

    private boolean isOutputPort(final Connectable connectable) {
        if (connectable.getConnectableType() != ConnectableType.OUTPUT_PORT) {
            return false;
        }
        return findOutputPort(connectable.getIdentifier()) != null;
    }

    private Set<String> getUpdatedVariables(final Map<String, String> newVariableValues) {
        final Set<String> updatedVariableNames = new HashSet<>();
    
        final MutableVariableRegistry registry = getVariableRegistry();
        for (final Map.Entry<String, String> entry : newVariableValues.entrySet()) {
            final String varName = entry.getKey();
            final String newValue = entry.getValue();
    
            final String curValue = registry.getVariableValue(varName);
            if (!Objects.equals(newValue, curValue)) {
                updatedVariableNames.add(varName);
            }
        }
    
        return updatedVariableNames;
    }

    private List<VariableImpact> getVariableImpact(final ComponentNode component) {
        return component.getEffectivePropertyValues().keySet().stream()
                .map(descriptor -> {
                    final String configuredVal = component.getEffectivePropertyValue(descriptor);
                    return configuredVal == null ? descriptor.getDefaultValue() : configuredVal;
                })
                .map(propVal -> Query.prepare(propVal).getVariableImpact())
                .collect(Collectors.toList());
    }

}
