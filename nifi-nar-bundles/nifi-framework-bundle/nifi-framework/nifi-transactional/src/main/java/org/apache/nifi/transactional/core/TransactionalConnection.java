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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.AuthorizationResult;
import org.apache.nifi.authorization.AuthorizationResult.Result;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.LocalPort;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.controller.queue.ConnectionEventListener;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.remote.RemoteGroupPort;

/**
 * Models a connection between connectable components. A connection may contain one or more relationships that map the source component to the destination component.
 */
public final class TransactionalConnection implements Connection, ConnectionEventListener {

    private final String id;
    private final AtomicReference<ProcessGroup> processGroup;
    private final AtomicReference<String> name;
    private final Connectable source;
    private final AtomicReference<Connectable> destination;
    private final AtomicReference<Collection<Relationship>> relationships;
    private final int hashCode;

    private volatile FlowFileQueue flowFileQueue;

    private TransactionalConnection(final Builder builder) {
        id = builder.id;
        name = new AtomicReference<>(builder.name);
        processGroup = new AtomicReference<>(builder.processGroup);
        source = builder.source;
        destination = new AtomicReference<>(builder.destination);
        relationships = new AtomicReference<>(Collections.unmodifiableCollection(builder.relationships));
        hashCode = new HashCodeBuilder(7, 67).append(id).toHashCode();
    }

    @Override
    public ProcessGroup getProcessGroup() {
        return processGroup.get();
    }

    @Override
    public String getIdentifier() {
        return id;
    }

    @Override
    public String getName() {
        return name.get();
    }

    @Override
    public void setName(final String name) {
        this.name.set(name);
    }

    @Override
    public Authorizable getParentAuthorizable() {
        return getProcessGroup();
    }

    @Override
    public Resource getResource() {
        return new Resource() {
            @Override
            public String getIdentifier() {
                return "/connections/" + TransactionalConnection.this.getIdentifier();
            }

            @Override
            public String getName() {
                String name = TransactionalConnection.this.getName();

                final Collection<Relationship> relationships = getRelationships();
                if (name == null && CollectionUtils.isNotEmpty(relationships)) {
                    name = StringUtils.join(relationships.stream().map(relationship -> relationship.getName()).collect(Collectors.toSet()), ", ");
                }

                if (name == null) {
                    name = "Connection";
                }

                return name;
            }

            @Override
            public String getSafeDescription() {
                return "Connection " + TransactionalConnection.this.getIdentifier();
            }
        };
    }

    @Override
    public void triggerDestinationEvent() {
    }

    @Override
    public void triggerSourceEvent() {
    }

    @Override
    public Authorizable getSourceAuthorizable() {
        final Connectable sourceConnectable = getSource();
        final Authorizable sourceAuthorizable;

        // if the source is a remote group port, authorize according to the RPG
        if (sourceConnectable instanceof RemoteGroupPort) {
            sourceAuthorizable = ((RemoteGroupPort) sourceConnectable).getRemoteProcessGroup();
        } else {
            sourceAuthorizable = sourceConnectable;
        }

        return sourceAuthorizable;
    }

    @Override
    public Authorizable getDestinationAuthorizable() {
        final Connectable destinationConnectable = getDestination();
        final Authorizable destinationAuthorizable;

        // if the destination is a remote group port, authorize according to the RPG
        if (destinationConnectable instanceof RemoteGroupPort) {
            destinationAuthorizable = ((RemoteGroupPort) destinationConnectable).getRemoteProcessGroup();
        } else {
            destinationAuthorizable = destinationConnectable;
        }

        return destinationAuthorizable;
    }

    @Override
    public AuthorizationResult checkAuthorization(Authorizer authorizer, RequestAction action, NiFiUser user, Map<String, String> resourceContext) {
        if (user == null) {
            return AuthorizationResult.denied("Unknown user.");
        }

        // check the source
        final AuthorizationResult sourceResult = getSourceAuthorizable().checkAuthorization(authorizer, action, user, resourceContext);
        if (Result.Denied.equals(sourceResult.getResult())) {
            return sourceResult;
        }

        // check the destination
        return getDestinationAuthorizable().checkAuthorization(authorizer, action, user, resourceContext);
    }

    @Override
    public void authorize(Authorizer authorizer, RequestAction action, NiFiUser user, Map<String, String> resourceContext)
            throws AccessDeniedException {
        if (user == null) {
            throw new AccessDeniedException("Unknown user.");
        }

        getSourceAuthorizable().authorize(authorizer, action, user, resourceContext);
        getDestinationAuthorizable().authorize(authorizer, action, user, resourceContext);
    }

    @Override
    public List<Position> getBendPoints() {
        return Collections.emptyList();
    }

    @Override
    public void setBendPoints(final List<Position> position) {
    }

    @Override
    public int getLabelIndex() {
        return 0;
    }

    @Override
    public void setLabelIndex(final int labelIndex) {
    }

    @Override
    public long getZIndex() {
        return 0;
    }

    @Override
    public void setZIndex(final long zIndex) {
    }

    @Override
    public Connectable getSource() {
        return source;
    }

    @Override
    public Connectable getDestination() {
        return destination.get();
    }

    @Override
    public Collection<Relationship> getRelationships() {
        return relationships.get();
    }

    @Override
    public FlowFileQueue getFlowFileQueue() {
        return flowFileQueue;
    }

    @Override
    public void setProcessGroup(final ProcessGroup newGroup) {
        final ProcessGroup currentGroup = this.processGroup.get();
        try {
            this.processGroup.set(newGroup);
        } catch (final RuntimeException e) {
            this.processGroup.set(currentGroup);
            throw e;
        }
    }

    @Override
    public void setRelationships(final Collection<Relationship> newRelationships) {
        final Collection<Relationship> currentRelationships = relationships.get();
        if (currentRelationships.equals(newRelationships)) {
            return;
        }

        try {
            getSource().verifyCanUpdate();
        } catch (final IllegalStateException ise) {
            throw new IllegalStateException("Cannot update the relationships for Connection", ise);
        }

        try {
            this.relationships.set(new ArrayList<>(newRelationships));
            getSource().updateConnection(this);
        } catch (final RuntimeException e) {
            this.relationships.set(currentRelationships);
            throw e;
        }
    }

    @Override
    public void setDestination(final Connectable newDestination) {
        final Connectable previousDestination = destination.get();
        if (previousDestination.equals(newDestination)) {
            return;
        }

        if (previousDestination.isRunning() && !(previousDestination instanceof Funnel || previousDestination instanceof LocalPort)) {
            throw new IllegalStateException("Cannot change destination of Connection because the current destination is running");
        }

        if (getFlowFileQueue().isUnacknowledgedFlowFile()) {
            throw new IllegalStateException(
                    "Cannot change destination of Connection because FlowFiles from this Connection are currently held by " + previousDestination);
        }

        if (newDestination instanceof Funnel && newDestination.equals(source)) {
            throw new IllegalStateException("Funnels do not support self-looping connections.");
        }

        try {
            previousDestination.removeConnection(this);
            this.destination.set(newDestination);
            getSource().updateConnection(this);

            newDestination.addConnection(this);
        } catch (final RuntimeException e) {
            this.destination.set(previousDestination);
            throw e;
        }
    }

    @Override
    public void lock() {
        flowFileQueue.lock();
    }

    @Override
    public void unlock() {
        flowFileQueue.unlock();
    }

    @Override
    public List<FlowFileRecord> poll(final FlowFileFilter filter, final Set<FlowFileRecord> expiredRecords) {
        return flowFileQueue.poll(filter, expiredRecords);
    }

    @Override
    public FlowFileRecord poll(final Set<FlowFileRecord> expiredRecords) {
        return flowFileQueue.poll(expiredRecords);
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof Connection)) {
            return false;
        }
        final Connection con = (Connection) other;
        return new EqualsBuilder().append(id, con.getIdentifier()).isEquals();
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public String toString() {
        return "Connection[ID=" + getIdentifier() + ", Source ID=" + getSource().getIdentifier() + ", Dest ID=" + getDestination().getIdentifier()
                + "]";
    }

    /**
     * Gives this Connection ownership of the given FlowFile and allows the Connection to hold on to the FlowFile but NOT provide the FlowFile to consumers. This allows us to ensure that the
     * Connection is not deleted during the middle of a Session commit.
     *
     * @param flowFile
     *            to add
     */
    @Override
    public void enqueue(final FlowFileRecord flowFile) {
        flowFileQueue.put(flowFile);
    }

    @Override
    public void enqueue(final Collection<FlowFileRecord> flowFiles) {
        flowFileQueue.putAll(flowFiles);
    }

    public static class Builder {

        private String id = UUID.randomUUID().toString();
        private String name;
        private ProcessGroup processGroup;
        private Connectable source;
        private Connectable destination;
        private Collection<Relationship> relationships;

        public Builder() {
        }

        public Builder id(final String id) {
            this.id = id;
            return this;
        }

        public Builder source(final Connectable source) {
            this.source = source;
            return this;
        }

        public Builder processGroup(final ProcessGroup group) {
            this.processGroup = group;
            return this;
        }

        public Builder destination(final Connectable destination) {
            this.destination = destination;
            return this;
        }

        public Builder relationships(final Collection<Relationship> relationships) {
            this.relationships = new ArrayList<>(relationships);
            return this;
        }

        public Builder name(final String name) {
            this.name = name;
            return this;
        }

        public TransactionalConnection build() {
            if (source == null) {
                throw new IllegalStateException("Cannot build a Connection without a Source");
            }
            if (destination == null) {
                throw new IllegalStateException("Cannot build a Connection without a Destination");
            }

            if (relationships == null) {
                relationships = new ArrayList<>();
            }

            if (relationships.isEmpty()) {
                // ensure relationships have been specified for processors, otherwise the anonymous relationship is used
                if (source.getConnectableType() == ConnectableType.PROCESSOR) {
                    throw new IllegalStateException("Cannot build a Connection without any relationships");
                }
                relationships.add(Relationship.ANONYMOUS);
            }

            return new TransactionalConnection(this);
        }
    }

    @Override
    public void verifyCanUpdate() {
        // StandardConnection can always be updated
    }

    @Override
    public void verifyCanDelete() {
        if (!flowFileQueue.isEmpty()) {
            throw new IllegalStateException("Queue not empty for " + this.getIdentifier());
        }

        if (source.isRunning()) {
            if (!ConnectableType.FUNNEL.equals(source.getConnectableType())) {
                throw new IllegalStateException("Source of Connection (" + source.getIdentifier() + ") is running");
            }
        }

        final Connectable dest = destination.get();
        if (dest.isRunning()) {
            if (!ConnectableType.FUNNEL.equals(dest.getConnectableType())) {
                throw new IllegalStateException("Destination of Connection (" + dest.getIdentifier() + ") is running");
            }
        }
    }

    @Override
    public Optional<String> getVersionedComponentId() {
        return Optional.empty();
    }

    @Override
    public void setVersionedComponentId(final String versionedComponentId) {
    }
}
