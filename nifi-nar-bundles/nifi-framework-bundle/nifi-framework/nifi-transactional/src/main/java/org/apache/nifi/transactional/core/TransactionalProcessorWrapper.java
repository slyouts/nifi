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

import org.apache.nifi.annotation.lifecycle.OnAdded;
import org.apache.nifi.annotation.lifecycle.OnConfigurationRestored;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.StandardFlowFileRecord;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.transactional.util.ReflectionUtils;

import java.io.Closeable;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class TransactionalProcessorWrapper extends AbstractTransactionalComponent implements TransactionalComponent {

    private final Processor processor;
    private final TransactionalProcessContext context;
    private final Queue<StandardFlowFileRecord> inputQueue;
    private final VariableRegistry variableRegistry;
    private final ClassLoader classLoader;

    private final Collection<ProvenanceEventRecord> provenanceEvents;

    private final Set<StatelessProcessSession> createdSessions;
    private final ComponentLog logger;

    private final TransactionalControllerServiceLookup lookup;

    private volatile boolean stopRequested = false;
    private volatile boolean isStopped = true;
    private volatile boolean initialized = false;


    public TransactionalProcessorWrapper(final String id, final Processor processor, final TransactionalProcessorWrapper parent, final TransactionalControllerServiceLookup lookup, final VariableRegistry registry,
                              final ClassLoader classLoader, final ParameterContext parameterContext) throws InvocationTargetException, IllegalAccessException {

        this.processor = processor;
        this.classLoader = classLoader;

        addParent(parent);

        this.lookup = lookup;

        this.provenanceEvents = new ArrayList<>();
        this.createdSessions = new CopyOnWriteArraySet<>();
        this.inputQueue = new LinkedList<>();
        this.variableRegistry = registry;
        this.context = new TransactionalProcessContext(processor, lookup, processor.getIdentifier(), new StatelessStateManager(), variableRegistry, parameterContext);
        this.context.setMaxConcurrentTasks(1);

        final TransactionalProcessorInitializationContext initContext = new TransactionalProcessorInitializationContext(id, processor, context);
        logger = initContext.getLogger();

        try (final CloseableNarLoader c = withNarClassLoader()) {
            processor.initialize(initContext);
            ReflectionUtils.invokeMethodsWithAnnotation(OnAdded.class, processor);
            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnConfigurationRestored.class, processor);
        }
    }

    private Processor getProcessor() {
        return this.processor;
    }

    public Set<Relationship> getRelationships() {
        return processor.getRelationships();
    }


    private void initialize() {
        //Validate context
        final Collection<ValidationResult> validationResult = context.validate();
        if (validationResult.stream().anyMatch(a -> !a.isValid()) || !this.validate()) {
            throw new IllegalArgumentException(processor + " is not valid: "
                + validationResult.stream().map(ValidationResult::toString).collect(Collectors.joining("\n")));
        }

        try (final CloseableNarLoader c = withNarClassLoader()) {
            ReflectionUtils.invokeMethodsWithAnnotation(OnScheduled.class, processor, context);
        } catch (final Exception e) {
            logger.error("Failed to perform @OnScheduled Lifecycle method: ", e);
        }

        initialized = true;
    }

    private CloseableNarLoader withNarClassLoader() {
        final ClassLoader contextclassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(this.classLoader);

        return new CloseableNarLoader() {
            @Override
            public void close() {
                Thread.currentThread().setContextClassLoader(contextclassLoader);
            }
        };
    }
    
    @Override
    public boolean runRecursive(final Queue<FlowFileRecord> output) {
        if (!initialized) {
            initialize();
        }

        final AtomicBoolean processingSuccess = new AtomicBoolean(true);
        final Set<Relationship> outputRelationships = new HashSet<>(getChildren().keySet());
        outputRelationships.addAll(getSuccessOutputPorts());
        outputRelationships.addAll(getFailureOutputPorts());

        do {
            this.isStopped = false;
            final AtomicBoolean nextStepCalled = new AtomicBoolean(false);

            try {
                logger.debug("Running {}.onTrigger with {} FlowFiles", new Object[] {this.processor.getClass().getSimpleName(), inputQueue.size()});

                try (final CloseableNarLoader c = withNarClassLoader()) { // Trigger processor with the appropriate class loader
                    processor.onTrigger(context, () -> {
                        final StatelessProcessSession session = new StatelessProcessSession(
                            inputQueue,
                            provenanceEvents,
                            processor,
                            outputRelationships,
                            () -> {
                                if (!nextStepCalled.get()) {
                                    nextStepCalled.set(true);
                                    boolean successfulRun = runChildren(output);
                                    processingSuccess.set(successfulRun);
                                }
                            });

                        createdSessions.add(session);
                        return session;
                    });
                }

                if (!nextStepCalled.get()) {
                    nextStepCalled.set(true);
                    boolean successfulRun = runChildren(output);
                    processingSuccess.set(successfulRun);
                }

                provenanceEvents.clear();
            } catch (final Exception t) {
                try (final CloseableNarLoader c = withNarClassLoader()) {
                    logger.error("Failed to trigger " + this.processor, t);
                }

                return false;
            }
        } while (!stopRequested && !inputQueue.isEmpty() && processingSuccess.get());

        this.isStopped = true;
        return processingSuccess.get();
    }

    private boolean runChildren(final Queue<InMemoryFlowFile> output) {
        final Queue<TransactionalFlowFile> penalizedFlowFiles = this.getPenalizedFlowFiles();
        if (penalizedFlowFiles.size() > 0) {
            output.addAll(penalizedFlowFiles);
            return false;
        }

        for (final Relationship relationship : this.getProcessor().getRelationships()) {
            if (isAutoTerminated(relationship)) {
                continue;
            }

            final Queue<TransactionalFlowFile> files = this.getAndRemoveFlowFilesForRelationship(relationship);
            if (files.size() == 0) {
                continue;
            }

            if (getFailureOutputPorts().contains(relationship)) {
                output.addAll(files);
                return false;
            }

            if (getSuccessOutputPorts().contains(relationship)) {
                output.addAll(files);
            }

            final List<TransactionalComponent> childComponents = getChildren().get(relationship);
            if (childComponents != null) {
                for (final TransactionalComponent child : childComponents) {
                    child.enqueueAll(files);
                    boolean successfulRun = child.runRecursive(output);

                    if (!successfulRun) {
                        return false;
                    }
                }
            }
        }

        return true;
    }

    public void shutdown() {
        this.stopRequested = true;

        for (Relationship relationship : this.getProcessor().getRelationships()) {
            if (isAutoTerminated(relationship)) {
                continue;
            }

            final List<TransactionalComponent> childComponents = getChildren().get(relationship);
            if (childComponents == null) {
                throw new IllegalArgumentException("No child for relationship: " + relationship.getName());
            }

            childComponents.forEach(TransactionalComponent::shutdown);
        }

        while (!this.isStopped) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
                break;
            }
        }

        try (final CloseableNarLoader c = withNarClassLoader()) {
            ReflectionUtils.invokeMethodsWithAnnotation(OnUnscheduled.class, processor, context);

            ReflectionUtils.invokeMethodsWithAnnotation(OnStopped.class, processor, context);

            ReflectionUtils.invokeMethodsWithAnnotation(OnShutdown.class, processor);
        } catch (final Exception e) {
            logger.error("Failed to properly shutdown " + processor + ": ", e);
        }

        logger.info(this.processor.getClass().getSimpleName() + " shutdown");
    }


    public void enqueueAll(final Queue<TransactionalFlowFile> list) {
        inputQueue.addAll(list);
    }

    public Queue<TransactionalFlowFile> getAndRemoveFlowFilesForRelationship(final Relationship relationship) {
        final Queue<TransactionalFlowFile> sortedList = createdSessions.stream()
            .flatMap(s -> s.getAndRemoveFlowFilesForRelationship(relationship).stream())
            .sorted(Comparator.comparing(TransactionalFlowFile::getCreationTime))
            .collect(Collectors.toCollection(LinkedList::new));

        return sortedList;
    }

    public Queue<TransactionalFlowFile> getPenalizedFlowFiles() {
        final Queue<TransactionalFlowFile> sortedList = createdSessions.stream()
            .flatMap(s -> s.getPenalizedFlowFiles().stream())
            .sorted(Comparator.comparing(TransactionalFlowFile::getCreationTime))
            .collect(Collectors.toCollection(LinkedList::new));
        return sortedList;

    }

    public ValidationResult setProperty(final PropertyDescriptor property, final String propertyValue) {
        return context.setProperty(property, propertyValue);
    }

    public ValidationResult setProperty(final String propertyName, final String propertyValue) {
        return context.setProperty(propertyName, propertyValue);
    }

    public void setAnnotationData(final String annotationData) {
        context.setAnnotationData(annotationData);
    }

    @Override
    public ComponentLog getLogger() {
        return logger;
    }

    @Override
    protected TransactionalProcessContext getContext() {
        return context;
    }

    /**
     * A simple interface that extends Closeable in order to provide a close() method that does not throw any checked
     * Exceptions. This is done so that the {@link #withNarClassLoader()} is able to be used without having to catch
     * an IOException that will never be thrown.
     */
    private interface CloseableNarLoader extends Closeable {
        @Override
        void close();
    }

}

