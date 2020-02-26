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

import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.LoggableComponent;
import org.apache.nifi.controller.TerminationAwareLogger;
import org.apache.nifi.controller.exception.ControllerServiceInstantiationException;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.controller.exception.ReportingTaskInstantiationException;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.StandardControllerServiceInvocationHandler;
import org.apache.nifi.controller.service.StandardControllerServiceNode;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.StandardValidationContextFactory;
import org.apache.nifi.registry.ComponentVariableRegistry;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.registry.variable.StandardComponentVariableRegistry;
import org.apache.nifi.reporting.ReportingInitializationContext;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.transactional.bootstrap.ValidationContextFactory;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.ReportingTaskDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Proxy;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ComponentFactory {
    private static final Logger logger = LoggerFactory.getLogger(ComponentFactory.class);
    private final ExtensionManager extensionManager;

    public ComponentFactory(final ExtensionManager extensionManager) {
        this.extensionManager = extensionManager;
    }

    public TransactionalProcessorWrapper createProcessor(final ProcessorDTO processorDTO, final TransactionalControllerServiceLookup controllerServiceLookup, final VariableRegistry variableRegistry,
            final Set<URL> classpathUrls, final ParameterContext parameterContext, NiFiProperties nifiProps) throws ProcessorInstantiationException {

        final String type = processorDTO.getType();
        final String identifier = processorDTO.getId();

        final Bundle bundle = getAvailableBundle(processorDTO.getBundle(), type);
        if (bundle == null) {
            throw new IllegalStateException(
                    "Unable to find bundle for coordinate " + processorDTO.getBundle().getGroup() + ":" + processorDTO.getBundle().getArtifact() + ":" + processorDTO.getBundle().getVersion());
        }

        final ClassLoader ctxClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            final ClassLoader detectedClassLoader = extensionManager.createInstanceClassLoader(type, identifier, bundle, classpathUrls == null ? Collections.emptySet() : classpathUrls);

            logger.debug("Setting context class loader to {} (parent = {}) to create {}", detectedClassLoader, detectedClassLoader.getParent(), type);
            final Class<?> rawClass = Class.forName(type, true, detectedClassLoader);
            Thread.currentThread().setContextClassLoader(detectedClassLoader);

            final Object extensionInstance = rawClass.newInstance();
            final ComponentLog componentLog = new SLF4JComponentLog(extensionInstance);
            final TerminationAwareLogger terminationAwareLogger = new TerminationAwareLogger(componentLog);

            final Processor processor = (Processor) extensionInstance;
            final ProcessorInitializationContext initializationContext = new TransactionalProcessorInitializationContext(processorDTO.getId(), processor, controllerServiceLookup, nifiProps);
            processor.initialize(initializationContext);

            // If no classpath urls were provided, check if we need to add additional classpath URL's based on configured properties.
            if (classpathUrls == null) {
                final Set<URL> additionalClasspathUrls = getAdditionalClasspathResources(processor.getPropertyDescriptors(), processor.getIdentifier(), processorDTO.getConfig().getProperties(),
                        parameterContext, variableRegistry, terminationAwareLogger);

                if (!additionalClasspathUrls.isEmpty()) {
                    return createProcessor(processorDTO, controllerServiceLookup, variableRegistry, additionalClasspathUrls, parameterContext, nifiProps);
                }
            }

            final TransactionalProcessorWrapper processorWrapper = new TransactionalProcessorWrapper(processorDTO.getId(), processor, null, controllerServiceLookup, variableRegistry,
                    detectedClassLoader, parameterContext);

            // Configure the Processor
            processorWrapper.setAnnotationData(processorDTO.getConfig().getAnnotationData());
            processorDTO.getConfig().getProperties().forEach(processorWrapper::setProperty);
            for (String relationship : processorDTO.getConfig().getAutoTerminatedRelationships()) {
                processorWrapper.addAutoTermination(new Relationship.Builder().name(relationship).build());
            }

            return processorWrapper;
        } catch (final Exception e) {
            throw new ProcessorInstantiationException(type, e);
        } finally {
            if (ctxClassLoader != null) {
                Thread.currentThread().setContextClassLoader(ctxClassLoader);
            }
        }
    }

    private Set<URL> getAdditionalClasspathResources(final List<PropertyDescriptor> propertyDescriptors, final String componentId, final Map<String, String> properties,
            final ParameterLookup parameterLookup, final VariableRegistry variableRegistry, final ComponentLog logger) {
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

    public TransactionalControllerServiceNode createControllerService(final ControllerServiceDTO controllerService, final VariableRegistry variableRegistry, final ControllerServiceLookup serviceLookup,
            final StateManagerProvider stateManagerProvider, final ParameterLookup parameterLookup, NiFiProperties nifiProps) {
        return createControllerService(controllerService, variableRegistry, null, serviceLookup, stateManagerProvider, parameterLookup, nifiProps);
    }

    private TransactionalControllerServiceNode createControllerService(final ControllerServiceDTO controllerService, final VariableRegistry variableRegistry, final Set<URL> classpathUrls,
            final ControllerServiceLookup serviceLookup, final StateManagerProvider stateManagerProvider, final ParameterLookup parameterLookup, NiFiProperties nifiProps) {
        final ClassLoader ctxClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            String type = controllerService.getType();
            final Bundle bundle = getAvailableBundle(controllerService.getBundle(), type);

            String identifier = controllerService.getId();
            final ClassLoader detectedClassLoader = extensionManager.createInstanceClassLoader(type, identifier , bundle, classpathUrls == null ? Collections.emptySet() : classpathUrls);
            final Class<?> rawClass = Class.forName(type, true, detectedClassLoader);
            Thread.currentThread().setContextClassLoader(detectedClassLoader);

            final Class<? extends ControllerService> controllerServiceClass = rawClass.asSubclass(ControllerService.class);
            final ControllerService serviceImpl = controllerServiceClass.newInstance();
            final StandardControllerServiceInvocationHandler invocationHandler = new StandardControllerServiceInvocationHandler(extensionManager, serviceImpl);

            // extract all interfaces... controllerServiceClass is non null so getAllInterfaces is non null
            final List<Class<?>> interfaceList = ClassUtils.getAllInterfaces(controllerServiceClass);
            final Class<?>[] interfaces = interfaceList.toArray(new Class<?>[0]);

            final ControllerService proxiedService;
            if (detectedClassLoader == null) {
                proxiedService = (ControllerService) Proxy.newProxyInstance(getClass().getClassLoader(), interfaces, invocationHandler);
            } else {
                proxiedService = (ControllerService) Proxy.newProxyInstance(detectedClassLoader, interfaces, invocationHandler);
            }

            logger.info("Created Controller Service of type {} with identifier {}", type, identifier);
            final ComponentLog serviceLogger = new SLF4JComponentLog(serviceImpl);

            final ControllerServiceInitializationContext initContext = new TransactionalControllerServiceInitializationContext(identifier, serviceLogger, serviceLookup, stateManager, nifiProps);
            serviceImpl.initialize(initContext);

            final LoggableComponent<ControllerService> originalLoggableComponent = new LoggableComponent<>(serviceImpl, bundleCoordinate, terminationAwareLogger);
            final LoggableComponent<ControllerService> proxiedLoggableComponent = new LoggableComponent<>(proxiedService, bundleCoordinate, terminationAwareLogger);

            final ComponentVariableRegistry componentVarRegistry = new StandardComponentVariableRegistry(variableRegistry);
            final ValidationContextFactory validationContextFactory = new StandardValidationContextFactory(serviceLookup, componentVarRegistry);
            final ControllerServiceNode serviceNode = new StandardControllerServiceNode(originalLoggableComponent, proxiedLoggableComponent, invocationHandler,
                    identifier, validationContextFactory, serviceProvider, componentVarRegistry, reloadComponent, extensionManager, validationTrigger);
            serviceNode.setName(rawClass.getSimpleName());

            invocationHandler.setServiceNode(serviceNode);
            return serviceNode;
        } finally {
            if (ctxClassLoader != null) {
                Thread.currentThread().setContextClassLoader(ctxClassLoader);
            }
        }

    }
    private ControllerService createControllerService(final ControllerServiceDTO controllerService, final VariableRegistry variableRegistry, final Set<URL> classpathUrls,
            final ControllerServiceLookup serviceLookup, final StateManager stateManager, final ParameterLookup parameterLookup, NiFiProperties nifiProps) {

        final String type = controllerService.getType();
        final String identifier = controllerService.getId();

        final Bundle bundle = getAvailableBundle(controllerService.getBundle(), type);
        if (bundle == null) {
            throw new IllegalStateException("Unable to find bundle for coordinate " + controllerService.getBundle().getGroup() + ":" + controllerService.getBundle().getArtifact() + ":"
                    + controllerService.getBundle().getVersion());
        }

        final ClassLoader ctxClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            final ClassLoader detectedClassLoader = extensionManager.createInstanceClassLoader(type, identifier, bundle, classpathUrls == null ? Collections.emptySet() : classpathUrls);

            logger.debug("Setting context class loader to {} (parent = {}) to create {}", detectedClassLoader, detectedClassLoader.getParent(), type);
            final Class<?> rawClass = Class.forName(type, true, detectedClassLoader);
            Thread.currentThread().setContextClassLoader(detectedClassLoader);

            final Object extensionInstance = rawClass.newInstance();
            final ComponentLog componentLog = new SLF4JComponentLog(extensionInstance);
            final TerminationAwareLogger terminationAwareLogger = new TerminationAwareLogger(componentLog);

            final ControllerService service = (ControllerService) extensionInstance;
            final ControllerServiceInitializationContext initializationContext = new TransactionalControllerServiceInitializationContext(identifier, service, serviceLookup, stateManager, nifiProps);
            service.initialize(initializationContext);

            // If no classpath urls were provided, check if we need to add additional classpath URL's based on configured properties.
            if (classpathUrls == null) {
                final Set<URL> additionalClasspathUrls = getAdditionalClasspathResources(service.getPropertyDescriptors(), service.getIdentifier(), controllerService.getProperties(), parameterLookup,
                        variableRegistry, terminationAwareLogger);

                if (!additionalClasspathUrls.isEmpty()) {
                    return createControllerService(controllerService, variableRegistry, additionalClasspathUrls, serviceLookup, stateManager, parameterLookup, nifiProps);
                }
            }

            return service;
        } catch (final Exception e) {
            throw new ControllerServiceInstantiationException(type, e);
        } finally {
            if (ctxClassLoader != null) {
                Thread.currentThread().setContextClassLoader(ctxClassLoader);
            }
        }
    }

    private Bundle getAvailableBundle(final BundleDTO bundle, final String componentType) {
        final BundleCoordinate bundleCoordinate = new BundleCoordinate(bundle.getGroup(), bundle.getArtifact(), bundle.getVersion());
        final Bundle availableBundle = extensionManager.getBundle(bundleCoordinate);
        if (availableBundle != null) {
            return availableBundle;
        }

        final List<Bundle> possibleBundles = extensionManager.getBundles(componentType);
        if (possibleBundles.isEmpty()) {
            throw new IllegalStateException("Could not find any NiFi Bundles that contain the Extension [" + componentType + "]");
        }

        if (possibleBundles.size() > 1) {
            throw new IllegalStateException(
                    "Found " + possibleBundles.size() + " different NiFi Bundles that contain the Extension [" + componentType + "] but none of them had a version of " + bundle.getVersion());
        }

        return possibleBundles.get(0);
    }

    private ReportingTask createReportingTask(ReportingTaskDTO reportingTaskDTO, VariableRegistry baseVarRegistry, TransactionalControllerServiceLookup serviceLookup, StateManager stateManager,
            Set<URL> classpaths, ParameterLookup empty, String nifiProps) throws ReportingTaskInstantiationException {

        final String type = reportingTaskDTO.getType();
        final String identifier = reportingTaskDTO.getId();
        final String name = reportingTaskDTO.getName();

        final Bundle bundle = getAvailableBundle(reportingTaskDTO.getBundle(), type);
        if (bundle == null) {
            throw new IllegalStateException("Unable to find bundle for coordinate " + reportingTaskDTO.getBundle().getGroup() + ":" + reportingTaskDTO.getBundle().getArtifact() + ":"
                    + reportingTaskDTO.getBundle().getVersion());
        }

        final ClassLoader ctxClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            final ClassLoader detectedClassLoader = extensionManager.createInstanceClassLoader(type, identifier, bundle, classpaths == null ? Collections.emptySet() : classpaths);

            logger.debug("Setting context class loader to {} (parent = {}) to create {}", detectedClassLoader, detectedClassLoader.getParent(), type);
            final Class<?> rawClass = Class.forName(type, true, detectedClassLoader);
            Thread.currentThread().setContextClassLoader(detectedClassLoader);

            final Object extensionInstance = rawClass.newInstance();
            final ComponentLog componentLog = new SLF4JComponentLog(extensionInstance);
            final TerminationAwareLogger terminationAwareLogger = new TerminationAwareLogger(componentLog);

            final ReportingTask reportingTask = (ReportingTask) extensionInstance;
            String schedPeriod = reportingTaskDTO.getSchedulingPeriod();
            SchedulingStrategy schedStrategy = SchedulingStrategy.TIMER_DRIVEN;
            final ReportingInitializationContext initializationContext = new TransactionalReportingInitializationContext(identifier, name, schedStrategy , schedPeriod, reportingTask, serviceLookup, nifiProps);
            processor.initialize(initializationContext);

            // If no classpath urls were provided, check if we need to add additional classpath URL's based on configured properties.
            if (classpathUrls == null) {
                final Set<URL> additionalClasspathUrls = getAdditionalClasspathResources(processor.getPropertyDescriptors(), processor.getIdentifier(), reportingTaskDTO.getConfig().getProperties(),
                        parameterContext, variableRegistry, componentLog);

                if (!additionalClasspathUrls.isEmpty()) {
                    return createProcessor(reportingTaskDTO, serviceLookup, variableRegistry, additionalClasspathUrls, parameterContext);
                }
            }

            final TransactionalProcessorWrapper processorWrapper = new TransactionalProcessorWrapper(reportingTaskDTO.getId(), processor, null, serviceLookup, variableRegistry,
                    detectedClassLoader, parameterContext);

            // Configure the Processor
            processorWrapper.setAnnotationData(reportingTaskDTO.getConfig().getAnnotationData());
            reportingTaskDTO.getConfig().getProperties().forEach(processorWrapper::setProperty);
            for (String relationship : reportingTaskDTO.getConfig().getAutoTerminatedRelationships()) {
                processorWrapper.addAutoTermination(new Relationship.Builder().name(relationship).build());
            }

            return processorWrapper;
        } catch (final Exception e) {
            throw new ReportingTaskInstantiationException(type, e);
        } finally {
            if (ctxClassLoader != null) {
                Thread.currentThread().setContextClassLoader(ctxClassLoader);
            }
        }
    }

    public ReportingTask createReportingTask(ReportingTaskDTO rtDTO, VariableRegistry baseVarRegistry, TransactionalControllerServiceLookup serviceLookup, StateManager stateManager,
            ParameterLookup paramLookup) throws ReportingTaskInstantiationException {
        return createReportingTask(rtDTO, baseVarRegistry, serviceLookup, stateManager, null, paramLookup);
    }
}
