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
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.transactional.util.ReflectionUtils;
import org.apache.nifi.util.NiFiProperties;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import static java.util.Objects.requireNonNull;

public class TransactionalControllerServiceLookup implements ControllerServiceLookup {
    private final ParameterContext parameterContext;
    private final Map<String, TransactionalControllerServiceNode> controllerServiceMap = new ConcurrentHashMap<>();
    private final Map<String, SLF4JComponentLog> controllerServiceLoggers = new HashMap<>();
    private final Map<String, StateManager> controllerServiceStateManagers = new HashMap<>();
    private final StateManagerProvider stateManagerProvider;
    private final NiFiProperties nifiProperties;
    private final StringEncryptor encryptor;

    public TransactionalControllerServiceLookup(final ParameterContext parameterContext, final StateManagerProvider stateManagerProvider, final NiFiProperties nifiProperties,
            final StringEncryptor encryptor) {
        this.parameterContext = parameterContext;
        this.stateManagerProvider = stateManagerProvider;
        this.nifiProperties = nifiProperties;
        this.encryptor = encryptor;
    }

    public Map<String, TransactionalControllerServiceNode> getControllerServices() {
        return controllerServiceMap;
    }

    public void addControllerService(final TransactionalControllerServiceNode serviceConfig) throws InitializationException {
        final String identifier = serviceConfig.getImplementation().getIdentifier();
        final SLF4JComponentLog logger = new SLF4JComponentLog(serviceConfig.getImplementation());
        controllerServiceLoggers.put(identifier, logger);

        final StateManager stateManager = stateManagerProvider.getStateManager(identifier);
        controllerServiceStateManagers.put(identifier, stateManager);

        final TransactionalProcessContext initContext = new TransactionalProcessContext(requireNonNull(service), this, serviceName, logger, stateManager, parameterContext, nifiProperties, encryptor);
        service.initialize(initContext);

        try {
            ReflectionUtils.invokeMethodsWithAnnotation(OnAdded.class, service);
        } catch (final InvocationTargetException | IllegalAccessException | IllegalArgumentException e) {
            throw new InitializationException(e);
        }

        final TransactionalControllerServiceNode config = new TransactionalControllerServiceNode(service, service, serviceName);
        controllerServiceMap.put(identifier, config);
    }

    protected TransactionalControllerServiceNode getConfiguration(final String identifier) {
        return controllerServiceMap.get(identifier);
    }

    @Override
    public ControllerService getControllerService(final String identifier) {
        final TransactionalControllerServiceNode serviceConfig = controllerServiceMap.get(identifier);
        return (serviceConfig == null) ? null : serviceConfig.getService();
    }

    @Override
    public boolean isControllerServiceEnabled(final String serviceIdentifier) {
        final TransactionalControllerServiceNode status = controllerServiceMap.get(serviceIdentifier);
        if (status == null) {
            throw new IllegalArgumentException("No ControllerService exists with identifier " + serviceIdentifier);
        }

        return status.isEnabled();
    }

    @Override
    public boolean isControllerServiceEnabled(final ControllerService service) {
        return isControllerServiceEnabled(service.getIdentifier());
    }

    @Override
    public boolean isControllerServiceEnabling(final String serviceIdentifier) {
        return false;
    }

    @Override
    public Set<String> getControllerServiceIdentifiers(final Class<? extends ControllerService> serviceType) {
        final Set<String> ids = new HashSet<>();
        for (final Map.Entry<String, TransactionalControllerServiceNode> entry : controllerServiceMap.entrySet()) {
            if (serviceType.isAssignableFrom(entry.getValue().getService().getClass())) {
                ids.add(entry.getKey());
            }
        }
        return ids;
    }

    @Override
    public String getControllerServiceName(final String serviceIdentifier) {
        final TransactionalControllerServiceNode status = controllerServiceMap.get(serviceIdentifier);
        return status == null ? null : serviceIdentifier;
    }

    public void enableControllerServices(final VariableRegistry variableRegistry) {
        for (final TransactionalControllerServiceNode config : controllerServiceMap.values()) {
            final ControllerService service = config.getService();
            final Collection<ValidationResult> validationResults = validate(service, config.getName(), variableRegistry);
            if (!validationResults.isEmpty()) {
                throw new RuntimeException("Failed to enable Controller Service {id=" + service.getIdentifier() + ", name=" + config.getName() + ", type=" + service.getClass() + "} because "
                        + "validation failed: " + validationResults);
            }

            try {
                enableControllerService(service, variableRegistry);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException("Failed to enable Controller Service {id=" + service.getIdentifier() + ", name=" + config.getName() + ", type=" + service.getClass() + "}", e);
            }
        }
    }

    public Collection<ValidationResult> validate(final ControllerService service, final String serviceName, final VariableRegistry variableRegistry) {
        final StateManager stateManager = controllerServiceStateManagers.get(service.getIdentifier());
        final SLF4JComponentLog logger = controllerServiceLoggers.get(service.getIdentifier());
        final TransactionalProcessContext processContext = new TransactionalProcessContext(service, this, serviceName, logger, stateManager, variableRegistry, parameterContext, nifiProperties);
        final StatelessValidationContext validationContext = new StatelessValidationContext(processContext, this, stateManager, variableRegistry, parameterContext);
        return service.validate(validationContext);
    }

    private void enableControllerService(final ControllerService service, final VariableRegistry registry) throws InvocationTargetException, IllegalAccessException {
        final TransactionalControllerServiceNode configuration = getConfiguration(service.getIdentifier());
        if (configuration == null) {
            throw new IllegalArgumentException("Controller Service " + service + " is not known");
        }

        if (configuration.isEnabled()) {
            throw new IllegalStateException("Cannot enable Controller Service " + service + " because it is not disabled");
        }

        final ConfigurationContext configContext = new StatelessConfigurationContext(service, configuration.getProperties(), this, registry, parameterContext);
        ReflectionUtils.invokeMethodsWithAnnotation(OnEnabled.class, service, configContext);

        configuration.setEnabled(true);
    }

    public void setControllerServiceAnnotationData(final ControllerService service, final String annotationData) {
        final TransactionalControllerServiceNode configuration = getControllerServiceConfigToUpdate(service);
        configuration.setAnnotationData(annotationData);
    }

    private TransactionalControllerServiceNode getControllerServiceConfigToUpdate(final ControllerService service) {
        final TransactionalControllerServiceNode configuration = getConfiguration(service.getIdentifier());
        if (configuration == null) {
            throw new IllegalArgumentException("Controller Service " + service + " is not known");
        }

        if (configuration.isEnabled()) {
            throw new IllegalStateException("Controller service " + service + " cannot be modified because it is not disabled");
        }

        return configuration;
    }

    public ValidationResult setControllerServiceProperty(final ControllerService service, final PropertyDescriptor property, final TransactionalProcessContext context, final VariableRegistry registry,
            final String value) {
        final StateManager serviceStateManager = controllerServiceStateManagers.get(service.getIdentifier());
        if (serviceStateManager == null) {
            throw new IllegalStateException("Controller service " + service + " has not been added to this TestRunner via the #addControllerService method");
        }

        final ValidationContext validationContext = new StatelessValidationContext(context, this, serviceStateManager, registry, parameterContext).getControllerServiceValidationContext(service);
        final ValidationResult validationResult = property.validate(value, validationContext);

        final TransactionalControllerServiceNode configuration = getControllerServiceConfigToUpdate(service);
        final String oldValue = configuration.getProperties().get(property);
        configuration.setProperty(property, value);

        if ((value == null && oldValue != null) || (value != null && !value.equals(oldValue))) {
            service.onPropertyModified(property, oldValue, value);
        }

        return validationResult;
    }

}
