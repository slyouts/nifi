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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;

import com.google.gson.JsonObject;

public class TransactionalFlowFile implements FlowFile {

    private final Map<String, String> attributes = new HashMap<>();
    private static AtomicLong nextID = new AtomicLong(0);

    private long id;
    private final long entryDate;
    private final long creationTime;
    private boolean penalized = false;

    private long lastEnqueuedDate = 0;
    private long enqueuedIndex = 0;
    private Path contentLocation;

    public TransactionalFlowFile(File contentLocation, Map<String, String> attributes) {
        this(contentLocation.toPath(), attributes);
    }

    public TransactionalFlowFile(Path contentLocation, Map<String, String> attributes) {
        this();
        this.attributes.putAll(attributes);
        this.contentLocation = contentLocation;
    }

    public TransactionalFlowFile(final TransactionalFlowFile toCopy) {
        this();
        this.id = toCopy.id;

        attributes.putAll(toCopy.getAttributes());
        this.penalized = toCopy.isPenalized();
        this.contentLocation = toCopy.contentLocation;
    }

    public TransactionalFlowFile(final TransactionalFlowFile toCopy, long offset, long size) {
        this();
        this.id = toCopy.id;

        attributes.putAll(toCopy.getAttributes());
        this.penalized = toCopy.isPenalized();

        this.contentLocation = toCopy.contentLocation;
    }

    public TransactionalFlowFile() {
        this.creationTime = System.nanoTime();
        this.id = nextID.getAndIncrement();
        this.entryDate = System.currentTimeMillis();
        this.lastEnqueuedDate = entryDate;
        attributes.put(CoreAttributes.FILENAME.key(), String.valueOf(System.nanoTime()) + ".statelessFlowFile");
        attributes.put(CoreAttributes.PATH.key(), "target");
        attributes.put(CoreAttributes.UUID.key(), UUID.randomUUID().toString());
    }

    //region SimpleMethods
    void setPenalized(boolean penalized) {
        this.penalized = penalized;
    }

    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public long getLineageStartDate() {
        return entryDate;
    }

    @Override
    public int compareTo(final FlowFile o) {
        return getAttribute(CoreAttributes.UUID.key()).compareTo(o.getAttribute(CoreAttributes.UUID.key()));
    }

    @Override
    public String getAttribute(final String attrName) {
        return attributes.get(attrName);
    }

    @Override
    public Map<String, String> getAttributes() {
        return Collections.unmodifiableMap(attributes);
    }

    @Override
    public long getEntryDate() {
        return entryDate;
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public boolean isPenalized() {
        return penalized;
    }

    public void putAttributes(final Map<String, String> attrs) {
        attributes.putAll(attrs);
    }

    public void removeAttributes(final Set<String> attrNames) {
        for (final String attrName : attrNames) {
            attributes.remove(attrName);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj instanceof FlowFile) {
            return ((FlowFile) obj).getId() == this.id;
        }
        return false;
    }

    @Override
    public Long getLastQueueDate() {
        return lastEnqueuedDate;
    }

    public void setLastEnqueuedDate(long lastEnqueuedDate) {
        this.lastEnqueuedDate = lastEnqueuedDate;
    }

    public long getPenaltyExpirationMillis() {
        return -1;
    }

    @Override
    public long getLineageStartIndex() {
        return 0;
    }

    @Override
    public long getQueueDateIndex() {
        return enqueuedIndex;
    }

    public void setEnqueuedIndex(long enqueuedIndex) {
        this.enqueuedIndex = enqueuedIndex;
    }
    //endregion Methods

    @Override
    public String toString() {
        JsonObject attributes = new JsonObject();
        this.attributes.forEach(attributes::addProperty);

        JsonObject result = new JsonObject();
        result.add("attributes", attributes);

        return result.toString();
    }

    @Override
    public long getSize() {
        try {
            return Files.size(contentLocation);
        } catch (IOException e) {
            return -1;
        }
    }

}