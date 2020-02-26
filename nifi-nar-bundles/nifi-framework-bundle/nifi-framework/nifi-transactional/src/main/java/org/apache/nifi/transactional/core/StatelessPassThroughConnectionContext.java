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

import org.apache.nifi.processor.Relationship;

import java.util.HashSet;
import java.util.Set;

public class StatelessPassThroughConnectionContext implements StatelessConnectionContext {
    private final Set<Relationship> connectedRelationships = new HashSet<>();

    @Override
    public void addConnection(final Relationship relationship) {
        connectedRelationships.add(relationship);
    }

    @Override
    public boolean isValid() {
        return connectedRelationships.size() == 1 && connectedRelationships.contains(StatelessPassThroughComponent.RELATIONSHIP);
    }
}
