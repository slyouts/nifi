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
package org.apache.nifi;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.nar.ExtensionDiscoveringManager;
import org.apache.nifi.nar.ExtensionMapping;
import org.apache.nifi.nar.NarClassLoaders;
import org.apache.nifi.nar.NarLoadResult;
import org.apache.nifi.nar.NarLoader;
import org.apache.nifi.nar.NarManifestEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads a set of NARs from the file system into the running application.
 *
 * NOTE: Initially this will only be used from the NarAutoLoader which is watching a directory for new files, but eventually
 * this may also be used for loading a NAR that was downloaded from the extension registry, and thus the load method
 * is synchronized to ensure only one set of NARs can be in process of loading at a given time.
 */
public class DocLessNarLoader implements NarLoader {

    private static Logger LOGGER = LoggerFactory.getLogger(DocLessNarLoader.class);

    private final File extensionsWorkingDir;
    private final NarClassLoaders narClassLoaders;
    private final ExtensionDiscoveringManager extensionManager;
    private final ExtensionMapping extensionMapping;

    private Set<BundleDetails> previouslySkippedBundles;

    public DocLessNarLoader(final File extensionsWorkingDir,
                             final NarClassLoaders narClassLoaders,
                             final ExtensionDiscoveringManager extensionManager,
                             final ExtensionMapping extensionMapping) {
        this.extensionsWorkingDir = extensionsWorkingDir;
        this.narClassLoaders = narClassLoaders;
        this.extensionManager = extensionManager;
        this.extensionMapping = extensionMapping;
    }

    @Override
    public synchronized NarLoadResult load(final Collection<File> narFiles) {
        LOGGER.info("Starting load process for {} NARs...", new Object[]{narFiles.size()});

        final List<File> unpackedNars = new ArrayList<>();

        for (final File narFile : narFiles) {
            LOGGER.debug("Unpacking {}...", new Object[]{narFile.getName()});
            final File unpackedNar = unpack(narFile);
            if (unpackedNar != null) {
                LOGGER.debug("Completed unpacking {}", new Object[]{narFile.getName()});
                unpackedNars.add(unpackedNar);
            }
        }

        if (previouslySkippedBundles != null && !previouslySkippedBundles.isEmpty()) {
            LOGGER.info("Including {} previously skipped bundle(s)", new Object[]{previouslySkippedBundles.size()});
            previouslySkippedBundles.forEach(b -> unpackedNars.add(b.getWorkingDirectory()));
        }

        if (unpackedNars.isEmpty()) {
            LOGGER.info("No NARs were unpacked, nothing to do");
            return new NarLoadResult(Collections.emptySet(), Collections.emptySet());
        }

        LOGGER.info("Creating class loaders for {} NARs...", new Object[]{unpackedNars.size()});

        final NarLoadResult narLoadResult = narClassLoaders.loadAdditionalNars(unpackedNars);
        final Set<Bundle> loadedBundles = narLoadResult.getLoadedBundles();
        final Set<BundleDetails> skippedBundles = narLoadResult.getSkippedBundles();

        LOGGER.info("Successfully created class loaders for {} NARs, {} were skipped",
                new Object[]{loadedBundles.size(), skippedBundles.size()});

        // Store skipped bundles for next iteration
        previouslySkippedBundles = new HashSet<>(skippedBundles);

        if (!loadedBundles.isEmpty()) {
            LOGGER.debug("Discovering extensions...");
            extensionManager.discoverExtensions(loadedBundles);

        }

        LOGGER.info("Finished NAR loading process!");
        return narLoadResult;
    }

    private File unpack(final File narFile) {
        try (final JarFile nar = new JarFile(narFile)) {
            final Manifest manifest = nar.getManifest();

            final Attributes attributes = manifest.getMainAttributes();
            final String groupId = attributes.getValue(NarManifestEntry.NAR_GROUP.getManifestName());
            final String narId = attributes.getValue(NarManifestEntry.NAR_ID.getManifestName());
            final String version = attributes.getValue(NarManifestEntry.NAR_VERSION.getManifestName());

            if (NarClassLoaders.FRAMEWORK_NAR_ID.equals(narId)) {
                LOGGER.error("Found a framework NAR, will not auto-load {}", new Object[]{narFile.getAbsolutePath()});
                return null;
            }

            final BundleCoordinate coordinate = new BundleCoordinate(groupId, narId, version);

            final Bundle bundle = extensionManager.getBundle(coordinate);
            if (bundle != null) {
                LOGGER.warn("Found existing bundle with coordinate {}, will not load {}",
                        new Object[]{coordinate, narFile.getAbsolutePath()});
                return null;
            }

            final File unpackedExtension = DocLessNarUnpacker.unpackNar(narFile, extensionsWorkingDir);
            DocLessNarUnpacker.mapExtension(unpackedExtension, coordinate, extensionMapping);
            return unpackedExtension;

        } catch (Exception e) {
            LOGGER.error("Error unpacking " + narFile.getAbsolutePath(), e);
            return null;
        }
    }

}
