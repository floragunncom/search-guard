/*
 * Copyright 2021 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.floragunn.searchsupport.json;

import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.documents.DocWriter;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.UnexpectedDocumentStructureException;
import com.google.common.base.Charsets;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class YamlRewriter {
    private static final Logger log = LogManager.getLogger(YamlRewriter.class);

    private String source;
    private String sourceFileName;

    private List<Insertion> insertionsAtBeginning = new ArrayList<>();
    private Map<String, List<Insertion>> insertionsAfter = new LinkedHashMap<>();
    private Map<String, List<Insertion>> insertionsBefore = new LinkedHashMap<>();
    private List<String> removals = new ArrayList<>();

    public YamlRewriter(File sourceFile) throws IOException {
        this.source = new String(Files.readAllBytes(sourceFile.toPath()), Charsets.UTF_8);
        this.sourceFileName = sourceFile.getName();
    }

    public void insertAtBeginning(Insertion insertion) {
        insertionsAtBeginning.add(insertion);
    }

    public void insertAfter(String existingAttribute, Insertion insertion) {
        insertionsAfter.computeIfAbsent(existingAttribute, (k) -> new ArrayList<>()).add(insertion);
    }

    public void insertBefore(String existingAttribute, Insertion insertion) {
        insertionsBefore.computeIfAbsent(existingAttribute, (k) -> new ArrayList<>()).add(insertion);
    }

    public void remove(String existingAttribute) {
        removals.add(existingAttribute);
    }

    public RewriteResult rewrite() throws RewriteException {

        if (insertionsAtBeginning.isEmpty() && insertionsAfter.isEmpty() && insertionsBefore.isEmpty() && removals.isEmpty()) {
            return new RewriteResult(source, false);
        }

        String current = source;

        for (Insertion insertion : insertionsAtBeginning) {
            current = insertion + "\n" + current;
        }

        for (Map.Entry<String, List<Insertion>> entry : insertionsAfter.entrySet()) {
            String attribute = entry.getKey();
            List<Insertion> insertions = entry.getValue();
            Pattern attributePattern = Pattern.compile("^" + Pattern.quote(attribute) + "\\s*:\\s*[^\\s]+.*$");

            Collections.reverse(insertions);

            for (Insertion insertion : insertions) {
                Matcher matcher = attributePattern.matcher(current);

                if (matcher.find()) {
                    int end = matcher.end();

                    current = current.substring(0, end) + "\n" + insertion + current.substring(end);
                } else {
                    current = current + "\n" + insertion;
                }
            }
        }

        for (Map.Entry<String, List<Insertion>> entry : insertionsBefore.entrySet()) {
            String attribute = entry.getKey();
            List<Insertion> insertions = entry.getValue();
            Pattern attributePattern = Pattern.compile("^" + Pattern.quote(attribute) + "\\s*:\\s*[^\\s]+.*$");

            for (Insertion insertion : insertions) {
                Matcher matcher = attributePattern.matcher(current);

                if (matcher.find()) {
                    int start = matcher.start();

                    current = current.substring(0, start) + insertion + "\n" + current.substring(start);
                } else {
                    current = current + "\n" + insertion;
                }
            }
        }

        for (String removal : removals) {
            Pattern attributePattern = Pattern.compile("^" + Pattern.quote(removal) + "\\s*:\\s*[^\\s]+.*$", Pattern.MULTILINE);
            Matcher matcher = attributePattern.matcher(current);

            if (matcher.find()) {
                current = current.substring(0, matcher.start()) + current.substring(matcher.end());
            }
        }

        try {
            boolean changed = verify(current);

            return new RewriteResult(current, changed);
        } catch (VerificationException e) {
            e.printStackTrace();
            log.debug("Verification failed", e);

            throw new RewriteException(e);
        }

    }

    public String getManualInstructions() {
        StringBuilder insertions = new StringBuilder();

        for (Insertion insertion : insertionsAtBeginning) {
            insertions.append(insertion).append("\n");
        }

        insertions.append("\n");

        for (Map.Entry<String, List<Insertion>> entry : insertionsAfter.entrySet()) {
            for (Insertion insertion : entry.getValue()) {
                insertions.append(insertion).append("\n");
            }

            insertions.append("\n");
        }

        for (Map.Entry<String, List<Insertion>> entry : insertionsBefore.entrySet()) {
            for (Insertion insertion : entry.getValue()) {
                insertions.append(insertion).append("\n");
            }

            insertions.append("\n");
        }

        StringBuilder removals = new StringBuilder();

        for (String removal : this.removals) {
            removals.append(removal).append("\n");
        }

        StringBuilder result = new StringBuilder(
                "The file " + sourceFileName + " cannot be automatically updated. Please perform the following modifications manually:\n\n");

        if (insertions.length() > 0) {
            result.append("Please insert these attributes:\n\n").append(insertions).append("\n");
        }

        if (removals.length() > 0) {
            result.append("Please remove these attributes:\n\n").append(removals).append("\n");
        }

        return result.toString();
    }

    private boolean verify(String result) throws VerificationException {
        Map<String, Object> tree1 = applyChangesToTree();
        Map<String, Object> tree2;
        try {
            tree2 = DocReader.yaml().readObject(result);
        } catch (Exception e) {
            throw new VerificationException("Error while parsing rewritten file", e);
        }

        ensureEquality(tree1, tree2);

        Map<String, Object> originalTree;

        try {
            originalTree = DocReader.yaml().readObject(source);
        } catch (Exception e) {
            throw new VerificationException("Error while parsing source file", e);
        }

        return !areEqual(tree2, originalTree);
    }

    private void ensureEquality(Map<?, ?> tree1, Map<?, ?> tree2) throws VerificationException {
        if (tree1.size() != tree2.size()) {
            throw new VerificationException("Trees do not match:\n" + tree1 + "\n" + tree2);
        }

        for (Object key : tree1.keySet()) {
            Object object1 = tree1.get(key);
            Object object2 = tree2.get(key);

            if (object1 == null && object2 == null) {
                continue;
            }

            if (object1 == null || object2 == null) {
                throw new VerificationException("Trees do not match at " + key + ":\n" + tree1 + "\n" + tree2);
            }

            if (object1 instanceof Map) {
                if (object2 instanceof Map) {
                    ensureEquality((Map<?, ?>) object1, (Map<?, ?>) object2);
                } else {
                    throw new VerificationException("Trees do not match at " + key + ":\n" + tree1 + "\n" + tree2);
                }
            } else if (!object1.equals(object2)) {
                throw new VerificationException("Trees do not match at " + key + ":\n" + tree1 + "\n" + tree2);
            }
        }
    }

    private boolean areEqual(Map<?, ?> tree1, Map<?, ?> tree2) {
        if (tree1.size() != tree2.size()) {
            return false;
        }

        for (Object key : tree1.keySet()) {
            Object object1 = tree1.get(key);
            Object object2 = tree2.get(key);

            if (object1 == null && object2 == null) {
                continue;
            }

            if (object1 == null || object2 == null) {
                return false;
            }

            if (object1 instanceof Map) {
                if (object2 instanceof Map) {
                    areEqual((Map<?, ?>) object1, (Map<?, ?>) object2);
                } else {
                    return false;
                }
            } else if (!object1.equals(object2)) {
                return false;
            }
        }

        return true;
    }

    private Map<String, Object> applyChangesToTree() throws VerificationException {
        try {
            Map<String, Object> tree = DocReader.yaml().readObject(source);

            applyChangesToTree(insertionsAtBeginning, tree);

            for (Map.Entry<String, List<Insertion>> entry : insertionsAfter.entrySet()) {
                applyChangesToTree(entry.getValue(), tree);
            }

            for (Map.Entry<String, List<Insertion>> entry : insertionsBefore.entrySet()) {
                applyChangesToTree(entry.getValue(), tree);
            }

            for (String removal : removals) {
                remove(removal, tree);
            }

            return tree;

        } catch (DocumentParseException | UnexpectedDocumentStructureException e) {
            throw new VerificationException("Error while parsing source file", e);
        }
    }

    private void applyChangesToTree(List<Insertion> insertions, Map<String, Object> tree) throws VerificationException {
        for (Insertion insertion : insertions) {
            if (insertion instanceof Attribute) {
                set((Attribute) insertion, tree);
            }
        }
    }

    private void set(Attribute attributeInsertion, Map<String, Object> tree) throws VerificationException {
        String[] path = attributeInsertion.key.split("\\.");
        Map<String, Object> parent = getParent(path, tree);
        parent.put(path[path.length - 1], attributeInsertion.value);
    }

    private void remove(String key, Map<String, Object> tree) throws VerificationException {
        tree.remove(key);

        String[] path = key.split("\\.");
        Map<String, Object> parent = getParent(path, tree);
        parent.remove(path[path.length - 1]);

        if (parent.isEmpty() && path.length > 1) {
            remove(key.substring(0, key.lastIndexOf('.')), tree);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getParent(String[] path, Map<String, Object> tree) throws VerificationException {
        if (path.length == 1) {
            return tree;
        }

        for (int i = 0; i < path.length - 1; i++) {
            Object element = tree.get(path[i]);

            if (element instanceof Map) {
                tree = (Map<String, Object>) element;
            } else if (element == null) {
                Map<String, Object> newContainer = new LinkedHashMap<>();
                tree.put(path[i], newContainer);
                tree = newContainer;
            } else {
                throw new VerificationException("Part of path is not a map: " + element + "; " + Arrays.asList(path));
            }
        }

        return tree;
    }

    public static abstract class Insertion {

    }

    public static class Comment extends Insertion {
        private final String comment;

        public Comment(String comment) {
            this.comment = comment;
        }

        public String toString() {
            return "# " + comment.replaceAll("\n", "\n# ");
        }
    }

    public static class Attribute extends Insertion {
        private final String key;
        private final Object value;

        public Attribute(String key, Object value) {
            this.key = key;
            this.value = value;
        }

        public String toString() {
            return key + ": " + DocWriter.json().writeAsString(value);
        }
    }

    public static class RewriteResult {
        private final String yaml;
        private final boolean changed;

        RewriteResult(String yaml, boolean changed) {
            this.yaml = yaml;
            this.changed = changed;
        }

        public String getYaml() {
            return yaml;
        }

        public boolean isChanged() {
            return changed;
        }
    }

    static class VerificationException extends Exception {
        private static final long serialVersionUID = 180827181611113288L;

        private VerificationException(String message, Throwable cause) {
            super(message, cause);
        }

        private VerificationException(String message) {
            super(message);
        }

        private VerificationException(Throwable cause) {
            super(cause);
        }

    }

    public class RewriteException extends Exception {

        private static final long serialVersionUID = 4693406547037756777L;

        private RewriteException() {
            super();
        }

        private RewriteException(String message, Throwable cause) {
            super(message, cause);
        }

        private RewriteException(String message) {
            super(message);
        }

        private RewriteException(Throwable cause) {
            super(cause);
        }

        public String getManualInstructions() {
            return YamlRewriter.this.getManualInstructions();
        }

    }
}
