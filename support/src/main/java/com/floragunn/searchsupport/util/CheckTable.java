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

package com.floragunn.searchsupport.util;

import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import com.google.common.base.Strings;

public interface CheckTable<R, C> {

    static <R, C> CheckTable<R, C> create(R row, Set<C> columns) {
        if (columns.size() == 1) {
            return new SingleCellCheckTable<R, C>(row, columns.iterator().next());
        } else {
            return new SingleRowCheckTable<R, C>(row, columns);
        }
    }

    static <R, C> CheckTable<R, C> create(Set<R> rows, C column) {
        if (rows.size() == 1) {
            return new SingleCellCheckTable<R, C>(rows.iterator().next(), column);
        } else {
            return new SingleColumnCheckTable<R, C>(rows, column);
        }
    }

    static <R, C> CheckTable<R, C> create(Set<R> rows, Set<C> columns) {
        if (rows.size() == 1) {
            if (columns.size() == 1) {
                return new SingleCellCheckTable<R, C>(rows.iterator().next(), columns.iterator().next());
            } else {
                return new SingleRowCheckTable<R, C>(rows.iterator().next(), columns);
            }
        } else if (columns.size() == 1) {
            return new SingleColumnCheckTable<R, C>(rows, columns.iterator().next());
        } else {
            return new ArrayCheckTable<>(rows, columns);
        }
    }

    boolean check(R row, C column);

    boolean checkIf(R row, Predicate<C> columnCheckPredicate);

    boolean checkIf(Iterable<R> rows, Predicate<C> columnCheckPredicate);

    boolean isChecked(R row, C column);

    boolean isComplete();

    String toString(String checkedIndicator, String uncheckedIndicator);

    String toTableString();

    String toTableString(String checkedIndicator, String uncheckedIndicator);

    static class SingleCellCheckTable<R, C> extends AbstractCheckTable<R, C> {
        private R row;
        private C column;
        private boolean checked = false;

        SingleCellCheckTable(R row, C column) {
            this.row = row;
            this.column = column;
        }

        @Override
        public boolean check(R row, C column) {
            if (!row.equals(this.row)) {
                throw new IllegalArgumentException("Invalid row: " + row);
            }

            if (!column.equals(this.column)) {
                throw new IllegalArgumentException("Invalid column: " + column);
            }

            if (!checked) {
                checked = true;
            }

            return checked;
        }

        @Override
        public boolean isComplete() {
            return checked;
        }

        @Override
        public boolean checkIf(R row, Predicate<C> columnCheckPredicate) {
            if (!row.equals(this.row)) {
                throw new IllegalArgumentException("Invalid row: " + row);
            }

            if (checked) {
                return true;
            }

            if (columnCheckPredicate.test(column)) {
                checked = true;
            }

            return checked;
        }

        @Override
        public boolean isChecked(R row, C column) {
            if (!row.equals(this.row)) {
                throw new IllegalArgumentException("Invalid row: " + row);
            }

            if (!column.equals(this.column)) {
                throw new IllegalArgumentException("Invalid column: " + column);
            }

            return checked;
        }

        @Override
        public String toString(String checkedIndicator, String uncheckedIndicator) {
            return row + "/" + column + ": " + (checked ? checkedIndicator : uncheckedIndicator);
        }

        @Override
        public String toTableString(String checkedIndicator, String uncheckedIndicator) {
            StringBuilder result = new StringBuilder();

            int rowHeaderWidth = row.toString().length() + 1;

            result.append(Strings.padEnd("", rowHeaderWidth, ' '));
            result.append("|");

            String columnLabel = column.toString();
            if (columnLabel.length() > STRING_TABLE_HEADER_WIDTH) {
                columnLabel = columnLabel.substring(0, STRING_TABLE_HEADER_WIDTH);
            }
            int columnWidth = columnLabel.length();

            result.append(" ").append(columnLabel).append(" |");

            result.append("\n");

            result.append(row.toString());
            result.append(" |");

            String v = checked ? checkedIndicator : uncheckedIndicator;

            result.append(" ").append(Strings.padEnd(v, columnWidth, ' ')).append(" |");

            return result.toString();
        }

    }

    static class SingleRowCheckTable<R, C> extends AbstractCheckTable<R, C> {
        private R row;
        private Set<C> columns;
        private ImmutableSet.Builder<C> unchecked;
        private int uncheckedCount;

        SingleRowCheckTable(R row, Set<C> columns) {
            this.row = row;
            this.columns = columns;
            this.unchecked = new ImmutableSet.Builder<>(this.columns);
            this.uncheckedCount = this.columns.size();
        }

        @Override
        public boolean check(R row, C column) {
            if (!row.equals(this.row)) {
                throw new IllegalArgumentException("Invalid row: " + row);
            }

            if (!columns.contains(column)) {
                throw new IllegalArgumentException("Invalid column: " + column);
            }

            if (this.unchecked.contains(column)) {
                this.unchecked.remove(column);
                this.uncheckedCount--;
            }

            return this.uncheckedCount == 0;
        }

        @Override
        public boolean isComplete() {
            return this.uncheckedCount == 0;
        }

        @Override
        public boolean checkIf(R row, Predicate<C> columnCheckPredicate) {
            if (!row.equals(this.row)) {
                throw new IllegalArgumentException("Invalid row: " + row);
            }

            Iterator<C> iter = this.unchecked.iterator();

            while (iter.hasNext()) {
                if (columnCheckPredicate.test(iter.next())) {
                    iter.remove();
                    this.uncheckedCount--;
                }
            }

            return this.uncheckedCount == 0;
        }

        @Override
        public boolean isChecked(R row, C column) {
            if (!row.equals(this.row)) {
                throw new IllegalArgumentException("Invalid row: " + row);
            }

            if (!columns.contains(column)) {
                throw new IllegalArgumentException("Invalid column: " + column);
            }

            return !this.unchecked.contains(column);
        }

        @Override
        public String toTableString(String checkedIndicator, String uncheckedIndicator) {
            StringBuilder result = new StringBuilder();

            int rowHeaderWidth = row.toString().length() + 1;

            result.append(Strings.padEnd("", rowHeaderWidth, ' '));
            result.append("|");

            int[] columnWidth = new int[this.columns.size()];

            int i = 0;
            for (C column : columns) {
                String columnLabel = column.toString();

                if (columnLabel.length() > STRING_TABLE_HEADER_WIDTH) {
                    columnLabel = columnLabel.substring(0, STRING_TABLE_HEADER_WIDTH);
                }

                columnWidth[i] = columnLabel.length();
                i++;
                result.append(" ").append(columnLabel).append(" |");
            }

            result.append("\n");

            result.append(row.toString());
            result.append(" |");

            i = 0;
            for (C column : columns) {
                String v = this.unchecked.contains(column) ? uncheckedIndicator : checkedIndicator;

                result.append(" ").append(Strings.padEnd(v, columnWidth[i], ' ')).append(" |");
                i++;
            }

            return result.toString();
        }

        @Override
        public String toString(String checkedIndicator, String uncheckedIndicator) {
            return toTableString(checkedIndicator, uncheckedIndicator);
        }

    }

    static class SingleColumnCheckTable<R, C> extends AbstractCheckTable<R, C> {
        private C column;
        private Set<R> rows;
        private ImmutableSet.Builder<R> unchecked;
        private int uncheckedCount;

        SingleColumnCheckTable(Set<R> rows, C column) {
            this.column = column;
            this.rows = rows;
            this.unchecked = new ImmutableSet.Builder<>(this.rows);
            this.uncheckedCount = this.rows.size();
        }

        @Override
        public boolean check(R row, C column) {
            if (!column.equals(this.column)) {
                throw new IllegalArgumentException("Invalid column: " + column);
            }

            if (!rows.contains(row)) {
                throw new IllegalArgumentException("Invalid row: " + row);
            }

            if (this.unchecked.contains(row)) {
                this.unchecked.remove(row);
                this.uncheckedCount--;
            }

            return this.uncheckedCount == 0;
        }

        @Override
        public boolean checkIf(R row, Predicate<C> columnCheckPredicate) {
            if (!rows.contains(row)) {
                throw new IllegalArgumentException("Invalid row: " + row);
            }

            if (unchecked.contains(row)) {
                if (columnCheckPredicate.test(column)) {
                    this.unchecked.remove(row);
                    this.uncheckedCount--;
                }
            }

            return this.uncheckedCount == 0;
        }

        @Override
        public boolean isChecked(R row, C column) {
            if (!column.equals(this.column)) {
                throw new IllegalArgumentException("Invalid column: " + column);
            }

            if (!rows.contains(row)) {
                throw new IllegalArgumentException("Invalid row: " + row);
            }

            return !this.unchecked.contains(row);
        }

        @Override
        public String toTableString(String checkedIndicator, String uncheckedIndicator) {
            StringBuilder result = new StringBuilder();

            int rowHeaderWidth = rows.stream().map((r) -> r.toString().length()).max(Comparator.naturalOrder()).get();

            result.append(Strings.padEnd("", rowHeaderWidth, ' '));
            result.append("|");

            String columnLabel = column.toString();
            int columnWidth = columnLabel.length();
            result.append(" ").append(columnLabel).append(" |");
            result.append("\n");

            for (R row : rows) {

                result.append(Strings.padEnd(row.toString(), rowHeaderWidth, ' '));
                result.append("|");
                String v = this.unchecked.contains(row) ? uncheckedIndicator : checkedIndicator;

                result.append(" ").append(Strings.padEnd(v, columnWidth, ' ')).append(" |\n");
            }

            return result.toString();
        }

        @Override
        public boolean isComplete() {
            return this.uncheckedCount == 0;
        }

    }

    static class ArrayCheckTable<R, C> extends AbstractCheckTable<R, C> {
        private Map<R, Integer> rows;
        private Map<C, Integer> columns;
        private boolean[][] table;
        private int checkedCount = 0;
        private int uncheckedCount;

        ArrayCheckTable(Set<R> rows, Set<C> columns) {
            this.rows = createIndexMap(rows);
            this.columns = createIndexMap(columns);
            this.table = new boolean[this.rows.size()][this.columns.size()];
            this.uncheckedCount = this.rows.size() * this.columns.size();
        }

        @Override
        public boolean check(R row, C column) {

            Integer rowIndex = rows.get(row);

            if (rowIndex == null) {
                throw new IllegalArgumentException("Invalid row: " + row);
            }

            Integer columnIndex = columns.get(column);

            if (columnIndex == null) {
                throw new IllegalArgumentException("Invalid column: " + column);
            }

            if (!this.table[rowIndex][columnIndex]) {
                this.table[rowIndex][columnIndex] = true;
                this.checkedCount++;
                this.uncheckedCount--;
            }

            return this.uncheckedCount == 0;
        }

        @Override
        public boolean isComplete() {
            return this.uncheckedCount == 0;
        }

        @Override
        public boolean isChecked(R row, C column) {
            Integer rowIndex = rows.get(row);

            if (rowIndex == null) {
                throw new IllegalArgumentException("Invalid row: " + row);
            }

            Integer columnIndex = columns.get(column);

            if (columnIndex == null) {
                throw new IllegalArgumentException("Invalid column: " + column);
            }

            return this.table[rowIndex][columnIndex];
        }

        @Override
        public boolean checkIf(R row, Predicate<C> columnCheckPredicate) {
            Integer rowIndex = rows.get(row);

            if (rowIndex == null) {
                throw new IllegalArgumentException("Invalid row: " + row);
            }

            for (Map.Entry<C, Integer> entry : columns.entrySet()) {
                if (!this.table[rowIndex][entry.getValue()]) {
                    if (columnCheckPredicate.test(entry.getKey())) {
                        this.table[rowIndex][entry.getValue()] = true;
                        this.checkedCount++;
                        this.uncheckedCount--;

                        if (this.uncheckedCount == 0) {
                            return true;
                        }
                    }
                }
            }

            return this.uncheckedCount == 0;
        }

        @Override
        public String toTableString(String checkedIndicator, String uncheckedIndicator) {
            StringBuilder result = new StringBuilder();

            int rowHeaderWidth = rows.keySet().stream().map((r) -> r.toString().length()).max(Comparator.naturalOrder()).get();

            result.append(Strings.padEnd("", rowHeaderWidth, ' '));
            result.append("|");

            int[] columnWidth = new int[this.columns.size()];

            int i = 0;
            for (C column : columns.keySet()) {
                String columnLabel = column.toString();

                if (columnLabel.length() > STRING_TABLE_HEADER_WIDTH) {
                    columnLabel = columnLabel.substring(0, STRING_TABLE_HEADER_WIDTH);
                }

                columnWidth[i] = columnLabel.length();
                i++;
                result.append(" ").append(columnLabel).append(" |");
            }

            result.append("\n");

            for (R row : rows.keySet()) {

                result.append(Strings.padEnd(row.toString(), rowHeaderWidth, ' '));
                result.append("|");

                i = 0;
                for (C column : columns.keySet()) {

                    String v = isChecked(row, column) ? checkedIndicator : uncheckedIndicator;

                    result.append(" ").append(Strings.padEnd(v, columnWidth[i], ' ')).append(" |\n");
                    i++;
                }
                result.append("\n");

            }

            return result.toString();
        }

    }

    static abstract class AbstractCheckTable<R, C> implements CheckTable<R, C> {

        static final int STRING_TABLE_HEADER_WIDTH = 40;

        @Override
        public boolean checkIf(Iterable<R> rows, Predicate<C> columnCheckPredicate) {
            Iterator<R> iter = rows.iterator();

            while (iter.hasNext()) {
                R row = iter.next();

                if (checkIf(row, columnCheckPredicate)) {
                    return true;
                }
            }

            return false;
        }

        @Override
        public String toString() {
            return toString("x", "");
        }

        @Override
        public String toTableString() {
            return toTableString("x", "");
        }

        @Override
        public String toString(String checkedIndicator, String uncheckedIndicator) {
            return toTableString(checkedIndicator, uncheckedIndicator);
        }

        static <T> Map<T, Integer> createIndexMap(Set<T> set) {
            int size = set.size();

            if (size == 2) {
                Iterator<T> iter = set.iterator();
                return new ImmutableMap.TwoElementMap<>(iter.next(), 0, iter.next(), 1);
            } else {
                int i = 0;
                Map<T, Integer> result = new LinkedHashMap<>(set.size());

                for (T e : set) {
                    result.put(e, i);
                    i++;
                }

                return result;
            }
        }

    }

}
