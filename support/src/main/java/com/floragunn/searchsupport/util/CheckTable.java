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

import org.bouncycastle.util.Arrays;

import com.google.common.base.Strings;

public interface CheckTable<R, C> {

    static <R, C> CheckTable<R, C> create(R row, Set<C> columns) {
        if (columns.size() == 0) {
            throw new RuntimeException("Trying to create empty CheckTable: " + columns);
        } else if (columns.size() == 1) {
            return new SingleCellCheckTable<R, C>(row, columns.iterator().next(), ImmutableSet.of(row), ImmutableSet.of(columns));
        } else {
            return new SingleRowCheckTable<R, C>(row, columns);
        }
    }

    static <R, C> CheckTable<R, C> create(Set<R> rows, C column) {
        if (rows.size() == 0) {
            throw new RuntimeException("Trying to create empty CheckTable: " + rows);
        } else if (rows.size() == 1) {
            return new SingleCellCheckTable<R, C>(rows.iterator().next(), column, ImmutableSet.of(rows), ImmutableSet.of(column));
        } else {
            return new SingleColumnCheckTable<R, C>(rows, column);
        }
    }

    static <R, C> CheckTable<R, C> create(Set<R> rows, Set<C> columns) {
        if (rows.size() == 0 || columns.size() == 0) {
            throw new RuntimeException("Trying to create empty CheckTable: " + rows + " " + columns);
        } else if (rows.size() == 1) {
            if (columns.size() == 1) {
                return new SingleCellCheckTable<R, C>(rows.iterator().next(), columns.iterator().next(), ImmutableSet.of(rows),
                        ImmutableSet.of(columns));
            } else {
                return new SingleRowCheckTable<R, C>(rows.iterator().next(), columns);
            }
        } else if (columns.size() == 1) {
            return new SingleColumnCheckTable<R, C>(rows, columns.iterator().next());
        } else if (columns.size() == 2) {
            Iterator<C> iter = columns.iterator();
            return new TwoColumnCheckTable<R, C>(rows, iter.next(), iter.next());
        } else {
            return new ArrayCheckTable<>(rows, columns);
        }
    }

    @SuppressWarnings("unchecked")
    static <R, C> CheckTable<R, C> empty() {
        return (CheckTable<R, C>) EMPTY;
    }

    boolean check(R row, C column);

    boolean checkIf(R row, Predicate<C> columnCheckPredicate);

    boolean checkIf(Iterable<R> rows, Predicate<C> columnCheckPredicate);

    void uncheckIf(R row, Predicate<C> columnCheckPredicate);

    void uncheckIf(Iterable<R> rows, Predicate<C> columnCheckPredicate);

    void uncheckIf(Predicate<R> rowCheckPredicate, C column);

    void uncheckIf(Predicate<R> rowCheckPredicate, Iterable<C> columns);

    void uncheckRowIf(Predicate<R> rowCheckPredicate);

    void uncheckRow(R row);

    void uncheckRowIfPresent(R row);

    void uncheckAll();

    boolean isChecked(R row, C column);

    boolean isComplete();

    boolean isBlank();

    String toString(String checkedIndicator, String uncheckedIndicator);

    String toTableString();

    String toTableString(String checkedIndicator, String uncheckedIndicator);

    CheckTable<R, C> getViewForMatchingColumns(Predicate<C> predicate);

    ImmutableSet<R> getRows();

    ImmutableSet<C> getColumns();

    ImmutableSet<R> getCompleteRows();

    ImmutableSet<C> getCompleteColumns();

    boolean isEmpty();

    static class SingleCellCheckTable<R, C> extends AbstractCheckTable<R, C> {
        private final R row;
        private final C column;
        private final ImmutableSet<R> rowSet;
        private final ImmutableSet<C> columnSet;
        private boolean checked = false;

        SingleCellCheckTable(R row, C column, ImmutableSet<R> rowSet, ImmutableSet<C> columnSet) {
            this.row = row;
            this.column = column;
            this.rowSet = rowSet;
            this.columnSet = columnSet;
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
        public boolean isBlank() {
            return !checked;
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
        public void uncheckIf(R row, Predicate<C> columnCheckPredicate) {
            if (!row.equals(this.row)) {
                throw new IllegalArgumentException("Invalid row: " + row);
            }

            if (checked && columnCheckPredicate.test(column)) {
                checked = false;
            }
        }

        @Override
        public void uncheckIf(Predicate<R> rowCheckPredicate, C column) {
            if (!column.equals(this.column)) {
                throw new IllegalArgumentException("Invalid column: " + column);
            }

            if (checked && rowCheckPredicate.test(row)) {
                checked = false;
            }
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

        @Override
        public ImmutableSet<R> getCompleteRows() {
            if (checked) {
                return rowSet;
            } else {
                return ImmutableSet.empty();
            }
        }

        @Override
        public ImmutableSet<C> getCompleteColumns() {
            if (checked) {
                return columnSet;
            } else {
                return ImmutableSet.empty();
            }
        }

        public ImmutableSet<R> getCheckedRows(C column) {
            return getCompleteRows();
        }

        public ImmutableSet<C> getCheckedColumns(R row) {
            return getCompleteColumns();
        }

        @Override
        public CheckTable<R, C> getViewForMatchingColumns(Predicate<C> predicate) {
            if (predicate.test(column)) {
                return this;
            } else {
                return empty();
            }
        }

        @Override
        public ImmutableSet<R> getRows() {
            return rowSet;
        }

        @Override
        public ImmutableSet<C> getColumns() {
            return columnSet;
        }

        @Override
        public void uncheckAll() {
            checked = false;
        }

        @Override
        public void uncheckRowIf(Predicate<R> rowCheckPredicate) {
            if (rowCheckPredicate.test(row)) {
                checked = false;
            }
        }

        @Override
        public void uncheckRow(R row) {
            if (this.row.equals(row)) {
                checked = false;
            } else {
                throw new IllegalArgumentException("Invalid row: " + row);
            }
        }

        @Override
        public void uncheckRowIfPresent(R row) {
            if (this.row.equals(row)) {
                checked = false;
            }
        }

    }

    static class SingleRowCheckTable<R, C> extends AbstractCheckTable<R, C> {
        private final R row;
        private final CheckList<C> columns;

        SingleRowCheckTable(R row, Set<C> columns) {
            this.row = row;
            this.columns = CheckList.create(columns, "column");
        }

        private SingleRowCheckTable(R row, CheckList<C> columns) {
            this.row = row;
            this.columns = columns;
        }

        @Override
        public boolean check(R row, C column) {
            if (!row.equals(this.row)) {
                throw new IllegalArgumentException("Invalid row: " + row);
            }

            return this.columns.check(column);
        }

        @Override
        public boolean isComplete() {
            return this.columns.isComplete();
        }

        @Override
        public boolean isBlank() {
            return this.columns.isBlank();
        }

        @Override
        public boolean checkIf(R row, Predicate<C> columnCheckPredicate) {
            if (!row.equals(this.row)) {
                throw new IllegalArgumentException("Invalid row: " + row);
            }

            return columns.checkIf(columnCheckPredicate);
        }

        @Override
        public void uncheckIf(R row, Predicate<C> columnCheckPredicate) {
            if (!row.equals(this.row)) {
                throw new IllegalArgumentException("Invalid row: " + row);
            }

            columns.uncheckIf(columnCheckPredicate);
        }

        @Override
        public void uncheckIf(Predicate<R> rowCheckPredicate, C column) {
            if (rowCheckPredicate.test(row)) {
                columns.uncheck(column);
            }
        }

        @Override
        public boolean isChecked(R row, C column) {
            if (!row.equals(this.row)) {
                throw new IllegalArgumentException("Invalid row: " + row);
            }

            return columns.isChecked(column);
        }

        @Override
        public String toTableString(String checkedIndicator, String uncheckedIndicator) {
            StringBuilder result = new StringBuilder();

            int rowHeaderWidth = row.toString().length() + 1;

            result.append(Strings.padEnd("", rowHeaderWidth, ' '));
            result.append("|");

            int[] columnWidth = new int[this.columns.size()];

            int i = 0;
            for (C column : columns.getElements()) {
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
            for (C column : columns.getElements()) {
                String v = columns.isChecked(column) ? checkedIndicator : uncheckedIndicator;

                result.append(" ").append(Strings.padEnd(v, columnWidth[i], ' ')).append(" |");
                i++;
            }

            return result.toString();
        }

        @Override
        public String toString(String checkedIndicator, String uncheckedIndicator) {
            return toTableString(checkedIndicator, uncheckedIndicator);
        }

        @Override
        public ImmutableSet<R> getCompleteRows() {
            if (isComplete()) {
                return ImmutableSet.of(row);
            } else {
                return ImmutableSet.empty();
            }
        }

        @Override
        public ImmutableSet<C> getCompleteColumns() {
            return columns.getCheckedElements();
        }

        @Override
        public CheckTable<R, C> getViewForMatchingColumns(Predicate<C> predicate) {
            ImmutableSet<C> matchingColumns = columns.getElements().matching(predicate);

            if (matchingColumns.isEmpty()) {
                return empty();
            } else if (matchingColumns.size() == columns.size()) {
                return this;
            } else {
                return new SingleRowCheckTable<>(row, columns.getView(matchingColumns));
            }
        }

        @Override
        public ImmutableSet<R> getRows() {
            return ImmutableSet.of(row);
        }

        @Override
        public ImmutableSet<C> getColumns() {
            return columns.getElements();
        }

        @Override
        public void uncheckAll() {
            columns.uncheckAll();
        }

        @Override
        public void uncheckRowIf(Predicate<R> rowCheckPredicate) {
            if (rowCheckPredicate.test(row)) {
                columns.uncheckAll();
            }
        }

        @Override
        public void uncheckRow(R row) {
            if (this.row.equals(row)) {
                columns.uncheckAll();
            } else {
                throw new IllegalArgumentException("Invalid row: " + row);
            }
        }

        @Override
        public void uncheckRowIfPresent(R row) {
            if (this.row.equals(row)) {
                columns.uncheckAll();
            }
        }

    }

    static class SingleColumnCheckTable<R, C> extends AbstractCheckTable<R, C> {
        private C column;
        private CheckList<R> rows;

        SingleColumnCheckTable(Set<R> rows, C column) {
            this.column = column;
            this.rows = CheckList.create(rows, "row");
        }

        private SingleColumnCheckTable(CheckList<R> rows, C column) {
            this.rows = rows;
            this.column = column;
        }

        @Override
        public boolean check(R row, C column) {
            if (!column.equals(this.column)) {
                throw new IllegalArgumentException("Invalid column: " + column);
            }

            return rows.check(row);
        }

        @Override
        public boolean checkIf(R row, Predicate<C> columnCheckPredicate) {
            if (columnCheckPredicate.test(column)) {
                return rows.check(row);
            } else {
                return isComplete();
            }
        }

        @Override
        public void uncheckIf(R row, Predicate<C> columnCheckPredicate) {
            if (columnCheckPredicate.test(column)) {
                rows.uncheck(row);
            }
        }

        @Override
        public void uncheckIf(Predicate<R> rowCheckPredicate, C column) {
            if (!column.equals(this.column)) {
                throw new IllegalArgumentException("Invalid column: " + column);
            }

            rows.uncheckIf(rowCheckPredicate);
        }

        @Override
        public boolean isChecked(R row, C column) {
            if (!column.equals(this.column)) {
                throw new IllegalArgumentException("Invalid column: " + column);
            }

            return rows.isChecked(row);
        }

        @Override
        public String toTableString(String checkedIndicator, String uncheckedIndicator) {
            StringBuilder result = new StringBuilder();

            int rowHeaderWidth = rows.getElements().stream().map((r) -> r.toString().length()).max(Comparator.naturalOrder()).get();

            result.append(Strings.padEnd("", rowHeaderWidth, ' '));
            result.append("|");

            String columnLabel = column.toString();
            int columnWidth = columnLabel.length();
            result.append(" ").append(columnLabel).append(" |");
            result.append("\n");

            for (R row : rows.getElements()) {

                result.append(Strings.padEnd(row.toString(), rowHeaderWidth, ' '));
                result.append("|");
                String v = this.rows.isChecked(row) ? checkedIndicator : uncheckedIndicator;

                result.append(" ").append(Strings.padEnd(v, columnWidth, ' ')).append(" |\n");
            }

            return result.toString();
        }

        @Override
        public boolean isComplete() {
            return this.rows.isComplete();
        }

        @Override
        public boolean isBlank() {
            return this.rows.isBlank();
        }

        @Override
        public ImmutableSet<R> getCompleteRows() {
            return this.rows.getCheckedElements();
        }

        @Override
        public ImmutableSet<C> getCompleteColumns() {
            if (isComplete()) {
                return ImmutableSet.of(column);
            } else {
                return ImmutableSet.empty();
            }
        }

        @Override
        public CheckTable<R, C> getViewForMatchingColumns(Predicate<C> predicate) {
            if (predicate.test(column)) {
                return this;
            } else {
                return empty();
            }
        }

        @Override
        public ImmutableSet<R> getRows() {
            return this.rows.getElements();
        }

        @Override
        public ImmutableSet<C> getColumns() {
            return ImmutableSet.of(column);
        }

        @Override
        public void uncheckAll() {
            rows.uncheckAll();
        }

        @Override
        public void uncheckRowIf(Predicate<R> rowCheckPredicate) {
            this.rows.uncheckIf(rowCheckPredicate);
        }

        @Override
        public void uncheckRow(R row) {
            this.rows.uncheck(row);
        }

        @Override
        public void uncheckRowIfPresent(R row) {
            this.rows.uncheckIfPresent(row);
        }

    }

    static class TwoColumnCheckTable<R, C> extends AbstractCheckTable<R, C> {
        private final C column1;
        private final C column2;
        private final CheckList<R> rows1;
        private final CheckList<R> rows2;

        TwoColumnCheckTable(Set<R> rows, C column1, C column2) {
            this.column1 = column1;
            this.column2 = column2;
            this.rows1 = CheckList.create(rows, "row");
            this.rows2 = CheckList.create(rows, "row");
        }

        @Override
        public boolean check(R row, C column) {
            if (column.equals(this.column1)) {
                if (rows1.check(row)) {
                    return isComplete();
                } else {
                    return false;
                }
            } else if (column.equals(this.column2)) {
                if (rows2.check(row)) {
                    return isComplete();
                } else {
                    return false;
                }
            } else {
                throw new IllegalArgumentException("Invalid column: " + column);
            }
        }

        @Override
        public boolean checkIf(R row, Predicate<C> columnCheckPredicate) {
            if (!rows1.isChecked(row) && columnCheckPredicate.test(column1)) {
                rows1.check(row);
            }

            if (!rows2.isChecked(row) && columnCheckPredicate.test(column2)) {
                rows2.check(row);
            }

            return isComplete();
        }

        @Override
        public void uncheckIf(R row, Predicate<C> columnCheckPredicate) {
            if (rows1.isChecked(row) && columnCheckPredicate.test(column1)) {
                rows1.uncheck(row);
            }

            if (rows2.isChecked(row) && columnCheckPredicate.test(column2)) {
                rows2.uncheck(row);
            }
        }

        @Override
        public void uncheckIf(Predicate<R> rowCheckPredicate, C column) {
            if (column.equals(column1)) {
                rows1.uncheckIf(rowCheckPredicate);
            } else if (column.equals(column2)) {
                rows2.uncheckIf(rowCheckPredicate);
            } else {
                throw new IllegalArgumentException("Invalid column: " + column);
            }
        }

        @Override
        public boolean isChecked(R row, C column) {
            if (column.equals(column1)) {
                return rows1.isChecked(row);
            } else if (column.equals(column2)) {
                return rows2.isChecked(row);
            } else {
                throw new IllegalArgumentException("Invalid column: " + column);
            }
        }

        @Override
        public String toTableString(String checkedIndicator, String uncheckedIndicator) {
            StringBuilder result = new StringBuilder();

            int rowHeaderWidth = rows1.getElements().stream().map((r) -> r.toString().length()).max(Comparator.naturalOrder()).get();

            result.append(Strings.padEnd("", rowHeaderWidth, ' '));
            result.append("|");

            String column1Label = column1.toString();
            int column1Width = column1Label.length();
            result.append(" ").append(column1Label).append(" |");

            String column2Label = column2.toString();
            int column2Width = column2Label.length();
            result.append(" ").append(column2Label).append(" |");

            result.append("\n");

            for (R row : rows1.getElements()) {

                result.append(Strings.padEnd(row.toString(), rowHeaderWidth, ' '));
                result.append("|");
                String v = this.rows1.isChecked(row) ? checkedIndicator : uncheckedIndicator;
                result.append(" ").append(Strings.padEnd(v, column1Width, ' ')).append(" |");
                v = this.rows2.isChecked(row) ? checkedIndicator : uncheckedIndicator;
                result.append(" ").append(Strings.padEnd(v, column2Width, ' ')).append(" |\n");
            }

            return result.toString();
        }

        @Override
        public boolean isComplete() {
            return this.rows1.isComplete() && this.rows2.isComplete();
        }

        @Override
        public boolean isBlank() {
            return this.rows1.isBlank() && this.rows2.isBlank();
        }

        @Override
        public ImmutableSet<R> getCompleteRows() {
            return this.rows1.getCheckedElements().intersection(this.rows2.getCheckedElements());
        }

        @Override
        public ImmutableSet<C> getCompleteColumns() {
            if (this.rows1.isComplete()) {
                if (this.rows2.isComplete()) {
                    return ImmutableSet.of(column1, column2);
                } else {
                    return ImmutableSet.of(column1);
                }
            } else {
                if (this.rows2.isComplete()) {
                    return ImmutableSet.of(column2);
                } else {
                    return ImmutableSet.empty();
                }
            }
        }

        @Override
        public CheckTable<R, C> getViewForMatchingColumns(Predicate<C> predicate) {
            if (predicate.test(column1)) {
                if (predicate.test(column2)) {
                    return this;
                } else {
                    return new SingleColumnCheckTable<>(rows1, column1);
                }
            } else {
                if (predicate.test(column2)) {
                    return new SingleColumnCheckTable<>(rows2, column2);
                } else {
                    return empty();
                }
            }
        }

        @Override
        public ImmutableSet<R> getRows() {
            return this.rows1.getElements();
        }

        @Override
        public ImmutableSet<C> getColumns() {
            return ImmutableSet.of(column1, column2);
        }

        @Override
        public void uncheckAll() {
            rows1.uncheckAll();
            rows2.uncheckAll();
        }

        @Override
        public void uncheckRowIf(Predicate<R> rowCheckPredicate) {
            this.rows1.uncheckIf(rowCheckPredicate);
            this.rows2.uncheckIf(rowCheckPredicate);

        }

        @Override
        public void uncheckRow(R row) {
            this.rows1.uncheck(row);
            this.rows2.uncheck(row);
        }

        @Override
        public void uncheckRowIfPresent(R row) {
            this.rows1.uncheckIfPresent(row);
            this.rows2.uncheckIfPresent(row);
        }

    }

    static class ArrayCheckTable<R, C> extends AbstractCheckTable<R, C> {
        private ImmutableMap<R, Integer> rows;
        private ImmutableMap<C, Integer> columns;
        private boolean[][] table;
        private int checkedCount = 0;
        private int uncheckedCount;
        private final int size;

        ArrayCheckTable(Set<R> rows, Set<C> columns) {
            this.rows = createIndexMap(rows);
            this.columns = createIndexMap(columns);
            this.table = new boolean[this.rows.size()][this.columns.size()];
            this.size = this.rows.size() * this.columns.size();
            this.uncheckedCount = size;
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
        public void uncheckAll() {
            this.checkedCount = 0;
            this.uncheckedCount = this.size;

            for (int i = 0; i < this.rows.size(); i++) {
                Arrays.fill(this.table[i], false);
            }
        }

        @Override
        public void uncheckRowIf(Predicate<R> rowCheckPredicate) {
            if (isBlank()) {
                return;
            }

            for (Map.Entry<R, Integer> entry : rows.entrySet()) {
                if (rowCheckPredicate.test(entry.getKey())) {
                    for (int i = 0; i < this.table[entry.getValue()].length; i++) {
                        if (this.table[entry.getValue()][i]) {
                            this.table[entry.getValue()][i] = false;
                            this.checkedCount--;
                            this.uncheckedCount++;

                            if (this.checkedCount == 0) {
                                return;
                            }
                        }
                    }
                }
            }
        }

        @Override
        public void uncheckRow(R row) {
            Integer rowIndex = rows.get(row);

            if (rowIndex == null) {
                throw new IllegalArgumentException("Invalid row: " + row);
            }

            if (isBlank()) {
                return;
            }

            for (int i = 0; i < this.table[rowIndex].length; i++) {
                if (this.table[rowIndex][i]) {
                    this.table[rowIndex][i] = false;
                    this.checkedCount--;
                    this.uncheckedCount++;

                    if (this.checkedCount == 0) {
                        return;
                    }
                }
            }
        }

        @Override
        public void uncheckRowIfPresent(R row) {
            Integer rowIndex = rows.get(row);

            if (rowIndex == null) {
                return;
            }

            if (isBlank()) {
                return;
            }

            for (int i = 0; i < this.table[rowIndex].length; i++) {
                if (this.table[rowIndex][i]) {
                    this.table[rowIndex][i] = false;
                    this.checkedCount--;
                    this.uncheckedCount++;

                    if (this.checkedCount == 0) {
                        return;
                    }
                }
            }
        }

        @Override
        public boolean isComplete() {
            return this.uncheckedCount == 0;
        }

        @Override
        public boolean isBlank() {
            return this.checkedCount == 0;
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
        public void uncheckIf(R row, Predicate<C> columnCheckPredicate) {
            Integer rowIndex = rows.get(row);

            if (rowIndex == null) {
                throw new IllegalArgumentException("Invalid row: " + row);
            }

            if (this.checkedCount == 0) {
                return;
            }

            for (Map.Entry<C, Integer> entry : columns.entrySet()) {
                if (this.table[rowIndex][entry.getValue()]) {
                    if (columnCheckPredicate.test(entry.getKey())) {
                        this.table[rowIndex][entry.getValue()] = false;
                        this.checkedCount--;
                        this.uncheckedCount++;

                        if (this.checkedCount == 0) {
                            return;
                        }
                    }
                }
            }
        }

        @Override
        public void uncheckIf(Predicate<R> rowCheckPredicate, C column) {
            Integer columnIndex = columns.get(column);

            if (columnIndex == null) {
                throw new IllegalArgumentException("Invalid column: " + column);
            }

            if (this.checkedCount == 0) {
                return;
            }

            for (Map.Entry<R, Integer> entry : rows.entrySet()) {
                if (this.table[entry.getValue()][columnIndex]) {
                    if (rowCheckPredicate.test(entry.getKey())) {
                        this.table[entry.getValue()][columnIndex] = false;
                        this.checkedCount--;
                        this.uncheckedCount++;

                        if (this.checkedCount == 0) {
                            return;
                        }
                    }
                }
            }
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

                    result.append(" ").append(Strings.padEnd(v, columnWidth[i], ' ')).append(" |");
                    i++;
                }
                result.append("\n");

            }

            return result.toString();
        }

        @Override
        public ImmutableSet<R> getCompleteRows() {
            ImmutableSet.Builder<R> result = new ImmutableSet.Builder<>();

            for (Map.Entry<R, Integer> entry : rows.entrySet()) {
                int index = entry.getValue();

                if (isRowCompleted(index)) {
                    result.with(entry.getKey());
                }
            }

            return result.build();
        }

        @Override
        public ImmutableSet<C> getCompleteColumns() {
            ImmutableSet.Builder<C> result = new ImmutableSet.Builder<>();

            for (Map.Entry<C, Integer> entry : columns.entrySet()) {
                int index = entry.getValue();

                if (isColumnCompleted(index)) {
                    result.with(entry.getKey());
                }
            }

            return result.build();
        }

        public ImmutableSet<R> getNonEmptyRows() {
            if (checkedCount == 0) {
                return ImmutableSet.empty();
            }

            ImmutableSet.Builder<R> result = new ImmutableSet.Builder<>();

            for (Map.Entry<R, Integer> entry : rows.entrySet()) {
                int index = entry.getValue();

                if (isRowNonEmpty(index)) {
                    result.with(entry.getKey());
                }
            }

            return result.build();
        }

        public ImmutableSet<C> getNonEmptyColumns() {
            if (checkedCount == 0) {
                return ImmutableSet.empty();
            }

            ImmutableSet.Builder<C> result = new ImmutableSet.Builder<>();

            for (Map.Entry<C, Integer> entry : columns.entrySet()) {
                int index = entry.getValue();

                if (isColumnNonEmpty(index)) {
                    result.with(entry.getKey());
                }
            }

            return result.build();
        }

        public ImmutableSet<R> getCheckedRows(C column) {
            if (checkedCount == 0) {
                return ImmutableSet.empty();
            }

            Integer columnIndex = columns.get(column);

            if (columnIndex == null) {
                throw new IllegalArgumentException("Invalid column: " + column);
            }

            ImmutableSet.Builder<R> result = new ImmutableSet.Builder<>();

            for (Map.Entry<R, Integer> entry : rows.entrySet()) {
                int rowIndex = entry.getValue();

                if (this.table[rowIndex][columnIndex]) {
                    result.with(entry.getKey());
                }
            }

            return result.build();
        }

        public ImmutableSet<C> getCheckedColumns(R row) {
            if (checkedCount == 0) {
                return ImmutableSet.empty();
            }

            Integer rowIndex = rows.get(row);

            if (rowIndex == null) {
                throw new IllegalArgumentException("Invalid row: " + row);
            }

            ImmutableSet.Builder<C> result = new ImmutableSet.Builder<>();

            for (Map.Entry<C, Integer> entry : columns.entrySet()) {
                int columnIndex = entry.getValue();

                if (this.table[rowIndex][columnIndex]) {
                    result.with(entry.getKey());
                }
            }

            return result.build();
        }

        private boolean isRowCompleted(int row) {
            int columnCount = columns.size();

            for (int i = 0; i < columnCount; i++) {
                if (!this.table[row][i]) {
                    return false;
                }
            }

            return true;
        }

        private boolean isColumnCompleted(int column) {
            int rowCount = rows.size();

            for (int i = 0; i < rowCount; i++) {
                if (!this.table[i][column]) {
                    return false;
                }
            }

            return true;
        }

        private boolean isRowNonEmpty(int row) {
            int columnCount = columns.size();

            for (int i = 0; i < columnCount; i++) {
                if (this.table[row][i]) {
                    return true;
                }
            }

            return false;
        }

        private boolean isColumnNonEmpty(int column) {
            int rowCount = rows.size();

            for (int i = 0; i < rowCount; i++) {
                if (this.table[i][column]) {
                    return true;
                }
            }

            return false;
        }

        @Override
        public CheckTable<R, C> getViewForMatchingColumns(Predicate<C> predicate) {
            ImmutableSet<C> matchingColumns = ImmutableSet.of(columns.keySet()).matching(predicate);

            if (matchingColumns.isEmpty()) {
                return empty();
            } else if (matchingColumns.size() == columns.size()) {
                return this;
            } else {
                return new View<>(this, matchingColumns);
            }
        }

        static class View<R, C> extends AbstractCheckTable<R, C> {

            private final ArrayCheckTable<R, C> delegate;
            private final ImmutableMap<C, Integer> columns;

            View(ArrayCheckTable<R, C> delegate, ImmutableSet<C> columns) {
                this.delegate = delegate;
                this.columns = delegate.columns.intersection(columns);
            }

            @Override
            public boolean check(R row, C column) {
                verifyColumn(column);

                if (delegate.check(row, column)) {
                    return true;
                }

                return isComplete();
            }

            @Override
            public void uncheckAll() {
                for (int i = 0; i < delegate.table.length; i++) {
                    for (Map.Entry<C, Integer> entry : columns.entrySet()) {
                        if (delegate.table[i][entry.getValue()]) {
                            delegate.table[i][entry.getValue()] = false;
                            delegate.checkedCount--;
                            delegate.uncheckedCount++;
                        }
                    }
                }
            }

            @Override
            public boolean checkIf(R row, Predicate<C> columnCheckPredicate) {

                Integer rowIndex = delegate.rows.get(row);

                if (rowIndex == null) {
                    throw new IllegalArgumentException("Invalid row: " + row);
                }

                for (Map.Entry<C, Integer> entry : columns.entrySet()) {
                    if (!delegate.table[rowIndex][entry.getValue()]) {
                        if (columnCheckPredicate.test(entry.getKey())) {
                            delegate.table[rowIndex][entry.getValue()] = true;
                            delegate.checkedCount++;
                            delegate.uncheckedCount--;

                            if (delegate.uncheckedCount == 0) {
                                return true;
                            }
                        }
                    }
                }

                return isComplete();
            }

            @Override
            public void uncheckIf(R row, Predicate<C> columnCheckPredicate) {
                Integer rowIndex = delegate.rows.get(row);

                if (rowIndex == null) {
                    throw new IllegalArgumentException("Invalid row: " + row);
                }

                if (delegate.checkedCount == 0) {
                    return;
                }

                for (Map.Entry<C, Integer> entry : columns.entrySet()) {
                    if (delegate.table[rowIndex][entry.getValue()]) {
                        if (columnCheckPredicate.test(entry.getKey())) {
                            delegate.table[rowIndex][entry.getValue()] = false;
                            delegate.checkedCount--;
                            delegate.uncheckedCount++;

                            if (delegate.checkedCount == 0) {
                                return;
                            }
                        }
                    }
                }
            }

            @Override
            public void uncheckIf(Predicate<R> rowCheckPredicate, C column) {
                Integer columnIndex = columns.get(column);

                if (columnIndex == null) {
                    throw new IllegalArgumentException("Invalid column: " + column);
                }

                delegate.uncheckIf(rowCheckPredicate, column);
            }

            @Override
            public void uncheckRowIf(Predicate<R> rowCheckPredicate) {
                if (delegate.isBlank()) {
                    return;
                }

                for (Map.Entry<R, Integer> entry : delegate.rows.entrySet()) {
                    if (rowCheckPredicate.test(entry.getKey())) {
                        for (Map.Entry<C, Integer> column : columns.entrySet()) {

                            int i = column.getValue();

                            if (delegate.table[entry.getValue()][i]) {
                                delegate.table[entry.getValue()][i] = false;
                                delegate.checkedCount--;
                                delegate.uncheckedCount++;

                                if (delegate.checkedCount == 0) {
                                    return;
                                }
                            }
                        }
                    }
                }
            }

            @Override
            public void uncheckRow(R row) {
                if (delegate.isBlank()) {
                    return;
                }

                Integer rowIndex = delegate.rows.get(row);

                if (rowIndex == null) {
                    throw new IllegalArgumentException("Invalid row: " + row);
                }

                for (Map.Entry<C, Integer> column : columns.entrySet()) {
                    int i = column.getValue();

                    if (delegate.table[rowIndex][i]) {
                        delegate.table[rowIndex][i] = false;
                        delegate.checkedCount--;
                        delegate.uncheckedCount++;

                        if (delegate.checkedCount == 0) {
                            return;
                        }
                    }
                }
            }

            @Override
            public void uncheckRowIfPresent(R row) {
                if (delegate.isBlank()) {
                    return;
                }

                Integer rowIndex = delegate.rows.get(row);

                if (rowIndex == null) {
                    return;
                }

                for (Map.Entry<C, Integer> column : columns.entrySet()) {
                    int i = column.getValue();

                    if (delegate.table[rowIndex][i]) {
                        delegate.table[rowIndex][i] = false;
                        delegate.checkedCount--;
                        delegate.uncheckedCount++;

                        if (delegate.checkedCount == 0) {
                            return;
                        }
                    }
                }
            }

            @Override
            public boolean isChecked(R row, C column) {
                return delegate.isChecked(row, column);
            }

            @Override
            public boolean isComplete() {
                if (delegate.isComplete()) {
                    return true;
                }

                for (Map.Entry<C, Integer> entry : columns.entrySet()) {
                    if (!delegate.isColumnCompleted(entry.getValue())) {
                        return false;
                    }
                }

                return true;
            }

            @Override
            public boolean isBlank() {
                if (delegate.isBlank()) {
                    return true;
                }

                for (Map.Entry<C, Integer> entry : columns.entrySet()) {
                    if (delegate.isColumnNonEmpty(entry.getValue())) {
                        return false;
                    }
                }

                return true;
            }

            @Override
            public String toTableString(String checkedIndicator, String uncheckedIndicator) {
                StringBuilder result = new StringBuilder();

                int rowHeaderWidth = delegate.rows.keySet().stream().map((r) -> r.toString().length()).max(Comparator.naturalOrder()).get();

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

                for (R row : delegate.rows.keySet()) {

                    result.append(Strings.padEnd(row.toString(), rowHeaderWidth, ' '));
                    result.append("|");

                    i = 0;
                    for (C column : columns.keySet()) {

                        String v = isChecked(row, column) ? checkedIndicator : uncheckedIndicator;

                        result.append(" ").append(Strings.padEnd(v, columnWidth[i], ' ')).append(" |");
                        i++;
                    }
                    result.append("\n");

                }

                return result.toString();
            }

            @Override
            public CheckTable<R, C> getViewForMatchingColumns(Predicate<C> predicate) {
                ImmutableSet<C> matchingColumns = ImmutableSet.of(columns.keySet()).matching(predicate);

                if (matchingColumns.isEmpty()) {
                    return empty();
                } else if (matchingColumns.size() == columns.size()) {
                    return this;
                } else {
                    return new View<>(delegate, matchingColumns);
                }
            }

            @Override
            public ImmutableSet<R> getCompleteRows() {
                ImmutableSet.Builder<R> result = new ImmutableSet.Builder<>();

                for (Map.Entry<R, Integer> entry : delegate.rows.entrySet()) {
                    int index = entry.getValue();

                    if (isRowCompleted(index)) {
                        result.with(entry.getKey());
                    }
                }

                return result.build();
            }

            @Override
            public ImmutableSet<C> getCompleteColumns() {
                ImmutableSet.Builder<C> result = new ImmutableSet.Builder<>();

                for (Map.Entry<C, Integer> entry : columns.entrySet()) {
                    int index = entry.getValue();

                    if (delegate.isColumnCompleted(index)) {
                        result.with(entry.getKey());
                    }
                }

                return result.build();
            }

            private boolean isRowCompleted(int row) {
                int columnCount = columns.size();

                for (int i = 0; i < columnCount; i++) {
                    if (!this.delegate.table[row][i]) {
                        return false;
                    }
                }

                return true;
            }

            private void verifyColumn(C column) {
                if (!columns.containsKey(column)) {
                    throw new IllegalArgumentException("Invalid column: " + column);
                }
            }

            @Override
            public ImmutableSet<R> getRows() {
                return delegate.getRows();
            }

            @Override
            public ImmutableSet<C> getColumns() {
                return ImmutableSet.of(columns.keySet());
            }

        }

        @Override
        public ImmutableSet<R> getRows() {
            return ImmutableSet.of(rows.keySet());
        }

        @Override
        public ImmutableSet<C> getColumns() {
            return ImmutableSet.of(columns.keySet());
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
        public void uncheckIf(Iterable<R> rows, Predicate<C> columnCheckPredicate) {
            if (isBlank()) {
                return;
            }

            Iterator<R> iter = rows.iterator();

            while (iter.hasNext()) {
                R row = iter.next();

                uncheckIf(row, columnCheckPredicate);

                if (isBlank()) {
                    return;
                }
            }
        }

        @Override
        public void uncheckIf(Predicate<R> rowCheckPredicate, Iterable<C> columns) {
            if (isBlank()) {
                return;
            }

            Iterator<C> iter = columns.iterator();

            while (iter.hasNext()) {
                C column = iter.next();

                uncheckIf(rowCheckPredicate, column);

                if (isBlank()) {
                    return;
                }
            }
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

        @Override
        public boolean isEmpty() {
            return false;
        }

        static <T> ImmutableMap<T, Integer> createIndexMap(Set<T> set) {
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

                return new ImmutableMap.MapBackedMap<>(result);
            }
        }

    }

    static CheckTable<?, ?> EMPTY = new CheckTable<Object, Object>() {

        @Override
        public boolean check(Object row, Object column) {
            return false;
        }

        @Override
        public boolean checkIf(Object row, Predicate<Object> columnCheckPredicate) {
            return false;
        }

        @Override
        public boolean checkIf(Iterable<Object> rows, Predicate<Object> columnCheckPredicate) {
            return false;
        }

        @Override
        public void uncheckIf(Object row, Predicate<Object> columnCheckPredicate) {

        }

        @Override
        public void uncheckIf(Iterable<Object> rows, Predicate<Object> columnCheckPredicate) {

        }

        @Override
        public void uncheckIf(Predicate<Object> rowCheckPredicate, Object column) {

        }

        @Override
        public void uncheckIf(Predicate<Object> rowCheckPredicate, Iterable<Object> columns) {

        }

        @Override
        public boolean isChecked(Object row, Object column) {
            return false;
        }

        @Override
        public boolean isComplete() {
            return false;
        }

        @Override
        public boolean isBlank() {
            return false;
        }

        @Override
        public String toString(String checkedIndicator, String uncheckedIndicator) {
            return "-/-";
        }

        @Override
        public String toTableString() {
            return "-/-";
        }

        @Override
        public String toTableString(String checkedIndicator, String uncheckedIndicator) {
            return "-/-";
        }

        @Override
        public CheckTable<Object, Object> getViewForMatchingColumns(Predicate<Object> predicate) {
            return this;
        }

        @Override
        public ImmutableSet<Object> getCompleteRows() {
            return ImmutableSet.empty();
        }

        @Override
        public ImmutableSet<Object> getCompleteColumns() {
            return ImmutableSet.empty();
        }

        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public ImmutableSet<Object> getRows() {
            return ImmutableSet.empty();
        }

        @Override
        public ImmutableSet<Object> getColumns() {
            return ImmutableSet.empty();
        }

        @Override
        public void uncheckAll() {

        }

        @Override
        public void uncheckRowIf(Predicate<Object> rowCheckPredicate) {

        }

        @Override
        public void uncheckRow(Object row) {

        }

        @Override
        public void uncheckRowIfPresent(Object row) {

        }

    };

}
