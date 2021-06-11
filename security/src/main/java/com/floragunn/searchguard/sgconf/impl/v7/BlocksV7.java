package com.floragunn.searchguard.sgconf.impl.v7;

import java.util.List;
import java.util.Objects;

public class BlocksV7 {

    private Type type;
    private Verdict verdict;
    private List<String> value;
    private String description;

    @Override
    public String toString() {
        return "BlocksV7{" +
                "type=" + type +
                ", value='" + value + '\'' +
                ", verdict='" + verdict + '\'' +
                ", description='" + description + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BlocksV7 blocksV7 = (BlocksV7) o;
        return type == blocksV7.type &&
                Objects.equals(value, blocksV7.value) && Objects.equals(verdict, blocksV7.verdict) &&
                Objects.equals(description, blocksV7.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, value, description, verdict);
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public List<String> getValue() {
        return value;
    }

    public void setValue(List<String> value) {
        this.value = value;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Verdict getVerdict() {
        return verdict;
    }

    public void setVerdict(Verdict verdict) {
        this.verdict = verdict;
    }

    public enum Type {
        ip("ip"), name("name"), net_mask("net_mask");

        private final String type;

        Type(String name) {
            type = name;
        }

        public String getType() {
            return type;
        }

        @Override
        public String toString() {
            return type;
        }
    }

    public enum Verdict {
        allow("allow"), disallow("disallow");

        private final String verdict;

        Verdict(String name) {
            verdict = name;
        }

        public String getVerdict() {
            return verdict;
        }

        @Override
        public String toString() {
            return verdict;
        }
    }
}
