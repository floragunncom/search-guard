package com.floragunn.searchsupport.xcontent;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Set;

import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentGenerator;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;


public class AttributeValueFromXContent implements XContent {

    public static Object get(ToXContent toXContent, String attributeName) {
        AttributeValueFromXContent xContent = new AttributeValueFromXContent(attributeName);

        try {
            try (XContentBuilder builder = XContentBuilder.builder(xContent)) {
                if (toXContent.isFragment()) {
                    builder.startObject();
                }
                toXContent.toXContent(builder, ToXContent.EMPTY_PARAMS);
                if (toXContent.isFragment()) {
                    builder.endObject();
                }
                return xContent.getAttributeValue();
            }
        } catch (IOException e) {
            throw new RuntimeException("Error while reading " + attributeName + " from " + toXContent, e);
        }
    }

    private String attributeName;
    private Object attributeValue;

    public AttributeValueFromXContent(String attributeName) {
        this.attributeName = attributeName;
    }

    @Override
    public XContentType type() {
        return XContentType.JSON;
    }

    @Override
    public byte streamSeparator() {
        return 0;
    }

    @Override
    public boolean detectContent(byte[] bytes, int i, int i1) {
        return false;
    }

    @Override
    public boolean detectContent(CharSequence charSequence) {
        return false;
    }

    @Override
    public XContentGenerator createGenerator(OutputStream os, Set<String> includes, Set<String> excludes) throws IOException {
        return new Generator();
    }

    @Override
    public XContentParser createParser(XContentParserConfiguration xContentParserConfiguration, String s) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public XContentParser createParser(XContentParserConfiguration xContentParserConfiguration, InputStream inputStream) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public XContentParser createParser(XContentParserConfiguration xContentParserConfiguration, byte[] bytes, int i, int i1) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public XContentParser createParser(XContentParserConfiguration xContentParserConfiguration, Reader reader) throws IOException {
        throw new UnsupportedOperationException();
    }
    
    class Generator implements XContentGenerator {

        private int depth = 0;
        private String currentKey = null;

        Generator() {
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public void flush() throws IOException {

        }

        @Override
        public XContentType contentType() {
            return null;
        }

        @Override
        public void usePrettyPrint() {

        }

        @Override
        public boolean isPrettyPrint() {
            return false;
        }

        @Override
        public void usePrintLineFeedAtEnd() {

        }

        @Override
        public void writeStartObject() throws IOException {
            depth++;
        }

        @Override
        public void writeEndObject() throws IOException {
            depth--;
        }

        @Override
        public void writeStartArray() throws IOException {
            depth++;
        }

        @Override
        public void writeEndArray() throws IOException {
            depth--;
        }

        @Override
        public void writeFieldName(String name) throws IOException {
            this.currentKey = name;
        }

        @Override
        public void writeNull() throws IOException {
            setObject(null);
        }

        @Override
        public void writeNullField(String name) throws IOException {
            setObject(name, null);
        }

        @Override
        public void writeBooleanField(String name, boolean value) throws IOException {
            setObject(name, value);
        }

        @Override
        public void writeBoolean(boolean value) throws IOException {
            setObject(value);
        }

        @Override
        public void writeNumberField(String name, double value) throws IOException {
            setObject(name, value);
        }

        @Override
        public void writeNumber(double value) throws IOException {
            setObject(value);
        }

        @Override
        public void writeNumberField(String name, float value) throws IOException {
            setObject(name, value);
        }

        @Override
        public void writeNumber(float value) throws IOException {
            setObject(value);
        }

        @Override
        public void writeNumberField(String name, int value) throws IOException {
            setObject(name, value);
        }

        @Override
        public void writeNumber(int value) throws IOException {
            setObject(value);
        }

        @Override
        public void writeNumberField(String name, long value) throws IOException {
            setObject(name, value);
        }

        @Override
        public void writeNumber(long value) throws IOException {
            setObject(value);
        }

        @Override
        public void writeNumber(short value) throws IOException {
            setObject(value);
        }

        @Override
        public void writeNumber(BigInteger value) throws IOException {
            setObject(value);
        }

        @Override
        public void writeNumberField(String name, BigInteger value) throws IOException {
            setObject(name, value);
        }

        @Override
        public void writeNumber(BigDecimal value) throws IOException {
            setObject(value);
        }

        @Override
        public void writeNumberField(String name, BigDecimal value) throws IOException {
            setObject(name, value);
        }

        @Override
        public void writeStringField(String name, String value) throws IOException {
            setObject(name, value);
        }

        @Override
        public void writeString(String value) throws IOException {
            setObject(value);
        }

        @Override
        public void writeStringArray(String[] strings) throws IOException {
            setObject(strings);
        }

        @Override
        public void writeString(char[] text, int offset, int len) throws IOException {
            setObject(new String(text, offset, len));
        }

        @Override
        public void writeUTF8String(byte[] value, int offset, int length) throws IOException {
            setObject(new String(value, offset, length, "UTF-8"));
        }

        @Override
        public void writeBinaryField(String name, byte[] value) throws IOException {
            setObject(name, value);
        }

        @Override
        public void writeBinary(byte[] value) throws IOException {
            setObject(value);
        }

        @Override
        public void writeBinary(byte[] value, int offset, int length) throws IOException {
            byte[] valueSection = new byte[length];
            System.arraycopy(value, offset, valueSection, 0, length);
            setObject(value);
        }

        @SuppressWarnings("deprecation")
        @Override
        public void writeRawField(String name, InputStream value) throws IOException {
            if (!value.markSupported()) {
                value = new BufferedInputStream(value);
            }

            XContentType xContentType = XContentFactory.xContentType(value);

            writeRawField(name, value, xContentType);
        }

        @Override
        public void writeRawField(String name, InputStream value, XContentType xContentType) throws IOException {
            writeFieldName(name);
            writeRawValue(value, xContentType);
        }

        @Override
        public void writeRawValue(InputStream value, XContentType xContentType) throws IOException {
            try (XContentParser parser = XContentFactory.xContent(xContentType).createParser(NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE, value)) {
                parser.nextToken();
                copyCurrentStructure(parser);
            }
        }

        @Override
        public void copyCurrentStructure(XContentParser parser) throws IOException {
            int nestingDepth = 0;

            for (XContentParser.Token token = parser.currentToken(); token != null; token = parser.nextToken()) {
                switch (token) {
                case FIELD_NAME:
                    writeFieldName(parser.currentName());
                    break;
                case START_ARRAY:
                    writeStartArray();
                    nestingDepth++;
                    break;
                case START_OBJECT:
                    writeStartObject();
                    nestingDepth++;
                    break;
                case END_ARRAY:
                    writeEndArray();
                    nestingDepth--;
                    break;
                case END_OBJECT:
                    writeEndObject();
                    nestingDepth--;
                    break;
                default:
                    copyCurrentEvent(parser);
                }

                if (nestingDepth == 0 && token != XContentParser.Token.FIELD_NAME) {
                    return;
                }
            }

        }
        
        @Override
        public void writeDirectField(String name, CheckedConsumer<OutputStream, IOException> writer) throws IOException {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            writer.accept(outputStream);

            try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(XContentParserConfiguration.EMPTY,
                    outputStream.toByteArray())) {
                parser.nextToken();
                copyCurrentStructure(parser);
            }
        }


        @Override
        public boolean isClosed() {
            return false;
        }

        private Object setObject(Object key, Object object) throws IOException {
            if (depth == 1 && attributeName.equals(key)) {
                attributeValue = object;
            }
            return object;
        }

        private Object setObject(Object object) throws IOException {
            object = setObject(this.currentKey, object);
            this.currentKey = null;
            return object;
        }

    }

    public Object getAttributeValue() {
        return attributeValue;
    }


}
