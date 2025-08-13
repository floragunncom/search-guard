package com.floragunn.searchsupport.util;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class LocalClusterAliasExtractor {

    public static String getLocalClusterAliasFromSearchRequest(SearchRequest searchRequest) {

        AtomicInteger ai = new AtomicInteger();
        SetOnce<String> localClusterAlias = new SetOnce();
        try {
            searchRequest.writeTo(new StreamOutput() {

                @Override
                public void writeOptionalString(String str) throws IOException {
                    if(ai.get() == 2) {
                        localClusterAlias.set(str);
                    }
                    ai.incrementAndGet();
                }

                @Override
                public void writeByte(byte b) throws IOException {

                }

                @Override
                public void writeBytes(byte[] b, int offset, int length) throws IOException {

                }

                @Override
                public void flush() throws IOException {

                }

                @Override
                public void close() throws IOException {

                }

                @Override
                public void writeBytes(byte[] b) throws IOException {
                }

                @Override
                public void writeBytes(byte[] b, int length) throws IOException {
                }

                @Override
                public void writeByteArray(byte[] b) throws IOException {
                }

                @Override
                public void writeWithSizePrefix(Writeable writeable) throws IOException {
                }

                @Override
                public void writeBytesReference(BytesReference bytes) throws IOException {
                }

                @Override
                public void writeOptionalBytesReference(BytesReference bytes) throws IOException {
                }

                @Override
                public void writeBytesRef(BytesRef bytes) throws IOException {
                }

                @Override
                public void writeInt(int i) throws IOException {
                }

                @Override
                public void writeVInt(int i) throws IOException {
                }

                @Override
                public void writeLong(long i) throws IOException {
                }

                @Override
                public void writeVLong(long i) throws IOException {
                }

                @Override
                public void writeOptionalVLong(Long l) throws IOException {
                }

                @Override
                public void writeZLong(long i) throws IOException {
                }

                @Override
                public void writeOptionalLong(Long l) throws IOException {
                }

                @Override
                public void writeOptionalSecureString(SecureString secureStr) throws IOException {
                }

                @Override
                public void writeOptionalInt(Integer integer) throws IOException {
                }

                @Override
                public void writeOptionalVInt(Integer integer) throws IOException {
                }

                @Override
                public void writeOptionalFloat(Float floatValue) throws IOException {
                }

                @Override
                public void writeOptionalText(Text text) throws IOException {
                }

                @Override
                public void writeText(Text text) throws IOException {
                }

                @Override
                public void writeString(String str) throws IOException {
                }

                @Override
                public void writeSecureString(SecureString secureStr) throws IOException {
                }

                @Override
                public void writeFloat(float v) throws IOException {
                }

                @Override
                public void writeDouble(double v) throws IOException {
                }

                @Override
                public void writeOptionalDouble(Double v) throws IOException {
                }

                @Override
                public void writeBoolean(boolean b) throws IOException {
                }

                @Override
                public void writeOptionalBoolean(Boolean b) throws IOException {
                }

                @Override
                public void write(int b) throws IOException {
                }

                @Override
                public void write(byte[] b, int off, int len) throws IOException {
                }

                @Override
                public void writeStringArray(String[] array) throws IOException {
                }

                @Override
                public void writeStringArrayNullable(String[] array) throws IOException {
                }

                @Override
                public void writeOptionalStringArray(String[] array) throws IOException {
                }

                @Override
                public void writeGenericMap(Map<String, Object> map) throws IOException {
                }

                @Override
                public void writeMapWithConsistentOrder(Map<String, ?> map) throws IOException {
                }

                @Override
                public <T> void writeGenericList(List<T> v, Writeable.Writer<T> writer) throws IOException {
                }

                @Override
                public void writeGenericString(String value) throws IOException {
                }

                @Override
                public void writeGenericNull() throws IOException {
                }

                @Override
                public void writeGenericValue(Object value) throws IOException {
                }

                @Override
                public void writeIntArray(int[] values) throws IOException {
                }

                @Override
                public void writeVIntArray(int[] values) throws IOException {
                }

                @Override
                public void writeLongArray(long[] values) throws IOException {
                }

                @Override
                public void writeVLongArray(long[] values) throws IOException {
                }

                @Override
                public void writeFloatArray(float[] values) throws IOException {
                }

                @Override
                public void writeDoubleArray(double[] values) throws IOException {
                }

                @Override
                public <T> void writeArray(Writeable.Writer<T> writer, T[] array) throws IOException {
                }

                @Override
                public <T> void writeOptionalArray(Writeable.Writer<T> writer, T[] array) throws IOException {
                }

                @Override
                public <T extends Writeable> void writeArray(T[] array) throws IOException {
                }

                @Override
                public <T extends Writeable> void writeOptionalArray(T[] array) throws IOException {
                }

                @Override
                public void writeOptionalWriteable(Writeable writeable) throws IOException {
                }

                @Override
                public void writeException(Throwable throwable) throws IOException {
                }

                @Override
                public void writeNamedWriteable(NamedWriteable namedWriteable) throws IOException {
                }

                @Override
                public void writeOptionalNamedWriteable(NamedWriteable namedWriteable) throws IOException {
                }

                @Override
                public void writeGeoPoint(GeoPoint geoPoint) throws IOException {
                }

                @Override
                public void writeZoneId(ZoneId timeZone) throws IOException {
                }

                @Override
                public void writeOptionalZoneId(ZoneId timeZone) throws IOException {
                }

                @Override
                public void writeCollection(Collection<? extends Writeable> collection) throws IOException {
                }

                @Override
                public <T> void writeCollection(Collection<T> collection, Writeable.Writer<T> writer) throws IOException {
                }

                @Override
                public void writeStringCollection(Collection<String> collection) throws IOException {
                }

                @Override
                public void writeOptionalStringCollection(Collection<String> collection) throws IOException {
                }

                @Override
                public <E extends Enum<E>> void writeEnum(E enumValue) throws IOException {
                }

                @Override
                public <E extends Enum<E>> void writeOptionalEnum(E enumValue) throws IOException {
                }

                @Override
                public <E extends Enum<E>> void writeEnumSet(EnumSet<E> enumSet) throws IOException {
                }

                @Override
                public void writeTimeValue(TimeValue timeValue) throws IOException {
                }

                @Override
                public void writeOptionalTimeValue(TimeValue timeValue) throws IOException {
                }

                @Override
                public <T extends Writeable> void writeMissingWriteable(Class<T> ignored) throws IOException {
                }

                @Override
                public void writeMissingString() throws IOException {
                }
            });
            final String alias = localClusterAlias.get();
            final String srString = searchRequest.toString();
            if(srString.contains("localClusterAlias="+alias)) {
                return alias;
            } else {

                int start = srString.indexOf("localClusterAlias=");
                int end = srString.indexOf(", ", start);

                String extracted = srString.substring(start+18, end);

                if(extracted.equals("null")) {
                    return null;
                }

                return extracted;
            }
        } catch (IOException e) {
            //can not happen
            throw new RuntimeException(e);
        }
    }
}
