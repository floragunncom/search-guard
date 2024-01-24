package com.floragunn.searchsupport.xcontent;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.junit.Test;

import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class XContentObjectConverterTest {

    private static final Logger log = LogManager.getLogger(XContentObjectConverterTest.class);
    public static final String VALUE_ONE = "one";
    public static final String VALUE_TWO = "two";

    @Test 
	public void shouldSupportsNulls() {
        assertThat(XContentObjectConverter.canConvert(null), equalTo(false));
        assertThat(XContentObjectConverter.convertOrNull(null), nullValue());
    }

    @Test 
	public void shouldCastChunkedToXContentObjectToToXContentObject() throws DocumentParseException {
        TestChunkedToXContentObjectImpl testChunkedToXContentObjectImpl = new TestChunkedToXContentObjectImpl(VALUE_ONE, VALUE_TWO);

        ToXContentObject toXContentObject = XContentObjectConverter.convertOrNull(testChunkedToXContentObjectImpl);

        assertThat(XContentObjectConverter.canConvert(testChunkedToXContentObjectImpl), equalTo(true));
        String serialized = Strings.toString(toXContentObject, ToXContent.EMPTY_PARAMS);
        log.warn("Serialized object '{}'", serialized);
        DocNode docNode = DocNode.parse(Format.JSON).from(serialized);
        assertThat(docNode, containsValue("$.first", VALUE_ONE));
        assertThat(docNode, containsValue("$.second", VALUE_TWO));
    }

    @Test 
	public void shouldCastToXContentObject() throws DocumentParseException {
        TestToXContentObjectImpl testToXContentObjectImpl = new TestToXContentObjectImpl(VALUE_ONE);

        ToXContentObject toXContentObject = XContentObjectConverter.convertOrNull(testToXContentObjectImpl);

        assertThat(XContentObjectConverter.canConvert(testToXContentObjectImpl), equalTo(true));
        String serialized = Strings.toString(toXContentObject, ToXContent.EMPTY_PARAMS);
        log.warn("Serialized object '{}'", serialized);
        DocNode docNode = DocNode.parse(Format.JSON).from(serialized);
        assertThat(docNode, containsValue("$.msg", VALUE_ONE));
    }

    @Test 
	public void shouldNotCastToXContentObject() {
        Object object = "I am not ToXContentObject neither ChunkedToXContentObject";

        assertThat(XContentObjectConverter.convertOrNull(object), nullValue());
        assertThat(XContentObjectConverter.canConvert(object), equalTo(false));
    }
}