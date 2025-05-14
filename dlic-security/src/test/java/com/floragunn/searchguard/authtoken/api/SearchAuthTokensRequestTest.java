/*
 * Copyright 2025 by floragunn GmbH - All rights reserved
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * This software is free of charge for non-commercial and academic use.
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */
package com.floragunn.searchguard.authtoken.api;

import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.core.TimeValue;
//import org.elasticsearch.search.Scroll;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Base64;

import java.io.IOException;

public class SearchAuthTokensRequestTest {

    /**
     * The class instance serialized with the class Scroll which has been available in ES 8.18
     */
    private final static String SERIALIZED_CLASS_FROM_ES_8_18 = "AAEOBP//////////AAAAAA==";

    @Test
    public void deserializationBackwardsCompatibilityTest() throws IOException {
        SearchAuthTokensRequest deserializedRequest = deserializeFromBaseEncodedString(SERIALIZED_CLASS_FROM_ES_8_18);

        //The ES 9 uses class TimeValue instead of Scroll
        TimeValue deserializedRequestScroll = deserializedRequest.getScroll();
        MatcherAssert.assertThat(deserializedRequestScroll, Matchers.equalTo(TimeValue.timeValueMinutes(7)));
    }

    // Code used to create serialized class instance in SG 3.1.0 for ES 8.18 where the class Scroll class do exist
    //    @Test
    //    public void serializeTheClass() throws IOException {
    //        Scroll scroll = new Scroll(TimeValue.timeValueMinutes(7));
    //        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource();
    //        SearchAuthTokensRequest request = new SearchAuthTokensRequest(null, scroll);
    //        BytesRefStreamOutput out = new BytesRefStreamOutput();
    //
    //        request.writeTo(out);
    //
    //        BytesRef bytesRef = out.get();
    //        byte[] serializedClass = bytesRef.bytes;
    //        String base64Encoded = Base64.getEncoder().encodeToString(serializedClass);
    //        System.out.println("Serialized class: '" + base64Encoded + "'");
    //
    //        SearchAuthTokensRequest deserializedRequest = deserializeFromBaseEncodedString(base64Encoded);
    //        Scroll deserializedRequestScroll = deserializedRequest.getScroll();
    //        MatcherAssert.assertThat(deserializedRequestScroll, Matchers.equalTo(scroll));
    //    }

    private SearchAuthTokensRequest deserializeFromBaseEncodedString(String base64Encoded) throws IOException {
        byte[] decodedBytes = Base64.getDecoder().decode(base64Encoded);
        return deserializeFromBaseEncodedString(decodedBytes);
    }

    private static SearchAuthTokensRequest deserializeFromBaseEncodedString(byte[] decodedBytes) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(decodedBytes);
        ByteBufferStreamInput in = new ByteBufferStreamInput(buffer);

        return new SearchAuthTokensRequest(in);
    }
}