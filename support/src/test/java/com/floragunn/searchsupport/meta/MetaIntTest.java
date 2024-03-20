/*
 * Copyright 2024 floragunn GmbH
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

package com.floragunn.searchsupport.meta;

public class MetaIntTest {
/*
    static TestDataStream ds_a1 = TestDataStream.name("ds_a1").documentCount(100).rolloverAfter(10).seed(1).attr("prefix", "a").build();
    static TestDataStream ds_a2 = TestDataStream.name("ds_a2").documentCount(110).rolloverAfter(10).seed(2).attr("prefix", "a").build();
    static TestDataStream ds_a3 = TestDataStream.name("ds_a3").documentCount(120).rolloverAfter(10).seed(3).attr("prefix", "a").build();
    static TestDataStream ds_ax = TestDataStream.name("ds_ax").build(); // Not existing data stream
    static TestDataStream ds_b1 = TestDataStream.name("ds_b1").documentCount(51).rolloverAfter(10).seed(4).attr("prefix", "b").build();
    static TestDataStream ds_b2 = TestDataStream.name("ds_b2").documentCount(52).rolloverAfter(10).seed(5).attr("prefix", "b").build();
    static TestDataStream ds_b3 = TestDataStream.name("ds_b3").documentCount(53).rolloverAfter(10).seed(6).attr("prefix", "b").build();
    static TestDataStream ds_hidden = TestDataStream.name("ds_hidden").documentCount(55).seed(8).attr("prefix", "h").build(); // This is hidden via the ds_hidden index template
    static TestIndex index_c1 = TestIndex.name("index_c1").documentCount(5).seed(7).attr("prefix", "c").build();

    static TestAlias alias_ab1 = new TestAlias("alias_ab1", ds_a1, ds_a2, ds_a3, ds_b1);
    static TestAlias alias_c1 = new TestAlias("alias_c1", index_c1);
    
    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().users(USERS)//
            .indexTemplates(new TestIndexTemplate("ds_test", "ds_*").dataStream().composedOf(TestComponentTemplate.DATA_STREAM_MINIMAL))//
            .indexTemplates(new TestIndexTemplate("ds_hidden", "ds_hidden*").priority(10).dataStream("hidden", true)
                    .composedOf(TestComponentTemplate.DATA_STREAM_MINIMAL))//
            .dataStreams(ds_a1, ds_a2, ds_a3, ds_b1, ds_b2, ds_b3, ds_hidden)//
            .indices(index_c1)//
            .aliases(alias_ab1, alias_c1)//
            .authzDebug(true)//
            //     .logRequests()//
            .useExternalProcessCluster()//
            .build();

*/
}
