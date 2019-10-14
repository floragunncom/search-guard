/*
 * Copyright 2016-2018 by floragunn GmbH - All rights reserved
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */
package com.floragunn.searchguard.auditlog.sink;

import org.apache.logging.log4j.Level;
import org.elasticsearch.common.settings.Settings;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.searchguard.test.helper.file.FileHelper;

public class SinkProviderTest {

	@Test
	public void testConfiguration() throws Exception {
		
		Settings settings = Settings.builder().loadFromPath(FileHelper.getAbsoluteFilePathFromClassPath("auditlog/endpoints/sink/configuration_all_variants.yml")).build();
		SinkProvider provider = new SinkProvider(settings, null, null, null);
		
		// make sure we have a debug sink as fallback
		Assert.assertEquals(DebugSink.class, provider.fallbackSink.getClass() );
		
		AuditLogSink sink = provider.getSink("DefaULT");
		Assert.assertEquals(sink.getClass(), DebugSink.class);

		sink = provider.getSink("endpoint1");
		Assert.assertEquals(InternalESSink.class, sink.getClass());

		sink = provider.getSink("endpoint2");
		Assert.assertEquals(ExternalESSink.class, sink.getClass());
		// todo: sink does not work

		sink = provider.getSink("endpoinT3");
		Assert.assertEquals(DebugSink.class, sink.getClass());
		
		// no valid type
		sink = provider.getSink("endpoint4");
		Assert.assertEquals(null, sink);

		sink = provider.getSink("endpoint2");
		Assert.assertEquals(ExternalESSink.class, sink.getClass());
		// todo: sink does not work, no valid config

		// no valid type
		sink = provider.getSink("endpoint6");
		Assert.assertEquals(null, sink);

		// no valid type
		sink = provider.getSink("endpoint7");
		Assert.assertEquals(null, sink);

		sink = provider.getSink("endpoint8");
		Assert.assertEquals(DebugSink.class, sink.getClass());

		// wrong type in config
		sink = provider.getSink("endpoint9");
		Assert.assertEquals(ExternalESSink.class, sink.getClass());

		// log4j, valid configuration
		sink = provider.getSink("endpoint10");
		Assert.assertEquals(Log4JSink.class, sink.getClass());
		Log4JSink lsink = (Log4JSink)sink;
		Assert.assertEquals("loggername", lsink.loggerName);
		Assert.assertEquals(Level.WARN, lsink.logLevel);

		// log4j, no level, fallback to default
		sink = provider.getSink("endpoint11");
		Assert.assertEquals(Log4JSink.class, sink.getClass());
		lsink = (Log4JSink)sink;
		Assert.assertEquals("loggername", lsink.loggerName);
		Assert.assertEquals(Level.INFO, lsink.logLevel);

		// log4j, wrong level, fallback to log4j default
		sink = provider.getSink("endpoint12");
		Assert.assertEquals(Log4JSink.class, sink.getClass());
		lsink = (Log4JSink)sink;
		Assert.assertEquals("loggername", lsink.loggerName);
		Assert.assertEquals(Level.DEBUG, lsink.logLevel);

	}

	@Test
	public void testNoMultipleEndpointsConfiguration() throws Exception {		
		Settings settings = Settings.builder().loadFromPath(FileHelper.getAbsoluteFilePathFromClassPath("auditlog/endpoints/sink/configuration_no_multiple_endpoints.yml")).build();
		SinkProvider provider = new SinkProvider(settings, null, null, null);
		InternalESSink sink = (InternalESSink)provider.defaultSink;
		Assert.assertEquals("myownindex", sink.index);
		Assert.assertEquals("auditevents", sink.type);
	}
	

}
