/*
 * Copyright 2016-2018 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.auditlog.sink;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.settings.Settings;

import com.floragunn.searchguard.auditlog.impl.AuditMessage;

public class KafkaSink extends AuditLogSink {

    private final String[] mandatoryProperties = new String []{"bootstrap_servers","topic_name"};
	private boolean valid = true;
	private Producer<Long, String> producer;
	private String topicName;

	public KafkaSink(final String name, final Settings settings, final String settingsPrefix, AuditLogSink fallbackSink) {
		super(name, settings, settingsPrefix, fallbackSink);
		
		Settings sinkSettings = settings.getAsSettings(settingsPrefix);
		checkMandatorySinkSettings(sinkSettings);

		if (!valid) {
			log.error("Failed to configure Kafka producer, please check the logfile.");
			return;
		}
		
        final Properties producerProps = new Properties();
        
        for(String key: sinkSettings.names()) {
            if(!key.equals("topic_name")) {
                producerProps.put(key.replace('_', '.'), sinkSettings.get(key));
            }
        }
        
		producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		topicName = sinkSettings.get("topic_name");

		//map path of
		//ssl.keystore.location
		//ssl.truststore.location
		//sasl.kerberos.kinit.cmd		
		
		final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        
        try {
            this.producer = AccessController.doPrivileged(new PrivilegedExceptionAction<KafkaProducer<Long, String>>() {
                @Override
                public KafkaProducer<Long, String> run() throws Exception {
                    return new KafkaProducer<Long, String>(producerProps);
                }
            });
        } catch (PrivilegedActionException e) {
            log.error("Failed to configure Kafka producer due to {}", e.getException(), e.getException());
            this.valid = false;
        }

	}

	@Override
	protected boolean doStore(AuditMessage msg) {
		if (!valid || producer == null) {
			return false;
		}
		
		ProducerRecord<Long, String> data = new ProducerRecord<Long, String>(topicName, msg.toJson());
		producer.send(data, new Callback() {
            
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
               if(exception == null) {
                   //log trace?
               } else {
                   log.error("Could not store message on Kafka topic {}", topicName, exception);
                   fallbackSink.store(msg);
               }
                
            }
        });
		return true;
	}

	@Override
	public boolean isHandlingBackpressure() {
		return true;
	}

	private void checkMandatorySinkSettings(Settings sinkSettings) {
	    for(String mandatory: mandatoryProperties) {
	        String value = sinkSettings.get(mandatory);
	        if (value == null || value.length() == 0) {
	            log.error("No value for {} provided in configuration, this endpoint will not work.", value);
	            this.valid = false;
	        }
	    }
	}

    @Override
    public void close() throws IOException {
        if(producer != null) {
            valid = false;
            producer.close();
        }
    }
}
