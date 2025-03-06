package com.floragunn.searchguard.dlic.rest.api;

import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.rest.RestRequest;

public class ProtectingContentRequestWrapper extends RestRequest {

		private final ReleasableBytesReference content;

	/**
	 * It is necessary to release resources allocated by the class by invocation of {@link #releaseContent()}
	 * @param requestWhichWillBeClosed source request, the content is read from the request
	 */
	protected ProtectingContentRequestWrapper(RestRequest requestWhichWillBeClosed) {
			super(requestWhichWillBeClosed);
			content = requestWhichWillBeClosed.content();
            this.content.mustIncRef();
		}

		@Override
		public ReleasableBytesReference content() {
			return content;
		}

        public void releaseContent() {
			boolean decremented = content.decRef();
			assert decremented : "Request content is already released";
		}
	}