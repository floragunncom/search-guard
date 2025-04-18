package com.floragunn.searchsupport.rest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.rest.RestRequest;

import java.util.Objects;

/** ES 9.0.0-beta1 code
 *
 * The class required invocation of {@link #releaseContent()} method to release resources allocated by the class.
 *
 * https://github.com/elastic/elasticsearch/blob/97bc2919ffb5d9e90f809a18233c49a52cf58faa/server/src/main/java/org/elasticsearch/rest/BaseRestHandler.java#L144
 * The above line of code always calls release method on http request object. Therefore, it is impossible to read
 * the request body from the code which process request in another thread (common case in SG). Therefore, we need a
 * request wrappers which allows reading the request body from another thread.
 * The wrapper needs to be used when method
 * {@link org.elasticsearch.rest.BaseRestHandler#prepareRequest(org.elasticsearch.rest.RestRequest, org.elasticsearch.client.internal.node.NodeClient)}
 * is implemented with usage code like this:
 * <code>
  threadPool.generic().submit(() -> {
    // here access to rest request body
  	});
  </code>
 */
public class ProtectingContentRequestWrapper extends RestRequest {

		private final ReleasableBytesReference content;

	/**
	 * It is necessary to release resources allocated by the class by invocation of {@link #releaseContent()}
	 * @param requestWhichWillBeClosed source request, the content is read from the request
	 */
	public ProtectingContentRequestWrapper(RestRequest requestWhichWillBeClosed) {
			super(requestWhichWillBeClosed);
			content = requestWhichWillBeClosed.content();
            this.content.mustIncRef();
		}

		@Override
		public ReleasableBytesReference content() {
			return content;
		}

	/**
	 * It is mandatory to invoke the method for each created instance of the class to release resources related to
	 * {@link #content}
	 */
	public void releaseContent() {
			boolean decremented = content.decRef();
			assert decremented : "Request content is already released";
		}

		public Runnable releaseAfter(Runnable runnable) {
			Objects.requireNonNull(runnable, "Runnable is required to release resources");
			return () -> {
				try {
					runnable.run();
				} finally {
					releaseContent();
				}
			};
		}

		public <T> ActionListener<T> releaseAfter(ActionListener<T> actionListener) {
			return ActionListener.runAfter(actionListener, this::releaseContent);
		}
	}