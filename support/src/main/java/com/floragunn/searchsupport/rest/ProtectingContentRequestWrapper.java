package com.floragunn.searchsupport.rest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.rest.RestRequest;

import java.util.Objects;

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