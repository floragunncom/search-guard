package com.floragunn.searchsupport.xcontent;

import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;

/**
 * The class provide static methods which can be used to convert an object to {@link ToXContentObject} type. The actual conversion is performed
 * by {@link this#convertOrNull(Object)} method. To check if conversion is possible, the method {@link this#canConvert(Object)} should be
 * invoked. The object can be converted into {@link ToXContentObject} if one of the following interfaces is implemented by the object:
 * <ul>
 *     <li>{@link ToXContentObject}</li>
 *     <li>{@link ChunkedToXContentObject}</li>
 * </ul>
 *
 *
 * This util class should be used instead of down-casting objects to {@link ToXContent} class.
 */
public final class XContentObjectConverter {

    private XContentObjectConverter() {
    }

    /**
     * Check if an object can be converted to {@link ToXContentObject} with the usage of method {@link this#convertOrNull(Object)}
     * @param object object which is checked
     * @return <code>true</code> if conversion is possible, <code>false</code> otherwise.
     */
    public static boolean canConvert(Object object) {
        return (object instanceof ToXContentObject) || (object instanceof ChunkedToXContentObject);
    }

    /**
     * Convert an object provided as method argument to {@link ToXContentObject}. To check if conversion is possible method
     * {@link this#canConvert(Object)} should be used.
     *
     * @param object object to convert
     * @return object of type {@link ToXContent} if conversion is possible, otherwise <code>null</code>.
     */
    public static ToXContentObject convertOrNull(Object object) {
        if (object instanceof ToXContentObject toXContentObject) {
            return toXContentObject;
        } else if (object instanceof ChunkedToXContentObject chunkedToXContentObject) {
            return ChunkedToXContentObject.wrapAsToXContentObject(chunkedToXContentObject);
        } else {
            return null;
        }
    }
}
