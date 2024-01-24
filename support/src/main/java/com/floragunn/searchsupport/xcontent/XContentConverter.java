package com.floragunn.searchsupport.xcontent;

import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.xcontent.ToXContent;

/**
 * The class provide static methods which can be used to convert an object to {@link ToXContent} type. The actual conversion is performed
 * by {@link this#convertOrNull(Object)} method. To check if conversion is possible, the method {@link this#canConvert(Object)} should be
 * invoked. The object can be converted into {@link ToXContent} if one of the following interfaces is implemented by the object:
 * <ul>
 *     <li>{@link ToXContent}</li>
 *     <li>{@link ChunkedToXContent}</li>
 * </ul>
 *
 *
 * This util class should be used instead of down-casting objects to {@link ToXContent} class.
 */
public final class XContentConverter {

    private XContentConverter() {
    }

    /**
     * Check if an object can be converted to {@link ToXContent} with the usage of method {@link this#convertOrNull(Object)}
     * 
     * @param object object which is checked
     * @return <code>true</code> if conversion is possible, <code>false</code> otherwise.
     */
    public static boolean canConvert(Object object) {
        return (object instanceof ToXContent) || (object instanceof ChunkedToXContent);
    }

    /**
     * Convert an object provided as method argument to {@link ToXContent}. To check if conversion is possible
     * method {@link this#canConvert(Object)} should be used.
     *
     * @param object object to convert
     * @return object of type {@link ToXContent} if conversion is possible, otherwise <code>null</code>.
     */
    public static ToXContent convertOrNull(Object object) {
        if (object instanceof ToXContent toXContent) {
            return toXContent;
        } else if (object instanceof ChunkedToXContent chunkedToXContent) {
            return ChunkedToXContent.wrapAsToXContent(chunkedToXContent);
        } else {
            return null;
        }
    }
}
