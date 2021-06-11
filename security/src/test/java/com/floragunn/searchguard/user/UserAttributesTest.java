package com.floragunn.searchguard.user;

import static com.floragunn.searchguard.test.SgMatchers.equalsAsJson;
import static com.floragunn.searchguard.test.SgMatchers.getStringSegment;
import static com.floragunn.searchguard.test.SgMatchers.segment;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

public class UserAttributesTest {

    private static User user1 = User.forUser("horst").attribute("map1", ImmutableMap.of("one", 1, "two", 2, "three", 3))
            .backendRoles("role1", "role2", "role3").build();

    @Test
    public void testReplaceAttributesBasic() throws StringInterpolationException {

        String input = "User: ${user.name}\nRoles: ${user.roles}\nAttributes: ${user.attrs}\nMap1: ${user.attrs.map1}\n";
        String result = UserAttributes.replaceAttributes(input, user1);

        Assert.assertEquals(result, user1.getName(), getStringSegment("User: (.*)", result));
        Assert.assertEquals(result, toQuotedCommaSeparatedString(user1.getRoles()), getStringSegment("Roles: (.*)", result));
        Assert.assertEquals(result, user1.getStructuredAttributes().toString(), getStringSegment("Attributes: (.*)", result));
        Assert.assertEquals(result, user1.getStructuredAttributes().get("map1").toString(), getStringSegment("Map1: (.*)", result));
    }

    @Test
    public void testReplaceAttributesJson() throws StringInterpolationException, IOException {

        String input = "User: ${user.name|toJson}\nRoles: ${user.roles|toJson}\nAttributes: ${user.attrs|toJson}\nMap1: ${user.attrs.map1|toJson}\n";
        String result = UserAttributes.replaceAttributes(input, user1);

        Assert.assertThat(result, segment("User: (.*)", equalsAsJson(user1.getName())));
        Assert.assertThat(result, segment("Roles: (.*)", equalsAsJson(user1.getRoles())));
        Assert.assertThat(result, segment("Attributes: (.*)", equalsAsJson(user1.getStructuredAttributes())));
        Assert.assertThat(result, segment("Map1: (.*)", equalsAsJson(user1.getStructuredAttributes().get("map1"))));
    }

    @Test
    public void testReplaceAttributesToList() throws StringInterpolationException, IOException {

        String input = "User: ${user.name|toList|toJson}\nRoles: ${user.roles|toList|toJson}\nAttributes: ${user.attrs|toList|toJson}\nMap1: ${user.attrs.map1|toList|toJson}\n";
        String result = UserAttributes.replaceAttributes(input, user1);

        Assert.assertThat(result, segment("User: (.*)", equalsAsJson(Arrays.asList(user1.getName()))));
        Assert.assertThat(result, segment("Roles: (.*)", equalsAsJson(user1.getRoles())));
        Assert.assertThat(result, segment("Attributes: (.*)", equalsAsJson(Arrays.asList(user1.getStructuredAttributes()))));
        Assert.assertThat(result, segment("Map1: (.*)", equalsAsJson(Arrays.asList(user1.getStructuredAttributes().get("map1")))));
    }

    @Test
    public void testReplaceAttributesHeadOfList() throws StringInterpolationException, IOException {

        String input = "User: ${user.name|head|toJson}\nRoles: ${user.roles|head|toJson}\nAttributes: ${user.attrs|head|toJson}\nMap1: ${user.attrs.map1|head|toJson}\n";
        String result = UserAttributes.replaceAttributes(input, user1);

        Assert.assertThat(result, segment("User: (.*)", equalsAsJson(user1.getName())));
        Assert.assertThat(result, segment("Roles: (.*)", equalsAsJson(user1.getRoles().iterator().next())));
        Assert.assertThat(result, segment("Attributes: (.*)", equalsAsJson(user1.getStructuredAttributes())));
        Assert.assertThat(result, segment("Map1: (.*)", equalsAsJson(user1.getStructuredAttributes().get("map1"))));
    }

    @Test
    public void testReplaceAttributesTailOfList() throws StringInterpolationException, IOException {

        String input = "User: ${user.name|tail|toJson}\nRoles: ${user.roles|tail|toJson}\nAttributes: ${user.attrs|tail|toJson}\nMap1: ${user.attrs.map1|tail|toJson}\n";
        String result = UserAttributes.replaceAttributes(input, user1);

        Set<String> rolesTail = new HashSet<String>(user1.getRoles());
        rolesTail.remove(user1.getRoles().iterator().next());

        Assert.assertThat(result, segment("User: (.*)", equalsAsJson(Collections.emptyList())));
        Assert.assertThat(result, segment("Roles: (.*)", equalsAsJson(rolesTail)));
        Assert.assertThat(result, segment("Attributes: (.*)", equalsAsJson(Collections.emptyList())));
        Assert.assertThat(result, segment("Map1: (.*)", equalsAsJson(Collections.emptyList())));
    }

    @Test
    public void testReplaceAttributesDefault() throws StringInterpolationException, IOException {

        String input = "A1: ${user.attrs.foo|toJson:-1}\nA2: ${user.attrs.bar|toJson:-\"blub\"}\n";

        String result = UserAttributes.replaceAttributes(input, user1);

        Set<String> rolesTail = new HashSet<String>(user1.getRoles());
        rolesTail.remove(user1.getRoles().iterator().next());

        Assert.assertThat(result, segment("A1: (.*)", equalsAsJson(1)));
        Assert.assertThat(result, segment("A2: (.*)", equalsAsJson("blub")));
    }
    
    @Test
    public void testReplaceAttributesJsonDefault() throws StringInterpolationException, IOException {

        String input = "A1: ${user.attrs.foo?:{\"a\": 1}|toJson}\nA2: ${user.attrs.bar?:true|toJson}\nA3: ${user.attrs.bar?:[1,2,3]|toJson}\n";

        String result = UserAttributes.replaceAttributes(input, user1);

        Set<String> rolesTail = new HashSet<String>(user1.getRoles());
        rolesTail.remove(user1.getRoles().iterator().next());

        Assert.assertThat(result, segment("A1: (.*)", equalsAsJson(ImmutableMap.of("a", 1))));
        Assert.assertThat(result, segment("A2: (.*)", equalsAsJson(true)));
        Assert.assertThat(result, segment("A3: (.*)", equalsAsJson(Arrays.asList(1,2,3))));

    }


    private static String toQuotedCommaSeparatedString(final Set<String> roles) {
        return Joiner.on(',').join(Iterables.transform(roles, s -> {
            return new StringBuilder(s.length() + 2).append('"').append(s).append('"').toString();
        }));
    }

}
