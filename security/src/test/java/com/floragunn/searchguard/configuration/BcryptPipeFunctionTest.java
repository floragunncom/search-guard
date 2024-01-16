package com.floragunn.searchguard.configuration;

import com.floragunn.fluent.collections.ImmutableList;
import org.bouncycastle.crypto.generators.OpenBSDBCrypt;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class BcryptPipeFunctionTest {

    public static final byte[] SALT = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 1, 2, 3, 4 };

    private BcryptPipeFunction bcryptPipeFunction;

    @Before
    public void before() {
        this.bcryptPipeFunction = new BcryptPipeFunction(5, () -> SALT);
    }

    @Test
    public void shouldHashWithBcrypt() {
        String password = "one";
        Object hash = bcryptPipeFunction.apply(password);
        assertThat(OpenBSDBCrypt.checkPassword((String) hash, password.getBytes()), equalTo(true));
        assertThat(hash, equalTo("$2y$05$..CA.uOD/eaGAOm..OGB/.8o5odudFwf0ql5C46dG45mVtMwc00hW"));
    }

    @Test
    public void shouldSupportHashingInCollections() {
        ImmutableList<String> collection = ImmutableList.of("four", "five", "six");

        List<String> result = (List<String>) bcryptPipeFunction.apply(collection);

        assertThat(result, hasSize(collection.size()));

        String hash = result.get(0);
        assertThat(OpenBSDBCrypt.checkPassword(hash, collection.get(0).getBytes()), equalTo(true));
        assertThat(hash, equalTo("$2y$05$..CA.uOD/eaGAOm..OGB/.9Qgl01fJlJH.rz7NG0.Ph7UT3/sV8sa"));

        hash = result.get(1);
        assertThat(OpenBSDBCrypt.checkPassword(hash, collection.get(1).getBytes()), equalTo(true));
        assertThat(hash, equalTo("$2y$05$..CA.uOD/eaGAOm..OGB/.IRnrOQ8zVZ3f4ML4hqRQDM.o5XXdiMy"));

        hash = result.get(2);
        assertThat(OpenBSDBCrypt.checkPassword(hash, collection.get(2).getBytes()), equalTo(true));
        assertThat(hash, equalTo("$2y$05$..CA.uOD/eaGAOm..OGB/.Fh7z6Oo1ii4DQLqyTd2O9fdGsnLGu8C"));
    }

    @Test
    public void shouldHashObjectWhichIsNotString() {
        Object value = bcryptPipeFunction.apply(7);

        assertThat(OpenBSDBCrypt.checkPassword((String) value, "7".getBytes()), equalTo(true));
    }

    @Test
    public void shouldSupportNulls() {
        Object value = bcryptPipeFunction.apply(null);

        assertThat(value, nullValue());
    }
}