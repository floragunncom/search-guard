package com.floragunn.searchguard.auth.blocking;

import inet.ipaddr.IPAddressString;
import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

public class IpRangeVerdictBasedBlockRegistryTest {


    @Test(expected = UnsupportedOperationException.class)
    public void when_block_is_called_throw_exception() {
        Set<IPAddressString> allows = new HashSet<>();
        Set<IPAddressString> disallows = new HashSet<>();
        IpRangeVerdictBasedBlockRegistry registry = new IpRangeVerdictBasedBlockRegistry(allows, disallows);
        registry.block(new IPAddressString("localhost"));
    }

    @Test
    public void when_singe_ip_is_allowed_block_others() {
        Set<IPAddressString> allows = new HashSet<>();
        Set<IPAddressString> disallows = new HashSet<>();

        allows.add(new IPAddressString("127.0.0.1"));

        IpRangeVerdictBasedBlockRegistry registry = new IpRangeVerdictBasedBlockRegistry(allows, disallows);
        Assert.assertThat(registry.isBlocked(new IPAddressString("127.0.0.1")), Is.is(false));
        Assert.assertThat(registry.isBlocked(new IPAddressString("1.2.3.4")), Is.is(true));
    }

    @Test
    public void when_singe_ip_is_blocked_allow_others() {
        Set<IPAddressString> allows = new HashSet<>();
        Set<IPAddressString> disallows = new HashSet<>();

        disallows.add(new IPAddressString("127.0.0.1"));

        IpRangeVerdictBasedBlockRegistry registry = new IpRangeVerdictBasedBlockRegistry(allows, disallows);
        Assert.assertThat(registry.isBlocked(new IPAddressString("127.0.0.1")), Is.is(true));
        Assert.assertThat(registry.isBlocked(new IPAddressString("1.2.3.4")), Is.is(false));
    }

    @Test
    public void when_singe_ip_is_blocked_in_range_allow_others() {
        Set<IPAddressString> allows = new HashSet<>();
        Set<IPAddressString> disallows = new HashSet<>();

        disallows.add(new IPAddressString("127.0.0.0/8"));

        IpRangeVerdictBasedBlockRegistry registry = new IpRangeVerdictBasedBlockRegistry(allows, disallows);
        Assert.assertThat(registry.isBlocked(new IPAddressString("127.0.0.1")), Is.is(true));
        Assert.assertThat(registry.isBlocked(new IPAddressString("1.2.3.4")), Is.is(false));
    }

    @Test
    public void when_ip_range_is_allowed_block_others() {
        Set<IPAddressString> allows = new HashSet<>();
        Set<IPAddressString> disallows = new HashSet<>();

        allows.add(new IPAddressString("127.0.0.0/8"));

        IpRangeVerdictBasedBlockRegistry registry = new IpRangeVerdictBasedBlockRegistry(allows, disallows);
        Assert.assertThat(registry.isBlocked(new IPAddressString("127.0.0.1")), Is.is(false));
        Assert.assertThat(registry.isBlocked(new IPAddressString("127.0.0.2")), Is.is(false));
        Assert.assertThat(registry.isBlocked(new IPAddressString("1.2.3.4")), Is.is(true));
    }

    @Test
    public void when_ip_v6_range_is_allowed_block_others() {
        Set<IPAddressString> allows = new HashSet<>();
        Set<IPAddressString> disallows = new HashSet<>();

        allows.add(new IPAddressString("1::/64"));

        IpRangeVerdictBasedBlockRegistry registry = new IpRangeVerdictBasedBlockRegistry(allows, disallows);
        Assert.assertThat(registry.isBlocked(new IPAddressString("1::1")), Is.is(false));
        Assert.assertThat(registry.isBlocked(new IPAddressString("1::2")), Is.is(false));
        Assert.assertThat(registry.isBlocked(new IPAddressString("2::1")), Is.is(true));
    }

    @Test
    public void when_ip_is_allowed_and_blocked_then_block() {
        Set<IPAddressString> allows = new HashSet<>();
        Set<IPAddressString> disallows = new HashSet<>();

        allows.add(new IPAddressString("127.0.0.0/8"));
        allows.add(new IPAddressString("10.10.20.0/30"));
        disallows.add(new IPAddressString("127.0.0.0/8"));

        IpRangeVerdictBasedBlockRegistry registry = new IpRangeVerdictBasedBlockRegistry(allows, disallows);
        Assert.assertThat(registry.isBlocked(new IPAddressString("127.0.0.1")), Is.is(true));
        Assert.assertThat(registry.isBlocked(new IPAddressString("10.10.20.3")), Is.is(false));
    }

}