package com.floragunn.searchguard.auth.blocking;

import java.util.HashSet;
import java.util.Set;

import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.searchguard.authc.blocking.IpRangeVerdictBasedBlockRegistry;

import inet.ipaddr.AddressStringException;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import inet.ipaddr.IncompatibleAddressException;

public class IpRangeVerdictBasedBlockRegistryTest {

    @Test
    public void when_singe_ip_is_allowed_block_others() throws AddressStringException, IncompatibleAddressException {
        Set<IPAddress> allows = new HashSet<>();
        Set<IPAddress> disallows = new HashSet<>();

        allows.add(new IPAddressString("127.0.0.1").toAddress());

        IpRangeVerdictBasedBlockRegistry registry = new IpRangeVerdictBasedBlockRegistry(allows, disallows);
        Assert.assertThat(registry.isBlocked(new IPAddressString("127.0.0.1").toAddress()), Is.is(false));
        Assert.assertThat(registry.isBlocked(new IPAddressString("1.2.3.4").toAddress()), Is.is(true));
    }

    @Test
    public void when_singe_ip_is_blocked_allow_others() throws AddressStringException, IncompatibleAddressException {
        Set<IPAddress> allows = new HashSet<>();
        Set<IPAddress> disallows = new HashSet<>();

        disallows.add(new IPAddressString("127.0.0.1").toAddress());

        IpRangeVerdictBasedBlockRegistry registry = new IpRangeVerdictBasedBlockRegistry(allows, disallows);
        Assert.assertThat(registry.isBlocked(new IPAddressString("127.0.0.1").toAddress()), Is.is(true));
        Assert.assertThat(registry.isBlocked(new IPAddressString("1.2.3.4").toAddress()), Is.is(false));
    }

    @Test
    public void when_singe_ip_is_blocked_in_range_allow_others() throws AddressStringException, IncompatibleAddressException {
        Set<IPAddress> allows = new HashSet<>();
        Set<IPAddress> disallows = new HashSet<>();

        disallows.add(new IPAddressString("127.0.0.0/8").toAddress());

        IpRangeVerdictBasedBlockRegistry registry = new IpRangeVerdictBasedBlockRegistry(allows, disallows);
        Assert.assertThat(registry.isBlocked(new IPAddressString("127.0.0.1").toAddress()), Is.is(true));
        Assert.assertThat(registry.isBlocked(new IPAddressString("1.2.3.4").toAddress()), Is.is(false));
    }

    @Test
    public void when_ip_range_is_allowed_block_others() throws AddressStringException, IncompatibleAddressException {
        Set<IPAddress> allows = new HashSet<>();
        Set<IPAddress> disallows = new HashSet<>();

        allows.add(new IPAddressString("127.0.0.0/8").toAddress());

        IpRangeVerdictBasedBlockRegistry registry = new IpRangeVerdictBasedBlockRegistry(allows, disallows);
        Assert.assertThat(registry.isBlocked(new IPAddressString("127.0.0.1").toAddress()), Is.is(false));
        Assert.assertThat(registry.isBlocked(new IPAddressString("127.0.0.2").toAddress()), Is.is(false));
        Assert.assertThat(registry.isBlocked(new IPAddressString("1.2.3.4").toAddress()), Is.is(true));
    }

    @Test
    public void when_ip_v6_range_is_allowed_block_others() throws AddressStringException, IncompatibleAddressException {
        Set<IPAddress> allows = new HashSet<>();
        Set<IPAddress> disallows = new HashSet<>();

        allows.add(new IPAddressString("1::/64").toAddress());

        IpRangeVerdictBasedBlockRegistry registry = new IpRangeVerdictBasedBlockRegistry(allows, disallows);
        Assert.assertThat(registry.isBlocked(new IPAddressString("1::1").toAddress()), Is.is(false));
        Assert.assertThat(registry.isBlocked(new IPAddressString("1::2").toAddress()), Is.is(false));
        Assert.assertThat(registry.isBlocked(new IPAddressString("2::1").toAddress()), Is.is(true));
    }

    @Test
    public void when_ip_is_allowed_and_blocked_then_block() throws AddressStringException, IncompatibleAddressException {
        Set<IPAddress> allows = new HashSet<>();
        Set<IPAddress> disallows = new HashSet<>();

        allows.add(new IPAddressString("127.0.0.0/8").toAddress());
        allows.add(new IPAddressString("10.10.20.0/30").toAddress());
        disallows.add(new IPAddressString("127.0.0.0/8").toAddress());

        IpRangeVerdictBasedBlockRegistry registry = new IpRangeVerdictBasedBlockRegistry(allows, disallows);
        Assert.assertThat(registry.isBlocked(new IPAddressString("127.0.0.1").toAddress()), Is.is(true));
        Assert.assertThat(registry.isBlocked(new IPAddressString("10.10.20.3").toAddress()), Is.is(false));
    }

}