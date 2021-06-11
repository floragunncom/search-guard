package com.floragunn.searchsupport.jobs.cluster;

import java.util.Iterator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.searchsupport.jobs.config.JobConfig;
import com.google.common.collect.Iterators;

public class DistributedJobConfigSource<JobType extends JobConfig> implements Iterable<JobType> {
    protected final Logger log = LogManager.getLogger(this.getClass());

    private final JobDistributor jobDistributor;
    private final Iterable<JobType> jobSupplier;

    public DistributedJobConfigSource(Iterable<JobType> jobSupplier, JobDistributor jobDistributor) {
        this.jobDistributor = jobDistributor;
        this.jobSupplier = jobSupplier;
    }

    @Override
    public Iterator<JobType> iterator() {
        return Iterators.filter(this.jobSupplier.iterator(), (job) -> this.jobDistributor.isJobSelected(job));
    }

}
