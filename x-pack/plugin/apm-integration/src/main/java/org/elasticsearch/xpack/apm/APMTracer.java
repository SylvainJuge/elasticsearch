/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.apm;

import co.elastic.apm.agent.impl.ElasticApmTracer;
import co.elastic.apm.agent.impl.ElasticApmTracerBuilder;
import co.elastic.apm.agent.impl.transaction.AbstractSpan;
import co.elastic.apm.agent.impl.transaction.Transaction;

import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.plugins.TracingPlugin;
import org.elasticsearch.tasks.Task;
import org.stagemonitor.configuration.source.SimpleSource;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.Map;

public class APMTracer extends AbstractLifecycleComponent implements TracingPlugin.Tracer {

    private final Map<Long, AbstractSpan<?>> taskSpans = ConcurrentCollections.newConcurrentMap();

    private volatile ElasticApmTracer tracer;

    @Override
    protected void doStart() {
        SimpleSource configSource = new SimpleSource()
            .add("service_name", "elasticsearch")
            .add("server_url", "https://8c5a522cb80f4f9d93e0ffa318290e2e.apm.eu-central-1.aws.cloud.es.io:443")
            .add("api_key", "UHFETjAzQUJ3SHFETFNjM2FGVmE6X0oyQWpWM2RTZy1zdDNsbXpuRHRFZw==")
            .add("application_packages", "org.elasticsearch")
            .add("hostname", "es-test") // needs to be provided as we can't execute 'uname -a' or 'hostname' commands to get it
            .add("log_level", "debug");

        tracer = AccessController.doPrivileged((PrivilegedAction<ElasticApmTracer>) () -> {
            ElasticApmTracer tracer = new ElasticApmTracerBuilder(List.of(configSource)).build();
            tracer.start(false);
            return tracer;
        });

        Transaction rootTransaction = tracer.startRootTransaction(APMTracer.class.getClassLoader());
        rootTransaction.withName("startup").end();
    }

    @Override
    protected void doStop() {
        // force all captured data to be sent
        tracer.getReporter().flush();
    }

    @Override
    protected void doClose() {
        tracer.stop();
    }

    @Override
    public void onTaskRegistered(Task task) {
        if (tracer == null) {
            return;
        }

        taskSpans.computeIfAbsent(task.getId(), taskId -> {
            Transaction transaction = tracer.startRootTransaction(APMTracer.class.getClassLoader())
                .withType("task")
                .withName(task.getAction());
            transaction.addCustomContext("es_task_id", task.getId());
            return transaction;
        });

    }

    @Override
    public void onTaskUnregistered(Task task) {
        final AbstractSpan<?> span = taskSpans.remove(task.getId());
        if (span != null) {
            span.end();
        }
    }
}
