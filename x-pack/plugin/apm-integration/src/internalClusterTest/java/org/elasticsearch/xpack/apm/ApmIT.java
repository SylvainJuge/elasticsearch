/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.apm;

import co.elastic.apm.agent.impl.ElasticApmTracer;
import co.elastic.apm.agent.impl.ElasticApmTracerBuilder;
import co.elastic.apm.agent.impl.transaction.Transaction;

import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.TracingPlugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskTracer;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;
import org.stagemonitor.configuration.source.SimpleSource;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class ApmIT extends ESIntegTestCase {

    private ElasticApmTracer tracer;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), APM.class);
    }

    @Before
    public void setup() {

        SimpleSource configSource = new SimpleSource()
            .add("service_name", "elasticsearch")
            .add("server_url", "https://8c5a522cb80f4f9d93e0ffa318290e2e.apm.eu-central-1.aws.cloud.es.io:443")
            .add("api_key", "UHFETjAzQUJ3SHFETFNjM2FGVmE6X0oyQWpWM2RTZy1zdDNsbXpuRHRFZw==")
            .add("application_packages", "org.elasticsearch")
            .add("hostname", "es-test") // needs to be provided as we can't execute 'uname -a' or 'hostname' commands to get it
            .add("log_level", "debug");

        tracer = new ElasticApmTracerBuilder(List.of(configSource)).build();
        tracer.start(false);

        Transaction rootTransaction = tracer.startRootTransaction(ApmIT.class.getClassLoader());
        rootTransaction.withName("hello from ES with Elastic agent").end();

        // force all captured data to be sent
        tracer.getReporter().flush();
    }

    @After
    public void cleanup() {
        tracer.stop();
    }

    public void testModule() {

        List<TracingPlugin> plugins = internalCluster().getMasterNodeInstance(PluginsService.class).filterPlugins(TracingPlugin.class);
        assertThat(plugins, hasSize(1));

        TransportService transportService = internalCluster().getInstance(TransportService.class);
        final TaskTracer taskTracer = transportService.getTaskManager().getTaskTracer();
        assertThat(taskTracer, notNullValue());

        final Task testTask = new Task(randomNonNegativeLong(), "test", "action", "", TaskId.EMPTY_TASK_ID, Collections.emptyMap());

        taskTracer.onTaskRegistered(testTask);
        taskTracer.onTaskUnregistered(testTask);

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            // ignored
        }

    }
}
