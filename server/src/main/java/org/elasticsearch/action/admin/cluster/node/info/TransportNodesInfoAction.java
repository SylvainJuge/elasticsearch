/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.info;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class TransportNodesInfoAction extends TransportNodesAction<
    NodesInfoRequest,
    NodesInfoResponse,
    TransportNodesInfoAction.NodeInfoRequest,
    NodeInfo> {

    private final NodeService nodeService;

    @Inject
    public TransportNodesInfoAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        NodeService nodeService,
        ActionFilters actionFilters
    ) {
        super(
            NodesInfoAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            NodesInfoRequest::new,
            NodeInfoRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.nodeService = nodeService;
    }

    @Override
    protected NodesInfoResponse newResponse(
        NodesInfoRequest nodesInfoRequest,
        List<NodeInfo> responses,
        List<FailedNodeException> failures
    ) {
        return new NodesInfoResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeInfoRequest newNodeRequest(NodesInfoRequest request) {
        return new NodeInfoRequest(request);
    }

    @Override
    protected NodeInfo newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new NodeInfo(in);
    }

    @Override
    protected NodeInfo nodeOperation(NodeInfoRequest nodeRequest, Task task) {
        NodesInfoRequest request = nodeRequest.request;
        Set<String> metrics = request.requestedMetrics();
        return nodeService.info(
            metrics.contains(NodesInfoMetrics.Metric.SETTINGS.metricName()),
            metrics.contains(NodesInfoMetrics.Metric.OS.metricName()),
            metrics.contains(NodesInfoMetrics.Metric.PROCESS.metricName()),
            metrics.contains(NodesInfoMetrics.Metric.JVM.metricName()),
            metrics.contains(NodesInfoMetrics.Metric.THREAD_POOL.metricName()),
            metrics.contains(NodesInfoMetrics.Metric.TRANSPORT.metricName()),
            metrics.contains(NodesInfoMetrics.Metric.HTTP.metricName()),
            metrics.contains(NodesInfoMetrics.Metric.REMOTE_CLUSTER_SERVER.metricName()),
            metrics.contains(NodesInfoMetrics.Metric.PLUGINS.metricName()),
            metrics.contains(NodesInfoMetrics.Metric.INGEST.metricName()),
            metrics.contains(NodesInfoMetrics.Metric.AGGREGATIONS.metricName()),
            metrics.contains(NodesInfoMetrics.Metric.INDICES.metricName())
        );
    }

    public static class NodeInfoRequest extends TransportRequest {

        NodesInfoRequest request;

        public NodeInfoRequest(StreamInput in) throws IOException {
            super(in);
            request = new NodesInfoRequest(in);
        }

        public NodeInfoRequest(NodesInfoRequest request) {
            this.request = request;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}
