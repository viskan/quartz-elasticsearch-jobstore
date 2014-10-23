package com.viskan.quartz.elasticsearch.integration;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;

/**
 * Embedded Elasticsearch server used by the integration test.
 *
 * @author Anton Johansson
 */
class ElasticsearchServer
{
	private final Node node;

	ElasticsearchServer()
	{
		Settings settings = ImmutableSettings.settingsBuilder()
			.put("http.port", "9200")
			.put("path.data", "target/elasticsearch-data")
			.build();

		node = nodeBuilder()
			.local(true)
			.settings(settings)
			.clusterName("quartz-elasticsearch-jobstore-cluster")
			.node();
	}

	void shutdown()
	{
		node.close();
	}
}
