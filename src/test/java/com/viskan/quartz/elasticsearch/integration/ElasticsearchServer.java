package com.viskan.quartz.elasticsearch.integration;

import static org.apache.commons.io.FileUtils.deleteQuietly;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.File;

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
	private static final String DATA_FOLDER = "target/elasticsearch-data";

	private final Node node;

	ElasticsearchServer()
	{
		deleteData();
		Settings settings = ImmutableSettings.settingsBuilder()
			.put("http.port", "9200")
			.put("path.data", DATA_FOLDER)
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
		deleteData();
	}

	private void deleteData()
	{
		deleteQuietly(new File(DATA_FOLDER));
	}
}
