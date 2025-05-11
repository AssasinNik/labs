package com.cherenkov.lab_1.configs

import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import co.elastic.clients.transport.ElasticsearchTransport
import co.elastic.clients.transport.rest_client.RestClientTransport
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.elasticsearch.client.elc.ElasticsearchTemplate
import org.springframework.data.elasticsearch.core.convert.ElasticsearchConverter
import org.springframework.data.elasticsearch.core.convert.MappingElasticsearchConverter
import org.springframework.data.elasticsearch.core.mapping.SimpleElasticsearchMappingContext
import org.springframework.data.elasticsearch.repository.config.EnableElasticsearchRepositories

@Configuration
@EnableElasticsearchRepositories
class ElasticsearchConfig {

    @Value("\${spring.data.elasticsearch.uris}")
    private lateinit var elasticsearchUrl: String

    @Bean
    fun restClient(): RestClient {
        val url = elasticsearchUrl.replace("http://", "")
        val parts = url.split(":")
        val host = parts[0]
        val port = if (parts.size > 1) parts[1].toInt() else 9200
        
        return RestClient.builder(HttpHost(host, port, "http")).build()
    }

    @Bean
    fun elasticsearchTransport(restClient: RestClient): ElasticsearchTransport {
        return RestClientTransport(restClient, JacksonJsonpMapper())
    }

    @Bean
    fun elasticsearchClient(transport: ElasticsearchTransport): ElasticsearchClient {
        return ElasticsearchClient(transport)
    }

    @Bean
    fun elasticsearchMappingContext(): SimpleElasticsearchMappingContext {
        return SimpleElasticsearchMappingContext()
    }

    @Bean
    fun elasticsearchConverter(mappingContext: SimpleElasticsearchMappingContext): ElasticsearchConverter {
        return MappingElasticsearchConverter(mappingContext)
    }

    @Bean
    fun elasticsearchTemplate(
        client: ElasticsearchClient,
        converter: ElasticsearchConverter
    ): ElasticsearchTemplate {
        return ElasticsearchTemplate(client, converter)
    }
} 