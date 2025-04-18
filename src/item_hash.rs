use aws_sdk_dynamodb as dynamo_db;
use aws_sdk_dynamodb::config::http::HttpResponse;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::query::QueryError;
use aws_sdk_dynamodb::types::AttributeValue;
use item_core::item_hash::ItemEventHash;
use serde_dynamo::from_item;
use std::collections::HashMap;

pub async fn get_item_event_hashes_by_source_id(
    source_id: &str,
    ddb_client: &dynamo_db::Client,
) -> Result<Vec<ItemEventHash>, SdkError<QueryError, HttpResponse>> {
    let mut item_event_hashes = Vec::new();

    let paginator = ddb_client
        .query()
        .table_name("items")
        .index_name("gsi_1_hash_index")
        .key_condition_expression("#pk = :pk_val")
        .expression_attribute_names("#pk", "pk")
        .expression_attribute_values(":pk_val", AttributeValue::S(source_id.to_string()))
        .scan_index_forward(false)
        .into_paginator()
        .items()
        .send();

    tokio::pin!(paginator);
    while let Some(item) = paginator.next().await {
        match from_item(item?) {
            Ok(parsed) => item_event_hashes.push(parsed),
            Err(e) => eprintln!("Deserialization error: {}", e),
        }
    }

    Ok(item_event_hashes)
}

// vec is sorted by latest (first)
pub async fn get_item_event_hashes_map_by_source_id(
    source_id: &str,
    ddb_client: &dynamo_db::Client,
) -> Result<HashMap<String, Vec<String>>, SdkError<QueryError, HttpResponse>> {
    let item_event_hashes = get_item_event_hashes_by_source_id(source_id, ddb_client).await?;
    let mut item_id_hash_map: HashMap<String, Vec<String>> = HashMap::new();
    for item_event_hash in item_event_hashes {
        item_id_hash_map
            .entry(item_event_hash.get_item_id().to_string())
            .or_insert(vec![item_event_hash.hash]);
    }

    Ok(item_id_hash_map)
}

pub async fn get_latest_item_event_hash_map_by_source_id(
    source_id: &str,
    ddb_client: &dynamo_db::Client,
) -> Result<HashMap<String, String>, SdkError<QueryError, HttpResponse>> {
    let item_event_hashes = get_item_event_hashes_by_source_id(source_id, ddb_client).await?;
    let mut item_id_hash_map: HashMap<String, String> = HashMap::new();
    for item_event_hash in item_event_hashes {
        // assumes hashes for each item to be sorted by latest
        // this is given by the nature of Sort-Key event_id being a ISO-8601 timestamp
        // when scan_index_forward false (desc)
        item_id_hash_map
            .entry(item_event_hash.get_item_id().to_string())
            .or_insert(item_event_hash.hash);
    }

    Ok(item_id_hash_map)
}
