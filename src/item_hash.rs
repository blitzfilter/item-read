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
    scan_index_forward: bool,
    ddb_client: &dynamo_db::Client,
) -> Result<Vec<ItemEventHash>, SdkError<QueryError, HttpResponse>> {
    let item_event_hashes: Vec<ItemEventHash> = ddb_client
        .query()
        .table_name("items")
        .index_name("gsi_1_hash_index")
        .key_condition_expression("#pk = :pk_val AND begins_with(#sk, :sk_prefix)")
        .expression_attribute_names("#pk", "party_id")
        .expression_attribute_names("#sk", "event_id")
        .expression_attribute_values(":pk_val", AttributeValue::S(format!("source#{source_id}")))
        .expression_attribute_values(":sk_prefix", AttributeValue::S("item#".to_string()))
        .scan_index_forward(scan_index_forward)
        .into_paginator()
        .send()
        .try_collect()
        .await?
        .iter()
        .flat_map(|qo| qo.to_owned().items.unwrap_or_default())
        .filter_map(|item| {
            let model: serde_dynamo::Result<ItemEventHash> = from_item(item);
            match model {
                Ok(m) => Some(m),
                Err(e) => {
                    eprintln!("Failed to parse item event hash: {}", e);
                    None
                }
            }
        })
        .collect();

    Ok(item_event_hashes)
}

// vec is sorted by latest (first)
pub async fn get_item_event_hashes_map_by_source_id(
    source_id: &str,
    ddb_client: &dynamo_db::Client,
) -> Result<HashMap<String, Vec<String>>, SdkError<QueryError, HttpResponse>> {
    let item_event_hashes =
        get_item_event_hashes_by_source_id(source_id, false, ddb_client).await?;
    let mut item_id_hash_map: HashMap<String, Vec<String>> = HashMap::new();
    for item_event_hash in item_event_hashes {
        match item_id_hash_map.get_mut(item_event_hash.get_item_id()) {
            Some(hashes) => hashes.push(item_event_hash.hash),
            None => {
                item_id_hash_map.insert(
                    item_event_hash.get_item_id().to_owned(),
                    vec![item_event_hash.hash],
                );
            }
        }
    }

    Ok(item_id_hash_map)
}

pub async fn get_latest_item_event_hash_map_by_source_id(
    source_id: &str,
    ddb_client: &dynamo_db::Client,
) -> Result<HashMap<String, String>, SdkError<QueryError, HttpResponse>> {
    let item_event_hashes =
        get_item_event_hashes_by_source_id(source_id, false, ddb_client).await?;
    let mut item_id_hash_map: HashMap<String, String> = HashMap::new();
    for item_event_hash in item_event_hashes {
        item_id_hash_map
            .entry(item_event_hash.get_item_id().to_string())
            .or_insert(item_event_hash.hash);
    }

    Ok(item_id_hash_map)
}
