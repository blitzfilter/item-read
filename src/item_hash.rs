use aws_sdk_dynamodb as dynamo_db;
use aws_sdk_dynamodb::types::AttributeValue;
use item_core::item_hash::ItemEventHash;
use serde_dynamo::from_item;
use std::collections::HashMap;

pub async fn get_all_item_event_hashes(
    source_id: &str,
    ddb_client: &dynamo_db::Client,
) -> Result<Vec<ItemEventHash>, dynamo_db::Error> {
    let mut all_item_event_hashes = Vec::new();

    let paginator = ddb_client
        .query()
        .table_name("items")
        .index_name("gsi_1_hash_index")
        .key_condition_expression("#pk = :pk_val")
        .expression_attribute_names("#pk", "pk")
        .expression_attribute_values(":pk_val", AttributeValue::S(source_id.to_string()))
        .into_paginator()
        .items()
        .send();

    tokio::pin!(paginator);
    while let Some(item) = paginator.next().await {
        match item {
            Ok(attrs) => match from_item(attrs) {
                Ok(parsed) => all_item_event_hashes.push(parsed),
                Err(e) => eprintln!("Deserialization error: {}", e),
            },
            Err(e) => {
                eprintln!("Paginator error: {}", e);
                break;
            }
        }
    }

    Ok(all_item_event_hashes)
}

// vec is sorted by latest (first)
pub async fn get_all_item_event_hashes_map(
    source_id: &str,
    ddb_client: &dynamo_db::Client,
) -> Result<HashMap<String, Vec<String>>, dynamo_db::Error> {
    let item_event_hashes = get_all_item_event_hashes(source_id, ddb_client).await?;
    let mut item_id_hash_map: HashMap<String, Vec<String>> = HashMap::new();
    for item_event_hash in item_event_hashes {
        item_id_hash_map
            .entry(item_event_hash.get_item_id().to_string())
            .or_insert(vec![item_event_hash.hash]);
    }

    Ok(item_id_hash_map)
}

pub async fn get_latest_item_event_hashes_map(
    source_id: &str,
    ddb_client: &dynamo_db::Client,
) -> Result<HashMap<String, String>, dynamo_db::Error> {
    let item_event_hashes = get_all_item_event_hashes(source_id, ddb_client).await?;
    let mut item_id_hash_map: HashMap<String, String> = HashMap::new();
    for item_event_hash in item_event_hashes {
        // assumes hashes for each item to be sorted by latest
        // this is given by the nature of Sort-Key event_id being a ISO-8601 timestamp
        item_id_hash_map
            .entry(item_event_hash.get_item_id().to_string())
            .or_insert(item_event_hash.hash);
    }

    Ok(item_id_hash_map)
}
