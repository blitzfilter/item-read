use aws_sdk_dynamodb as dynamo_db;
use aws_sdk_dynamodb::config::http::HttpResponse;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::query::QueryError;
use aws_sdk_dynamodb::types::AttributeValue;
use item_core::item_model::ItemModel;
use serde_dynamo::from_item;

pub async fn get_materialized_item(
    item_id: &str,
    ddb_client: &dynamo_db::Client,
) -> Result<Option<ItemModel>, SdkError<QueryError, HttpResponse>> {
    let item_events = get_item_events_by_item_id_sort_latest(item_id, ddb_client).await?;
    Ok(ItemModel::try_from(&item_events[..]).ok())
}

pub async fn get_item_events_by_item_id_sort_latest(
    item_id: &str,
    ddb_client: &dynamo_db::Client,
) -> Result<Vec<ItemModel>, SdkError<QueryError, HttpResponse>> {
    get_item_events_by_item_id(item_id, true, ddb_client).await
}

pub async fn get_item_events_by_item_id_sort_oldest(
    item_id: &str,
    ddb_client: &dynamo_db::Client,
) -> Result<Vec<ItemModel>, SdkError<QueryError, HttpResponse>> {
    get_item_events_by_item_id(item_id, false, ddb_client).await
}

pub async fn get_item_events_by_item_id(
    item_id: &str,
    sort_latest: bool,
    ddb_client: &dynamo_db::Client,
) -> Result<Vec<ItemModel>, SdkError<QueryError, HttpResponse>> {
    let item_events: Vec<ItemModel> = ddb_client
        .query()
        .table_name("items")
        .key_condition_expression("#pk = :pk_val AND begins_with(#sk, :sk_prefix)")
        .expression_attribute_names("#pk", "pk")
        .expression_attribute_names("#sk", "sk")
        .expression_attribute_values(":pk_val", AttributeValue::S(format!("item#{item_id}")))
        .expression_attribute_values(":sk_prefix", AttributeValue::S("item#".to_string()))
        .scan_index_forward(!sort_latest)
        .into_paginator()
        .send()
        .try_collect()
        .await?
        .iter()
        .flat_map(|qo| qo.to_owned().items.unwrap_or_default())
        .filter_map(|item| {
            let model: serde_dynamo::Result<ItemModel> = from_item(item);
            match model {
                Ok(m) => Some(m),
                Err(e) => {
                    eprintln!("Failed to parse item event: {}", e);
                    None
                }
            }
        })
        .collect();

    Ok(item_events)
}
