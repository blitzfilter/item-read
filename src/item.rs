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
    let item_events = get_item_events_by_item_id_sort_oldest(item_id, ddb_client).await?;

    if item_events.is_empty() {
        Ok(None)
    } else {
        let mut party_id = None;
        let mut created = None;
        let mut event_id = None;
        let mut state = None;
        let mut price = None;
        let mut category = None;
        let mut name_en = None;
        let mut description_en = None;
        let mut name_de = None;
        let mut description_de = None;
        let mut url = None;
        let mut image_url = None;

        for event in item_events {
            party_id = party_id.or(event.party_id);
            created = created.or(event.created);
            event_id = event_id.or(event.event_id);
            state = state.or(event.state);
            price = price.or(event.price);
            category = category.or(event.category);
            name_en = name_en.or(event.name_en);
            description_en = description_en.or(event.description_en);
            name_de = name_de.or(event.name_de);
            description_de = description_de.or(event.description_de);
            url = url.or(event.url);
            image_url = image_url.or(event.image_url);
        }

        Ok(Some(ItemModel {
            item_id: item_id.to_string(),
            party_id,
            created,
            event_id,
            state,
            price,
            category,
            name_en,
            description_en,
            name_de,
            description_de,
            url,
            image_url,
            hash: None,
        }))
    }
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

async fn get_item_events_by_item_id(
    item_id: &str,
    sort_latest: bool,
    ddb_client: &dynamo_db::Client,
) -> Result<Vec<ItemModel>, SdkError<QueryError, HttpResponse>> {
    let mut item_events = Vec::new();

    let paginator = ddb_client
        .query()
        .table_name("items")
        .key_condition_expression("#pk = :pk_val AND begins_with(#sk, :sk_prefix)")
        .expression_attribute_names("#pk", "pk")
        .expression_attribute_names("#sk", "sk")
        .expression_attribute_values(":pk_val", AttributeValue::S(item_id.to_string()))
        .expression_attribute_values(":sk_prefix", AttributeValue::S("item#".to_string()))
        .scan_index_forward(!sort_latest)
        .into_paginator()
        .items()
        .send();

    tokio::pin!(paginator);
    while let Some(item) = paginator.next().await {
        match from_item(item?) {
            Ok(parsed) => item_events.push(parsed),
            Err(e) => eprintln!("Deserialization error: {}", e),
        }
    }

    Ok(item_events)
}
