use item_read::item_hash::{
    get_item_event_hashes_by_source_id, get_item_event_hashes_map_by_source_id,
    get_latest_item_event_hash_map_by_source_id,
};
use test_api::localstack::get_dynamodb_client;
use test_api::test_api_macros::blitzfilter_dynamodb_test;

#[blitzfilter_dynamodb_test]
async fn should_return_all_item_event_hashes_for_given_source_id() {
    let item_events_res = get_item_event_hashes_by_source_id(
        "https://a1militaria.com",
        false,
        get_dynamodb_client().await,
    )
    .await;
    assert!(item_events_res.is_ok());
    let item_events = item_events_res.unwrap();

    assert_eq!(item_events.len(), 11);
}

#[blitzfilter_dynamodb_test]
async fn should_return_all_item_event_hashes_for_given_source_id_as_map() {
    let item_events_res = get_item_event_hashes_map_by_source_id(
        "https://a1militaria.com",
        get_dynamodb_client().await,
    )
    .await;
    assert!(item_events_res.is_ok());
    let item_events = item_events_res.unwrap();

    assert_eq!(item_events.values().flatten().count(), 11);
}

#[blitzfilter_dynamodb_test]
async fn should_return_all_item_event_hashes_for_given_source_id_as_map_sorted_by_latest() {
    let hashes_res = get_item_event_hashes_map_by_source_id(
        "https://a1militaria.com",
        get_dynamodb_client().await,
    )
    .await;
    assert!(hashes_res.is_ok());
    let hashes = hashes_res.unwrap();

    let specific_hashes_opt = hashes.get("https://a1militaria.com#50109");
    assert!(specific_hashes_opt.is_some());
    let actual = specific_hashes_opt.unwrap();
    let expected = &vec![
        "3f4b6790c333fe109d802ff327f4e450fffd85977e4cb053cec841241e0a8c17".to_string(),
        "aebef34f2900811878bc9660b60dac8ad6976db572fd4a44021480f65299475f".to_string(),
        "b3645927df0c1e80484cda90192467a50b05e5a74777f1724b3dfc47e5ae3cc3".to_string(),
    ];

    assert_eq!(actual, expected);
}

#[blitzfilter_dynamodb_test]
async fn should_return_latest_item_event_hash_for_given_source_id_as_map() {
    let latest_hashes_res = get_latest_item_event_hash_map_by_source_id(
        "https://a1militaria.com",
        get_dynamodb_client().await,
    )
    .await;
    assert!(latest_hashes_res.is_ok());
    let latest_hashes = latest_hashes_res.unwrap();

    let actual_opt = latest_hashes.get("https://a1militaria.com#50388");
    assert!(actual_opt.is_some());
    let actual = actual_opt.unwrap();

    let expected = "03ca94cc5fa51f006000cb5446c8f7cca398cd65af2d8cbe2c2e151c64d6b26b";
    assert_eq!(actual, expected);
}
