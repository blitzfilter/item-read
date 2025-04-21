use item_core::item_model::ItemModel;
use item_core::item_state::ItemState;
use item_read::item::{get_item_events_by_item_id, get_item_events_by_item_id_sort_latest, get_item_events_by_item_id_sort_oldest, get_materialized_item};
use test_api::dynamodb::get_client;
use test_api::test_api_macros::blitzfilter_dynamodb_test;

#[blitzfilter_dynamodb_test]
async fn should_materialize_item_1() {
    let item_res = get_materialized_item("https://a1militaria.com#50109", get_client().await).await;
    assert!(item_res.is_ok());
    let item_opt = item_res.unwrap();
    assert!(item_opt.is_some());
    let actual = item_opt.unwrap();

    let expected = ItemModel {
        item_id: "https://a1militaria.com#50109".to_string(),
        created: Some("2025-04-20T21:28:44.798902994Z".to_string()),
        source_id: Some("https://a1militaria.com".to_string()),
        event_id: Some("https://a1militaria.com#50109#2025-04-20T21:28:44.798902994Z".to_string()),
        state: Some(ItemState::SOLD),
        price: Some(160f32),
        category: None,
        name_en: Some("75th (Stirlingshire) Regiment O/Rs badge pre 1881".to_string()),
        description_en: Some("This is genuine example of this much copied badge. It is Bloomer 205. Nicely polished to high points and two good lugs to rear. The 75th were raised in 1787 by Robert Abecromby,They first saw action in India, fighting at Seringapatam and Mysore. (Hence the Royal Tiger in the badge).During the Napoleonic Wars the 75th were stationed in the Mediterranean. Later, during the colonial period they se...".to_string()),
        name_de: None,
        description_de: None,
        url: Some("https://a1militaria.com/shop.php?code=50109".to_string()),
        image_url: Some("https://a1militaria.com/photos/50109.jpg".to_string()),
        hash: Some("3f4b6790c333fe109d802ff327f4e450fffd85977e4cb053cec841241e0a8c17".to_string())
    };

    assert_eq!(actual, expected);
}

#[blitzfilter_dynamodb_test]
async fn should_materialize_item_2() {
    let item_res = get_materialized_item("https://a1militaria.com#50388", get_client().await).await;
    assert!(item_res.is_ok());
    let item_opt = item_res.unwrap();
    assert!(item_opt.is_some());
    let actual = item_opt.unwrap();

    let expected = ItemModel {
        item_id: "https://a1militaria.com#50388".to_string(),
        created: Some("2025-04-22T21:28:44.803674239Z".to_string()),
        source_id: Some("https://a1militaria.com".to_string()),
        event_id: Some("https://a1militaria.com#50388#2025-04-22T21:28:44.803674239Z".to_string()),
        state: Some(ItemState::RESERVED),
        price: Some(37f32),
        category: None,
        name_en: Some("My collection of 8th (Kings) Irish Hussars Senior NCOs Silver Sleeve Badges".to_string()),
        description_en: Some("I have a collection of 6  8th Irish Hussars Sleeve Badges all of which have design differences, One in solid silver, two which are either plated or silver, 3 others in Silver plate. All with Kings Crown. I am selling these individually but would prefer to sell them as a collection so if you want to make an offer for all 6 please let me know and I will reduce the price by 15%".to_string()),
        name_de: None,
        description_de: None,
        url: Some("https://a1militaria.com/shop.php?code=50388".to_string()),
        image_url: Some("https://a1militaria.com/photos/50388.jpg".to_string()),
        hash: Some("03ca94cc5fa51f006000cb5446c8f7cca398cd65af2d8cbe2c2e151c64d6b26b".to_string())
    };

    assert_eq!(actual, expected);
}

#[blitzfilter_dynamodb_test]
async fn should_return_all_item_events_for_given_id() {
    let items_res =
        get_item_events_by_item_id("https://a1militaria.com#50388", false, get_client().await)
            .await;
    assert!(items_res.is_ok());
    let items = items_res.unwrap();

    assert_eq!(items.len(), 5);
}

#[blitzfilter_dynamodb_test]
async fn should_return_all_item_events_for_given_id_sorted_by_latest() {
    let items_res =
        get_item_events_by_item_id_sort_latest("https://a1militaria.com#50388", get_client().await)
            .await;
    assert!(items_res.is_ok());
    let items = items_res.unwrap();

    assert_eq!(items.len(), 5);
    
    let latest_opt = items.get(0);
    assert!(latest_opt.is_some());
    let latest = latest_opt.unwrap();
    assert_eq!(latest.clone().created.unwrap(), "2025-04-22T21:28:44.803674239Z");
}

#[blitzfilter_dynamodb_test]
async fn should_return_all_item_events_for_given_id_sorted_by_oldest() {
    let items_res =
        get_item_events_by_item_id_sort_oldest("https://a1militaria.com#50388", get_client().await)
            .await;
    assert!(items_res.is_ok());
    let items = items_res.unwrap();

    assert_eq!(items.len(), 5);

    let latest_opt = items.get(0);
    assert!(latest_opt.is_some());
    let latest = latest_opt.unwrap();
    assert_eq!(latest.clone().created.unwrap(), "2025-04-18T21:28:44.803674239Z");
}