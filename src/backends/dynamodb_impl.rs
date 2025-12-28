use std::collections::HashMap;

use anyhow::Context;
use aws_sdk_dynamodb::types::WriteRequest;
use bytes::Bytes;

#[derive(Clone)]
pub struct DynamodbDistributedCache {
    pub table_name: String,
    pub client: aws_sdk_dynamodb::Client,
}

impl DynamodbDistributedCache {
    pub fn new(
        table_name: impl Into<String>,
        aws_config: &aws_config::SdkConfig,
    ) -> anyhow::Result<Self> {
        let client = aws_sdk_dynamodb::Client::new(aws_config);

        Ok(Self {
            table_name: table_name.into(),
            client,
        })
    }
}

const DYNAMODB_CACHE_KEY_ATTR: &str = "CacheKey";
const DYNAMODB_CACHE_VALUE_ATTR: &str = "CacheValue";

#[async_trait::async_trait]
impl crate::DistributedCache for DynamodbDistributedCache {
    async fn cache_bytes(&self, key: &str, item: &[u8]) -> anyhow::Result<()> {
        let key_cloned = key.to_string();
        let item_cloned = item.to_vec();

        self.client
            .put_item()
            .table_name(&self.table_name)
            .item(
                DYNAMODB_CACHE_KEY_ATTR,
                aws_sdk_dynamodb::types::AttributeValue::S(key_cloned),
            )
            .item(
                DYNAMODB_CACHE_VALUE_ATTR,
                aws_sdk_dynamodb::types::AttributeValue::B(item_cloned.into()),
            )
            .send()
            .await?;

        Ok(())
    }

    async fn retrieve_bytes(&self, key: &str) -> anyhow::Result<Bytes> {
        let item = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key(
                DYNAMODB_CACHE_KEY_ATTR,
                aws_sdk_dynamodb::types::AttributeValue::S(key.to_string()),
            )
            .send()
            .await?
            .item
            .ok_or_else(|| anyhow::anyhow!("DynamodbDistributedCache: key not found"))?;

        item.get(DYNAMODB_CACHE_VALUE_ATTR)
            .and_then(|attr| attr.as_b().ok())
            .map(|b| Bytes::from(b.as_ref().to_vec()))
            .ok_or_else(|| anyhow::anyhow!("DynamodbDistributedCache: value not found"))
    }
}

#[async_trait::async_trait]
impl crate::BatchingDistributedCache for DynamodbDistributedCache {
    async fn cache_batch<'a, I>(&self, items: I) -> anyhow::Result<()>
    where
        I: IntoIterator<Item = (&'a str, Bytes)> + Send,
    {
        let items: anyhow::Result<Vec<WriteRequest>> = items
            .into_iter()
            .map(|(k, v)| {
                Ok(aws_sdk_dynamodb::types::WriteRequest::builder()
                    .put_request(
                        aws_sdk_dynamodb::types::PutRequest::builder()
                            .item(
                                DYNAMODB_CACHE_KEY_ATTR,
                                aws_sdk_dynamodb::types::AttributeValue::S(k.to_string()),
                            )
                            .item(
                                DYNAMODB_CACHE_VALUE_ATTR,
                                aws_sdk_dynamodb::types::AttributeValue::B(v.to_vec().into()),
                            )
                            .build()
                            .context("Failed to build put request")?,
                    )
                    .build())
            })
            .collect();

        self.client
            .batch_write_item()
            .request_items(self.table_name.clone(), items?)
            .send()
            .await?;

        Ok(())
    }

    async fn retrieve_batch<'a, I>(&self, keys: I) -> anyhow::Result<Vec<(&'a str, Bytes)>>
    where
        I: IntoIterator<Item = &'a str> + Send + Clone,
    {
        let keys_vec: Vec<String> = keys.clone().into_iter().map(str::to_string).collect();

        let keys_attr_values: Vec<HashMap<String, aws_sdk_dynamodb::types::AttributeValue>> =
            keys_vec
                .iter()
                .map(|k| {
                    [(
                        DYNAMODB_CACHE_KEY_ATTR.to_string(),
                        aws_sdk_dynamodb::types::AttributeValue::S(k.clone()),
                    )]
                    .into_iter()
                    .collect::<HashMap<_, _>>()
                })
                .collect();

        let resp = self
            .client
            .batch_get_item()
            .request_items(
                self.table_name.clone(),
                aws_sdk_dynamodb::types::KeysAndAttributes::builder()
                    .set_keys(Some(keys_attr_values))
                    .build()?,
            )
            .send()
            .await?;

        let table_responses = resp
            .responses
            .as_ref()
            .and_then(|responses| responses.get(&self.table_name))
            .context("No responses for the specified table")?;

        Ok(keys
            .into_iter()
            .filter_map(|k| {
                table_responses.iter().find_map(|item| {
                    let key_attr = item.get(DYNAMODB_CACHE_KEY_ATTR)?;
                    let key_str = key_attr.as_s().ok()?;
                    if key_str == k {
                        let value_attr = item.get(DYNAMODB_CACHE_VALUE_ATTR)?;
                        let value_bytes = value_attr.as_b().ok()?;
                        return Some((k, Bytes::from(value_bytes.as_ref().to_vec())));
                    }
                    None
                })
            })
            .collect())
    }
}
