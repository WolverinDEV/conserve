// Copyright 2023 Martin Pool

#![cfg(feature = "s3-integration-test")]

//! Test s3 transport, only when the `s3-integration-test`
//! feature is enabled.
//!
//! Run this with e.g.
//!
//!     cargo t --features=s3-integration-test --test s3-integration
//!
//! This must be run with AWS credentials available, e.g. in
//! the environment, because it writes to a real temporary bucket.
//!
//! A new bucket is created per test, with object expiry. This test will
//! attempt to delete the bucket when it stops, but this can't be guaranteed.

// This is (currently) written as explicit blocking calls on a runtime
// rather than "real" async, or making use or rstest's async features,
// to be more similar to the code under test.

use ::aws_config::AppName;
use ::aws_types::SdkConfig;
use aws_sdk_s3::Config;
use rand::Rng;
use rstest::{fixture, rstest};
use tokio::runtime::Runtime;

struct TempBucket {
    runtime: Runtime,
    bucket_name: String,
    client: aws_sdk_s3::Client,
}

#[fixture]
#[once]
fn temp_bucket() -> TempBucket {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Create runtime");
    println!("make a bucket");
    let app_name = AppName::new(format!(
        "conserve-s3-integration-test-{}",
        conserve::version()
    ))
    .unwrap();
    let config = runtime.block_on(::aws_config::from_env().app_name(app_name).load());
    let bucket_name = format!(
        "conserve-s3-integration-{:x}",
        rand::thread_rng().gen::<u64>()
    );
    let client = aws_sdk_s3::Client::new(&config);
    let request = client.create_bucket().bucket(&bucket_name).send();
    runtime.block_on(request).expect("Create bucket");
    println!("Created bucket {bucket_name}");
    TempBucket {
        runtime,
        bucket_name,
        client,
    }
}

#[rstest]
fn hello(temp_bucket: &TempBucket) {}
