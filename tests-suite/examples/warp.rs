/*
 *           佛曰:
 *                   写字楼里写字间，写字间里程序员；
 *                   程序人员写程序，又拿程序换酒钱。
 *                   酒醒只在网上坐，酒醉还来网下眠；
 *                   酒醉酒醒日复日，网上网下年复年。
 *                   但愿老死电脑间，不愿鞠躬老板前；
 *                   奔驰宝马贵者趣，公交自行程序员。
 *                   别人笑我忒疯癫，我笑自己命太贱；
 *                   不见满街漂亮妹，哪个归得程序员？
 *
 * @Author: tabshi
 * @Date: 2024-01-16 17:30:28
 * @LastEditors: Tab Shi tabshi@outlook.com
 * @LastEditTime: 2024-01-16 18:05:44
 * @FilePath: \Toursql\tests-suite\examples\warp.rs
 * @Description:
 *
 * Copyright (c) 2024 by ${git_name_email}, All Rights Reserved.
 */

use serde::{Deserialize, Serialize};
use test_suite::TEST_DB;
use tourin_db::{execute_sql_db, traits::Selectable};
use tourin_derive::TourSql;
use warp::Filter;

#[derive(TourSql, Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Person {
    name: String,
    age: u64,
}

#[tokio::main]
async fn main() {
    let create_table_sql = Person::create_table();
    match execute_sql_db(create_table_sql.to_string(), Some(TEST_DB)).await {
        Ok(_) => {}
        Err(e) => {
            if e.contains("table already exists") {
                println!("Database already exists, skipping initialization");
            } else {
                println!("Failed to initialize database: {}", e);
            }
        }
    }

    let routes = warp::any().map(|| "Hello, World!");

    warp::serve(routes).run(([127, 0, 0, 1], 3030)).await;
}
