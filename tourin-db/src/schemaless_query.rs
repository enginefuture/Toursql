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
 * @Date: 2024-01-15 12:53:48
 * @LastEditors: Tab Shi tabshi@outlook.com
 * @LastEditTime: 2024-01-16 17:20:13
 * @FilePath: \Toursql\tourin-db\src\schemaless_query.rs
 * @Description:
 *
 * Copyright (c) 2024 by ${git_name_email}, All Rights Reserved.
 */

use crate::{execute_sql_db, SLEDDB};
use gluesql::prelude::Payload;
use serde_json::Value;
use snowflaked::Generator;

pub async fn insert_schess(
    table: &str,
    mut form: Value,
    db: SLEDDB,
) -> anyhow::Result<(u64, Vec<Payload>)> {
    let mut generator = Generator::new(1);
    let id: u64 = generator.generate();

    if let Some(obj) = form.as_object_mut() {
        if obj.contains_key("id") {
            return Err(anyhow::anyhow!("form already has an 'id' field"));
        }
        obj.insert("id".to_string(), Value::from(id));
    } else {
        return Err(anyhow::anyhow!("form is not object"));
    }

    let value_str = serde_json::to_string(&form).unwrap();
    let sql = format!("INSERT INTO {} VALUES ('{}');", table, value_str);
    let output = execute_sql_db(sql, Some(db)).await;

    // 修改这里，以便在成功的情况下返回 id
    output
        .map(|payload| (id, payload))
        .map_err(|err| anyhow::anyhow!("GlueSQL error: {:?}", err))
}
