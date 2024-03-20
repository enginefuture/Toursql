pub mod left_join;
pub mod schemaless_query;
pub mod traits;
pub mod utils;
pub use crate::utils::process_payload;
use gluesql::prelude::{Value as Gvalue, *};
pub use left_join::*;
pub use schemaless_query::insert_schess;
use std::collections::HashMap;
use traits::Selectable;

pub type SLEDDB = Glue<SledStorage>;

pub async fn execute_sql_db(sql: String, db: Option<SLEDDB>) -> Result<Vec<Payload>, Error> {
    let mut glue = db.unwrap();
    let output = glue.execute(sql).await;
    output
}

//根据Id删除数据
pub fn delete_from_id(table_name: &str, ids: &[u64]) -> String {
    let id_list = ids
        .iter()
        .map(|id| id.to_string())
        .collect::<Vec<String>>()
        .join(", ");
    format!("DELETE FROM {} WHERE id IN ({});", table_name, id_list)
}

//获取一条数据

pub async fn count_table<T, U>(
    builder: T,
    condition: Option<HashMap<&str, String>>,
    db: Option<SLEDDB>,
) -> Result<i64, String>
where
    T: Selectable<U> + Send + Sync,
{
    let mut sql = builder.count();
    if let Some(condition_map) = condition {
        let condition_strings: Vec<String> = condition_map
            .iter()
            .map(|(key, value)| format!(" {}='{}' ", key, value))
            .collect();
        let joined_condition = condition_strings.join("AND");
        sql = format!("{} WHERE{}", sql, joined_condition);
    } else {
        sql = format!("{};", sql,);
    }
    // println!("count sql:{}",sql);
    let output = execute_sql_db(sql, db).await;
    // println!("count output:{:?}",output);
    match output {
        Ok(output) => {
            if let Payload::Select { rows, .. } = &output[0] {
                if rows.is_empty() {
                    return Ok(0);
                } else {
                    // We only care about the first row, as COUNT(*) only returns one value.
                    if let Gvalue::I64(count) = rows[0][0] {
                        return Ok(count);
                    }
                    Err("Failed to retrieve count".to_string())
                }
            } else {
                return Err("未知错误".to_string());
            }
        }
        Err(_) => {
            return Err("查询错误".to_string());
        }
    }
}

pub async fn get_list_page<T, U>(
    builder: T,
    page: u32,
    page_size: u32,
    order: &str,
    condition: Option<HashMap<&str, String>>,
    db: Option<SLEDDB>,
) -> Result<Vec<U>, String>
where
    T: Selectable<U> + Send + Sync,
{
    let offset = (page - 1) * page_size;
    let mut sql = format!("{}", builder.select());
    // let mut sql = format!("{} ORDER BY {} DESC LIMIT {} OFFSET {}", builder.select(), order, page_size, offset);
    if let Some(condition_map) = condition {
        let condition_strings: Vec<String> = condition_map
            .iter()
            .map(|(key, value)| format!(" {}='{}' ", key, value))
            .collect();
        let joined_condition = condition_strings.join("AND");
        sql = format!(
            "{} WHERE{}ORDER BY {} DESC LIMIT {} OFFSET {}",
            sql, joined_condition, order, page_size, offset
        );
    } else {
        sql = format!(
            "{} ORDER BY {} DESC LIMIT {} OFFSET {}",
            sql, order, page_size, offset
        );
    }
    // println!("sql----{:?}", sql);
    let output = execute_sql_db(sql, db).await;
    // println!("output----{:?}", output);
    match output {
        Ok(output) => {
            if let Payload::Select { labels, rows, .. } = &output[0] {
                if rows.is_empty() {
                    return Err("表没有数据".to_string());
                } else {
                    let mut list: Vec<U> = Vec::new();
                    for row in rows.iter() {
                        let payload = Payload::Select {
                            labels: labels.clone(),
                            rows: vec![row.clone()],
                        };
                        list.push(T::from_payload(&payload));
                    }
                    Ok(list)
                }
            } else {
                return Err("未知错误".to_string());
            }
        }
        Err(_) => {
            return Err("查询错误".to_string());
        }
    }
}

pub async fn get_list_common<T, U>(builder: T, db: Option<SLEDDB>) -> Result<Vec<U>, String>
where
    T: Selectable<U> + Send + Sync,
{
    let sql = builder.select();
    let output = execute_sql_db(sql, db).await;
    match output {
        Ok(output) => {
            if let Payload::Select { labels, rows, .. } = &output[0] {
                if rows.is_empty() {
                    return Err("表没有数据".to_string());
                } else {
                    let mut list: Vec<U> = Vec::new();
                    for row in rows.iter() {
                        let payload = Payload::Select {
                            labels: labels.clone(),
                            rows: vec![row.clone()],
                        };
                        list.push(T::from_payload(&payload));
                    }
                    Ok(list)
                }
            } else {
                return Err("未知错误".to_string());
            }
        }
        Err(_) => {
            return Err("查询错误".to_string());
        }
    }
}

pub async fn get_struct_list<T>(sql: String, db: Option<SLEDDB>) -> Result<Vec<T>, String>
where
    T: for<'a> From<&'a Payload> + Send + Sync,
{
    let sql = sql;
    let output = execute_sql_db(sql, db).await;
    match output {
        Ok(output) => {
            if let Payload::Select { labels, rows, .. } = &output[0] {
                if rows.is_empty() {
                    return Ok(Vec::new());
                } else {
                    let mut list = Vec::new();
                    for row in rows.iter() {
                        let payload = Payload::Select {
                            labels: labels.clone(),
                            rows: vec![row.clone()],
                        };
                        list.push(T::from(&payload));
                    }
                    Ok(list)
                }
            } else {
                return Err("未知错误".to_string());
            }
        }
        Err(_) => {
            return Err("查询错误".to_string());
        }
    }
}

#[macro_export]
macro_rules! generate_fields_and_inner_join {
    ($b:ident, $alias:literal) => {{
        let b_union_str = $b.union_str();
        let (b_key, b_value) = b_union_str.iter().next().unwrap();
        let b_fields = b_value
            .iter()
            .map(|field| {
                let field_without_prefix =
                    field.strip_prefix(format!("{}.", b_key).as_str()).unwrap();
                format!("{}.{} as '{}'", $alias, field_without_prefix, field)
            })
            .collect::<Vec<String>>()
            .join(", ");
        let b_part_inner = format!(
            "LEFT OUTER JOIN {} as {} ON a.{}_id = {}.id",
            b_key, $alias, b_key, $alias
        );
        let b_fileds_inner = format!(", {}", b_fields);
        (b_part_inner, b_fileds_inner, b_key.clone())
    }};
}

pub fn convert_glue_value_to_serde_value(value: &gluesql::prelude::Value) -> serde_json::Value {
    match value {
        gluesql::prelude::Value::Str(s) => serde_json::Value::String(s.to_owned()),
        gluesql::prelude::Value::I64(i) => serde_json::Value::Number(serde_json::Number::from(*i)),
        gluesql::prelude::Value::F64(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        gluesql::prelude::Value::List(list) => {
            if list
                .iter()
                .all(|item| matches!(item, gluesql::prelude::Value::Map(_)))
            {
                serde_json::Value::Array(
                    list.iter()
                        .map(|item| match item {
                            gluesql::prelude::Value::Map(map) => {
                                let value_map: serde_json::Map<String, serde_json::Value> = map
                                    .iter()
                                    .map(|(key, value)| {
                                        (key.clone(), convert_glue_value_to_serde_value(value))
                                    })
                                    .collect();
                                serde_json::Value::Object(value_map)
                            }
                            _ => unreachable!(),
                        })
                        .collect(),
                )
            } else {
                serde_json::Value::Array(
                    list.iter()
                        .map(|value| convert_glue_value_to_serde_value(value))
                        .collect(),
                )
            }
        }
        gluesql::prelude::Value::Map(map) => {
            let value_map: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .map(|(key, value)| (key.clone(), convert_glue_value_to_serde_value(value)))
                .collect();
            serde_json::Value::Object(value_map)
        }
        gluesql::prelude::Value::Bool(b) => serde_json::Value::Bool(*b),
        gluesql::prelude::Value::Null => serde_json::Value::Null,
        _ => {
            println!(
                "convert_glue_value_to_serde_value Unprocessed type: {:?}",
                value
            );
            serde_json::Value::Null
        }
    }
}

pub fn convert_glue_map_to_serde_map(
    glue_map: HashMap<String, gluesql::prelude::Value>,
) -> HashMap<String, serde_json::Value> {
    glue_map
        .into_iter()
        .map(|(key, glue_value)| (key, convert_glue_value_to_serde_value(&glue_value)))
        .collect()
}
