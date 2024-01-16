use serde_json::Value;
use gluesql::prelude::{Payload,Value as GlueValue};
use std::collections::HashMap;

use crate::{convert_glue_value_to_serde_value,convert_glue_map_to_serde_map};


pub async fn transform_tuple<'a>(
    prefix: &'a String,
    input: (&'a String, &Vec<String>),
) -> (&'a String, String, Vec<String>) {
    let (c_key, c_value) = input;
    let c_proxy_key = prefix.to_string();

    let c_proxy_value = c_value
        .into_iter()
        .filter_map(|field| {
            let field_without_prefix = field.strip_prefix(format!("{}.{}0", c_key, prefix).as_str())?;
            Some(field_without_prefix.to_string())
        })
        .collect::<Vec<String>>();

    (c_key, c_proxy_key, c_proxy_value)
}


pub fn convert_serde_value_to_glue_value(value: &Value) -> GlueValue {
    match value {
        Value::String(s) => GlueValue::Str(s.to_owned()),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                GlueValue::I64(i)
            } else if let Some(f) = n.as_f64() {
                GlueValue::F64(f)
            } else {
                GlueValue::Null
            }
        }
        Value::Bool(b) => GlueValue::Bool(*b),
        Value::Array(arr) => {
            GlueValue::List(arr.iter().map(|value| convert_serde_value_to_glue_value(value)).collect())
        }
        Value::Object(obj) => {
            let glue_map: HashMap<String, GlueValue> = obj
                .iter()
                .map(|(key, value)| (key.clone(), convert_serde_value_to_glue_value(value)))
                .collect();
            GlueValue::Map(glue_map)
        }
        _ => GlueValue::Null,
    }
}

pub fn convert_serde_values_to_glue_list_map(values: Vec<Value>) -> GlueValue {
    let glue_values = values.into_iter().map(|value| convert_serde_value_to_glue_value(&value)).collect();
    GlueValue::List(glue_values)
}


fn values_equal(a: &gluesql::prelude::Value, b: &gluesql::prelude::Value) -> bool {
    match (a, b) {
        (gluesql::prelude::Value::Null, gluesql::prelude::Value::Null) => true,
        _ => a == b,
    }
}


pub fn compare_payload_rows(payload1: &Payload, payload2: &Payload) -> bool {
    match (payload1, payload2) {
        (
            Payload::Select { labels: _, rows: rows1 },
            Payload::Select { labels: _, rows: rows2 },
        ) => {
            if rows1.len() != rows2.len() {
                return false;
            }

            for (row1, row2) in rows1.iter().zip(rows2.iter()) {
                let values1 = &row1;
                let values2 = &row2;

                if values1.len() != values2.len() {
                    return false;
                }

                for (value1, value2) in values1.iter().zip(values2.iter()) {
                    if !values_equal(value1, value2) {
                        return false;
                    }
                }
            }

            true
        }
        _ => false,
    }
}

pub fn process_payload(payload: &Payload) -> Vec<HashMap<String, Value>> {
    if let Payload::Select { labels, rows } = payload {
        let mut result = Vec::new();

        for row in rows {
            let mut item = HashMap::new();

            for (idx, gvalue) in row.into_iter().enumerate() {
                let key = &labels[idx];
                let value = convert_glue_value_to_serde_value(gvalue);
                item.insert(key.clone(), value);
            }

            result.push(item);
        }

        result
    } else {
        panic!("Unsupported payload type");
    }
}


pub fn process_payload_select_map(payload: &Payload) -> Vec<HashMap<String, Value>> {
    // println!("process_payload_select_map: {:?}", payload);
    if let Payload::SelectMap (value)  = payload {
        let mut result = Vec::new();

        for value_map in value{
            let conver_value = convert_glue_map_to_serde_map(value_map.clone());
            result.push(conver_value);
        }
        result
    } else {
        panic!("Unsupported payload type");
    }
}