use super::traits::Selectable;
use crate::SLEDDB;
use crate::{execute_sql_db, generate_fields_and_inner_join};
use gluesql::prelude::{Payload, Value as Gvalue};
use std::collections::HashMap;

#[allow(unused_macros)]
macro_rules! generate_fields_and_left_join {
    ($b:ident,$a:ident, $alias:literal) => {{
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
            "LEFT OUTER JOIN {} as {} ON {}.{}_id = a.id",
            b_key, $alias, $alias, $a
        );
        let b_fileds_inner = format!(", {}", b_fields);
        (b_part_inner, b_fileds_inner, b_key.clone())
    }};
}

pub async fn many_one_4_page<A, C, D, T, U1, B1, U2, B2, U3, B3, U4, B4>(
    a: A,
    b1: B1,
    b2: B2,
    b3: B3,
    b4: B4,
    c: C,
    id: Option<u64>,
    order: &str,
    page: u32,
    page_size: u32,
    condition: Option<HashMap<String, String>>,
    db: Option<SLEDDB>,
) -> Result<Vec<D>, String>
where
    A: Selectable<T> + Send + Sync,
    B1: Selectable<U1> + Send + Sync,
    B2: Selectable<U2> + Send + Sync,
    B3: Selectable<U3> + Send + Sync,
    B4: Selectable<U4> + Send + Sync,
    C: Selectable<D> + Send + Sync,
{
    let offset = (page - 1) * page_size;

    let a_union_str = a.union_str();
    let (b1_part_inner, b1_fileds_inner, b1_key) = generate_fields_and_inner_join!(b1, "b1");
    let (b2_part_inner, b2_fileds_inner, b2_key) = generate_fields_and_inner_join!(b2, "b2");
    let (b3_part_inner, b3_fileds_inner, b3_key) = generate_fields_and_inner_join!(b3, "b3");
    let (b4_part_inner, b4_fileds_inner, b4_key) = generate_fields_and_inner_join!(b4, "b4");
    let (a_key, a_value) = a_union_str.iter().next().unwrap();
    let a_fields = a_value
        .iter()
        .map(|field| {
            let field_without_prefix = field.strip_prefix(format!("{}.", a_key).as_str()).unwrap();
            format!("a.{} as '{}'", field_without_prefix, field)
        })
        .collect::<Vec<String>>()
        .join(", ");

    let mut sql = match id {
        Some(id) => format!(
            "SELECT {}{}{}{}{} FROM {} as a {} {} {} {} WHERE a.id ={}",
            a_fields,
            b1_fileds_inner,
            b2_fileds_inner,
            b3_fileds_inner,
            b4_fileds_inner,
            a_key,
            b1_part_inner,
            b2_part_inner,
            b3_part_inner,
            b4_part_inner,
            id
        ),
        None => format!(
            "SELECT {}{}{}{}{} FROM {} as a {} {} {} {}",
            a_fields,
            b1_fileds_inner,
            b2_fileds_inner,
            b3_fileds_inner,
            b4_fileds_inner,
            a_key,
            b1_part_inner,
            b2_part_inner,
            b3_part_inner,
            b4_part_inner
        ),
    };

    if let Some(condition_map) = condition {
        let condition_strings: Vec<String> = condition_map
            .iter()
            .map(|(key, value)| format!(" a.{}='{}' ", key, value))
            .collect();
        let joined_condition = condition_strings.join("AND");
        sql = format!(
            "{} WHERE{}ORDER BY a.{} DESC LIMIT {} OFFSET {}",
            sql, joined_condition, order, page_size, offset
        );
    } else {
        sql = format!(
            "{} ORDER BY a.{} DESC LIMIT {} OFFSET {}",
            sql, order, page_size, offset
        );
    }

    // println!("sql----{:?}", sql);
    let output = execute_sql_db(sql, db).await;
    // println!("output----{:?}", output);
    // let c_union_str = c.union_str();
    // let (c_key, c_value) = c_union_str.iter().next().unwrap();
    let c_union_str = c.union_str();
    let (c_key, c_value) = c_union_str.iter().next().unwrap();
    // println!("c_value----{:?}", c_value);
    // println!("c_key----{:?}", c_key);
    // println!("b1_key----{:?}", b1_key);
    // println!("b2_key----{:?}", b2_key);
    // println!("b3_key----{:?}", b3_key);
    // println!("b4_key----{:?}", b4_key);

    let c_fields = c_value
        .iter()
        .map(|field| {
            // println!("field----{:?}", field);
            let field_without_prefix = field
                .strip_prefix(format!("{}.{}0", c_key, a_key).as_str())
                .or_else(|| field.strip_prefix(format!("{}.{}0", c_key, b1_key).as_str()))
                .or_else(|| field.strip_prefix(format!("{}.{}0", c_key, b2_key).as_str()))
                .or_else(|| field.strip_prefix(format!("{}.{}0", c_key, b3_key).as_str()))
                .or_else(|| field.strip_prefix(format!("{}.{}0", c_key, b4_key).as_str()))
                .unwrap();
            if field.contains(&format!("{}0", a_key)) {
                format!("{}.{}", a_key, field_without_prefix)
            } else if field.contains(&format!("{}0", b1_key)) {
                format!("{}.{}", b1_key, field_without_prefix)
            } else if field.contains(&format!("{}0", b2_key)) {
                format!("{}.{}", b2_key, field_without_prefix)
            } else if field.contains(&format!("{}0", b3_key)) {
                format!("{}.{}", b3_key, field_without_prefix)
            } else {
                format!("{}.{}", b4_key, field_without_prefix)
            }
        })
        .collect::<Vec<String>>()
        .join(", ");
    // println!("c_fields----{}", c_fields);
    match output {
        Ok(output) => {
            if let Payload::Select { labels, rows, .. } = &output[0] {
                // println!("labels----{:?}", labels);
                // println!("rows----{:?}", rows);
                if rows.is_empty() {
                    // 未查询到数据不等于报错
                    // return Err("表没有数据".to_string());
                    return Ok(Vec::new());
                } else {
                    let mut list: Vec<D> = Vec::new();
                    let c_fields_vec: Vec<&str> = c_fields.split(", ").collect();
                    // println!("c_fields_vec----{:?}", c_fields_vec);
                    let c_fields_indices: Vec<usize> = c_fields_vec
                        .iter()
                        .enumerate()
                        .filter_map(|(_, field)| {
                            if let Some(position) = labels.iter().position(|label| label == field) {
                                Some(position)
                            } else {
                                println!("Field not found: {:?}", field);
                                None
                            }
                        })
                        .collect();
                    // println!("c_fields_indices----{:?}", c_fields_indices);
                    for row in rows.iter() {
                        let c_lables: Vec<String> = c_value
                            .iter()
                            .map(|s| s.strip_prefix(&format!("{}.", c_key)).unwrap().to_string())
                            .collect();
                        let selected_values: Vec<Gvalue> = c_fields_indices
                            .iter()
                            .map(|&index| row[index].clone())
                            .collect();
                        // println!("selected_values----{:?}", selected_values);
                        let payload = Payload::Select {
                            labels: c_lables,
                            rows: vec![selected_values],
                        };
                        list.push(C::from_payload(&payload));
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

pub async fn many_one_3_page<A, C, D, T, U1, B1, U2, B2, U3, B3>(
    a: A,
    b1: B1,
    b2: B2,
    b3: B3,
    c: C,
    id: Option<u64>,
    order: &str,
    page: u32,
    page_size: u32,
    condition: Option<HashMap<String, String>>,
    db: Option<SLEDDB>,
) -> Result<Vec<D>, String>
where
    A: Selectable<T> + Send + Sync,
    B1: Selectable<U1> + Send + Sync,
    B2: Selectable<U2> + Send + Sync,
    B3: Selectable<U3> + Send + Sync,
    C: Selectable<D> + Send + Sync,
{
    let offset = (page - 1) * page_size;
    let a_union_str = a.union_str();
    let (b1_part_inner, b1_fileds_inner, b1_key) = generate_fields_and_inner_join!(b1, "b1");
    let (b2_part_inner, b2_fileds_inner, b2_key) = generate_fields_and_inner_join!(b2, "b2");
    let (b3_part_inner, b3_fileds_inner, b3_key) = generate_fields_and_inner_join!(b3, "b3");
    let (a_key, a_value) = a_union_str.iter().next().unwrap();
    let a_fields = a_value
        .iter()
        .map(|field| {
            let field_without_prefix = field.strip_prefix(format!("{}.", a_key).as_str()).unwrap();
            format!("a.{} as '{}'", field_without_prefix, field)
        })
        .collect::<Vec<String>>()
        .join(", ");

    let mut sql = match id {
        Some(id) => format!(
            "SELECT {}{}{}{} FROM {} as a {} {} {} WHERE a.id ={}",
            a_fields,
            b1_fileds_inner,
            b2_fileds_inner,
            b3_fileds_inner,
            a_key,
            b1_part_inner,
            b2_part_inner,
            b3_part_inner,
            id
        ),
        None => format!(
            "SELECT {}{}{}{} FROM {} as a {} {} {}",
            a_fields,
            b1_fileds_inner,
            b2_fileds_inner,
            b3_fileds_inner,
            a_key,
            b1_part_inner,
            b2_part_inner,
            b3_part_inner
        ),
    };

    if let Some(condition_map) = condition {
        let condition_strings: Vec<String> = condition_map
            .iter()
            .map(|(key, value)| format!(" a.{}='{}' ", key, value))
            .collect();
        let joined_condition = condition_strings.join("AND");
        sql = format!(
            "{} WHERE{}ORDER BY a.{} DESC LIMIT {} OFFSET {}",
            sql, joined_condition, order, page_size, offset
        );
    } else {
        sql = format!(
            "{} ORDER BY a.{} DESC LIMIT {} OFFSET {}",
            sql, order, page_size, offset
        );
    }

    // println!("sql----{:?}", sql);
    let output = execute_sql_db(sql, db).await;
    // println!("output----{:?}", output);
    let c_union_str = c.union_str();
    let (c_key, c_value) = c_union_str.iter().next().unwrap();
    // println!("c_value----{:?}", c_value);

    let c_fields = c_value
        .iter()
        .map(|field| {
            let field_without_prefix = field
                .strip_prefix(format!("{}.{}0", c_key, a_key).as_str())
                .or_else(|| field.strip_prefix(format!("{}.{}0", c_key, b1_key).as_str()))
                .or_else(|| field.strip_prefix(format!("{}.{}0", c_key, b2_key).as_str()))
                .or_else(|| field.strip_prefix(format!("{}.{}0", c_key, b3_key).as_str()))
                .unwrap();
            if field.contains(&format!("{}0", a_key)) {
                format!("{}.{}", a_key, field_without_prefix)
            } else if field.contains(&format!("{}0", b1_key)) {
                format!("{}.{}", b1_key, field_without_prefix)
            } else if field.contains(&format!("{}0", b2_key)) {
                format!("{}.{}", b2_key, field_without_prefix)
            } else {
                format!("{}.{}", b3_key, field_without_prefix)
            }
        })
        .collect::<Vec<String>>()
        .join(", ");
    // println!("c_fields----{}", c_fields);
    match output {
        Ok(output) => {
            if let Payload::Select { labels, rows, .. } = &output[0] {
                if rows.is_empty() {
                    return Err("表没有数据".to_string());
                } else {
                    let mut list: Vec<D> = Vec::new();
                    let c_fields_vec: Vec<&str> = c_fields.split(", ").collect();
                    // println!("c_fields_vec----{:?}", c_fields_vec);
                    let c_fields_indices: Vec<usize> = c_fields_vec
                        .iter()
                        .map(|field| labels.iter().position(|label| label == field).unwrap())
                        .collect();
                    // println!("c_fields_indices----{:?}", c_fields_indices);
                    for row in rows.iter() {
                        let c_lables: Vec<String> = c_value
                            .iter()
                            .map(|s| s.strip_prefix(&format!("{}.", c_key)).unwrap().to_string())
                            .collect();
                        let selected_values: Vec<Gvalue> = c_fields_indices
                            .iter()
                            .map(|&index| row[index].clone())
                            .collect();
                        // println!("selected_values----{:?}", selected_values);
                        let payload = Payload::Select {
                            labels: c_lables,
                            rows: vec![selected_values],
                        };
                        list.push(C::from_payload(&payload));
                    }
                    Ok(list)
                }
            } else {
                return Err("未知错误".to_string());
            }
        }
        Err(e) => {
            return Err(e.to_string());
        }
    }
}

pub async fn many_one_1_page<A, C, D, T, U1, B1>(
    a: A,
    b1: B1,
    c: C,
    id: Option<u64>,
    order: &str,
    page: u32,
    page_size: u32,
    condition: Option<HashMap<String, String>>,
    db: Option<SLEDDB>,
) -> Result<Vec<D>, String>
where
    A: Selectable<T> + Send + Sync,
    B1: Selectable<U1> + Send + Sync,
    C: Selectable<D> + Send + Sync,
{
    let offset = (page - 1) * page_size;
    let a_union_str = a.union_str();
    let (b1_part_inner, b1_fileds_inner, b1_key) = generate_fields_and_inner_join!(b1, "b1");
    let (a_key, a_value) = a_union_str.iter().next().unwrap();
    let a_fields = a_value
        .iter()
        .map(|field| {
            let field_without_prefix = field.strip_prefix(format!("{}.", a_key).as_str()).unwrap();
            format!("a.{} as '{}'", field_without_prefix, field)
        })
        .collect::<Vec<String>>()
        .join(", ");

    let mut sql = match id {
        Some(id) => format!(
            "SELECT {}{} FROM {} as a {} WHERE a.id ={}",
            a_fields, b1_fileds_inner, a_key, b1_part_inner, id
        ),
        None => format!(
            "SELECT {}{} FROM {} as a {} ",
            a_fields, b1_fileds_inner, a_key, b1_part_inner,
        ),
    };

    if let Some(condition_map) = condition {
        let condition_strings: Vec<String> = condition_map
            .iter()
            .map(|(key, value)| format!(" a.{}='{}' ", key, value))
            .collect();
        let joined_condition = condition_strings.join("AND");
        sql = format!(
            "{} WHERE{}ORDER BY a.{} DESC LIMIT {} OFFSET {}",
            sql, joined_condition, order, page_size, offset
        );
    } else {
        sql = format!(
            "{} ORDER BY a.{} DESC LIMIT {} OFFSET {}",
            sql, order, page_size, offset
        );
    }

    // println!("sql----{:?}", sql);
    let output = execute_sql_db(sql, db).await;
    // println!("output----{:?}", output);
    let c_union_str = c.union_str();
    let (c_key, c_value) = c_union_str.iter().next().unwrap();
    // println!("c_value----{:?}", c_value);

    let c_fields = c_value
        .iter()
        .map(|field| {
            let field_without_prefix = field
                .strip_prefix(format!("{}.{}0", c_key, a_key).as_str())
                .or_else(|| field.strip_prefix(format!("{}.{}0", c_key, b1_key).as_str()))
                .unwrap();
            if field.contains(&format!("{}0", a_key)) {
                format!("{}.{}", a_key, field_without_prefix)
            } else {
                format!("{}.{}", b1_key, field_without_prefix)
            }
        })
        .collect::<Vec<String>>()
        .join(", ");
    // println!("c_fields----{}", c_fields);
    match output {
        Ok(output) => {
            if let Payload::Select { labels, rows, .. } = &output[0] {
                if rows.is_empty() {
                    return Err("表没有数据".to_string());
                } else {
                    let mut list: Vec<D> = Vec::new();
                    let c_fields_vec: Vec<&str> = c_fields.split(", ").collect();
                    // println!("c_fields_vec----{:?}", c_fields_vec);
                    let c_fields_indices: Vec<usize> = c_fields_vec
                        .iter()
                        .map(|field| labels.iter().position(|label| label == field).unwrap())
                        .collect();
                    // println!("c_fields_indices----{:?}", c_fields_indices);
                    for row in rows.iter() {
                        let c_lables: Vec<String> = c_value
                            .iter()
                            .map(|s| s.strip_prefix(&format!("{}.", c_key)).unwrap().to_string())
                            .collect();
                        let selected_values: Vec<Gvalue> = c_fields_indices
                            .iter()
                            .map(|&index| row[index].clone())
                            .collect();
                        // println!("selected_values----{:?}", selected_values);
                        let payload = Payload::Select {
                            labels: c_lables,
                            rows: vec![selected_values],
                        };
                        list.push(C::from_payload(&payload));
                    }
                    Ok(list)
                }
            } else {
                return Err("未知错误".to_string());
            }
        }
        Err(e) => {
            return Err(e.to_string());
        }
    }
}

pub async fn many_one_1_common<A, C, D, T, U1, B1>(
    a: A,
    b1: B1,
    c: C,
    id: Option<u64>,
    order: &str,
    condition: Option<HashMap<String, String>>,
    db: Option<SLEDDB>,
) -> Result<Vec<D>, String>
where
    A: Selectable<T> + Send + Sync,
    B1: Selectable<U1> + Send + Sync,
    C: Selectable<D> + Send + Sync,
{
    let a_union_str = a.union_str();
    let (b1_part_inner, b1_fileds_inner, b1_key) = generate_fields_and_inner_join!(b1, "b1");
    let (a_key, a_value) = a_union_str.iter().next().unwrap();
    let a_fields = a_value
        .iter()
        .map(|field| {
            let field_without_prefix = field.strip_prefix(format!("{}.", a_key).as_str()).unwrap();
            format!("a.{} as '{}'", field_without_prefix, field)
        })
        .collect::<Vec<String>>()
        .join(", ");

    let mut sql = match id {
        Some(id) => format!(
            "SELECT {}{} FROM {} as a {} WHERE a.id ={}",
            a_fields, b1_fileds_inner, a_key, b1_part_inner, id
        ),
        None => format!(
            "SELECT {}{} FROM {} as a {} ",
            a_fields, b1_fileds_inner, a_key, b1_part_inner,
        ),
    };

    if let Some(condition_map) = condition {
        let condition_strings: Vec<String> = condition_map
            .iter()
            .map(|(key, value)| format!(" a.{}='{}' ", key, value))
            .collect();
        let joined_condition = condition_strings.join("AND");
        sql = format!("{} WHERE{}ORDER BY a.{} DESC", sql, joined_condition, order);
    } else {
        sql = format!("{} ORDER BY a.{} DESC", sql, order);
    }

    // println!("sql----{:?}", sql);
    let output = execute_sql_db(sql, db).await;
    // println!("output----{:?}", output);
    let c_union_str = c.union_str();
    let (c_key, c_value) = c_union_str.iter().next().unwrap();
    // println!("c_value----{:?}", c_value);

    let c_fields = c_value
        .iter()
        .map(|field| {
            let field_without_prefix = field
                .strip_prefix(format!("{}.{}0", c_key, a_key).as_str())
                .or_else(|| field.strip_prefix(format!("{}.{}0", c_key, b1_key).as_str()))
                .unwrap();
            if field.contains(&format!("{}0", a_key)) {
                format!("{}.{}", a_key, field_without_prefix)
            } else {
                format!("{}.{}", b1_key, field_without_prefix)
            }
        })
        .collect::<Vec<String>>()
        .join(", ");
    // println!("c_fields----{}", c_fields);
    match output {
        Ok(output) => {
            if let Payload::Select { labels, rows, .. } = &output[0] {
                if rows.is_empty() {
                    return Ok(Vec::<D>::new());
                } else {
                    let mut list: Vec<D> = Vec::new();
                    let c_fields_vec: Vec<&str> = c_fields.split(", ").collect();
                    // println!("c_fields_vec----{:?}", c_fields_vec);
                    let c_fields_indices: Vec<usize> = c_fields_vec
                        .iter()
                        .map(|field| labels.iter().position(|label| label == field).unwrap())
                        .collect();
                    // println!("c_fields_indices----{:?}", c_fields_indices);
                    for row in rows.iter() {
                        let c_lables: Vec<String> = c_value
                            .iter()
                            .map(|s| s.strip_prefix(&format!("{}.", c_key)).unwrap().to_string())
                            .collect();
                        let selected_values: Vec<Gvalue> = c_fields_indices
                            .iter()
                            .map(|&index| row[index].clone())
                            .collect();
                        // println!("selected_values----{:?}", selected_values);
                        let payload = Payload::Select {
                            labels: c_lables,
                            rows: vec![selected_values],
                        };
                        list.push(C::from_payload(&payload));
                    }
                    Ok(list)
                }
            } else {
                return Err("未知错误".to_string());
            }
        }
        Err(e) => {
            return Err(e.to_string());
        }
    }
}
