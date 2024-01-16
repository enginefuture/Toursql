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
 * @LastEditTime: 2024-01-16 17:25:27
 * @FilePath: \Toursql\tourin-db\src\traits.rs
 * @Description:
 *
 * Copyright (c) 2024 by ${git_name_email}, All Rights Reserved.
 */

//tourin-derive中已经实现了此trait
use gluesql::prelude::Payload;
pub trait Selectable<T>: Sized {
    fn select(&self) -> String;
    fn delete(&self) -> String;
    fn count(&self) -> String;
    fn update(
        &self,
        updates: ::std::collections::HashMap<String, String>,
    ) -> Result<String, String>;
    fn from_payload(payload: &Payload) -> T;
    fn union_str(&self) -> std::collections::HashMap<String, Vec<String>>;
}
