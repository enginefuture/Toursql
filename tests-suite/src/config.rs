use std::{env, path::PathBuf};

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
 * @Date: 2024-01-16 17:32:47
 * @LastEditors: Tab Shi tabshi@outlook.com
 * @LastEditTime: 2024-01-16 18:06:20
 * @FilePath: \Toursql\tests-suite\src\config.rs
 * @Description:
 *
 * Copyright (c) 2024 by ${git_name_email}, All Rights Reserved.
 */
use gluesql::{
    prelude::Glue,
    sled_storage::{sled, SledStorage},
};
use lazy_static::lazy_static;
use tourin_db::SLEDDB;

//获取项目根目录
pub fn find_project_root() -> Option<PathBuf> {
    let mut current_dir = env::current_dir().expect("查找项目根目录错误");

    while !current_dir.join("workspace_root.txt").exists() {
        if !current_dir.pop() {
            // 已经到达文件系统的根，未找到workspace_root.txt
            return None;
        }
    }

    Some(current_dir)
}

lazy_static! {
    pub static ref TEST_DB: SLEDDB = {
        let root_path = find_project_root()
            .expect("查找项目根目录错误. 请确保 'workspace_root.txt' 在项目根目录.");
        let db_path = root_path.join("data/test");
        let config = sled::Config::default()
            .path(db_path)
            .temporary(true)
            .mode(sled::Mode::HighThroughput);
        let mut storage = SledStorage::try_from(config).expect("SledStorage::new");
        storage.set_transaction_timeout(Some(60000));
        let glue = Glue::new(storage);
        glue
    };
}
