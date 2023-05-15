# Toursql
```rut

use gluesql::prelude::{ Payload};
pub trait Selectable<T>: Sized {
    fn select(&self) -> String;
    fn delete(&self) -> String;
    fn update(&self, updates: ::std::collections::HashMap<String, String>) -> Result<String, String>;
    fn from_payload(payload: &Payload) -> T;
    fn union_str(&self) -> std::collections::HashMap<String, Vec<String>>;
}
```
