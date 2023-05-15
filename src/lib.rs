use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use quote::format_ident;
use syn::{parse_macro_input, Data, DeriveInput,Field, Fields, Ident,Type,PathArguments, GenericArgument,parse_quote};
use std::default::Default;





//判断是否有某个属性
fn has_attribute(field: &Field, attribute_name: &str) -> bool {
    field
        .attrs
        .iter()
        .any(|attr| attr.path.is_ident(attribute_name))
}

//判断是否是数字类型
fn is_numeric(ty: &Type) -> bool {
    let numeric_types = ["i64","i8", "i16", "i32", "i128", "u64", "u8", "f64"];
    let ty_str = ty.to_token_stream().to_string();
    numeric_types.iter().any(|t| ty_str == *t)
}

fn is_option_fn(field: &Field) -> bool {
    let ty = &field.ty;
    match ty {
        Type::Path(type_path) => {
            let segment = &type_path.path.segments.last().unwrap();
            let ident = &segment.ident;
            if ident == "Option" {
                return true;
            }
        }
        _ => {}
    }
    false
}

fn option_for_value<T: Default>(input: Option<T>) -> T {
    match input {
        Some(value) => value,
        None => T::default(),
    }
}

fn is_valid_type(ty: &Type) -> bool {
    match ty {
        Type::Path(type_path) => {
            let segment = &type_path.path.segments.last().unwrap();
            let ident = &segment.ident;

            let valid_primitive_types = [
                "i64","i16", "i8", "i32", "i128", "u64", "u8", "f64", "Decimal", "String", "NaiveDateTime", "bool", "Uuid", "Value",
            ];

            if valid_primitive_types.contains(&ident.to_string().as_str()) {
                return true;
            } else if ident == "Vec" || ident == "Option" || ident == "HashMap" {
                if let PathArguments::AngleBracketed(args) = &segment.arguments {
                    return args.args.iter().all(|arg| {
                        if let GenericArgument::Type(inner_ty) = arg {
                            is_valid_type(inner_ty)
                        } else {
                            false
                        }
                    });
                }
            }
        }
        _ => {}
    }

    false
}


#[proc_macro_derive(TourSql, attributes(primaryKey, notNull,unique,nullable,defaultBool))]
pub fn check_fields_derive(input: TokenStream) -> TokenStream {
    // 解析输入的语法树
    let input = parse_macro_input!(input as DeriveInput);

   

    // 获取结构体的名称和字段信息
    let struct_name = input.ident;
     // 获取结构体字段信息
     let fields = match input.data {
        Data::Struct(s) => s.fields,
        _ => panic!("Only structs are supported"),
    };


    // 遍历结构体字段，获取每个字段的名称和类型
    let field_names: Vec<_> = fields.iter().map(|f| f.ident.as_ref().unwrap()).collect();
    let field_types: Vec<_> = fields.iter().map(|f| &f.ty).collect();
    let mut field_values: Vec<_> = Vec::new();
    



    // 检查每个字段的类型是否正确，并生成字段名的向量和占位符的向量,insert相关
    for field in &fields {
        let ty = &field.ty;
        if !is_valid_type(ty) {
            let name = field.ident.as_ref().unwrap().to_string();
            panic!("Invalid type for field '{}'", name);
        } else {
            // 生成拼接字符串的代码
            let field_name = &field.ident;
            let field_value = match ty {
                syn::Type::Path(type_path) if type_path.path.segments.first().unwrap().ident == "Option" => {
                    let inner_type = match &type_path.path.segments.first().unwrap().arguments {
                        syn::PathArguments::AngleBracketed(inner_type) => inner_type,
                        _ => panic!("Unable to parse inner type of Option"),
                    };
                    let inner_type_ident = inner_type.args.first().unwrap();
    
                    match inner_type_ident {
                        syn::GenericArgument::Type(syn::Type::Path(inner_type_path)) if inner_type_path.path.segments.first().unwrap().ident == "NaiveDateTime" => {
                            quote! { self.#field_name.as_ref().map(|value| format!("{}", value)) }
                        }
                        syn::GenericArgument::Type(syn::Type::Path(inner_type_path)) if inner_type_path.path.segments.first().unwrap().ident == "String" => {
                            quote! { self.#field_name.as_ref().map(|value| format!("\"{}\"", value)) }
                        }
                        syn::GenericArgument::Type(syn::Type::Path(inner_type_path)) if inner_type_path.path.segments.first().unwrap().ident == "Uuid" => {
                            quote! { self.#field_name.as_ref().map(|value| format!("UUID(\"{})\"", value)) }
                        }
                        syn::GenericArgument::Type(syn::Type::Path(inner_type_path)) if inner_type_path.path.segments.first().unwrap().ident == "Vec" => {

                            if let syn::PathArguments::AngleBracketed(angle_bracketed_args) = &inner_type_path.path.segments.first().unwrap().arguments {
                                let inner_type = angle_bracketed_args.args.first().unwrap();
                                if inner_type.to_token_stream().to_string() == "String" {
                                    // 生成适用于 Vec<String> 类型的代码
                                    quote! {
                                        self.#field_name.as_ref().map(|values| {
                                            let mut output = String::new();
                                            for value in values {
                                                output.push_str(&format!("\"{}\",", value));
                                            }
                                            output
                                        })
                                    }
                                } else if inner_type.to_token_stream().to_string() == "Value"{
                                    quote! {
                                        self.#field_name.as_ref().map(|value| format!("'{}'", serde_json::to_string(value).unwrap()))
                                    }
                                }
                                else {
                                    // 生成其他类型的代码
                                    quote! { self.#field_name.as_ref().map(|value| format!("\"{:?}\"", value)) }
                                }
                            }else {
                                // 生成其他类型的代码
                                quote! { self.#field_name.as_ref().map(|value| format!("{:?}", value)) }
                            }
                            // Assuming the inner type is Vec<i8>
                            // quote! { self.#field_name.as_ref().map(|value| format!("\"{:?}\"", value)) }
                        }
                        syn::GenericArgument::Type(syn::Type::Path(inner_type_path)) if inner_type_path.path.segments.first().unwrap().ident == "HashMap" => {
                            // 获取 HashMap 的键和值类型
                            let (key_type, value_type) = if let syn::PathArguments::AngleBracketed(angle_bracketed_args) = &inner_type_path.path.segments.first().unwrap().arguments {
                                let mut types_iter = angle_bracketed_args.args.iter();
                                (types_iter.next().unwrap(), types_iter.next().unwrap())
                            } else {
                                panic!("Unable to parse inner types of HashMap");
                            };
                        
                            // 检查键类型是否为 String，值类型是否为 serde_json::Value
                            if key_type.to_token_stream().to_string() == "String" && value_type.to_token_stream().to_string() == "Value" {
                                // 生成适用于 HashMap<String, serde_json::Value> 类型的代码
                                quote! {
                                    self.#field_name.as_ref().map(|value| format!("'{}'", serde_json::to_string(value).unwrap()))
                                }
                            } else {
                                // 生成其他类型的代码
                                quote! {
                                    self.#field_name.as_ref().map(|value| format!("{:?}", value))
                                }
                            }
                        }
                        _ => quote! { self.#field_name.as_ref().map(|value| value) },
                    }
                }
                syn::Type::Path(type_path) if type_path.path.segments.first().unwrap().ident == "NaiveDateTime" => {
                    quote! { Some(format!("{}", self.#field_name)) }
                }
                syn::Type::Path(type_path) if type_path.path.segments.first().unwrap().ident == "String" => {
                    quote! { Some(format!("\"{}\"", self.#field_name)) }
                }
                syn::Type::Path(type_path) if type_path.path.segments.first().unwrap().ident == "Uuid" => {
                    quote! { Some(format!("UUID\"{}\"", self.#field_name)) }
                }
                syn::Type::Path(type_path) if type_path.path.segments.first().unwrap().ident == "Vec" => {
                    // Assuming the inner type is Vec<i8>
                    quote! { Some(format!("\"{:?}\"", self.#field_name)) }
                }
                syn::Type::Path(type_path) if type_path.path.segments.first().unwrap().ident == "HashMap" => {
                    // Assuming the inner type is HashMap<String, String>
                    quote! {  Some(format!("{:?}", self.#field_name)) }
                }
                _ => quote! { Some(self.#field_name) },
            };
            // 将生成的字段值追加到 field_values 向量中
            field_values.push(field_value);
        }
    }


    

    //创建builder结构体,并生成builder方法，实现动态传参
    let builder_struct = format_ident!("{}Builder", struct_name);
    let builder_methods = quote! {
        #(
            pub fn #field_names(mut self, #field_names: #field_types) -> Self {
                self.#field_names = Some(#field_names);
                self.set_fields.insert(format!("{}", stringify!(#field_names)));
                self
            }
        )*
    };



    //生成表查询字段
    let mut sql_columns = Vec::new();
    //根据结构体字段匹配数据库字段类型
    for field in &fields {
        let field_name = field.ident.as_ref().unwrap().to_string();
        let field_type = &field.ty;
        let field_type_string = quote! { #field_type }.to_string();

        // 检查 Option 类型并提取内部类型
        let (is_option, inner_type_string) = if field_type_string.starts_with("Option") {
            let re = regex::Regex::new(r"Option\s*<\s*(.*)\s*>").unwrap();
            if let Some(caps) = re.captures(&field_type_string) {
                (true, caps[1].to_string().replace(" ", ""))
            } else {
                panic!("Failed to extract inner type from Option");
            }
        } else {
            (false, field_type_string)
        };
        
        let sql_type = if inner_type_string.starts_with("Vec") {
            "List"
        } else if inner_type_string.starts_with("HashMap") {
            "MAP"
        } else {
            match inner_type_string.as_str() {
                "u64" => "INTEGER",
                "i8" => "INT8",
                "String" => "TEXT",
                "bool" => "BOOLEAN",
                "NaiveDateTime" => "TIMESTAMP",
                "Uuid" => "UUID",
                _ => panic!("Unsupported type for SqlTable:{}",inner_type_string),
            }
        };
        let is_primary_key = has_attribute(&field, "primaryKey");
        let is_not_null = has_attribute(&field, "notNull");
        let is_unique = has_attribute(&field, "unique");
        let is_nullable = has_attribute(&field, "nullable");
        let is_op = is_option_fn(&field);
        let is_default_bool = has_attribute(&field, "defaultBool");

        let primary_key = if is_primary_key { " PRIMARY KEY" } else { "" };
        let not_null = if is_not_null { " NOT NULL" } else { "" };
        let unique = if is_unique { " UNIQUE" } else { "" };
        let nullable = if is_nullable { " NULL" } else { "" };
        let op = if is_op { " NULL" } else { "" };
        let default_bool = if is_default_bool { " DEFAULT false" } else { "" };
        sql_columns.push(format!(
            "{} {}{}{}{}{}{}{}",
            field_name, sql_type, primary_key,unique, not_null,nullable,op,default_bool
        ));
        
    }
    //生成创建表的sql语句
    let sql_create = format!(
        "CREATE TABLE {}({});",
        struct_name,
        sql_columns.join(", ")
    );
    
    
    
   //结构体默认值，为builder方法提供默认值
    let default_values: Vec<_> = fields.iter().map(|f| {
        let field_type = &f.ty;
        let field_type_str = field_type.to_token_stream().to_string();
        if field_type_str.starts_with("Option") {
            quote! { None }
        } else {
            match field_type_str.as_str() {
                "i8" => quote! { 0 },
                "i16" => quote! { 0 },
                "i32" => quote! { 0 },
                "i64" => quote! { 0 },
                "i128" => quote! { 0 },
                "u64" => quote! { 0 },
                "u8" => quote! { 0 },
                "f64" => quote! { 0.0 },
                "Decimal" => quote! { "test".to_string() },
                "String" => quote! { "test".to_string() },
                "NaiveDateTime" => quote! { NaiveDateTime::from_timestamp(0, 0)  },
                "bool" => quote! { false },
                "Uuid" => quote! { Uuid::nil() },
                "HashMap" => quote! { "test".to_string() },
                "Vec<i32>" => quote! { vec![0] },
                "Vec<u8>" => quote! { vec![0] },
                "Vec < i8 >" => quote! { vec![0] },
                _ => panic!("Unsupported default value for type '{}'", field_type_str),
            }
        }
    }).collect();

    // let selectable_trait = format_ident!("{}Select", struct_name);


    let field_data: Vec<_> = fields.iter().map(|f| {
        let field_name = f.ident.as_ref().unwrap();
        let field_type = &f.ty;
        let field_type_str = field_type.to_token_stream().to_string();
        (field_name, field_type, field_type_str)
    }).collect();

    let mut u64_fields = Vec::new();
    let mut i8_fields = Vec::new();
    let mut i16_fields = Vec::new();
    let mut i32_fields = Vec::new();
    let mut i128_fields = Vec::new();
    let mut u8_fields = Vec::new();
    let mut f64_fields = Vec::new();
    let mut String_fields = Vec::new();
    let mut NaiveDateTime_fields = Vec::new();
    let mut bool_fields = Vec::new();
    let mut Uuid_fields = Vec::new();
    let mut Option_fields = Vec::new();
    // let mut Option_inner_types = Vec::new();
    // let mut Op

    for field in &fields {
        let ident = field.ident.as_ref().unwrap();
        let ty = &field.ty;

        if let syn::Type::Path(ref type_path) = ty {
            let type_ident = &type_path.path.segments.first().unwrap().ident;
    
            if type_ident == "Option" {
                let inner_type = match &type_path.path.segments.first().unwrap().arguments {
                    syn::PathArguments::AngleBracketed(inner_type) => inner_type,
                    _ => panic!("Unable to parse inner type of Option"),
                };
                if let syn::GenericArgument::Type(syn::Type::Path(ref inner_type_path)) = inner_type.args.first().unwrap() {
                    let inner_type_segment = inner_type_path.path.segments.first().unwrap();
                    let inner_type_ident = inner_type_segment.ident.clone();
    
                    // 检查内部的Vec
                    if inner_type_ident == "Vec" {
                        if let syn::PathArguments::AngleBracketed(inner_vec_type) = &inner_type_segment.arguments {
                            let mut full_type = format!("{}<", inner_type_ident);
                            let inner_vec_type_ident = inner_vec_type.args.first().unwrap();
    
                            if let syn::GenericArgument::Type(inner_vec_type) = inner_vec_type_ident {
                                full_type.push_str(&quote!(#inner_vec_type).to_string());
                            }
    
                            full_type.push_str(">");
                            Option_fields.push((ident, full_type));
                        }
                    } else {
                        Option_fields.push((ident, inner_type_ident.to_string()));
                    }
                }
            } else if type_ident == "u64" {
                u64_fields.push(ident);
            } else if type_ident == "i8" {
                i8_fields.push(ident);
            } else if type_ident == "i16" {
                i16_fields.push(ident);
            } else if type_ident == "i32" {
                i32_fields.push(ident);
            } else if type_ident == "i128" {
                i128_fields.push(ident);
            } else if type_ident == "u8" {
                u8_fields.push(ident);
            } else if type_ident == "f64" {
                f64_fields.push(ident);
            } else if type_ident == "String" {
                String_fields.push(ident);
            } else if type_ident == "NaiveDateTime" {
                NaiveDateTime_fields.push(ident);
            } else if type_ident == "bool" {
                bool_fields.push(ident);
            } else if type_ident == "Uuid" {
                Uuid_fields.push(ident);
            }
        }
    }


    //根据字段，匹配sql是否增加单引号
    let option_string_fields_match: Vec<_> = Option_fields.iter().map(|(field_ident, inner_type_ident)| {
        let inner_type_str = inner_type_ident.to_string();
    
        let string_match = if inner_type_str == "String" {
            quote! {
                stringify!(#field_ident) => format!("'{}'", value),
            }
        }else if inner_type_str == "Value" {
            quote! {
                stringify!(#field_ident) => format!("'{}'", value),
            }
        }else if inner_type_str.starts_with("HashMap") {
            quote! {
                stringify!(#field_ident) => format!("'{}'", value),
            }
        }else if inner_type_str.starts_with("Vec") {
            quote! {
                stringify!(#field_ident) => format!("'{}'", value),
            }
        }
        else {
            quote! {}
        };
    
        string_match
    }).collect();




    //用于from_payload
    let option_fields_match: Vec<_> = Option_fields.iter().map(|(field_ident, inner_type_ident)| {
        let inner_type_str = inner_type_ident.to_string();
    
        let u64_match = if inner_type_str == "u64" {
            quote! {
                if let gluesql::prelude::Value::I64(value) = value {
                    set_sturct.#field_ident = Some(*value as u64);
                }
            }
        } else {
            quote! {}
        };
        let i8_match = if inner_type_str == "i8" {
            quote! {
                if let gluesql::prelude::Value::I8(value) = value {
                    set_sturct.#field_ident = Some(*value);
                }
            }
        } else {
            quote! {}
        };
    
        let string_match = if inner_type_str == "String" {
            quote! {
                match value {
                    gluesql::prelude::Value::Str(value_str) => {
                        set_sturct.#field_ident = Some(value_str.clone());
                    }
                    gluesql::prelude::Value::I64(value_i64) => {
                        set_sturct.#field_ident = Some(value_i64.to_string());
                    }
                    _ => {}
                }
                // if let gluesql::prelude::Value::Str(value) = value {
                //     set_sturct.#field_ident = Some(value.clone());
                // }
            }
        } else {
            quote! {}
        };
        let bool_match = if inner_type_str == "bool" {
            quote! {
                if let gluesql::prelude::Value::Bool(value) = value {
                    set_sturct.#field_ident = Some(*value);
                }
            }
        } else {
            quote! {}
        };
        let vec_match_i8 = if inner_type_str == "Vec<i8>" {
            quote! {
                if let gluesql::prelude::Value::List(list) = value {
                    let mut vec = list.iter().filter_map(|item| {
                        if let gluesql::prelude::Value::I64(i) = item {
                            Some(*i as i8)
                        } else {
                            panic!("Unexpected item type in gluesql::prelude::Value::List, expected I8");
                        }
                    }).collect::<Vec<i8>>();
                    set_sturct.#field_ident = Some(vec);
                }
                
            }
        }else {
            quote! {}
        };
        let vec_match_string = if inner_type_str == "Vec<String>" {
            quote! {
                if let gluesql::prelude::Value::List(list) = value {
                    let mut vec = list.iter().filter_map(|item| {
                        if let gluesql::prelude::Value::Str(s) = item {
                            Some(s.clone())
                        } else {
                            panic!("Unexpected item type in gluesql::prelude::Value::List, expected Str");
                        }
                    }).collect::<Vec<String>>();
                    set_sturct.#field_ident = Some(vec);
                }
                
            }
        }else {
            quote! {}
        };
        let vec_match_value = if inner_type_str == "Vec<Value>" {
            quote! {
                if let gluesql::prelude::Value::List(list) = value {
                    let mut vec = list.iter().filter_map(|item| {
                        // println!("item: {:?}", item);
                        Some(convert_glue_value_to_serde_value(item))
                    }).collect::<Vec<serde_json::Value>>();
                    set_sturct.#field_ident = Some(vec);
                }   
            }
        }else {
            quote! {}
        };

        // let vec_match = if inner_type_str.starts_with("Vec<") {
        //     quote! {
        //         if let gluesql::prelude::Value::List(list) = value {
        //             println!("list: {:?}", list);
        //             let mut vec = match &inner_type_str[..] {
        //                 "Vec<i8>"=> {
        //                     list.iter().filter_map(|item| {
        //                         if let gluesql::prelude::Value::I64(i) = item {
        //                             Some(*i as i8)
        //                         } else {
        //                             panic!("Unexpected item type in gluesql::prelude::Value::List, expected I8");
        //                         }
        //                     }).collect::<Vec<i8>>()
        //                 },
        //                 "Vec<String>" => {
        //                     list.iter().filter_map(|item| {
        //                         if let gluesql::prelude::Value::Str(s) = item {
        //                             Some(s.clone())
        //                         } else {
        //                             panic!("Unexpected item type in gluesql::prelude::Value::List, expected Str");
        //                         }
        //                     }).collect::<Vec<String>>()
        //                 },
        //                 "Vec<Value>" => {
        //                     list.iter().filter_map(|item| {
        //                         println!("item: {:?}", item);
        //                         Some(convert_glue_value_to_serde_value(item))
        //                     }).collect::<Vec<serde_json::Value>>()
        //                 },
        //                 _ => panic!("Unsupported inner type for Vec"),
        //             };
        //             set_sturct.#field_ident = Some(vec);
        //         }
        //     }
        // } else {
        //     quote! {
        //         panic!("Unsupported Option for Vec");
        //     }
        // };
        let hashmap_match = if inner_type_str == "HashMap" {
            quote! {
                if let gluesql::prelude::Value::Map(value_map_glue) = value {
                    let value_map: std::collections::HashMap<String, serde_json::Value> = value_map_glue
                        .iter()
                        .map(|(key, value)| {
                            let serde_value = convert_glue_value_to_serde_value(value);
                            (key.clone(), serde_value)
                        })
                        .collect();
                    set_sturct.#field_ident = Some(value_map);
                }
            }
        } else {
            quote! {}
        };
        // 对其他类型也进行类似的处理
    
        quote! {
            stringify!(#field_ident) => {
                #u64_match
                #i8_match
                #string_match
                #bool_match
                #vec_match_i8
                #vec_match_string
                #vec_match_value
                #hashmap_match
                // 在这里插入其他类型的匹配代码
                
            },
        }
    }).collect();


    let expanded = quote! {
        // pub use tourin_derive::#selectable_trait;
        impl #struct_name {
            pub fn new(#(#field_names: #field_types),*) -> Self {
                #struct_name {
                    #(#field_names: #field_names,)*
                }
            }

            pub fn insert(&self) -> String {
                let name = stringify!(#struct_name);
                let fields = vec![#(format!("{:?}", stringify!(#field_names))),*].join(", ");
                let field_values = vec![
                    #(match #field_values {
                        Some(value) => value.to_string(),
                        None => "null".to_string(),
                    }),*
                ]
                .join(", ");
                format!("INSERT INTO {}({}) VALUES ({});", name, fields, field_values)
            }


            pub fn drop_table() -> String {
                format!("DROP TABLE IF EXISTS {};", stringify!(#struct_name))
            }

            // 生成builder方法
            pub fn builder() -> #builder_struct {
                #builder_struct::default()
            }

            pub fn create_table() -> &'static str {
                #sql_create
            }
            pub fn union_str(&self) -> std::collections::HashMap<String, Vec<String>> {
                let mut union_str = std::collections::HashMap::new();
                let struct_name = stringify!(#struct_name);
                let field_names = vec![#(format!("{}.{}", struct_name, stringify!(#field_names))),*];
                union_str.insert(struct_name.to_string(), field_names);
                union_str
            }
        }


        
        pub struct #builder_struct {
            #(#field_names: Option<#field_types>,)*
            set_fields: ::std::collections::HashSet<String>,
        }

        impl #builder_struct {
            #builder_methods



            pub fn build(self) -> #struct_name {
                #struct_name {
                    #(#field_names: self.#field_names.unwrap_or_default()),*
                }
            }

            // 生成selectable trait，只有build_selectable方法才能调用select方法
            //修改
            pub fn build_selectable(self) -> impl Selectable<#struct_name> {

                struct SelectableWrapper(#struct_name, ::std::collections::HashSet<String>);
                //修改
                impl Selectable<#struct_name> for SelectableWrapper {

                    fn from_payload(payload: &gluesql::prelude::Payload) -> #struct_name {
                        // ...
                        if let gluesql::prelude::Payload::Select { labels, rows, .. } = payload {
                            if !rows.is_empty() {
                                let row = &rows[0];
                                let mut set_sturct = #struct_name::builder().build();
                                for (i, label) in labels.iter().enumerate() {
                                    let value = row.get_value(&labels, label).unwrap();
                                    match label.as_str() {
                                        #(
                                        stringify!(#u64_fields) => {
                                            if let gluesql::prelude::Value::I64(value) = value {
                                                set_sturct.#u64_fields = *value as u64;
                                            }
                                        },
                                        )*
                                        #(
                                        stringify!(#i8_fields) => {
                                            if let gluesql::prelude::Value::I8(value) = value {
                                                set_sturct.#i8_fields = *value;
                                            }
                                        },
                                        )*
                                        #(
                                        stringify!(#i16_fields) => {
                                            if let gluesql::prelude::Value::I16(value) = value {
                                                set_sturct.#i16_fields = *value;
                                            }
                                        },
                                        )*
                                        #(
                                        stringify!(#i32_fields) => {
                                            if let gluesql::prelude::Value::I32(value) = value {
                                                set_sturct.#i32_fields = *value;
                                            }
                                        },
                                        )*
                                        #(
                                        stringify!(#i128_fields) => {
                                            if let gluesql::prelude::Value::I128(value) = value {
                                                set_sturct.#i128_fields = *value;
                                            }
                                        },
                                        )*
                                        #(
                                        stringify!(#u8_fields) => {
                                            if let gluesql::prelude::Value::U8(value) = value {
                                                set_sturct.#u8_fields = *value;
                                            }
                                        },
                                        )*
                                        #(
                                        stringify!(#f64_fields) => {
                                            if let gluesql::prelude::Value::F64(value) = value {
                                                set_sturct.#f64_fields = *value;
                                            }
                                        },
                                        )*
                                        #(
                                        stringify!(#String_fields) => {
                                            // if let gluesql::prelude::Value::Str(value) = value {
                                            //     set_sturct.#String_fields = value.clone();
                                            // } 
                                            match value {
                                                gluesql::prelude::Value::Str(value_str) => {
                                                    set_sturct.#String_fields = value_str.clone();
                                                }
                                                gluesql::prelude::Value::I64(value_i64) => {
                                                    set_sturct.#String_fields = value_i64.to_string();
                                                }
                                                _ => {} // You can handle other types or ignore them here
                                            }

                                        },
                                        )*
                                        #(
                                        stringify!(#NaiveDateTime_fields) => {
                                            if let gluesql::prelude::Value::Timestamp(value) = value {
                                                set_sturct.#NaiveDateTime_fields = *value;
                                            }
                                        },
                                        )*
                                        #(
                                        stringify!(#bool_fields) => {
                                            if let gluesql::prelude::Value::Bool(value) = value {
                                                set_sturct.#bool_fields = *value;
                                            }
                                        },
                                        )*
                                        #(
                                        stringify!(#Uuid_fields) => {
                                            if let gluesql::prelude::Value::Uuid(value) = value {
                                                set_sturct.#Uuid_fields = Uuid::from_u128(*value);
                                            }
                                        },
                                        )*
                                        #(
                                            #option_fields_match
                                        )*
                                        
                                        _ => {}
                                    }
                                }
                                return set_sturct;
        
                            } else {
                                #struct_name::builder().build()
                            }
         
                        } else {
                            #struct_name::builder().build()
                        }
                        //..
                        
                    }
                    
                    
                    fn select(&self) -> String{
                        let table_name = stringify!(#struct_name);
                        let field_names = vec![#(stringify!(#field_names)),*];
                        let field_values = vec![#(format!("{:?}", self.0.#field_names)),*];                
                        let mut conditions = Vec::new();
                        for field in self.1.iter() {
                            let field_name = field;
                            let field_value = match field_name.as_str() {
                                #(
                                stringify!(#field_names) => format!("{:?}", self.0.#field_names),
                                )*
                                _ => "unknown field".to_string(),
                            };
                            conditions.push(format!("{} = {}", field_name, field_value));
                        }
                        let mut sql = if conditions.is_empty() {
                            format!("SELECT * FROM {}", table_name)
                        } else {
                            format!("SELECT * FROM {} WHERE {}", table_name, conditions.join(" AND "))
                        };
                    
                        // Remove "Some(" and ")" from the string
                        sql = sql.replace("Some(", "").replace(")", "");
                        sql
                    }

                    

                    fn delete(&self) -> String {
                        let table_name = stringify!(#struct_name);
                
                        let mut conditions = Vec::new();
                        for field in self.1.iter() {
                            let field_name = field;
                            let field_value = match field_name.as_str() {
                                #(
                                stringify!(#field_names) => format!("{:?}", self.0.#field_names),
                                )*
                                _ => "unknown field".to_string(),
                            };
                            conditions.push(format!("{} = {}", field_name, field_value));
                        }
                        let mut sql = format!("DELETE FROM {} WHERE {}", table_name, conditions.join(" AND ")); 
                        sql = sql.replace("Some(", "").replace(")", "");
                        sql
                    }

                                    
                    fn update(&self, updates: ::std::collections::HashMap<String, String>) -> Result<String, String> {
                        let table_name = stringify!(#struct_name);
                
                        let mut conditions = Vec::new();
                        for field in self.1.iter() {
                            let field_name = field;
                            let field_value = match field_name.as_str() {
                                #(
                                stringify!(#field_names) => format!("{:?}", self.0.#field_names),
                                )*
                                _ => "unknown field".to_string(),
                            };
                            conditions.push(format!("{} = {}", field_name, field_value));
                        }
                
                        let mut update_fields = Vec::new();
                        for (field, value) in updates.iter() {
                            let field_names = vec![#(stringify!(#field_names)),*];
                            if field_names.contains(&field.as_str()) {
                                let value_str = match field.as_str() {
                                    #(stringify!(#String_fields) => format!("'{}'", value),)*
                                    #(stringify!(#NaiveDateTime_fields) => format!("'{}'", value),)*
                                    #(stringify!(#Uuid_fields) => format!("'{}'", value),)*
                                    #(#option_string_fields_match)*
                                    _ => format!("{}", value),
                                };
                                update_fields.push(format!("{} = {}", field, value_str));
                            } else {
                                return Err(format!("{} is not a field of {}", field, table_name));
                            }
                
                        }

                        let mut sql = format!("UPDATE {} SET {} WHERE {}", table_name, update_fields.join(", "), conditions.join(" AND "));
                        sql = sql.replace("Some(", "").replace(")", "");
                        Ok(sql)
                    }

                    fn union_str(&self) -> std::collections::HashMap<String, Vec<String>> {
                        let mut union_str = std::collections::HashMap::new();
                        let struct_name = stringify!(#struct_name);
                        let field_names = vec![#(format!("{}.{}", struct_name, stringify!(#field_names))),*];
                        union_str.insert(struct_name.to_string(), field_names);
                        union_str
                    }

                }
                
                SelectableWrapper(#struct_name {
                    #(#field_names: self.#field_names.unwrap_or_default()),*
                }, self.set_fields)
            }
        }
        
         // 在这里直接添加对 set_fields 的默认值实现
        impl Default for #builder_struct {
            fn default() -> Self {
                #builder_struct {
                    #(#field_names: None,)*
                    set_fields: ::std::collections::HashSet::new(),
                }
            }
        }

        impl From<&gluesql::prelude::Payload> for #struct_name {
            fn from(payload: &gluesql::prelude::Payload) -> Self {
                if let gluesql::prelude::Payload::Select { labels, rows, .. } = payload {
                    if !rows.is_empty() {
                        let row = &rows[0];
                        let mut set_sturct = #struct_name::builder().build();
                        for (i, label) in labels.iter().enumerate() {
                            let value = row.get_value(&labels, label).unwrap();
                            match label.as_str() {
                                #(
                                stringify!(#u64_fields) => {
                                    if let gluesql::prelude::Value::I64(value) = value {
                                        set_sturct.#u64_fields = *value as u64;
                                    }
                                },
                                )*
                                #(
                                stringify!(#i8_fields) => {
                                    if let gluesql::prelude::Value::I8(value) = value {
                                        set_sturct.#i8_fields = *value;
                                    }
                                },
                                )*
                                #(
                                stringify!(#i16_fields) => {
                                    if let gluesql::prelude::Value::I16(value) = value {
                                        set_sturct.#i16_fields = *value;
                                    }
                                },
                                )*
                                #(
                                stringify!(#i32_fields) => {
                                    if let gluesql::prelude::Value::I32(value) = value {
                                        set_sturct.#i32_fields = *value;
                                    }
                                },
                                )*
                                #(
                                stringify!(#i128_fields) => {
                                    if let gluesql::prelude::Value::I128(value) = value {
                                        set_sturct.#i128_fields = *value;
                                    }
                                },
                                )*
                                #(
                                stringify!(#u8_fields) => {
                                    if let gluesql::prelude::Value::U8(value) = value {
                                        set_sturct.#u8_fields = *value;
                                    }
                                },
                                )*
                                #(
                                stringify!(#f64_fields) => {
                                    if let gluesql::prelude::Value::F64(value) = value {
                                        set_sturct.#f64_fields = *value;
                                    }
                                },
                                )*
                                #(
                                stringify!(#String_fields) => {
                                    if let gluesql::prelude::Value::Str(value) = value {
                                        set_sturct.#String_fields = value.clone();
                                    }
                                },
                                )*
                                #(
                                stringify!(#NaiveDateTime_fields) => {
                                    if let gluesql::prelude::Value::Timestamp(value) = value {
                                        set_sturct.#NaiveDateTime_fields = *value;
                                    }
                                },
                                )*
                                #(
                                stringify!(#bool_fields) => {
                                    if let gluesql::prelude::Value::Bool(value) = value {
                                        set_sturct.#bool_fields = *value;
                                    }
                                },
                                )*
                                #(
                                stringify!(#Uuid_fields) => {
                                    if let gluesql::prelude::Value::Uuid(value) = value {
                                        set_sturct.#Uuid_fields = Uuid::from_u128(*value);
                                    }
                                },
                                )*
                                #(
                                    #option_fields_match
                                )*
                                
                                _ => {}
                            }
                        }
                        return set_sturct;

                    } else {
                        #struct_name::builder().build()
                    }
 
                } else {
                    #struct_name::builder().build()
                }
            }
        }
        
    };


    // 返回生成的代码
    TokenStream::from(expanded)

  
}

