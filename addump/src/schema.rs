use std::collections::BTreeMap;
use std::fmt::Write;

use esedb::data::Data;
use esedb::table::{Table, Value};


// to pick apart the schema, we have a bit of a bootstrapping problem,
// since the schema is in the database itself
//
// we make a couple of assumptions when searching for the schema:
// * $ROOT_OBJECT$ has DNT (Distinguished Name Tag) 2
// * the objectClass attribute is ATTc0
//   (encoding=2.5.5.2=Object-Identifiers attribute=2.5.4.0 objectClass)
// * the object class of the schema root is 196617 (1.2.840.113556.1.3.9 dMD)
// * the naming attribute for the top objects is ATTm589825
//   (encoding=2.5.5.12=String(Unicode) attribute=1.2.840.113556.1.4.1 name)
// * there are up to two instances of this object
// * if there are two, one of the instances is $ROOT_OBJECT$ -> Boot -> Schema,
//   which we are not interested in
//
// we make further assumptions when decoding the schema:
// * attributes and classes are immediate children of the schema root
//
// * the object class of a class in the schema is 196621 (1.2.840.113556.1.3.13 classSchema)
// * the attribute for the object class number of a class in the schema is ATTc131094
//   (encoding=2.5.5.2=Object-Identifier attribute=1.2.840.113556.1.2.22 governsID)
// * the attribute for the LDAP name of a class in the schema is ATTm131532
//   (encoding=2.5.5.12=String(Unicode) attribute=1.2.840.113556.1.2.460 lDAPDisplayName)
//
// * the object class of an attribute in the schema is 196622 (1.2.840.113556.1.3.14 attributeSchema)
// * the attribute for the column number of an attribute in the schema is ATTc131102
//   (encoding=2.5.5.2=Object-Identifier attribute=1.2.840.113556.1.2.30 attributeID)
// * the attribute for the LDAP name of an attribute is the same as for classes (ATTm131532)
// * the attribute for the encoding of the value of an attribute is ATTc131104
//   (encoding=2.5.5.2=Object-Identifier attribute=1.2.840.113556.1.2.32 attributeSyntax)
// * taking the value of ATTc131104, subtracting 0x0008_0000 and adding 'a' returns the
//   letter between "ATT" and the attribute ID in the database column names


// finding the schema:
pub const DNT_COLUMN_NAME: &str = "DNT_col";
pub const PARENT_DNT_COLUMN_NAME: &str = "PDNT_col";
pub const OBJECT_CLASS_COLUMN_NAME: &str = "ATTc0";
pub const TOP_OBJECT_NAME_COLUMN_NAME: &str = "ATTm589825";
pub const BOOT_OBJECT_NAME: &str = "Boot";
pub const ROOT_OBJECT_DNT: i32 = 2;
pub const SCHEMA_ROOT_OBJECT_CLASS: i32 = 196617;

// taking the schema apart:
pub const SCHEMA_OBJECT_LDAP_NAME_COLUMN_NAME: &str = "ATTm131532";
pub const SCHEMA_CLASS_OBJECT_CLASS: i32 = 196621;
pub const SCHEMA_CLASS_OBJECT_CLASS_COLUMN_NAME: &str = "ATTc131094";
pub const SCHEMA_ATTRIBUTE_OBJECT_CLASS: i32 = 196622;
pub const SCHEMA_ATTRIBUTE_ID_COLUMN_NAME: &str = "ATTc131102";
pub const SCHEMA_ATTRIBUTE_SYNTAX_COLUMN_NAME: &str = "ATTc131104";


#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Attribute {
    pub id: i32,
    pub syntax: i32,
    pub ldap_name: String,
}
impl Attribute {
    pub fn to_column_name(&self) -> String {
        let mut ret = String::with_capacity(3 + 1 + 6);
        ret.push_str("ATT");

        let lowercase_a: i32 = u32::from('a').try_into().unwrap();
        let lowercase_z: i32 = u32::from('z').try_into().unwrap();
        let syntax_char = self.syntax + lowercase_a - 0x0008_0000;
        assert!(syntax_char >= lowercase_a && syntax_char <= lowercase_z);
        ret.push(char::from_u32(syntax_char.try_into().unwrap()).unwrap());

        write!(ret, "{}", self.id).unwrap();

        ret
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ObjectClass {
    pub id: i32,
    pub ldap_name: String,
}

pub fn find_schema_root<'t, 'r>(data_table: &'t Table, data_rows: &'r [BTreeMap<i32, Value>]) -> &'r BTreeMap<i32, Value> {
    // obtain some important column indexes
    let dnt_column_index = data_table.columns.iter()
        .find(|c| c.name == DNT_COLUMN_NAME)
        .expect("failed to find key (DNT) column while bootstrapping schema")
        .column_id;
    let parent_dnt_column_index = data_table.columns.iter()
        .find(|c| c.name == PARENT_DNT_COLUMN_NAME)
        .expect("failed to find parent reference column while bootstrapping schema")
        .column_id;
    let object_class_column_index = data_table.columns.iter()
        .find(|c| c.name == OBJECT_CLASS_COLUMN_NAME)
        .expect("failed to find objectClass column while bootstrapping schema")
        .column_id;
    let top_name_column_index = data_table.columns.iter()
        .find(|c| c.name == TOP_OBJECT_NAME_COLUMN_NAME)
        .expect("failed to find top naming column while bootstrapping schema")
        .column_id;

    // find Boot
    let boot_entry_opt = data_rows.iter()
        .find(|row|
            column_contains_value(row, parent_dnt_column_index, &Data::Long(ROOT_OBJECT_DNT))
            && column_contains_value(row, top_name_column_index, &Data::LongText(BOOT_OBJECT_NAME.to_owned()))
        );
    let boot_dnt_opt = if let Some(boot_entry) = boot_entry_opt {
        let boot_dnt_value = boot_entry.get(&dnt_column_index)
            .expect("Boot entry has no DNT?!");
        Some(extract_dnt(boot_dnt_value))
    } else {
        None
    };

    // find schema root that is not a child of Boot
    let schema_root = data_rows.iter()
        .find(|row|
            boot_dnt_opt.map(|boot_dnt| !column_contains_value(row, parent_dnt_column_index, &Data::Long(boot_dnt)))
                .unwrap_or(true)
            && column_contains_value(row, object_class_column_index, &Data::Long(SCHEMA_ROOT_OBJECT_CLASS))
        )
        .expect("schema root not found");
    schema_root
}

pub fn collect_schema_classes(data_table: &Table, data_rows: &[BTreeMap<i32, Value>], schema_root: &BTreeMap<i32, Value>) -> BTreeMap<i32, ObjectClass> {
    // obtain some important column indexes
    let dnt_column_index = data_table.columns.iter()
        .find(|c| c.name == DNT_COLUMN_NAME)
        .expect("failed to find key (DNT) column while bootstrapping schema")
        .column_id;
    let parent_dnt_column_index = data_table.columns.iter()
        .find(|c| c.name == PARENT_DNT_COLUMN_NAME)
        .expect("failed to find parent reference column while bootstrapping schema")
        .column_id;
    let object_class_column_index = data_table.columns.iter()
        .find(|c| c.name == OBJECT_CLASS_COLUMN_NAME)
        .expect("failed to find objectClass column while bootstrapping schema")
        .column_id;

    let schema_object_class_column_index = data_table.columns.iter()
        .find(|c| c.name == SCHEMA_CLASS_OBJECT_CLASS_COLUMN_NAME)
        .expect("failed to find governsID column while bootstrapping schema")
        .column_id;
    let ldap_name_column_index = data_table.columns.iter()
        .find(|c| c.name == SCHEMA_OBJECT_LDAP_NAME_COLUMN_NAME)
        .expect("failed to find lDAPDisplayName column while bootstrapping schema")
        .column_id;

    let schema_root_dnt_value = schema_root.get(&dnt_column_index)
        .expect("schema root has no DNT?!");
    let schema_root_dnt = extract_dnt(schema_root_dnt_value);

    // find the class children
    let mut id_to_object_class = BTreeMap::new();
    let class_rows = data_rows.iter()
        .filter(|row|
            column_contains_value(row, parent_dnt_column_index, &Data::Long(schema_root_dnt))
            && column_contains_value(row, object_class_column_index, &Data::Long(SCHEMA_CLASS_OBJECT_CLASS))
        );
    for class_row in class_rows {
        let Some(Data::Long(schema_object_class)) = get_first_value(class_row, schema_object_class_column_index) else { continue };
        let Some(Data::LongText(ldap_name)) = get_first_value(class_row, ldap_name_column_index) else { continue };
        let object_class = ObjectClass {
            id: *schema_object_class,
            ldap_name: ldap_name.clone(),
        };
        id_to_object_class.insert(object_class.id, object_class);
    }
    id_to_object_class
}

pub fn collect_schema_attributes(data_table: &Table, data_rows: &[BTreeMap<i32, Value>], schema_root: &BTreeMap<i32, Value>) -> BTreeMap<String, Attribute> {
    // obtain some important column indexes
    let dnt_column_index = data_table.columns.iter()
        .find(|c| c.name == DNT_COLUMN_NAME)
        .expect("failed to find key (DNT) column while bootstrapping schema")
        .column_id;
    let parent_dnt_column_index = data_table.columns.iter()
        .find(|c| c.name == PARENT_DNT_COLUMN_NAME)
        .expect("failed to find parent reference column while bootstrapping schema")
        .column_id;
    let object_class_column_index = data_table.columns.iter()
        .find(|c| c.name == OBJECT_CLASS_COLUMN_NAME)
        .expect("failed to find objectClass column while bootstrapping schema")
        .column_id;

    let attribute_id_column_index = data_table.columns.iter()
        .find(|c| c.name == SCHEMA_ATTRIBUTE_ID_COLUMN_NAME)
        .expect("failed to find attributeID column while bootstrapping schema")
        .column_id;
    let attribute_syntax_column_index = data_table.columns.iter()
        .find(|c| c.name == SCHEMA_ATTRIBUTE_SYNTAX_COLUMN_NAME)
        .expect("failed to find attributeSyntax column while bootstrapping schema")
        .column_id;
    let ldap_name_column_index = data_table.columns.iter()
        .find(|c| c.name == SCHEMA_OBJECT_LDAP_NAME_COLUMN_NAME)
        .expect("failed to find lDAPDisplayName column while bootstrapping schema")
        .column_id;

    let schema_root_dnt_value = schema_root.get(&dnt_column_index)
        .expect("schema root has no DNT?!");
    let schema_root_dnt = extract_dnt(schema_root_dnt_value);

    // find the class children
    let mut database_column_to_attribute = BTreeMap::new();
    let attribute_rows = data_rows.iter()
        .filter(|row|
            column_contains_value(row, parent_dnt_column_index, &Data::Long(schema_root_dnt))
            && column_contains_value(row, object_class_column_index, &Data::Long(SCHEMA_ATTRIBUTE_OBJECT_CLASS))
        );
    for attribute_row in attribute_rows {
        let Some(Data::Long(attribute_id)) = get_first_value(attribute_row, attribute_id_column_index) else { continue };
        let Some(Data::Long(syntax)) = get_first_value(attribute_row, attribute_syntax_column_index) else { continue };
        let Some(Data::LongText(ldap_name)) = get_first_value(attribute_row, ldap_name_column_index) else { continue };
        let attribute = Attribute {
            id: *attribute_id,
            syntax: *syntax,
            ldap_name: ldap_name.clone(),
        };
        let column_name = attribute.to_column_name();
        database_column_to_attribute.insert(column_name, attribute);
    }
    database_column_to_attribute
}

fn column_contains_value(row: &BTreeMap<i32, Value>, column_index: i32, expected_value: &Data) -> bool {
    let Some(value) = row.get(&column_index) else { return false };
    value.to_data_vec().into_iter()
        .any(|v| v == expected_value)
}

fn get_first_value(row: &BTreeMap<i32, Value>, column_index: i32) -> Option<&Data> {
    let value = row.get(&column_index)?;
    match value {
        Value::Simple(data) => Some(data),
        Value::Complex { data, .. } => Some(data),
        Value::Multiple { values, .. } => values.get(0),
    }
}

fn extract_dnt(dnt_value: &Value) -> i32 {
    match dnt_value {
        Value::Simple(Data::Long(dnt)) => *dnt,
        _ => panic!("unexpected DNT value {:?}", dnt_value),
    }
}
