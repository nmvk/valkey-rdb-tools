use rdb_to_arrow::{schema_for, TypeTag};

use crate::args::{OutputFormat, SchemaArgs};

pub fn run(args: &SchemaArgs) -> Result<(), Box<dyn std::error::Error>> {
    let tags: Vec<TypeTag> = match &args.type_name {
        Some(name) => {
            let tag = TypeTag::from_cli_name(name).ok_or_else(|| {
                format!(
                    "unknown type '{}'. Valid types: string, list, set, zset, hash, geo, hll",
                    name
                )
            })?;
            vec![tag]
        }
        None => TypeTag::ALL.to_vec(),
    };

    match args.output {
        OutputFormat::Text => print_text(&tags),
        OutputFormat::Json => print_json(&tags)?,
    }

    Ok(())
}

fn print_text(tags: &[TypeTag]) {
    for (i, &tag) in tags.iter().enumerate() {
        if i > 0 {
            println!();
        }
        println!("--- {} ---", tag.as_str());
        let schema = schema_for(tag);
        for field in schema.fields() {
            let nullable = if field.is_nullable() { "nullable" } else { "required" };
            println!("  {:<20} {:<12} {}", field.name(), field.data_type(), nullable);
        }
    }
}

fn print_json(tags: &[TypeTag]) -> Result<(), Box<dyn std::error::Error>> {
    let mut schemas = serde_json::Map::new();

    for &tag in tags {
        let schema = schema_for(tag);
        let fields: Vec<serde_json::Value> = schema
            .fields()
            .iter()
            .map(|f| {
                serde_json::json!({
                    "name": f.name(),
                    "type": format!("{}", f.data_type()),
                    "nullable": f.is_nullable(),
                })
            })
            .collect();
        schemas.insert(
            tag.as_str().to_string(),
            serde_json::Value::Array(fields),
        );
    }

    let output = serde_json::to_string_pretty(&serde_json::Value::Object(schemas))?;
    println!("{output}");
    Ok(())
}
