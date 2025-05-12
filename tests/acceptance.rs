use anyhow::Result;
use redis::Commands;

#[test]
fn test_set_and_get() -> Result<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut con = client.get_connection()?;
    let _: () = con.set("my_key", 42)?;
    let value: i32 = con.get("my_key")?;
    assert_eq!(42, value);
    Ok(())
}

#[test]
fn test_get_non_existent_key() -> Result<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut con = client.get_connection()?;
    let value: Option<i32> = con.get("does_not_exist")?;
    assert_eq!(None, value);
    Ok(())
}
