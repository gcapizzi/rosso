use anyhow::Result;

#[test]
fn test_strings() -> Result<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut con = client.get_connection()?;
    redis::cmd("SET").arg("my_key").arg(42).exec(&mut con)?;
    let value: i32 = redis::cmd("GET").arg("my_key").query(&mut con)?;
    assert_eq!(42, value);
    let new_value: i32 = redis::cmd("INCR").arg("my_key").query(&mut con)?;
    assert_eq!(43, new_value);
    Ok(())
}
