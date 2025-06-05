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

#[test]
fn test_expiration() -> Result<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut con = client.get_connection()?;

    redis::cmd("SET")
        .arg("my_expiring_key")
        .arg(42)
        .arg("EX")
        .arg(1)
        .exec(&mut con)?;
    let value: i32 = redis::cmd("GET").arg("my_expiring_key").query(&mut con)?;
    assert_eq!(42, value);

    std::thread::sleep(std::time::Duration::from_secs(2));

    let value: Option<i32> = redis::cmd("GET").arg("my_expiring_key").query(&mut con)?;
    assert_eq!(None, value);

    Ok(())
}

#[test]
fn test_parallel_connections() -> Result<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut con = client.get_connection()?;

    redis::cmd("SET")
        .arg("my_parallel_key")
        .arg(42)
        .exec(&mut con)?;

    let mut children = vec![];
    for _ in 0..10 {
        children.push(std::thread::spawn(move || {
            let client = redis::Client::open("redis://127.0.0.1/").unwrap();
            let mut con = client.clone().get_connection().unwrap();
            redis::cmd("INCR")
                .arg("my_parallel_key")
                .exec(&mut con)
                .unwrap();
        }));
    }

    for child in children {
        let _ = child.join();
    }

    let value: i32 = redis::cmd("GET").arg("my_parallel_key").query(&mut con)?;
    assert_eq!(52, value);

    Ok(())
}
