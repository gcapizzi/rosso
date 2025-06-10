use anyhow::Result;

#[test]
fn test_strings() -> Result<()> {
    let key_name = random_key_name();
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut con = client.get_connection()?;

    let res: Option<String> = redis::cmd("SET")
        .arg(&key_name)
        .arg(42)
        .arg("NX")
        .query(&mut con)?;
    assert_eq!(Some("OK".to_string()), res);

    let prev_value: Option<String> = redis::cmd("SET")
        .arg(&key_name)
        .arg(43)
        .arg("GET")
        .query(&mut con)?;
    assert_eq!(Some("42".to_string()), prev_value);

    let value: i32 = redis::cmd("GET").arg(&key_name).query(&mut con)?;
    assert_eq!(43, value);

    let new_value: i32 = redis::cmd("INCR").arg(&key_name).query(&mut con)?;
    assert_eq!(44, new_value);

    Ok(())
}

#[test]
fn test_expiration() -> Result<()> {
    let key_name = random_key_name();
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut con = client.get_connection()?;

    redis::cmd("SET")
        .arg(&key_name)
        .arg(42)
        .arg("EX")
        .arg(1)
        .exec(&mut con)?;
    let value: i32 = redis::cmd("GET").arg(&key_name).query(&mut con)?;
    assert_eq!(42, value);

    std::thread::sleep(std::time::Duration::from_secs(2));

    let value: Option<i32> = redis::cmd("GET").arg(&key_name).query(&mut con)?;
    assert_eq!(None, value);

    Ok(())
}

#[test]
fn test_concurrent_incrs() -> Result<()> {
    let key_name = random_key_name();
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut con = client.get_connection()?;

    redis::cmd("SET").arg(&key_name).arg(42).exec(&mut con)?;

    let mut children = vec![];
    for _ in 0..10 {
        let k = key_name.clone();
        children.push(std::thread::spawn(move || {
            let client = redis::Client::open("redis://127.0.0.1/").unwrap();
            let mut con = client.clone().get_connection().unwrap();
            redis::cmd("INCR").arg(&k).exec(&mut con).unwrap();
        }));
    }

    for child in children {
        let _ = child.join();
    }

    let value: i32 = redis::cmd("GET").arg(&key_name).query(&mut con)?;
    assert_eq!(52, value);

    Ok(())
}

fn random_key_name() -> String {
    std::iter::repeat_with(fastrand::alphanumeric)
        .take(20)
        .collect()
}
