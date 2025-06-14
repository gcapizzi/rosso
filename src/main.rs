fn main() -> std::io::Result<()> {
    rosso::server::start("127.0.0.1:6379")
}
