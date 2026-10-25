mod infrastructure;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut args = std::env::args().skip(1);
    let mut port = 8766_u16;
    let mut host = String::from("http://127.0.0.1:8765");
    let mut token_file = String::from("runtime/host/access-token");
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--port" => port = args.next().ok_or("--port needs a number")?.parse()?,
            "--host" => host = args.next().ok_or("--host needs a loopback URL")?,
            "--token-file" => token_file = args.next().ok_or("--token-file needs a path")?,
            "--help" | "-h" => {
                println!(
                    "furica-web-demo [--port 8766] [--host http://127.0.0.1:8765] [--token-file runtime/host/access-token]\nWeb shell for a separately running furica-host. The Host bearer token stays server-side."
                );
                return Ok(());
            }
            _ => return Err(format!("unknown argument: {arg}").into()),
        }
    }
    let host = infrastructure::web::host_url(&host)?;
    use std::io::Read;
    let mut token = String::new();
    std::fs::File::open(token_file)
        .map_err(|_| "Host access-token file is missing or unreadable; start Host first, or specify --token-file")?
        .take(8193)
        .read_to_string(&mut token)?;
    let token = token.trim_end_matches(['\r', '\n']).to_owned();
    if token.len() < 32 || token.len() > 8192 || token.contains(['\r', '\n']) {
        return Err("Host token must contain at least 32 bytes and no line breaks".into());
    }
    infrastructure::web::serve(host, token, port).await
}
