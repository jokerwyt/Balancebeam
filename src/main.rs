mod request;
mod response;

use std::{collections::HashMap, sync::{Arc, atomic::{AtomicBool, Ordering}}};

use clap::Parser;
use rand::{Rng, SeedableRng};
use tokio::{sync::{mpsc::{channel, Sender}, Mutex, RwLock}, time::Duration};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::sleep;


/// Contains information parsed from the command-line invocation of balancebeam. The Clap macros
/// provide a fancy way to automatically construct a command-line argument parser.
#[derive(Parser, Debug)]
#[command(about = "Fun with load balancing")]
struct CmdOptions {
    /// "IP/port to bind to"
    #[arg(short, long, default_value = "0.0.0.0:1100")]
    bind: String,
    /// "Upstream host to forward requests to"
    #[arg(short, long)]
    upstream: Vec<String>,
    /// "Perform active health checks on this interval (in seconds)"
    #[arg(long, default_value = "10")]
    active_health_check_interval: usize,
    /// "Path to send request to for active health checks"
    #[arg(long, default_value = "/")]
    active_health_check_path: String,
    /// "Maximum number of requests to accept per IP per minute (0 = unlimited)"
    #[arg(long, default_value = "0")]
    max_requests_per_minute: usize,
}

struct Window {
    counter: usize,
    last_upd_min: u64,
}

/// Contains information about the state of balancebeam (e.g. what servers we are currently proxying
/// to, what servers have failed, rate limiting counts, etc.)
///
/// You should add fields to this struct in later milestones.
/// 
struct ProxyState {
    /// How frequently we check whether upstream servers are alive (Milestone 4)
    #[allow(dead_code)]
    active_health_check_interval: usize,
    /// Where we should send requests when doing active health checks (Milestone 4)
    #[allow(dead_code)]
    active_health_check_path: String,
    /// Maximum number of requests an individual IP can make in a minute (Milestone 5)
    #[allow(dead_code)]
    max_requests_per_minute: usize,
    /// Addresses of servers that we are proxying to
    upstream_addresses: RwLock<Vec<(String, AtomicBool)>>,

    rate_limiter: Mutex<HashMap<String, Arc<Mutex<Window>>>>
}

#[derive(Debug)]
struct UpstreamStateUpdate {
    alive: Vec<(String, AtomicBool)>,
}


async fn setup_health_check_thread(
    tx: Sender<UpstreamStateUpdate>, state: Arc<ProxyState>, upstreams: Vec<String>) { // check interval in secs
        
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(state.active_health_check_interval.try_into().unwrap())).await;
            let mut alive = vec![];

            for up in &upstreams {
                let is_alive =  match TcpStream::connect(up).await {
                    Ok(mut stream) => {                        
                        let req = 
                        request::build_health_check_req(&state.active_health_check_path);
                        if request::write_to_stream(&req, &mut stream).await.or(Err(())).is_err() {
                            false
                        } else {
                            match  
                                response::read_from_stream(&mut stream, &http::Method::GET).await
                                .or(Err(())) {
                                    Ok(resp) => {
                                        if resp.status().as_u16() != 200 {
                                            false
                                        } else {
                                            true
                                        }
                                    }
                                    Err(_) => false
                                }
                        }
                    }
                    Err(_) => false
                };

                alive.push((up.to_string(), AtomicBool::new(is_alive)));
            }

            tx.send(UpstreamStateUpdate { alive }).await.unwrap();

        }
    });
}

#[tokio::main]
async fn main() {
    
    // Initialize the logging library. You can print log messages using the `log` macros:
    // https://docs.rs/log/0.4.8/log/ You are welcome to continue using print! statements; this
    // just looks a little prettier.
    if let Err(_) = std::env::var("RUST_LOG") {
        std::env::set_var("RUST_LOG", "debug");
    }
    pretty_env_logger::init();

    // Parse the command line arguments passed to this program
    let options = CmdOptions::parse();
    if options.upstream.len() < 1 {
        log::error!("At least one upstream server must be specified using the --upstream option.");
        std::process::exit(1);
    }

    // Start listening for connections
    let listener = match TcpListener::bind(&options.bind).await {
        Ok(listener) => listener,
        Err(err) => {
            log::error!("Could not bind to {}: {}", options.bind, err);
            std::process::exit(1);
        }
    };
    log::info!("Listening for requests on {}", options.bind);

    // Handle incoming connections
    let state = Arc::new(ProxyState {
        upstream_addresses: RwLock::new(options.upstream.iter().map(|x| (x.to_string(), AtomicBool::new(true))).collect()),
        active_health_check_interval: options.active_health_check_interval,
        active_health_check_path: options.active_health_check_path,
        max_requests_per_minute: options.max_requests_per_minute,
        rate_limiter: Mutex::new(HashMap::<String, Arc<Mutex<Window>>>::new())
    });

    // Setup health check thread
    let (tx, mut rx) = channel::<UpstreamStateUpdate>(1024);
    setup_health_check_thread(tx, state.clone(), options.upstream).await;

    loop{
        if let Ok(stream) = listener.accept().await {
            // try to get update from the health check thread
            let mut last_upd = None;
            loop {
                match rx.try_recv() {
                    Ok(x) => {
                        last_upd = Some(x);
                    }
                    Err(_) => break
                }
            }
            if let Some(x) = last_upd {
                let mut guard = state.upstream_addresses.write().await;
                *guard = x.alive;
                log::info!("Upstream servers: {:?} onlines", guard.len());
            }
            
            // Handle the connection!
            tokio::spawn(handle_connection(stream.0, state.clone()));
        }
    }
}


async fn connect_to_upstream(state: &Arc<ProxyState>) -> Result<TcpStream, std::io::Error> {
    let upstreams = state.upstream_addresses.read().await;
    
    if upstreams.len() == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "No upstream servers available",
        ));
    }
    
    let mut rng = rand::rngs::StdRng::from_entropy();
    loop {
        let upstream_idx = rng.gen_range(0..upstreams.len());
        if upstreams[upstream_idx].1.load(Ordering::Acquire) == false {
            continue;
        }
        let upstream_ip = &upstreams[upstream_idx].0;

        match TcpStream::connect(&upstream_ip).await {
            Ok(x) => {
                return Ok(x)
            }
            Err(_) => {
                // It happends rarely, but it's possible that the upstream server is removed
                log::warn!("Failed to connect to upstream server: {}", upstream_ip);
                
                upstreams[upstream_idx].1.store(false, Ordering::Release);
            }
        }
    }
}

async fn send_response(client_conn: &mut TcpStream, response: &http::Response<Vec<u8>>) {
    let client_ip = client_conn.peer_addr().unwrap().ip().to_string();
    log::info!(
        "{} <- {}",
        client_ip,
        response::format_response_line(&response)
    );
    if let Err(error) = response::write_to_stream(&response, client_conn).await {
        log::warn!("Failed to send response to client: {}", error);
        return;
    }
}

async fn handle_connection(mut client_conn: TcpStream, state: Arc<ProxyState>) {
    let client_ip = client_conn.peer_addr().unwrap().ip().to_string();
    log::info!("Connection received from {}", client_ip);

    // Open a connection to a random destination server
    let mut upstream_conn = match connect_to_upstream(&state).await {
        Ok(stream) => stream,
        Err(_error) => {
            let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
            send_response(&mut client_conn, &response).await;
            return;
        }
    };
    let upstream_ip = upstream_conn.peer_addr().unwrap().ip().to_string();


    let mut guard = state.rate_limiter.lock().await;
    let window = match guard.get_mut(&client_ip) {
        Some(x) => x.clone(),
        None => {
            guard.entry(client_ip.clone())
                .or_insert(Arc::new(Mutex::new(Window { counter: 0, last_upd_min: 0 }))).clone()
        },
    };
    drop(guard);


    // The client may now send us one or more requests. Keep trying to read requests until the
    // client hangs up or we get an error.
    loop {
        // Read a request from the client
        let mut request = match request::read_from_stream(&mut client_conn).await {
            Ok(request) => request,
            // Handle case where client closed connection and is no longer sending requests
            Err(request::Error::IncompleteRequest(0)) => {
                log::debug!("Client finished sending requests. Shutting down connection");
                return;
            }
            // Handle I/O error in reading from the client
            Err(request::Error::ConnectionError(io_err)) => {
                log::info!("Error reading request from client stream: {}", io_err);
                return;
            }
            Err(error) => {
                log::debug!("Error parsing request: {:?}", error);
                let response = response::make_http_error(match error {
                    request::Error::IncompleteRequest(_)
                    | request::Error::MalformedRequest(_)
                    | request::Error::InvalidContentLength
                    | request::Error::ContentLengthMismatch => http::StatusCode::BAD_REQUEST,
                    request::Error::RequestBodyTooLarge => http::StatusCode::PAYLOAD_TOO_LARGE,
                    request::Error::ConnectionError(_) => http::StatusCode::SERVICE_UNAVAILABLE,
                });
                send_response(&mut client_conn, &response).await;
                continue;
            }
        };
        

        let reach_rate_limit = async {
            let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() / 60;
            // Get the window for this client
            
            let mut guard = window.lock().await;
            if guard.last_upd_min == now {
                // If the window is for the same minute, increment the counter
                guard.counter += 1;
                if guard.counter > state.max_requests_per_minute {
                    // dbg!("{} {}", guard.counter, state.max_requests_per_minute);
                    true // it has reached the limit
                } else {
                    false
                }
            } else {
                // If the window is for a different minute, reset the counter
                guard.counter = 1;
                guard.last_upd_min = now;
                false
            }
        };


        if state.max_requests_per_minute != 0 && reach_rate_limit.await {
            // If the counter is greater than the max requests per minute, send a 429
            let response = response::make_http_error(http::StatusCode::TOO_MANY_REQUESTS);
            send_response(&mut client_conn, &response).await;
            continue;
        }

        log::info!(
            "{} -> {}: {}",
            client_ip,
            upstream_ip,
            request::format_request_line(&request)
        );

        // Add X-Forwarded-For header so that the upstream server knows the client's IP address.
        // (We're the ones connecting directly to the upstream server, so without this header, the
        // upstream server will only know our IP, not the client's.)
        request::extend_header_value(&mut request, "x-forwarded-for", &client_ip);

        // Forward the request to the server
        if let Err(error) = request::write_to_stream(&request, &mut upstream_conn).await {
            log::error!(
                "Failed to send request to upstream {}: {}",
                upstream_ip,
                error
            );
            let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
            send_response(&mut client_conn, &response).await;
            return;
        }
        log::debug!("Forwarded request to server");

        // Read the server's response
        let response = 
            match response::read_from_stream(&mut upstream_conn, request.method()).await {
                Ok(response) => response,
                Err(error) => {
                    log::error!("Error reading response from server: {:?}", error);
                    let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
                    send_response(&mut client_conn, &response).await;
                    return;
                }
            };
        // Forward the response to the client
        send_response(&mut client_conn, &response).await;
        log::debug!("Forwarded response to client");
    }
}
