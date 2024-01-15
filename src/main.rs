use std::collections::HashMap;
use std::convert::Infallible;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{SystemTime, Duration};
use std::{cmp, env};

use log::debug;
use serde_derive::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio::{sync::broadcast, task, time};
use warp::{Filter, filters::ws::WebSocket};
use rtdlib::Tdlib;
use futures_util::{SinkExt, StreamExt};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Query {
    client_key: String,
}

async fn handle_websocket_upgrade(
    websocket: WebSocket,
    client_key: String,
    mut tdlib_receive_receiver: broadcast::Receiver<ChannelMessage>,
    tdlib_send_sender: broadcast::Sender<ChannelMessage>,
) {
    debug!("connected {}", client_key);

    let (mut websocket_sender, mut websocket_receiver) = websocket.split();

    let client_key_2 = client_key.clone();

    task::spawn(async move {
        while let Ok((update_client_key, update)) = tdlib_receive_receiver.recv().await {
            if update_client_key != client_key_2 {
                continue;
            }

            debug!("update {:?}", update);

            websocket_sender.send(warp::ws::Message::text(update)).await.unwrap();
        }
    });

    task::spawn(async move {
        while let Some(message) = websocket_receiver.next().await {
            debug!("message {:?}", message);

            if let Ok(message) = message {
                if let Ok(message) = message.to_str() {
                    tdlib_send_sender.send((client_key.clone(), message.to_string())).unwrap();
                }
            }
        }
    });
}

type ChannelMessage = (String, String);

fn with_tdlib_receive_receiver(
    tdlib_receive_sender: broadcast::Sender<ChannelMessage>,
) -> impl Filter<Extract = (broadcast::Receiver<ChannelMessage>,), Error = Infallible> + Clone {
    warp::any().map(move || tdlib_receive_sender.subscribe())
}

fn with_tdlib_send_sender(
    tdlib_send_sender: broadcast::Sender<ChannelMessage>,
) -> impl Filter<Extract = (broadcast::Sender<ChannelMessage>,), Error = Infallible> + Clone {
    warp::any().map(move || tdlib_send_sender.clone())
}

fn with_tdlib_by_client_key(
    tdlib_by_client_key: Arc<Mutex<HashMap<String, Tdlib>>>,
) -> impl Filter<Extract = (Arc<Mutex<HashMap<String, Tdlib>>>,), Error = Infallible> + Clone {
    warp::any().map(move || tdlib_by_client_key.clone())
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let (tdlib_receive_sender, _tdlib_receive_receiver) = broadcast::channel::<ChannelMessage>(128);
    let tdlib_receive_sender_2 = tdlib_receive_sender.clone();

    let (tdlib_send_sender, mut tdlib_send_receiver) = broadcast::channel::<ChannelMessage>(128);

    let tdlib_by_client_key = Arc::new(Mutex::new(HashMap::<String, Tdlib>::new()));
    let tdlib_by_client_key_2 = tdlib_by_client_key.clone();
    let tdlib_by_client_key_3 = tdlib_by_client_key.clone();

    task::spawn(async move {
        let mut last_received = SystemTime::now();

        loop {
            let delay = cmp::max(
                Duration::ZERO,
                cmp::min(
                    Duration::from_millis(100),
                    last_received.elapsed().unwrap(),
                ),
            );

            debug!("delay {:?}", delay);
            time::sleep(delay).await;

            for (client_key, tdlib) in tdlib_by_client_key.lock().await.iter_mut() {
                if let Some(update) = tdlib.receive(0.0) {
                    debug!("update {:?}", update);

                    tdlib_receive_sender.send((client_key.clone(), update)).unwrap();

                    last_received = SystemTime::now();
                }
            }
        }
    });

    task::spawn(async move {
        loop {
            let (client_key, update) = tdlib_send_receiver.recv().await.unwrap();

            let mut tdlib_by_client_key = tdlib_by_client_key_2.lock().await;

            let tdlib = tdlib_by_client_key.get_mut(&client_key).unwrap();

            tdlib.send(&update);
        }
    });

    let routes = warp::path::end()
        .and(warp::ws())
        .and(warp::query::<Query>())
        .and(with_tdlib_receive_receiver(tdlib_receive_sender_2))
        .and(with_tdlib_send_sender(tdlib_send_sender))
        .and(with_tdlib_by_client_key(tdlib_by_client_key_3))
        .map(
            move |
            websocket: warp::ws::Ws,
            query: Query,
            tdlib_receive_receiver: broadcast::Receiver<ChannelMessage>,
            tdlib_send_sender: broadcast::Sender<ChannelMessage>,
            tdlib_by_client_key: Arc<Mutex<HashMap<String, Tdlib>>>,
            | {
                websocket.on_upgrade(|websocket| async move {
                    let mut tdlib_by_client_key = tdlib_by_client_key.lock().await;

                    if !tdlib_by_client_key.contains_key(&query.client_key) {
                        let tdlib = Tdlib::new();
                        tdlib_by_client_key.insert(query.client_key.clone(), tdlib);
                    }

                    handle_websocket_upgrade(
                        websocket,
                        query.client_key,
                        tdlib_receive_receiver.resubscribe(),
                        tdlib_send_sender.clone(),
                    ).await
                })
            }
        );

    let ip = env::var("RTDLIB_SERVER_LISTEN_IP").unwrap_or("127.0.0.1".to_string());
    let port = env::var("RTDLIB_SERVER_LISTEN_PORT").unwrap_or("3000".to_string());

    let ip: IpAddr = ip.parse().unwrap();
    let port: u16 = port.parse().unwrap();

    let addr: SocketAddr = (ip, port).into();

    debug!("listening on {}", addr);

    warp::serve(routes).run(addr).await;
}
