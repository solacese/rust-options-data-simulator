use rand::Rng;
use rumqtt::{MqttClient, MqttOptions, QoS, SecurityOptions};
use serde::Serialize;
use std::{thread, time};
use structopt::StructOpt;

/// Structure that represents a single option
#[derive(Serialize)]
struct OptionData {
    direction: u8,
    strike: f32,
    risk_free_rate: f32,
    volatility: f32,
    dividend_yield: f32,
}

/// Structure that represents an OptionMarketData event that contains Option chains
#[derive(Serialize)]
struct OptionsMarketData {
    ticker: String,
    spot: f32,
    option_expiry: f32,
    option_data: Vec<OptionData>,
}

/// Function that generates a single option with randomized data
fn generate_option(direction: u8, strike: f32) -> OptionData {
    let mut rng = rand::thread_rng();
    OptionData {
        direction: direction,
        strike: strike,
        risk_free_rate: rng.gen_range(0.01, 0.10),
        volatility: rng.gen_range(0.01, 0.10),
        dividend_yield: rng.gen_range(0.1, 1.0),
    }
}

/// Function that generates an OptionChain
fn generate_options_data(spot: f32, number_of_options: u16) -> Vec<OptionData> {
    let mut option_data_array: Vec<OptionData> = Vec::new();
    option_data_array.push(generate_option(1, spot));
    option_data_array.push(generate_option(0, spot));

    for x in 1..number_of_options / 2 {
        option_data_array.push(generate_option(1, spot + x as f32));
        option_data_array.push(generate_option(1, spot - x as f32));
        option_data_array.push(generate_option(0, spot + x as f32));
        option_data_array.push(generate_option(0, spot - x as f32));
    }

    option_data_array
}

#[derive(StructOpt, Debug)]
#[structopt(name = "option_market_data_simulator")]
struct Opt {
    /// The client-id of the MQTT connection
    #[structopt(long, default_value = "options_market_data_simulator")]
    mqtt_client_id: String,

    /// The hostname of the MQTT broker
    #[structopt(long, default_value = "localhost")]
    mqtt_host: String,

    /// The mqtt port
    #[structopt(long, default_value = "1883")]
    mqtt_port: u16,

    /// The mqtt username
    #[structopt(long, default_value = "default")]
    mqtt_username: String,

    /// The mqtt password
    #[structopt(long, default_value = "default")]
    mqtt_password: String,

    /// A csv string list for ticker symbols to generate the options for
    #[structopt(long, default_value = "AAPL,TSLA,MSFT,FB,GOOG,NFLX,CRM,AMD,INTL")]
    tickers: String,

    /// Number of option prices per symbol
    #[structopt(long, default_value = "100")]
    number_of_options: u16,

    /// Publish rate per second
    #[structopt(long, default_value = "10")]
    publish_rate: u64,
}

fn main() {
    let opt = Opt::from_args();

    // Setup the mqtt connection
    let mqtt_options =
        MqttOptions::new(opt.mqtt_client_id, opt.mqtt_host, opt.mqtt_port).set_security_opts(
            SecurityOptions::UsernamePassword(opt.mqtt_username, opt.mqtt_password),
        );

    let (mut mqtt_client, _) = MqttClient::start(mqtt_options).unwrap();

    //Run a publisher loop
    loop {
        for ticker in opt.tickers.split(",") {
            let mut rng = rand::thread_rng();
            let options_market_data = OptionsMarketData {
                ticker: ticker.to_owned(),
                spot: rng.gen_range(900.0, 1000.0),
                option_expiry: rng.gen_range(0.00, 0.45),
                option_data: generate_options_data(1000.0, opt.number_of_options),
            };

            let json = serde_json::to_string(&options_market_data).unwrap();

            println!("Publish {} option chain...", json);

            let mut topic_string = "OPTIONS/MARKETDATA/".to_owned();
            topic_string.push_str(&ticker);

            mqtt_client
                .publish(topic_string, QoS::AtLeastOnce, false, json)
                .unwrap();
        }
        thread::sleep(time::Duration::from_secs(opt.publish_rate));
    }
}
