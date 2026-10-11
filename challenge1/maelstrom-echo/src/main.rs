use std::io;
use std::thread;
use serde_json::{Value,json};
use tokio;
use std::sync::Arc;
#[tokio::main]
async fn main() {

  let available_cpu_cores=thread::available_parallelism().unwrap().get();
  let semaphore=Arc::new(tokio::sync::Semaphore::new(available_cpu_cores));
   
   loop { 
    let mut input=String::new();
    io::stdin().read_line(&mut input).expect("Failed to read line");

    let sem_clone=Arc::clone(&semaphore);
    let permit=sem_clone.acquire_owned().await.unwrap();

    let handle=tokio::spawn(async move {

        let _permit=permit;
        let mut input1 : Value =serde_json::from_str(&input).expect("Failed to parse JSON");

        let message_type=input1["body"]["type"].as_str().unwrap_or("failed to read type os message");


        if message_type=="init"{
            let response=json!({
                "src":input1["body"]["node_id"],
                "dest":input1["src"],
                "body":{
                    "type":"init_ok",
                    "in_reply_to":input1["body"]["msg_id"]
                }
            });

            println!("{}",response);
        }else if message_type=="echo"{

            
            let response=json!({
                "src":input1["dest"],
                "dest":input1["src"],
                "body":{
                    "type":"echo_ok",
                    "msg_id":input1["body"]["msg_id"],
                    "in_reply_to":input1["body"]["msg_id"],
                    "echo":input1["body"]["echo"]
                }
            });

            println!("{}",response.to_string());
        }else{
            eprintln!("Unknown message type: {}", message_type);
        }
    });
    
    }
    


}
