use std::io;
use std::thread;
use serde_json::{Value,json};
use tokio;
use std::sync::Arc;

struct SeenNumbers{
    seen_numbers: Arc<tokio::sync::Mutex<std::collections::HashSet<u64>>>,
}

#[tokio::main]
async fn main() {

  let available_cpu_cores=thread::available_parallelism().unwrap().get();
  let semaphore=Arc::new(tokio::sync::Semaphore::new(available_cpu_cores));
  let mut seen=Arc::new(SeenNumbers{
      seen_numbers: Arc::new(tokio::sync::Mutex::new(std::collections::HashSet::new())),
  });
   
   loop { 
    let mut input=String::new();
    io::stdin().read_line(&mut input).expect("Failed to read line");

    let sem_clone=Arc::clone(&semaphore);
    let permit=sem_clone.acquire_owned().await.unwrap();
    let seen=Arc::clone(&seen);

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
        }else if message_type=="broadcast"{
            let number=input1["body"]["message"].as_u64().unwrap();
            let mut seen_numbers=seen.seen_numbers.lock().await;
            seen_numbers.insert(number);

            let response=json!({
                "src":input1["dest"],
                "dest":input1["src"],
                "body":{
                    "type":"broadcast_ok",
                    "msg_id":input1["body"]["msg_id"],
                    "in_reply_to":input1["body"]["msg_id"]
                }
            });
            println!("{}",response.to_string());
            
        }else if message_type=="read"{
            let seen_numbers=seen.seen_numbers.lock().await;
            let numbers: Vec<u64> = seen_numbers.iter().cloned().collect();
            let response=json!({
                "src":input1["dest"],
                "dest":input1["src"],
                "body":{
                    "type":"read_ok",
                    "msg_id":input1["body"]["msg_id"],
                    "in_reply_to":input1["body"]["msg_id"],
                    "messages":numbers
                }

            
            });

            
            println!("{}",response.to_string());
        }else if message_type=="topology"{
            let response=json!({
                "src":input1["dest"],
                "dest":input1["src"],
                "body":{
                    "type":"topology_ok",
                    "msg_id":input1["body"]["msg_id"],
                    "in_reply_to":input1["body"]["msg_id"]
                }
            });

            println!("{}",response.to_string());

        }

            
    });
    
    
    


}

}