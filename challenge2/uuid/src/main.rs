use std::io;
use std::thread;
use serde_json::{Value,json};
use tokio;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const  max_seq: u64 = 9999;
#[derive(Default)]
struct SequenceGenerator{
    last_timestamp: Option<u64>,
    seq:u64,
}
async fn snowflake_id_generator(node_Id: u64, sequence_generator: Arc<tokio::sync::Mutex<SequenceGenerator>>) -> u64 {

    loop{
        let mut state=sequence_generator.lock().await;

        let timestamp=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();

        let timestamp=timestamp as u64;

        match state.last_timestamp{
            Some(last_timestamp)=>{
               if timestamp==last_timestamp{
                    state.seq+=1;
                    if state.seq>max_seq{
                        continue;
                    }
                }else if timestamp>last_timestamp{
                    state.seq=0;
                    
                }
            },
            None=>{
                state.seq=0;
            }
        }

        state.last_timestamp=Some(timestamp);

        return (timestamp as u64)<<22 | (node_Id<<12) | state.seq;


        
    }

    
}
#[tokio::main]
async fn main() {

  let available_cpu_cores=thread::available_parallelism().unwrap().get();
  let semaphore=Arc::new(tokio::sync::Semaphore::new(available_cpu_cores));
   
  let mut saved_node_id=None;
  let mut sequence_generator=Arc::new(tokio::sync::Mutex::new(SequenceGenerator::default()));
  

   loop { 
    let mut input=String::new();
    io::stdin().read_line(&mut input).expect("Failed to read line");

    let sem_clone=Arc::clone(&semaphore);
    let permit=sem_clone.acquire_owned().await.unwrap();
    let input1: Value =serde_json::from_str(&input).expect("Failed to parse JSON");
   
    if input1["body"]["type"].as_str() == Some("init") {
    let node_name = input1["body"]["node_id"].as_str().unwrap();

    let node_id: u64 = node_name
        .strip_prefix('n')
        .unwrap()
        .parse()
        .unwrap();

    
    saved_node_id = Some(node_id);

    println!("{}", json!({
    "src": node_name,
        "dest": input1["src"],
        "body": {
            "type": "init_ok",
            "in_reply_to": input1["body"]["msg_id"]
        }
    }));

    continue;
    }

    let node_id=saved_node_id.unwrap();
    let seq=Arc::clone(&sequence_generator);






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
        }else if message_type=="generate"{

            

            
            let id=snowflake_id_generator(node_id,seq).await;

             let response=json!({
                "src":input1["dest"],
                "dest":input1["src"],
                "body":{
                    "type":"generate_ok",
                    "msg_id":input1["body"]["msg_id"],
                    "in_reply_to":input1["body"]["msg_id"],
                    "id":id
                }
            });

            println!("{}",response.to_string());
        }
        
        else{
            eprintln!("Unknown message type: {}", message_type);
        }
    });
    
    }
}
