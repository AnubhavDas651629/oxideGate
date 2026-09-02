//use is equal to import libraries from rust
// serde is library for serializing and deserialzing data
//Deserializing means taking raw text (like an incoming JSON string) and converting it into a Rust struct.
//Serializing means taking a Rust struct and converting it back into raw text (like JSON) to send out over the network.
use serde::{Deserialize, Serialize};

// what clients send us. A subset of the openAI cgat completion req
#[derive(Debug, Deserialize, Serialize)]
// in rust by default everything is private, pub make it public => the struct can be seen over in main.rs
pub struct ChatCompletionRequest {
    pub model: String,
    pub messages: Vec<Message>, //Vector, As for message, we need to define the "Message" struct somewhere
    pub max_tokens: Option<u32>, // by wrappin in option we tell rust this field might be null, or some(500)
    pub temperature: Option<f32>,
    #[serde(default)]
    // #[serde(default)] tells the library: "If the user doesn't send the stream field in their JSON, don't crash! Just use the default value."
    pub stream: bool, // bools deafult is false
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Message {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ChatCompletionResponse {
    pub id: String,
    pub object: String,
    pub created: u64,
    pub model: String,
    pub choices: Vec<Choice>,
    pub usage: Usage,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Choice {
    pub index: u32,
    pub message: Message,
    pub finish_reason: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Usage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
}
