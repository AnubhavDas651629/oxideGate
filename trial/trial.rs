// //enum

// enum Direction{
//     Up,
//     Down,
//     Left,
//     Right,
// }

// //to use

// let player_move = Direction::Up

// //instead of if we use match in rust

// match player_move{
//     Direction::Up => println!("Moving up"),
//     Direction::Down => println!("Moving Down"),
// }

// //instead of else if we use this

// match player_move{
//     Direction::Up => println!("Moving up"),
//     _=> println!("we dont care about any other direction")
// }

// //advanced enums(that hold data)

// enum webEvent {
//     PageLoad,
//     keyPress(char), //holds a single data
//     CLick{x: i64, y: i64}
// }

// let event1 = webEvent::keyPress('A')
// let event2 = webEvent::CLick{x:250, y: 120}

// // conditions for data enums

// let current_event = webEvent::CLick {x: 10, y: 50};
// match current_event{
//     webEvent::PageLoad => println!("Page loaded"),
//     webEvent::keyPress(c) => println!("user pressed: {}", c),
//     WebEvent::Click { x, y } => println!("User clicked at coordinates {}, {}", x, y),
// }

// // struct ---------------------------

// // defining the struct, every peice of sata has a name and a type
// struct User{
//     username: String,
//     email: String,
//     sign_in_count: u64,
//     active: bool
// }

// let mut user1 = User{
//     email: String::from("someone@example.com"),
//     username: String::from("someusername123"),
//     active: true,
//     sign_in_count: 1
// }

// //access data
// println!("user email is {}", user1.email);

// //changing data
// user1.email = String::from("new_email@example.com")

// //tuple structs
// struct Color(i32, i32, i32);
// struct Point(i32, i32, i32);

// let black = Color(0,0,0);
// let origin = Point(0,0,0);

// //impl methods --------------------------------

// struct Rectangle{
//     width: u32,
//     height: u32
// }

// impl Rectangle{
//     // all functions related to rectangle goes here
// }

// //read only(&self) -> if a method only needs to look at the data inside the struct but not change it
// impl Rectangle{
//     fn area(&self) -> u32{
//         self.width * self.height
//     }
// }

// let rect = Rectangle {width: 30, height: 50};
// println!{"The area is {}", rect.area()}

// //mutable method(&mut self)
// impl User{
//     fn deactivate(&mut self) {
//         self.active = false;
//         println!("{} has been deactivated", self.username)
//     }
// }
// //to use the variable must eb defined as mut
// let mut my_user = User{ ... };
// my_user.deactivate();

// // Associated functions(No self) -> Sometimes, you want a function that is related to the Struct, but doesn't actually need an existing instance of the Struct to run.
// impl User{
//     fn new (username: String, email: String) -> User{
//         User{
//             username,
//             email,
//             sign_in_count:0,
//             active: true,
//         }
//     }
// }
// let new_user = User::new(String::from("dave")),
// String::from("dave@test.com");
