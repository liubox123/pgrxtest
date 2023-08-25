
use pgrx::bgworkers::*;
use pgrx::datum::{FromDatum, IntoDatum};
use pgrx::prelude::*;
use std::time::Duration;
use std::thread;
use actix_web::{middleware, rt, web, App, HttpRequest, HttpServer,Responder,HttpResponse,http::header};
use pgrx::PgBuiltInOids;
use pgrx::pg_sys;
// use 
use lazy_static::*;
use std::sync::mpsc::{channel,Sender,sync_channel,Receiver};
use std::sync::Mutex;
use std::sync::Arc;
use serde_urlencoded;
// use form_urlencoded;
/*
    In order to use this bgworker with pgrx, you'll need to edit the proper `postgresql.conf` file in
    `~/.pgrx/data-PGVER/postgresql.conf` and add this line to the end:

    ```
    shared_preload_libraries = 'bgworker.so'
    ```

    Background workers **must** be initialized in the extension's `_PG_init()` function, and can **only**
    be started if loaded through the `shared_preload_libraries` configuration setting.

    Executing `cargo pgrx run <PGVER>` will, when it restarts the specified Postgres instance, also start
    this background worker
*/

pgrx::pg_module_magic!();

#[pg_guard]
pub extern "C" fn _PG_init() {
    BackgroundWorkerBuilder::new("Background Worker Example")
        .set_function("background_worker_main")
        .set_library("pgbgtest")
        .set_argument(55i32.into_datum())
        .enable_spi_access()
        .load();
    // let handletest  = thread::spawn(move ||{println!("1111");});
    // handletest.join().expect("msxxxxxg");
}
pub struct TestData{
    data:i32,
    strinfo:String
}
// async fn getdata(req: HttpRequest,sender: web::Data<Sender<TestData>>,recvser:web::Data<Arc<Mutex<Receiver<String>>>>) ->impl Responder {

async fn getdata(req: HttpRequest,sender: web::Data<Sender<TestData>>,recvser:web::Data<Arc<Mutex<Receiver<String>>>>) ->impl Responder {
    println!("REQ: {:?}", req);
    // println!("req string :{:?}", req.query_string());
    let xxx= serde_urlencoded::from_str::<Vec<(String,String)>>(req.query_string()).unwrap();
    // println!("xxx: {:#?}", xxx[0].1);
    // let x = form_urlencoded::parse( req.query_string().as_bytes());
    sender.send(TestData{data:1,strinfo:xxx[0].1}).expect("send error\n");
    let objects = Arc::new(Mutex::new(String::from("")));
    println!("REQ:2");
    let clone = Arc::clone(&objects);
    // BackgroundWorker::get_extra()
    // BackgroundWorker::worker_continue()
    // BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);
    println!("REQ:3");
    let mut outstr2 = String::from("");
    let outstr = Mutex::new(String::from(""));

    println!("REQ:4");
    // let ret = *objects.lock().unwrap();
    //     ret
    
    let ssss = format!("{}", objects.lock().unwrap());
    println!("{}",ssss);
    let x = recvser.lock().unwrap().recv().unwrap().replace("//n", "<br>");;
    // let strbase = String::from(include_str!("index.html"));
    // let newstr =strbase.replace("!!!!!!!!!!!!!!", Box::leak(x.into_boxed_str())).replace("//n", "<br>");
    HttpResponse::Ok().content_type("text/text; charset=utf-8")
    .body(Box::leak(x.into_boxed_str()).to_owned())
}
async fn index(req: HttpRequest) ->impl Responder {

    // let ret = *objects.lock().unwrap();
    //     ret
    
    // let ssss = format!("{}", objects.lock().unwrap());
    // println!("{}",ssss);
    // let x = recvser.lock().unwrap().recv().unwrap();
    let strbase = String::from(include_str!("index.html"));
    // let newstr =strbase.replace("!!!!!!!!!!!!!!", Box::leak(x.into_boxed_str())).replace("//n", "<br>");
    // HttpResponse::Ok().customize()
    // HttpResponse::Ok().body("ok")
    HttpResponse::Ok().content_type("text/html; charset=utf-8")
    .body(Box::leak(strbase.into_boxed_str()).to_owned())
            // .body(Box::leak(newstr.into_boxed_str()).to_owned())
    // recvser
    // "hello "
    // Box::leak(newstr.into_boxed_str()).to_owned()
        // let x = Box::leak( (*objects.lock().unwrap()).into_boxed_str()); x
    // Box::leak(outstr.into_boxed_str())
    // result.unwrap()
    // "Hello world!\r\n"
}



#[pg_guard]
#[no_mangle]
pub extern "C" fn background_worker_main(arg: pg_sys::Datum) {
    let arg = unsafe { i32::from_polymorphic_datum(arg, false, pg_sys::INT4OID) };

    // these are the signals we want to receive.  If we don't attach the SIGTERM handler, then
    // we'll never be able to exit via an external notification
   
    println!(
        "Hello from inside the {} BGWorker!  Argument value={}",
        BackgroundWorker::get_name(),
        arg.unwrap()
    );
    // let handletest  = thread::spawn(move||{        println!(
    //     "ttttttttt {} BGWorker!  ",
    //     BackgroundWorker::get_name()
    //     // arg.unwrap()
    // );});
    // handletest.join().expect("msxxxxxg");
    let (tx, rx) = channel::<TestData>();
    let (stx,srx) = sync_channel::<String>(1);
    let handle =thread::spawn(move||{
        let objects = Arc::new(Mutex::new(srx));
        println!("REQ:2");
        let clone = Arc::clone(&objects);
        println!(
            "start local thread for server {} BGWorker!  ",
            BackgroundWorker::get_name()
        );
        let htser =  HttpServer::new(move|| {
            App::new()
            .service(web::resource("/").route(web::get().to(index)))
            .service(web::resource("/a").route(web::get().to(getdata)).app_data(web::Data::new(tx.clone())).app_data(web::Data::new(objects.clone())))
        })

        .bind(("0.0.0.0", 18000));
        match htser {
            Ok(v)=>{
                rt::System::new().block_on(v.run());
            },
            Err(e)=>{
                println!(
                    "bind error {} BGWorker!  ",
                    BackgroundWorker::get_name()
                );
            }
        }
        //  rt::System::new().block_on(
        //     match (
       
        // );
        
        println!(
            "end local thread for server {} BGWorker!  ",
            BackgroundWorker::get_name()
            // arg.unwrap()
        );
    });
    
    
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    // we want to be able to use SPI against the specified database (postgres), as the superuser which
    // did the initdb. You can specify a specific user with Some("my_user")
    BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);

    while BackgroundWorker::wait_latch(Some(Duration::from_millis(100))) {
        // println!("testxxxx4 ");
        if BackgroundWorker::sighup_received() {
            // on SIGHUP, you might want to reload some external configuration or something
        }

        // within a transaction, execute an SQL statement, and log its results
        let result: Result<(), pgrx::spi::Error> = BackgroundWorker::transaction(|| {
            let rret =   rx.try_recv();
            match rret{
                Ok(v)=>{
                    
                    Spi::connect(|client| {

                        let tuple_table = client.select(
                            "SELECT 'Hi' rr, id, ''||a rrr FROM (SELECT id, 43 from generate_series(1,10) id) a ",
                            None,
                            None,
                        ).unwrap();
                        let mut strall = String::from("");
                        for i in 1..tuple_table.columns().unwrap()+1{
                            strall+=&String::from(format!("{:?}\t|",tuple_table.column_name(i).unwrap()) ) ;
                        }
                        // tuple_table.column_name(1)
                        for tuple in tuple_table {
                            // for i in 1.. tuple.columns(){
                            //     let od =  tuple.get_datum_by_ordinal(1).unwrap();
                            //     match od{

                            //     }
                            // }
                            // tuple_table.column_name(i).unwrap()
                            strall+= &String::from("//n");
                            for i in 1..tuple.columns()+1{
                                let od = PgBuiltInOids::try_from(tuple.get_datum_by_ordinal(i).unwrap().oid());
                                // let stroid = String::type_oid();
                                match od {
                                    Ok(v)=>{
                                        match v{
                                            
                                            PgBuiltInOids::XIDOID =>{
                                                
                                                strall+=&String::from(tuple.get_datum_by_ordinal(i).unwrap().value::<String>().unwrap().unwrap());
                                            },
                                            PgBuiltInOids::TEXTOID =>{
                                                
                                                strall+=&String::from(tuple.get_datum_by_ordinal(i).unwrap().value::<String>().unwrap().unwrap());
                                            },
                                            PgBuiltInOids::INT4OID =>{
                                           
                                                strall+=&String::from(tuple.get_datum_by_ordinal(i).unwrap().value::<i32>().unwrap().unwrap().to_string());
                                            },
                                            _=>{
                                                strall+=&String::from(format!("oid:({:?})", tuple.get_datum_by_ordinal(i).unwrap().oid().as_u32()));
                                                // println!("match unknow");
                                            }
                                            
                                        }
                                        strall+= &String::from("\t|");
                                    },
                                    Err(e)=>{
                                        println!("match error");
                                    }
                                    // PgBuiltInOids::=>{
                                    //     strall+=&String::from(tuple.get_datum_by_ordinal(i).unwrap().value::<String>().unwrap().unwrap());
                                    // }
                                }
                                // strall+=&String::from(format!("\t oid: ({:?})", tuple.get_datum_by_ordinal(i).unwrap().oid().as_u32()));
                            }
                            // let a = tuple.get_datum_by_ordinal(1).unwrap().value::<String>().unwrap().unwrap();
                            // let b = tuple.get_datum_by_ordinal(2).unwrap().value::<i32>().unwrap().unwrap();
                            // let c = tuple.get_datum_by_ordinal(3).unwrap().value::<String>().unwrap().unwrap();
                            // strall+=&String::from(format!("from bgworker: ({:?}, {:?}, {:?})", a, b, c));
                            // println!("from bgworker: ({:?}, {:?}, {:?})", a, b, c);
                        }
                        stx.send(strall);
                        // Ok::<(), E>(())
                        // Ok(())
                    });
                    println!("{}",format!("{}",v.data));
                },
                Err(e)=>{
                    // println!("get nothing");
                }
            }
            // Spi::connect(|client| {

            //     let tuple_table = client.select(
            //         "SELECT 'Hi', id, ''||a FROM (SELECT id, 43 from generate_series(1,10) id) a ",
            //         None,
            //         None,
            //     )?;
            //     for tuple in tuple_table {
                    
            //         let a = tuple.get_datum_by_ordinal(1)?.value::<String>()?;
            //         let b = tuple.get_datum_by_ordinal(2)?.value::<i32>()?;
            //         let c = tuple.get_datum_by_ordinal(3)?.value::<String>()?;
                   
            //         println!("from bgworker: ({:?}, {:?}, {:?})", a, b, c);
            //     }
            //     Ok(())
            // })
            Ok(())
        });
        // println!("testxxxx6 ");
        result.unwrap_or_else(|e| panic!("got an error: {:#?}", e))
        // result.unwrap()

    }
    // });
    
    // handlebgw.join();
    log!("Goodbye from inside the {} BGWorker! ", BackgroundWorker::get_name());
}