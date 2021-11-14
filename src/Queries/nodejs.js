//Se requiere los drivers oficiales de MongoDB Nod.js
var mongodb =require("mongodb"); //libreria de mongo

var client = mongodb.MongoClient;  //Nos ofrece un cliente
var url = "mongodb://localhost:27017/"; //Mediante este protocolo 


//Codigo de gestión de base de datos tipico en un servidor 
client.connect(url, function (err, client){
    
    var db = client.db("Proyecto");  //Conexion a la base de datos
    var collection = db.collection("movie_dataset");  //Coleción 

    var query = {};  //Query deseada 
    
    var cursor = collection.find(query);//.project(projection);
  
    cursor.forEach(
        function(doc){
            console.log(doc);

        },
        function(err){
            client.close();
        }
    );
});

