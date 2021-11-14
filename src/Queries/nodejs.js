//Se requiere los drivers oficiales de MongoDB Nod.js
var mongodb =require("mongodb") //libreria de mongo

var client = mongodb.MongoClient;  //Nos ofrece un cliente
var url = "mongodb://localhost:27017/"; //Mediante este protocolo 
var movie_title ='Toy Story'


//Codigo de gestión de base de datos tipico en un servidor 
client.connect(url, function (err, client){
    
    var db = client.db("movie-dataset");  //Conexion a la base de datos
    var collection = db.collection("movies_metadata");  //Coleción 

    var query = {
        original_title: 'Toy Story'
    };  //Query deseada 

    var projection = {
        original_title: 1
    }

    //var cursor = collection.find(query).project(projection);
   /* var cursor = collection.aggregate([
        {
            $match:{
                original_title: 'Toy Story'       //Agregación con la misma funcion que .find()
            }
        }

    ]);
*/
   /* 
    collection.insertMany([                           //Inserts
        {curso: 'MasterMind', tematica: 'MongoDB' }
    ]);
   */ 
    cursor.forEach(
        function(doc){
            console.log(doc);

        },
        function(err){
            client.close();
        }
    );
});

