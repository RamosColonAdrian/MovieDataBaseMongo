//Dentro de la base de datos existen películas con contenido adulto y no adulto, se requiere un conteo de la cantidad de peliculas de cada tipo 
db = db.getSiblingDB("Proyecto");
db.getCollection("movie_dataset").aggregate(
    [
        { 
            "$group" : { 
                "_id" : "$adult", 
                "quantity" : { 
                    "$sum" : 1.0
                }
            }
        }
    ], 
    { 
        "allowDiskUse" : false
    }
);

//Se debe realizar un muestreo de la base de datos y conocer el numero de peliculas que duran más de media hora agrupandolas por idioma. 
db = db.getSiblingDB("Proyecto");
db.getCollection("movie_dataset").aggregate(
    [
        { 
            "$match" : { 
                "runtime" : { 
                    "$gte" : 90.0
                }
            }
        }, 
        { 
            "$group" : { 
                "_id" : "$original_language", 
                "quantity" : { 
                    "$sum" : 1.0
                }
            }
        }
    ], 
    { 
        "allowDiskUse" : false
    }
);


//Se requiere un listado de peliculas donde haya participado Tom Hanks
db = db.getSiblingDB("Proyecto");
db.getCollection("credits").aggregate(
    [
        { 
            "$match" : { 
                "movies.cast.name" : "Tom Hanks"
            }
        }, 
        { 
            "$lookup" : { 
                "from" : "movie_dataset", 
                "localField" : "_id", 
                "foreignField" : "_id", 
                "as" : "movies_info"
            }
        }, 
        { 
            "$unwind" : { 
                "path" : "$movies_info"
            }
        }, 
        { 
            "$project" : { 
                "movieTile" : "$movies_info.original_title"
            }
        }
    ], 
    { 
        "allowDiskUse" : false
    }
);

//Se necesita una lista de actores que han trabajado con Sean Connery en una pelicula.

db = db.getSiblingDB("Proyecto");
db.getCollection("credits").aggregate(
    [
        { 
            "$match" : { 
                "movies.cast.name" : "Sean Connery"
            }
        }, 
        { 
            "$lookup" : { 
                "from" : "movie_dataset", 
                "localField" : "_id", 
                "foreignField" : "_id", 
                "as" : "movies_info"
            }
        }, 
        { 
            "$unwind" : { 
                "path" : "$movies.cast"
            }
        }, 
        { 
            "$project" : { 
                "actor" : "$movies.cast.name"
            }
        }, 
        { 
            "$match" : { 
                "actor" : { 
                    "$ne" : "Sean Connery"
                }
            }
        }
    ], 
    { 
        "allowDiskUse" : false
    }
);


//Se necesitan la nota media de de cada collección almacenada en nuestra base de datos 
db = db.getSiblingDB("Proyecto");
db.getCollection("movie_dataset").aggregate(
    [
        { 
            "$match" : { 
                "belongs_to_collection.name" : { 
                    "$ne" : null
                }
            }
        }, 
        { 
            "$group" : { 
                "_id" : "$belongs_to_collection.name", 
                "note" : { 
                    "$avg" : "$vote_average"
                }
            }
        }
    ], 
    { 
        "allowDiskUse" : false
    }
);


//La mayoría de las consultas en nuestra base de datos son sobre los titulos de las peliculas, sería necesario la creación de un índice  que agilice las busquedas
db = db.getSiblingDB("Proyecto");
db.getCollection("movie_dataset").createIndex(
    { "original_title":1} ,
    { name: "original_title" }
)

//Se ha solicitado un listado de las peliculas producidas por la compañia de century fox o por la warner y que duren 1 hora y 50min o  más 
db = db.getSiblingDB("Proyecto");
db.getCollection("movie_dataset").find(
    { 
        "$and" : [
            { 
                "$or" : [
                    { 
                        "production_companies.name" : { 
                            "$regex" : /century fox/i
                        }
                    }, 
                    { 
                        "production_companies" : { 
                            "$regex" : /warner/i
                        }
                    }
                ]
            }, 
            { 
                "runtime" : { 
                    "$gte" : 110.0
                }
            }

        ]
    },
    { 
        "original_title" : 1.0
    }
);


//Se necesita una lista de peliculas y sus fechas de lanzamiento que cumplan las siguientes condiciones: deben de haber sido lanzadas entre el 2000 y el 2006 y sus datos tienen que estar validados
db = db.getSiblingDB("Proyecto");
db.getCollection("movie_dataset").find(
    { 
        "$and" : [
            { 
                "releasedate" : { 
                    "$gte" : ISODate("2000-01-01T00:00:00.000+0000"), 
                    "$lte" : ISODate("2006-01-01T00:00:00.000+0000")
                }
            }, 
            { 
                "releasedate" : { 
                    "$exists" : true, 
                    "$type" : "date"
                }
            }
        ]
    }, 
    { 
        "original_title" : 1.0, 
        "release_date" : "$releasedate"
    }
);

//Se precisa saber el numero de peliculaes pertenecientes a cada coleccion de peliculas y sus gastos
db = db.getSiblingDB("Proyecto");
db.getCollection("movie_dataset").aggregate(
    [
        { 
            "$match" : { 
                "belongs_to_collection" : { 
                    "$ne" : 0.0
                }, 
                "budget" : { 
                    "$ne" : 0.0
                }
            }
        }, 
        { 
            "$group" : { 
                "_id" : "$belongs_to_collection", 
                "movie_number" : { 
                    "$sum" : 1.0
                }, 
                "total_expenses" : { 
                    "$sum" : "$budget"
                }
            }
        }, 
        { 
            "$project" : { 
                "movie_number" : 1.0, 
                "total_expenses" : 1.0
            }
        }
    ], 
    { 
        "allowDiskUse" : false
    }
);
