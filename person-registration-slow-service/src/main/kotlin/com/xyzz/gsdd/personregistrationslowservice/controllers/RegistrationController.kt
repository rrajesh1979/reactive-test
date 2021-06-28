package com.xyzz.gsdd.personregistrationslowservice.controllers

import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.ReadConcern
import com.mongodb.WriteConcern
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import com.mongodb.connection.ConnectionPoolSettings
import com.xyzz.gsdd.personregistrationslowservice.models.Person
import org.bson.Document
import org.bson.types.ObjectId
import org.springframework.http.HttpStatus.CREATED
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit

@RestController("/register")
class RegistrationController//            MONGODB_URI = System.getenv("MONGODB_URI")//Initialize MongoDB Client
    () {
    var mongoClient: MongoClient? = null
    val BOOKS_DB = "learn"
    val BOOKS_COLLECTION = "books"

    init {
        var MONGODB_URI: String? = null
        try {
//            MONGODB_URI = System.getenv("MONGODB_URI")
            MONGODB_URI = "mongodb://localhost:27017"
            println(MONGODB_URI)
            val connectionPoolSettings = ConnectionPoolSettings.builder()
                .maxSize(100)
                .maxWaitTime(120000, TimeUnit.MILLISECONDS)
                .build()
            val mongoClientSettings = MongoClientSettings.builder()
                .applyConnectionString(ConnectionString(MONGODB_URI))
                .writeConcern(WriteConcern.MAJORITY)
                .readConcern(ReadConcern.MAJORITY)
                .retryWrites(true)
                .applyToConnectionPoolSettings { builder: ConnectionPoolSettings.Builder ->
                    builder.applySettings(
                        connectionPoolSettings
                    )
                }
                .build()
            mongoClient = MongoClients.create(mongoClientSettings)

        } catch (e: Exception) {
            println("Error initializing")
        }
    }


    @PostMapping
    @ResponseStatus(CREATED)
    fun register(@RequestBody person: Mono<Person>): Mono<Person> {
        val newBook = Document()
        val id = ObjectId()
        newBook.append("_id", id)
            .append("bookId", "1000")
            .append("bookDetails", "bookDetails")

        try {
            var booksCollection: MongoCollection<Document>? = null
            booksCollection =
                mongoClient!!.getDatabase(BOOKS_DB).getCollection(BOOKS_COLLECTION)
            booksCollection.insertOne(newBook)
        } catch (e: Exception) {
            println("Failed ::" + e.message)
        }

        return person.delayElement(Duration.ofMillis(1)) // Mimic blocking nature
                .map { it.copy(id = UUID.randomUUID()) }
    }

}
