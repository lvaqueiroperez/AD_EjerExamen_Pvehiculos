package pvehiculos;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.lt;
import com.mongodb.client.model.Projections;
import static com.mongodb.client.model.Projections.include;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import org.bson.Document;

public class Pvehiculos {

    private static MongoClient mongoClient;
    private static MongoDatabase database;
    private static MongoCollection<Document> collection;

    //VARIABLES MONGO
    //numeros desde mongo siempre serán en java double !!!
    static double id;
    static String dni;
    static String codveh;

    public static void conexionMongo() {

        //NOS CONECTAMOS A MONGO
        mongoClient = new MongoClient("localhost", 27017);

        //NOS CONECTAMOS A UNA BD
        database = mongoClient.getDatabase("test");

        //se supone que no nos va a pedir credenciales, así que no los ponemos de momento
        collection = database.getCollection("vendas");

    }

    public static void desconexionMongo() {

        mongoClient.close();

    }
    
    //OBJETO DE CONEXIÓN OBJECT DB
    public static EntityManagerFactory emf = Persistence.createEntityManagerFactory("/home/oracle/objectdb_2.7.5_01/db/vehicli.odb");

    //EJERCICIO
    public static void datos() {

       
        //COMO VAMOS A RETORNAR MÁS DE UN RESULTADO, HAY QUE USAR UN ITERABLE !!!!
        FindIterable<Document> datosMongo = collection.find();
        
         //OBTENEMOS LOS DATOS DE MONGO
        for (Document z : datosMongo) {

            id = z.getDouble("_id");
            System.out.println("ID: " + id);
            dni = z.getString("dni");
            System.out.println("DNI: " + dni);
            codveh = z.getString("codveh");
            System.out.println("CODVEH: " + codveh);
            
            //SEGUIMOS CON OBJECTDB
            
            
            
            
            
            

        }

    }

    public static void main(String[] args) {

        conexionMongo();
        datos();
        desconexionMongo();

    }

}
