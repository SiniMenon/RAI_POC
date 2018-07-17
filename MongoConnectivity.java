import java.net.UnknownHostException;
import java.util.List;
import java.util.Set;
import com.mongodb.*;
import com.mongodb.MongoClient;


public class MongoConnectivity {

        public static void main(String[] args) {
            try {

                MongoClient mongoClient = new MongoClient("localhost");

                List<String> databases = mongoClient.getDatabaseNames();

              for (String dbName : databases) {
                    System.out.println("- Database: " + dbName);

                    DB db = mongoClient.getDB(dbName);

                    Set<String> collections = db.getCollectionNames();
                    for (String colName : collections) {
                        System.out.println("\t + Collection: " + colName);
                    }
                }

                DB db = mongoClient.getDB("myMongoDB");
                DBCollection collection =   db.getCollection("sample");
                BasicDBObject document = new BasicDBObject();
                document.put("name", "Anu");
                document.put("Desig", "SA");
                collection.insert(document);

                mongoClient.close();

            } catch (UnknownHostException ex) {
                ex.printStackTrace();
            }

        }
    }

