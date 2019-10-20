/**
 * 
 */
package fifaworldcup;

import java.util.ArrayList;
import java.util.Scanner;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoClient;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.Collation.Builder;
import com.mongodb.client.model.CollationStrength;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.operation.OrderBy;
import java.util.Arrays;

/**
 * @author pio-f
 *
 */
public class MongoConnection {
	
private MongoClient mongoClient;
private MongoDatabase mongoDatabase;
private MongoCollection<Document> player, match, cup;
private Builder b;
private Collation c;

public MongoConnection() {

    mongoClient = new MongoClient( "localhost" , 27017 );
    mongoDatabase = mongoClient.getDatabase("FIFAWorld");
    player = mongoDatabase.getCollection("Players");
    match = mongoDatabase.getCollection("Matches");
    cup = mongoDatabase.getCollection("Cups");
    b = Collation.builder().locale("it").caseLevel(false).collationStrength(CollationStrength.SECONDARY);
    c = b.build();
    System.out.println("Connect to database " + mongoDatabase.getName() + " successfully");

}

public ArrayList<Document> findCups() {
    ArrayList<Document> documents = new ArrayList<>();
    for (Document document : cup.find())
        documents.add(document);
    return documents;
} 
	
    //Query di Base
    public ArrayList<Document> findPlayerName(String playerName){
            ArrayList<Document> results = new ArrayList();
            Bson query = Filters.regex("Player Name", ".*"+playerName+".*","i");  //$gt=greater; $lt=less; $eq=equal; $ne=not equal
            for(Document document :  player.find(query).collation(c)){
                    String matchID = document.getString("MatchID"); 
                                    Bson condition = new Document("$eq", matchID);
                                    Bson filter2 = new Document("MatchID", condition);
                                            for(Document document2: match.find(filter2).collation(c)) {
                                                    results.add(document2);
                                            }

                            }

            return results;
            }


    public ArrayList<Document>findYear(String year) {
            ArrayList<Document> results = new ArrayList();
            Bson filter = Filters.eq("Year", year);
            for(Document document :  match.find(filter).collation(c)){
                    results.add(document);
            }
            return results;
    }	


    public ArrayList<Document> findMatch(String teamA, String teamB ) {
            ArrayList<Document> results = new ArrayList();
            Bson filter = Filters.and(Filters.eq("Home Team Name", teamA), Filters.eq("Away Team Name", teamB));
            Bson filter2 = Filters.and(Filters.eq("Home Team Name", teamB), Filters.eq("Away Team Name", teamA));
            Bson filter3 = Filters.or(filter, filter2);
            for(Document document :  match.find(filter3).collation(c)){
                    results.add(document);
            }
            return results;
    }

    public ArrayList<Document> findTeam(String team) {
            ArrayList<Document> results = new ArrayList();
            Bson filter = Filters.or(Filters.eq("Home Team Name", team), Filters.eq("Away Team Name", team));
            for(Document document :  match.find(filter).collation(c)){
                    results.add(document);
            }
            return results;
    }

    public ArrayList<Document> findCoach(String coach) {
            ArrayList<Document> results = new ArrayList();
            Bson filter = Filters.regex("Coach Name",".*"+coach+".*","i");
            mongoDatabase.createCollection("tempCollection");
            MongoCollection<Document> temp = mongoDatabase.getCollection("tempCollection");
            for(Document document : player.find(filter).collation(c)) {
                    temp.insertOne(document);
            }
                    MongoCursor <String> files = temp.distinct("MatchID", String.class).iterator();
                    while(files.hasNext()) {
                            MongoCollection<Document> matches = mongoDatabase.getCollection("Matches");
                            Bson condition = new Document("$eq", files.next());
                            Bson filter2 = new Document("MatchID", condition);
                                    for(Document document2: matches.find(filter2)) {
                                            results.add(document2);
                                    }	
                    }

                    temp.drop();
                    return results;
    }


    public ArrayList<Document> findCountry(String country) {
        ArrayList<Document> results = new ArrayList();
            Bson filter = Filters.eq("Country", country);
            for(Document document :  cup.find(filter).collation(c)){
                    results.addAll(findYear(document.getString("Year")));
            }
            return results;
    }

    //query ricerca avanzata attributi singoli di Match
    public ArrayList<Document> findSingleAttributeMatch(String attribute, String value) {
            ArrayList<Document> results = new ArrayList();
            Bson filter = Filters.eq(attribute, value);
            for(Document document :  match.find(filter).collation(c)){
                    results.add(document);

            }
            return results;
    }

    //query ricerca avanzata attributi singoli di Player
    public ArrayList<Document> findSingleAttributePlayer(String attribute, String value) {
            ArrayList<Document> results = new ArrayList();
            Bson filter = Filters.regex(attribute, ".*"+value+".*","i");
            mongoDatabase.createCollection("tempCollection");
            MongoCollection<Document> temp = mongoDatabase.getCollection("tempCollection");
            for(Document document : player.find(filter).collation(c)) {
                    temp.insertOne(document);
            }
            MongoCursor <String> files = temp.distinct("MatchID", String.class).iterator();
            while(files.hasNext()) {
                    MongoCollection<Document> matches = mongoDatabase.getCollection("Matches");
                    Bson condition = new Document("$eq", files.next());
                    Bson filter2 = new Document("MatchID", condition);
                            for(Document document2: matches.find(filter2)) {
                                    results.add(document2);

                            }	
            }

            temp.drop();
            return results;

    }


    public ArrayList<Document> findEvent( String event) {
            ArrayList<Document> results = new ArrayList();
            Bson filter = Filters.regex("Event", ".*"+event+".*");
            mongoDatabase.createCollection("tempCollection");
            MongoCollection<Document> temp = mongoDatabase.getCollection("tempCollection");
            for(Document document : player.find(filter).collation(c)) {
                    temp.insertOne(document);
            }
            MongoCursor <String> files = temp.distinct("MatchID", String.class).iterator();
            while(files.hasNext()) {
                    MongoCollection<Document> matches = mongoDatabase.getCollection("Matches");
                    Bson condition = new Document("$eq", files.next());
                    Bson filter2 = new Document("MatchID", condition);
                            for(Document document2: matches.find(filter2)) {
                                    results.add(document2);
                            }	
            }

            temp.drop();
            return results;

    }

    public ArrayList<Document> findAttendance(String operator, String attendance) {
            Bson filter;
            ArrayList<Document> results = new ArrayList();
            if(operator.equals(">=")){
                    filter = Filters.gte("Attendance", attendance);
            }
            else if(operator.equals("<=")) {
                    filter = Filters.lte("Attendance", attendance);
            }
            else
                    filter = Filters.eq("Attendance", attendance);

                    for(Document document :  match.find(filter).collation(c)){
                            results.add(document);	
            }
                    return results;
    }

    //tutti gli attributi di Match	
    public ArrayList<Document> findAllMatch(ArrayList<Bson> filters) {
            Bson query = Filters.and(filters);
            ArrayList<Document> results = new ArrayList();
            for(Document document :  match.find(query).collation(c)){
                    results.add(document);
            }
            return results;
    }

    //tutti gli attributi di Match con Country
    public ArrayList<Document> findAllMatch(ArrayList<Bson> filters, String country) {
            ArrayList<Document> results = new ArrayList();
            mongoDatabase.createCollection("tempCollection");
            MongoCollection<Document> temp = mongoDatabase.getCollection("tempCollection");
            Bson filter = Filters.eq("Country", country);
            for(Document document :  cup.find(filter).collation(c)){
                    Bson filter2 = Filters.eq("Year", document.getString("Year"));
                    for(Document document2 :  match.find(filter2).collation(c)){
                            temp.insertOne(document2);
                    }
            }
            Bson query = Filters.and(filters);
            for(Document document :  temp.find(query).collation(c)){
                    results.add(document);
            }
            temp.drop();
            return results;
    }

    //tutti gli attributi di Player
    public ArrayList<Document> findAllPlayer(ArrayList<Bson>filters) {
            ArrayList<Document> results = new ArrayList();
            Bson query = Filters.and(filters);
            mongoDatabase.createCollection("tempCollection");
            MongoCollection<Document> temp = mongoDatabase.getCollection("tempCollection");
            for(Document document : player.find(query).collation(c)) {
                    temp.insertOne(document);
            }
            MongoCursor <String> files = temp.distinct("MatchID", String.class).iterator();
            while(files.hasNext()) {
                    MongoCollection<Document> matches = mongoDatabase.getCollection("Matches");
                    Bson condition = new Document("$eq", files.next());
                    Bson filter2 = new Document("MatchID", condition);
                            for(Document document2: matches.find(filter2).collation(c)) {
                                    results.add(document2);
                            }	
            }

            temp.drop();
            return results;

    }


    public ArrayList<Document> seePlayersinMatch(String matchID) {
            ArrayList<Document> results = new ArrayList();
            Bson filter = Filters.eq("MatchID",matchID);
            for(Document document: player.find(filter).collation(c).sort(Sorts.descending("Team Initials"))) {
                    results.add(document);
            }
            return results;
    }

    public void printDocumentMatch(ArrayList<Document>results) {
            for(int i=0; i<results.size(); i++) {
                    System.out.print(results.get(i).getString("Year")+" ");
                    System.out.print(results.get(i).getString("Datetime")+" ");
                    System.out.print(results.get(i).getString("Home Team Name")+" ");
                    System.out.print(results.get(i).getString("Away Team Name")+" ");
                    System.out.print(results.get(i).getString("Home Team Goals")+" ");
                    System.out.print(results.get(i).getString("Away Team Goals")+" ");
                    System.out.print(results.get(i).getString("City")+" ");
                    System.out.print(results.get(i).getString("Stadium")+" ");
                    System.out.print(results.get(i).getString("Stage")+" ");
                    System.out.print(results.get(i).getString("Half-time Home Goals")+" ");
                    System.out.print(results.get(i).getString("Half-time Away Goals")+" ");
                    System.out.print(results.get(i).getString("Referee")+" ");
                    System.out.print(results.get(i).getString("Assistant 1")+" ");
                    System.out.print(results.get(i).getString("Assistant 2")+" ");
                    System.out.print(results.get(i).getString("Attendance")+" ");
                    System.out.println(results.get(i).getString("Win conditions")+" ");
            }
    }

    public void printDocumentPlayer(ArrayList<Document>results) {
            for(int i=0; i<results.size(); i++) {
                    System.out.print(results.get(i).getString("Player Name")+" ");
                    System.out.print(results.get(i).getString("Shirt Number")+" ");
                    System.out.print(results.get(i).getString("Team Initials")+" ");
                    System.out.print(results.get(i).getString("Line-up")+" ");
                    if(results.get(i).getString("Event")!=null) {
                            System.out.print(results.get(i).getString("Event"));
                    }
                    if (results.get(i).getString("Position")!=null) {
                            System.out.print(results.get(i).getString("Position")+" ");
                    }
                    System.out.print(results.get(i).getString("Coach Name")+" ");
                            System.out.println();
                    }
            }


    public ArrayList<Document> countEventFromPlayerName(String playerName, String year){
            ArrayList<Document> players = new ArrayList();
            ArrayList<Document> results = new ArrayList();
            ArrayList<String> events = new ArrayList();
            events.add("G");
            events.add("OG");
            events.add("Y");
            events.add("R");
            events.add("SY");
            events.add("P");
            events.add("MP");
            events.add("I");
            events.add("O");
            Document document = new Document(); 
            document.append("Player Name", playerName);
            document.append("Year", year);
            ArrayList<Document>matches = findYear(year);
            MongoCollection<Document> temp = mongoDatabase.getCollection("tempCollection");
            temp.insertMany(matches);
            MongoCursor <String> files = temp.distinct("MatchID", String.class).iterator();
            while(files.hasNext()) {
                Bson condition = new Document("$eq", files.next());
                Bson filter2 = new Document("MatchID", condition);
                for(Document document2: player.find(filter2).collation(c)) {
                        players.add(document2);
                }
            }
            MongoCollection<Document> temp2 = mongoDatabase.getCollection("tempCollection2");
            temp2.insertMany(players);
            for(int i=0; i<events.size(); i++) {
                MongoCursor <Document> file = temp2.aggregate( Arrays.asList(
                Aggregates.match(Filters.and(Filters.regex("Player Name", ".*"+playerName+".*","i"), Filters.regex("Event",".*"+events.get(i)+".*","i" ))),
                Aggregates.group(playerName, Accumulators.sum(events.get(i), 1)))).collation(c).iterator();
                while(file.hasNext()) {
                    Integer event = file.next().getInteger(events.get(i));
                    document.append(events.get(i),event);
                }
            }
            results.add(document);
            temp.drop();
            temp2.drop();
            return results;
    }

    public ArrayList<Document> countEventFromPlayerName(String playerName){
        ArrayList<Document> players = new ArrayList();
        ArrayList<Document> results = new ArrayList();
        ArrayList<String> events = new ArrayList();
        events.add("G");
        events.add("OG");
        events.add("Y");
        events.add("R");
        events.add("SY");
        events.add("P");
        events.add("MP");
        events.add("I");
        events.add("O");
        Document document = new Document(); 
        document.append("Player Name", playerName);
        for(int i=0; i<events.size(); i++) {
            MongoCursor <Document> file = player.aggregate( Arrays.asList(
            Aggregates.match(Filters.and(Filters.regex("Player Name", ".*"+playerName+".*","i"), Filters.regex("Event",".*"+events.get(i)+".*","i" ))),
            Aggregates.group(playerName, Accumulators.sum(events.get(i), 1)))).collation(c).iterator();
            while(file.hasNext()) {
                    Integer event = file.next().getInteger(events.get(i));
                    document.append(events.get(i),event);
            }
        }
        results.add(document);
        return results;
    }

    public ArrayList<Document> findMixedAttribute(ArrayList<Bson> playerFilters, ArrayList<Bson>matchFilters){
            ArrayList<Document> players = findAllPlayer(playerFilters);
            ArrayList<Document> results = new ArrayList();
            MongoCollection<Document> temp = mongoDatabase.getCollection("tempCollection");
            temp.insertMany(players);
            Bson query = Filters.and(matchFilters);
            for(Document document: temp.find(query).collation(c)) {
                    results.add(document);
            }
            temp.drop();
            return results;
    }

}
