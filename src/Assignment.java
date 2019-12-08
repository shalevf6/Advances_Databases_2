import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.*;

/*
constructor for the ass
 */
public class Assignment {
    private Connection conn = null;
    private int numOfMovies;
    private String username = "";
    private String password = "";
    private String connectionUrl = "";
    private final String driver = "oracle.jdbc.driver.OracleDriver";

    /**
     * 1.2.1 constructor for the Ass object
     */
    public Assignment(String connection, String username, String password) {
        numOfMovies = 0;
        this.username = username;
        this.password = password;
        this.connectionUrl = connection;
        connect();
    }
    /**
     * The function makes the connection to the DB
     */
    private void connect()
    {
        try
        {
            Class.forName(this.driver); //registration of the driver
            this.conn = DriverManager.getConnection(this.connectionUrl, this.username, this.password);
            conn.setAutoCommit(false);
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
    /**
     * The function closes the connection to the DB
     */
    public void disconnect()
    {
        try
        {
            this.conn.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
    /**
     * 1.2.2 this function loads csv file to the program
     * path is the path to the movie file csv
     */
    public void fileToDataBase(String path)
    {
        String line = "";
        int numOfLines = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(path)))
        {
            while ((line = br.readLine()) != null) {
                numOfLines ++;
                String[] movie = line.split(",");
                addNewLineMediaitems(movie[0],Integer.parseInt( movie[1]));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        numOfMovies = numOfLines;
        System.out.println(numOfLines + " Films added to the program data");
    }

    private void addNewLineMediaitems(String title,int prod_year) {
        if(this.conn==null){
            connect();
        }
        PreparedStatement ps = null;
        String query = "INSERT INTO MEDIAITEMS(TITLE,PROD_YEAR)"+
                " VALUES(?,?)";
        try{
            ps = conn.prepareStatement(query);
            ps.setString(1, title);
            ps.setInt(2, prod_year);
            ps.executeUpdate();
            conn.commit();
        }catch (SQLException e) {
            try{
                conn.rollback();
            }catch (SQLException e2) {
                e2.printStackTrace();
            }
            e.printStackTrace();
        }finally{
            try{
                if(ps != null){
                    ps.close();
                }
            }catch (SQLException e3) {
                e3.printStackTrace();
            }
        }
    }
    
    /**
     * 1.2.3 - calculates the similarity between every pair of items in the MediaItems table and inserts or updates
     * the row in the Similarity table
     */
    public void calculateSimilarity()
    {
        List<Long> itemsList1 = getAllItems();
        List<Long> itemsList2 = new ArrayList<>(itemsList1);
        Set<Pair<Integer,Integer>> pairSet = new HashSet<>();

        // get the maximal distance between all the items
        int maximalDistance = getMaximalDistance();

        // go through every pair of items, calculate its similarity and a new record of a similarity in case it's missing
        int i = 0;
        while (i < itemsList1.size()) {
            int j = i + 1;
            while (j < itemsList2.size()) {
                Pair newPair = new Pair(i, j);
                if (i != j && !pairSet.contains(newPair)) {
                    pairSet.add(newPair);
                    double similarity = getSimilarity(itemsList1.get(i), itemsList2.get(j), maximalDistance);
                    addOrUpdateNewSimilarity(itemsList1.get(i), itemsList2.get(j), similarity);
                }
                j++;
            }
            i++;
        }
    }

    /**
     * get all the long numbers (items) from the db
     * @return - an arraylist containing all the long numbers from the db
     */
    private List<Long> getAllItems()
    {
        if(this.conn == null){
            connect();
        }

        List<Long> allItems = new  ArrayList<>();
        PreparedStatement ps = null;
        String query = "SELECT MID FROM MEDIAITEMS";

        try{
            ps = conn.prepareStatement(query);
            ResultSet rs = ps.executeQuery();
            while(rs.next()){
                allItems.add(rs.getLong("MID"));
            }
            rs.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                if(ps != null) {
                    ps.close();
                }
            }
            catch (SQLException e3) {
                e3.printStackTrace();
            }
        }
        return allItems;
    }

    /**
     * gets the maximal distance between all the items using the MaximalDistance Oracle function
     * @return - the maximal distance between all the items
     */
    private int getMaximalDistance()
    {
        if(this.conn == null){
            connect();
        }

        int maximalDistance = 0;
        CallableStatement cs = null;
        String call = "{? = call MaximalDistance()}";

        try{
            cs = conn.prepareCall(call);
            cs.registerOutParameter(1, oracle.jdbc.OracleTypes.NUMBER);
            cs.execute();
            maximalDistance = cs.getInt(1);

        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                if(cs != null) {
                    cs.close();
                }
            }
            catch (SQLException e3) {
                e3.printStackTrace();
            }
        }
        return maximalDistance;
    }

    /**
     * gets the similarity between two given items
     * @param item1 - the first item in the similarity calculation
     * @param item2 - the second item in the similarity calculation
     * @param maximalDistance - the maximal distance between all items
     * @return - the similarity between the two items
     */
    private double getSimilarity (Long item1, Long item2, int maximalDistance)
    {
        if(this.conn == null){
            connect();
        }

        double similarity = 0;
        CallableStatement cs = null;
        String call = "{? = call SimCalculation (?,?,?)}";
        try{
            cs = conn.prepareCall(call);
            cs.setLong(2, item1);
            cs.setLong(3, item2);
            cs.setInt(4, maximalDistance);
            cs.registerOutParameter(1, oracle.jdbc.OracleTypes.FLOAT);
            cs.execute();
            similarity = cs.getDouble(1);

        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                if(cs != null){
                    cs.close();
                }
            }
            catch (SQLException e3) {
                e3.printStackTrace();
            }
        }
        return similarity;
    }

    /**
     * adds or updates a new similarity record in the Similarity table
     * @param item1 - the first item that was compared
     * @param item2 - the second item that was compared
     * @param similarity - the similarity between the two items
     */
    private void addOrUpdateNewSimilarity(Long item1, Long item2, double similarity)
    {
        if(this.conn==null){
            connect();
        }

        PreparedStatement ps = null;
        String query = "INSERT INTO Similarity"+
                " VALUES(?,?,?)";
        try{
            ps = conn.prepareStatement(query);
            ps.setLong(1, item1);
            ps.setLong(2, item2);
            ps.setDouble(3, similarity);
            ps.executeUpdate();
            conn.commit();
        }
        catch (SQLException e) {
            try {
                conn.rollback();
            }
            catch (SQLException e2) {
                e2.printStackTrace();
            }
            e.printStackTrace();
        }
        finally {
            try {
                if(ps != null) {
                    ps.close();
                }
            }
            catch (SQLException e3) {
                e3.printStackTrace();
            }
        }
    }

    /**
     * 1.2.4 - prints all the titles and similarity values of all the items which the similarity between them and a
     * given long number is at least 0.3
     * @param mid - a given long number
     */
    public void printSimilarItems(long mid)
    {
        if(this.conn == null){
            connect();
        }

        List<Pair<String, String>> similarItems = new  ArrayList<>();
        PreparedStatement psMID1 = null;
        PreparedStatement psMID2 = null;

        // a query for getting all the items which the similarity between them and the 'similarity' parameter is
        // at least 0.3, where the mid parameter is in the MID1 column
        String getMID1Query = "SELECT MEDIAITEMS.TITLE as Title,SIMILARITY.MID2,SIMILARITY.SIMILARITY as Similarity FROM SIMILARITY " +
                "INNER JOIN MEDIAITEMS ON (SIMILARITY.MID2=MEDIAITEMS.MID AND SIMILARITY.MID2 !=?) WHERE MID1=? ORDER BY SIMILARITY DESC";
        // a query for getting all the items which the similarity between them and the 'similarity' parameter is
        // at least 0.3, where the mid parameter is in the MID2 column
        String getMID2Query = "SELECT MEDIAITEMS.TITLE as Title,SIMILARITY.MID2,SIMILARITY.SIMILARITY as Similarity FROM SIMILARITY " +
                "INNER JOIN MEDIAITEMS ON (SIMILARITY.MID1=MEDIAITEMS.MID AND SIMILARITY.MID1 !=?) WHERE MID2=? ORDER BY SIMILARITY DESC";
        try{
            psMID1 = conn.prepareStatement(getMID1Query);
            psMID1.setLong(1, mid);
            psMID1.setLong(2, mid);
            ResultSet rsMID1 = psMID1.executeQuery();
            addToSimilarItems(similarItems, rsMID1);
            rsMID1.close();
            psMID2 = conn.prepareStatement(getMID2Query);
            psMID2.setLong(1, mid);
            psMID2.setLong(2, mid);
            ResultSet rsMID2 = psMID2.executeQuery();
            addToSimilarItems(similarItems, rsMID2);
            rsMID2.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                if(psMID1 != null) {
                    psMID1.close();
                }
                if(psMID2 != null) {
                    psMID2.close();
                }
            }
            catch (SQLException e3) {
                e3.printStackTrace();
            }
        }

        similarItems.sort((o1, o2) -> {
            if (o1.getSim() < o2.getSim())
                return 1;
            else {
                if (o1.getSim() > o2.getSim())
                    return -1;
            }
            return 0;
        });

        similarItems = deleteDuplications(similarItems);

        // prints every similar item's title and similarity
        for (Pair<String, String> titleAndSimilarity : similarItems)
        {
            System.out.println(titleAndSimilarity.getL() + " " + titleAndSimilarity.getR());
        }
    }

    public List<Pair<String, String>> deleteDuplications(List<Pair<String, String>> list) {
        List<Pair<String, String>> uniqueList = new ArrayList<>();
        for (int i = 0; i < list.size(); i++) {
            boolean newPair = true;
            String l = list.get(i).getL();
            for (int j = 0; j < uniqueList.size() && newPair; j++) {
                if (l.equals(uniqueList.get(j).getL())) {
                    newPair = false;
                }
            }
            if (newPair) {
                uniqueList.add(list.get(i));
            }
        }
        return uniqueList;
    }

    /**
     * adds all items in a given result set to a given similar items list
     * @param similarItems - a given similar items list
     * @param rsMID - a given result set
     * @throws SQLException - to the function that called it
     */
    private void addToSimilarItems(List<Pair<String, String>> similarItems, ResultSet rsMID) throws SQLException {
        while(rsMID.next()){
            if (rsMID.getDouble("Similarity") >=  0.3) {
                Pair pair = new Pair(rsMID.getString("Title"), rsMID.getString("Similarity"));
                pair.setSim(rsMID.getDouble("Similarity"));
                similarItems.add(pair);
            }
        }
    }

    /**
     * this class represents a pair
     * @param <L> - the left object of the pair
     * @param <R> - the right object of the pair
     */
    public class Pair<L,R> {

        private L l;
        private R r;
        private double sim;

        Pair(L l, R r){
            this.l = l;
            this.r = r;
        }

        L getL(){
            return l;
        }

        R getR(){
            return r;
        }

        public double getSim() {
            return sim;
        }

        public void setSim(double sim) {

            this.sim = sim;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Pair) {
                Pair pair = (Pair)obj;
                return this.l.toString().equals(pair.getL().toString());
            }
            return false;
        }

        @Override
        public int hashCode() {
            if (l instanceof Integer && r instanceof Integer) {
                Integer left = (Integer) l;
                Integer right = (Integer) r;
                return (int) (Math.pow(2, left) * Math.pow(3, right));
            }
            else
                return 0;
        }
    }
}