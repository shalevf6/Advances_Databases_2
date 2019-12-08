import java.sql.Connection;
import java.sql.DriverManager;

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

    /**
     * 1.2.5
     * @param username
     * @return
     */
    public static boolean isExistUsername(String username) {
        return false;
    }

    /**
     * 1.2.6
     * @param username
     * @param password
     * @param first_name
     * @param last_name
     * @param day_of_birth
     * @param month_of_birth
     * @param year_of_birth
     * @return
     */
    public static String insertUser(String username, String password, String first_name, String last_name,
                                    String day_of_birth, String month_of_birth, String year_of_birth) {
        return "";
    }

    /**
     * 1.2.7
     * @param top_n
     * @return
     */
    public static List<MediaItem> getTopNItems(int top_n) {
        return null;
    }

    /**
     * 1.2.8
     * @param username
     * @param password
     * @return
     */
    public static String validateUser(String username, String password) {
        return "";
    }

    /**
     * 1.2.9
     * @param username
     * @param password
     * @return
     */
    public static String validateAdministrator(String username, String password) {
        return "";
    }

    /**
     * 1.2.10
     * @param userid
     * @param mid
     */
    public static void insertToHistory(String userid, String mid) {

    }

    /**
     * 1.2.11
     * @param userid
     * @return
     */
    public static List<String, Date> getHistory(String userid) {
        return null;
    }

    /**
     * 1.2.12
     * @param userid
     */
    public static void insertToLog(String userid) {

    }

    /**
     * 1.2.13
     * @param n
     * @return
     */
    public static int getNumberOfRegisteredUsers(int n) {
        return 0;
    }

    /**
     * 1.2.14
     * @return
     */
    public static List<Users> getUsers() {
        return null;
    }

    /**
     * 1.2.15
     * @param userid
     * @return
     */
    public static Users getUser(String userid) {
        return null;
    }
}