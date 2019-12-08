
public class Main {
    public static void main(String[] args) {
        Assignment ass_2 = new Assignment("jdbc:oracle:thin:@132.72.65.216:1521:oracle","idsh","abcd");

        ass_2.fileToDataBase("C:\\Users\\Shalev\\Desktop\\University\\שנה ד - סמסטר א'\\נושאים מתקדמים בבסיסי נתונים\\עבודות\\עבודה 1\\films.csv");

        ass_2.calculateSimilarity();

        ass_2.disconnect();
    }
}
