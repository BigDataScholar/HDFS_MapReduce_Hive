package tools;

/**
 * @Auther: 传智新星
 * @Date: 2019/11/24 18:03
 * @Description:
 */
public class test1 {


    public static void main(String[] args) {


        String s = "203.208.60.187 - - [04/Jan/2012:00:00:02 +0800] \"GET /archiver/tid-3003.html HTTP/1.1\" 200 2056 \"-\" \"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)\"";

        String[] s1 = s.split(" ");


        System.out.println(s1[11].substring(1));
        System.out.println(s1[0]);

    }
}