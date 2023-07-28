public class DataEncoder {
    public static int encode(String code){
        int n = code.length();
        if(code.startsWith("User")){
            int id = Integer.parseInt(code.substring(4,n));
            return (1 << 29) + id;
        }else if(code.startsWith("Product")){
            int id = Integer.parseInt(code.substring(7,n));
            return (2 << 29) + id;
        }else if(code.startsWith("Review")){
            int id = Integer.parseInt(code.substring(6,n));
            return (3 << 29) + id;
        }
        return 0;
    }
    public static String decode(int code){
        int prefix = code / (1 << 29);
        int id = code % (1 << 29);
        if(prefix == 1){
            return "User" + id;
        }else if(prefix == 2){
            return "Product" + id;
        }else if(prefix == 3){
            return "Review" + id;
        }
        return "";
    }
}
