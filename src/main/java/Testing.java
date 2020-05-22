public class Testing {
    public static  void main(String[] args){
        double zMax=2.375, zMin= 0.00004;
        double tenMax=zMax*10, tenMin=zMin*10;
        double Ceil=Math.floor(tenMax)/10;
        double Floor=Math.ceil(tenMin)/10;
        System.out.println("Ceil: "+Ceil+" Floor: "+Floor);
    }
}
