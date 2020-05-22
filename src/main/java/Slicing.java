import java.io.IOException;
import java.lang.String;
import java.util.Iterator;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;





public class Slicing {

    public static class TokenizerMapper
            //set the input of map:<Object,Text>
            //set the output of map:<IntWritable, Text>
            extends Mapper<LongWritable, Text, DoubleWritable, Text> {//edited: integer first, then text/string



        public void map(LongWritable key, Text value, Context context)    //the input of mapper
        throws IOException,InterruptedException {
            java.lang.String temp=value.toString();         //temp is the String format of the line
            String[] co= temp.split(" ");  //split the string into words
            //get the vertex (String) of the triangle
            String x1=co[8], y1=co[9], z1=co[10],x2=co[12],y2=co[13],z2=co[14],x3=co[16],y3=co[17],z3=co[18];
            //converse to the float format
            double Z1=Double.parseDouble(z1), Z2=Double.parseDouble(z2),Z3=Double.parseDouble(z3);
            double zMax=Math.max(Math.max(Z1,Z2),Z3), zMin=Math.min(Math.min(Z1,Z2),Z3);
            //the output value (String)
            String coordinates = x1 + ' ' + y1 + ' ' + z1 + ',' + x2 + ' ' + y2 + ' ' + z2 + ',' + x3 + ' ' + y3 + ' ' + z3 + ';';
            System.out.println(coordinates);

            double tenMax=zMax*10, tenMin=zMin*10;
            double Ceil=Math.floor(tenMax)/10;
            double Floor=Math.ceil(tenMin)/10;
            if(Math.abs(Floor-zMin)<0.000001)Floor+=0.1;
            if(Math.abs(Ceil-zMax)<0.000001)Ceil-=0.1;

            //To check if the triangle is right on the slicing plane, I introduce the integer overlap
            int overlap=0;
            if(Floor>Ceil||Math.abs(Floor-Ceil)<0.0000001) {//Floor is no smaller than Ceil, meaning the triangle is on the plane
                double nullify=Double.MAX_VALUE;
                DoubleWritable Nullify= new DoubleWritable(nullify);
                context.write(Nullify,new Text(coordinates));
                overlap=1;// set overlap to 1
            }
            if(overlap==0) {  //Iteration occurs only when the triangle is not on the plane

                double height;
                for (height = Floor; height < Ceil || Math.abs(height-Ceil)<0.000001; height += 0.1) {  //use "or" instead of <= because of double syntax
                    double tenHeight=height*10;
                    height=Math.round(tenHeight)/10.0;
                    DoubleWritable Height = new DoubleWritable(height);
                    context.write(Height, new Text(coordinates));
                }
            }




        }

    }

    public static class IntSumReducer
            //edited: input of reducer: <IntWritable,Text>
            //output of reducer:<IntWritable,Text>
            extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
        //reduce:keep the input of reduce unchanged
        public void reduce(DoubleWritable key, Iterable<Text> values,
                          Context context
        ) throws IOException,InterruptedException {

            double z0 = key.get();

            StringBuffer intersection = new StringBuffer();
            for(Text t:values){
                //each value is the points of triangle

                System.out.println("the triangle:");
                System.out.println(t.toString());

                String temp_striangle = t.toString();//striangle is triangle in string
                String striangle = temp_striangle.substring(0, temp_striangle.length() - 1);//remove the ";"


                String[] co = striangle.split(",");// every co is the coordinate of the triangle
                //transfer string to coordinates
                double[] co_x = new double[3];
                double[] co_y = new double[3];
                double[] co_z = new double[3];
                for (int i = 0; i <= 2; i++) {//the i th coordinate
                    String[] sep = co[i].split(" ");//sep stores x,y,z of the coordinate
                    co_x[i] = Double.parseDouble(sep[0]);
                    co_y[i] = Double.parseDouble(sep[1]);
                    co_z[i] = Double.parseDouble(sep[2]);
                }
                Coordinate[] corners = new Coordinate[3];
                for (int j = 0; j <= 2; j++) {
                    corners[j] = new Coordinate(co_x[j], co_y[j], co_z[j]);//corners 0,1,2 stores the coordinates separately
                }


                Coordinate inter1 = Slicing.getIntersect(z0, corners[0], corners[1]);
                if (inter1 != null) {
                    String Str_inter1=inter1.vectPrint();
                    intersection.append(Str_inter1);
                    intersection.append(",");

                }


                Coordinate inter2 = Slicing.getIntersect(z0, corners[0], corners[2]);
                if (inter2 != null&&inter2!=inter1) {//To avoid repeats




                    String Str_inter2=inter2.vectPrint();
                    intersection.append(Str_inter2);

                    intersection.append(",");
                }


                Coordinate inter3 = Slicing.getIntersect(z0, corners[1], corners[2]);
                if (inter3 != null&&inter3!=inter1&&inter3!=inter2) {
                    String Str_inter3=inter3.vectPrint();
                    intersection.append(Str_inter3);
                    intersection.append(",");
                }
                intersection.append("    ");

            }
            String Str_intersection = intersection.toString();

            context.write(key, new Text(Str_intersection));
        }
    }










    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(Slicing.class);
        job.setMapperClass(Slicing.TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);


        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }





    public static Coordinate getIntersect(double z, Coordinate A, Coordinate B){
        double subA=z-A.z;
        double subB=z-B.z;
        if(Math.abs(subA)<0.0000001){
            return A;
        }
        else if(Math.abs(subB)<0.0000001){
            return B;
        }
        else if(subA*subB<0){
            double factor=subA/(subA-subB);
            Coordinate edge= A.sub(B,A);
            return A.add(A,edge.mul(factor));
        }
        return null;
    }


    public static class Coordinate{
        public double x, y, z;

        public Coordinate(double x, double y, double z) {
            this.x = x;
            this.y = y;
            this.z = z;
        }

        public double scalarProduct(Slicing.Coordinate p) {
            return this.x * p.x + this.y * p.y + this.z * p.z;
        }

        public Slicing.Coordinate mul(double factor) {
            return new Slicing.Coordinate(this.x * factor, this.y * factor, this.z * factor);
        }

        public Slicing.Coordinate add(Slicing.Coordinate a, Slicing.Coordinate b) {
            return new Slicing.Coordinate(a.x + b.x, a.y + b.y, a.z + b.z);
        }

        public Slicing.Coordinate sub(Slicing.Coordinate a, Slicing.Coordinate b) {
            return new Slicing.Coordinate(a.x - b.x, a.y - b.y, a.z - b.z);
        }

        public String vectPrint() {
            return  String.valueOf(this.x) + ' ' + String.valueOf(this.y) + ' ' + String.valueOf(this.z) + ' ';
        }
    }



}
