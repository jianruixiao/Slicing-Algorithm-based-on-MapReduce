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
            extends Mapper<LongWritable, Text, FloatWritable, Text> {//edited: integer first, then text/string



        public void map(LongWritable key, Text value, Context context)    //the input of mapper
        throws IOException,InterruptedException {
            java.lang.String temp=value.toString();         //temp is the String format of the line
            String[] co= temp.split(" ");  //split the string into words
            //get the vertex (String) of the triangle
            String x1=co[8], y1=co[9], z1=co[10],x2=co[12],y2=co[13],z2=co[14],x3=co[16],y3=co[17],z3=co[18];
            //converse to the float format
            float Z1=Float.parseFloat(z1), Z2=Float.parseFloat(z2),Z3=Float.parseFloat(z3);
            float zMax=Math.max(Math.max(Z1,Z2),Z3), zMin=Math.min(Math.min(Z1,Z2),Z3);
            float avg=(zMax+zMin)/2;   //Get the average z value
            FloatWritable AVG=new FloatWritable(avg);
            // Next: Add the triangle info into value
            //divide the coordinates using commas
            String coordinates=x1+' '+y1+' '+z1+','+x2+' '+y2+' '+z2+','+x3+' '+y3+' '+z3+';';
            System.out.println(coordinates);
           context.write(AVG, new Text(coordinates));
        }

    }

    public static class IntSumReducer
            //edited: input of reducer: <IntWritable,Text>
            //output of reducer:<IntWritable,Text>
            extends Reducer<FloatWritable, Text, FloatWritable, Text> {
        //reduce:keep the input of reduce unchanged
        public void reduce(FloatWritable key, Iterable<Text> values,
                          Context context
        ) throws IOException,InterruptedException {
            float z0 = key.get();

            StringBuffer intersection = new StringBuffer();
            for(Text t:values){
               //each value is the points of triangle

                System.out.println("the triangle:");
                System.out.println(t.toString());

                String temp_striangle = t.toString();//striangle is triangle in string
                String striangle = temp_striangle.substring(0, temp_striangle.length() - 1);//remove the ";"


                String[] co = striangle.split(",");// every co is the coordinate of the triangle
                //transfer string to coordinates
                float[] co_x = new float[3];
                float[] co_y = new float[3];
                float[] co_z = new float[3];
                for (int i = 0; i <= 2; i++) {//the i th coordinate
                    String[] sep = co[i].split(" ");//sep stores x,y,z of the coordinate
                    co_x[i] = Float.parseFloat(sep[0]);
                    co_y[i] = Float.parseFloat(sep[1]);
                    co_z[i] = Float.parseFloat(sep[2]);
                }
                Coordinate[] corners = new Coordinate[3];
                for (int j = 0; j <= 2; j++) {
                    corners[j] = new Coordinate(co_x[j], co_y[j], co_z[j]);//corners 0,1,2 stores the coordinates separately
                }


                Coordinate inter1 = Slicing.getIntersect(z0, corners[0], corners[1]);
                if (inter1 != null) {
                    intersection.append(inter1.vectPrint());
                    intersection.append(",");

                }


                Coordinate inter2 = Slicing.getIntersect(z0, corners[0], corners[2]);
                if (inter2 != null) {


                    intersection.append(inter2.vectPrint());
                    intersection.append(",");
                }


                Coordinate inter3 = Slicing.getIntersect(z0, corners[1], corners[2]);
                if (inter3 != null) {


                    intersection.append(inter3.vectPrint());
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
        job.setOutputKeyClass(FloatWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(FloatWritable.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }





    public static Coordinate getIntersect(float z, Coordinate A, Coordinate B){
        float subA=z-A.z;
        float subB=z-B.z;
        if(Math.abs(subA)<0.0000001){
            return A;
        }
        else if(Math.abs(subB)<0.0000001){
            return B;
        }
        else if(subA*subB<0){
            float factor=subA/(subA-subB);
            Coordinate edge= A.sub(B,A);
            return A.add(A,edge.mul(factor));
        }
        return null;
    }


    public static class Coordinate{
        public float x, y, z;

        public Coordinate(float x, float y, float z) {
            this.x = x;
            this.y = y;
            this.z = z;
        }

        public float scalarProduct(Slicing.Coordinate p) {
            return this.x * p.x + this.y * p.y + this.z * p.z;
        }

        public Slicing.Coordinate mul(float factor) {
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
