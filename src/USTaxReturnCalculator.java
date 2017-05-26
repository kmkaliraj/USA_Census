/**
 * Created by kalirajkalimuthu on 11/24/16.
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class USTaxReturnCalculator {

    public static class TaxDataWritable implements WritableComparable<TaxDataWritable> {

        private Text state, zipcode;
        private LongWritable  income_level;

        public TaxDataWritable() {
            state = new Text();
            zipcode = new Text();
            income_level = new LongWritable();
        }
        public Text getState() {

            return state;
        }

        public void setState(Text state) {

            this.state = state;
        }

        public Text getZipcode() {

            return zipcode;
        }

        public void setZipcode(Text zipcode) {

            this.zipcode = zipcode;
        }

        public LongWritable getIncome_level() {

            return income_level;
        }

        public void setIncome_level(LongWritable income_level) {

            this.income_level = income_level;
        }

        public void readFields(DataInput in) throws IOException {
            state.readFields(in);
          //  zipcode.readFields(in);
            income_level.readFields(in);

        }


        public void write(DataOutput out) throws IOException {
            state.write(out);
          //  zipcode.write(out);
            income_level.write(out);
        }

        public int compareTo(TaxDataWritable o) {
            int stateCmp = state.compareTo(o.state);

            if (stateCmp != 0) {
                return stateCmp;
            } else {
                    return income_level.compareTo(o.income_level);
            }

        }

        public boolean equals(TaxDataWritable o) {

                return state.equals(o.state)
                        && income_level.equals(o.income_level);
        }

        public int hashCode()
        {
            return state.hashCode();
        }

        public String toString(){
            return this.state+"  "+income_level;
        }

    }


    public static class TaxReturnMapper extends Mapper<LongWritable, Text, TaxDataWritable, FloatWritable> {

        private FloatWritable tax_return = new FloatWritable();
        private TaxDataWritable taxkey = new TaxDataWritable();
        private Text state = new Text();
        private Text zipcode = new Text();
        private LongWritable income_level = new LongWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (key.get() == 0 && value.toString().contains("STATE"))
                return;
            else {
                String[] words = line.split(",");
                state.set(words[1]);
                taxkey.setState(state);
                zipcode.set(words[2]);
                taxkey.setZipcode(zipcode);
                income_level.set(Long.parseLong(words[3]));
                taxkey.setIncome_level(income_level);
                tax_return.set(Float.parseFloat(words[4]));

                context.write(taxkey, tax_return);

            }
        }
    }

    public static class TaxReturnReducer extends Reducer<TaxDataWritable,FloatWritable, TaxDataWritable, FloatWritable> {

        public void reduce(TaxDataWritable key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            float sum = 0.0F;
            for (FloatWritable val : values) {
                sum += val.get();
            }
            context.write(key,new FloatWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "US Tax Return");

        job.setOutputKeyClass(TaxDataWritable.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setMapperClass(TaxReturnMapper.class);
        job.setReducerClass(TaxReturnReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);



        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);


    }


}