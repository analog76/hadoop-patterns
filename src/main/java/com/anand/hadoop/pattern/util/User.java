package com.anand.hadoop.pattern.util;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by analog76 on 6/28/14.
 */
public class User implements Writable{

    int id;
    int age;
    int sex;
    String jobDesc;
    String zip;


    public User(){

    }

    public User(String line){
        parse(line);
    }


    public void parse(String line){
        String[] str=line.split("\\|");
        this.id=Integer.parseInt(str[0]);
        this.age=Integer.parseInt(str[1]);
        String s= str[2];
        if(s.equalsIgnoreCase("M")){
            this.sex=1;
        }else
            this.sex=0;

        this.jobDesc= str[3];
        this.zip=str[4];

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeInt(age);
        dataOutput.writeInt(sex);
        dataOutput.writeUTF(jobDesc);
        dataOutput.writeUTF(zip);


    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id =  dataInput.readInt();
        age = dataInput.readInt();
        sex = dataInput.readInt();
        jobDesc=dataInput.readUTF();
        zip= dataInput.readUTF();
    }

    public int getId() {
        return id;
    }

    public int getAge() {
        return age;
    }

    public int getSex() {
        return sex;
    }

    public String getJobDesc() {
        return jobDesc;
    }

    public String getZip() {
        return zip;
    }


    public static void main(String[] args){

        User us = new User();
        String line = "4|24|M|technician|43537";
        us.parse(line);
        us.toString();
    }
}
