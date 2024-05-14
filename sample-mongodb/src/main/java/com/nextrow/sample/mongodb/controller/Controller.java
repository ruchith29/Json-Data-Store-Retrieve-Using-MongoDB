package com.nextrow.sample.mongodb.controller;


import com.mongodb.client.*;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.util.FileCopyUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

@RestController
public class Controller {

    @Autowired
    private ResourceLoader resourceLoader;

    @GetMapping("/getData")
    public void getData()
    {
        try
        {
            // for reading json file
            Resource resource=resourceLoader.getResource("classpath:sample.json");
            InputStreamReader inputStreamReader=new InputStreamReader(resource.getInputStream());
            String text= FileCopyUtils.copyToString(inputStreamReader);

            // converting data to json object
            JSONObject jsonObject=new JSONObject(text);


            // making connection with mongodb
            MongoClient mongoClient = MongoClients.create();
            MongoDatabase mongoDatabase;

            // creating database by parsing the data
            for (String databaseName:jsonObject.keySet()){

                mongoDatabase=mongoClient.getDatabase(databaseName);
                System.out.println(databaseName+" : is created.");

                // storing the values of that database into json objects
                JSONObject object=jsonObject.getJSONObject(databaseName);

                // create table for database
                for (String tableName:object.keySet()) {
                    mongoDatabase.createCollection(tableName);
                    System.out.println("Table is created: " + tableName);
                }

                // adding values to table
                for (String tableName:object.keySet()) {

                    // creating a collection for inserting the values through it
                    MongoCollection<Document> table = mongoDatabase.getCollection(tableName);

                    // json-array to store the values and iterate over them
                    JSONArray jsonArray=object.getJSONArray(tableName);

                    for (int i=0;i<jsonArray.length();i++)
                    {
                        // storing each array from array to an object
                        JSONObject arrayValues=jsonArray.getJSONObject(i);

                        // document to store the data in key value pairs such that data can be directly added to database
                        Document document=new Document(arrayValues.toMap());

                        // inserting single row data
                        table.insertOne(document);
                    }
                }

            }

            System.out.println("Insertion done!.");

        }
        catch(Exception e)
        {
            System.out.println(e);
        }
    }

    @GetMapping("/returnData")
    public HashMap<String, Object> returnData(){

        // database connection for mongodb
        MongoClient mongoClient=MongoClients.create();
        MongoDatabase mongoDatabase=mongoClient.getDatabase("components");

        // map to store the database name and its respective values
        HashMap<String, Object> totalData=new HashMap<>();

        // map to store table name and the map values
        HashMap<String,Object> tableData=new HashMap<>();

        try
        {
            // iterating over database to get table and its data
            for (String tName: mongoDatabase.listCollectionNames()){

                // to store the column values
                ArrayList<Document> tableValues=new ArrayList<>();

                // to get the tables data
                MongoCollection<Document> keyValues=mongoDatabase.getCollection(tName);

                // iterating over the table data and storing data in form of document such that json format is maintained
                for (Document document : keyValues.find()) {
                    // removed the "_id" key for every value
                    document.remove("_id");
                    // adding document to arraylist of document type
                    tableValues.add(document);
                }
                // storing the table name and its data
                tableData.put(tName,tableValues);

                System.out.println(tName);
            }
            // storing the database name and all the tables data
            totalData.put("components",tableData);

            return totalData;

        }

        catch (Exception e)
        {
            System.out.println(e);
        }

        return null;

    }
}