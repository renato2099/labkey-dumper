/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gov.nasa.jpl.celgene.kafka;

import java.io.IOException;
import java.net.URL;
import java.util.*;

import gov.nasa.jpl.celgene.labkey.LabkeyDumper;
import org.apache.commons.lang3.StringEscapeUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.labkey.remoteapi.CommandException;
import org.labkey.remoteapi.Connection;
import org.labkey.remoteapi.query.ContainerFilter;
import org.labkey.remoteapi.query.SelectRowsCommand;
import org.labkey.remoteapi.query.SelectRowsResponse;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * Gets studies from labKey and puts them into kafka
 */
public class LabkeyKafkaProducer {

    /**
     * Tag to be used for specifying data source
     */
    public final static String SOURCE_TAG = "DataSource";
    /**
     * Tag for specifying things coming out of OODT
     */
    public final static String SOURCE_VAL = "LABKEY";
    /**
     * Default waiting time between calls is 30 secs.
     */
    private static final long DEFAULT_WAIT = 30;
    /**
     * Topic name
     */
    public static String TOPIC = "celgene-updates";
    /**
     * Kafka url
     */
    public static String KAFKA_URL = "localhost:9092";
    /**
     * Kafka serializer class
     */
    public static String SERIALIZER = "kafka.serializer.StringEncoder";
    /**
     * Previously seen UUIDs
     */
    private static Set<UUID> PREV_SEEN = new HashSet<UUID>();
    /**
     * Kafka producer
     */
    private kafka.javaapi.producer.Producer<String, String> producer;
    /**
     * Labkey db connection
     */
    private Connection connection = null;
    /**
     * Labkey db url
     */
    private URL labkeyUrl;

    public static final String TOP_PROJECT = "/";
    private volatile boolean running = true;

    /**
     * Constructor
     */
    public LabkeyKafkaProducer(URL labkeyUrl, String username, String password) {
        this.connection = new Connection(labkeyUrl.toString(), username, password);
        this.labkeyUrl = labkeyUrl;
        this.initializeKafkaProducer();
    }

    /**
     * Constructor
     */
    public LabkeyKafkaProducer(URL labkeyUrl, String username, String password, Properties properties) {
        this.connection = new Connection(labkeyUrl.toString(), username, password);
        this.labkeyUrl = labkeyUrl;
        this.initializeKafkaProducer(properties);
    }

    /**
     * Executes LabkeyKafkaProducer as a single run
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        String url = null;
        String user = null;
        String pass = null;
        String projectName = TOP_PROJECT;
        long waitTime = DEFAULT_WAIT;

        // check args TODO improve?
        String usage = "java LabkeyKafkaProducer [--url <url>] [--user <user/email>] [--pass <pass>] " +
                "[--project <Project Name>] [--wait <secs>] [--kafka-topic <topic_name>] [--kafka-url]\n";

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--url")) {
                url = args[++i];
            } else if (args[i].equals("--user")) {
                user = args[++i];
            } else if (args[i].equals("--pass")) {
                pass = args[++i];
            } else if (args[i].equals("--project")) {
                projectName = args[++i];
            } else if (args[i].equals("--wait")) {
                waitTime = Long.valueOf(args[++i]);
            } else if (args[i].equals("--kafka-topic")) {
                TOPIC = args[++i];
            } else if (args[i].equals("--kafka-url")) {
                KAFKA_URL = args[++i];
            }
        }

        if (LabkeyDumper.isEmpty(url) || LabkeyDumper.isEmpty(user) || LabkeyDumper.isEmpty(pass) || LabkeyDumper.isEmpty(projectName)) {
            System.err.println(usage);
            System.exit(1);
        }

        // get KafkaProducer
        LabkeyKafkaProducer lp = new LabkeyKafkaProducer(new URL(url), user, pass);
        // get new studies
        while (lp.isRunning()) {
            // TODO we are querying for all of them, and publishing new ones, but updates are not handled
            // TODO Option1: From time to time query everything, and check if any of them have been updated
            // TODO Option2: Get a listener to the labkey system and pull only when changes were done (best option)
            List<Map<String, Object>> newStudies = lp.getNewStudies(projectName);
            // send new studies to kafka
            lp.sendLabkeyStudies(newStudies);
            // waiting
            System.out.println(String.format("Waiting for %d seconds...",waitTime));
            lp.politeWait(waitTime * 1000);
        }

        lp.closeProducer();
    }

    /**
     * Wait for l seconds
     * @param l
     */
    private void politeWait(long l) {
        try {
            Thread.sleep(l);
        } catch(InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Send labKey studies into kafka
     * @param newStudies
     */
    public void sendLabkeyStudies(List<Map<String, Object>> newStudies) {
        for(Map<String, Object> row : newStudies) {
            this.sendKafka(new KeyedMessage<String, String>(TOPIC, generateStudyJSON(row).toJSONString()));
        }
    }

    /**
     * Gets new studies from a specific project name.
     * @param projectName
     * @return
     * @throws IOException
     * @throws CommandException
     */
    private List<Map<String, Object>> getNewStudies(String projectName) throws IOException,
            CommandException {
        List<Map<String, Object>> projectStudies = getAllProjectStudies(projectName);
        List<Map<String, Object>> newStudies = new ArrayList<Map<String, Object>>();

        for (Map<String, Object> row : projectStudies) {
            Object rowUuid = row.get("container");
            // if not seen before then, add as previous
            if (!PREV_SEEN.contains(UUID.fromString(rowUuid.toString()))) {
                PREV_SEEN.add(UUID.fromString(rowUuid.toString()));
                newStudies.add(row);
            }
        }
        return newStudies;
    }

    /**
     * Gets studies from a specific project
     */
    public List<Map<String, Object>> getAllProjectStudies(String projectName) throws IOException,
            CommandException {
        // create a SelectRowsCommand to call the selectRows.api
        SelectRowsCommand cmd = new SelectRowsCommand("study", "Study");
        cmd.setContainerFilter(ContainerFilter.CurrentAndSubfolders);
        // execute the command against the connection
        // within the Api Test project folder
        SelectRowsResponse resp = cmd.execute(this.connection, "/");
        //System.out.println(resp.getRowCount() + " rows were returned.");
        return resp.getRows();
    }

    /**
     * Sends messages into Kafka
     */
    private void sendKafka(KeyedMessage<String, String> message) {
        this.producer.send(message);
    }

    /**
     * Closes kafka producer
     */
    public void closeProducer() {
        this.producer.close();
    }

    /**
     * Kafka producer initializer
     */
    private void initializeKafkaProducer(Properties properties) {
        ProducerConfig producerConfig = new ProducerConfig(properties);
        // TODO this is not going to give us the best performance, change serializer
        this.producer = new kafka.javaapi.producer.Producer<String, String>(producerConfig);
    }

    /**
     * Kafka producer default initializer
     */
    private void initializeKafkaProducer() {
        Properties properties = new Properties();
        properties.put("metadata.broker.list", KAFKA_URL);
        properties.put("serializer.class", SERIALIZER);
        ProducerConfig producerConfig = new ProducerConfig(properties);
        // TODO this is not going to give us the best performance, change serializer
        this.producer = new kafka.javaapi.producer.Producer<String, String>(producerConfig);
    }

    /**
     * Generate JSON object
     *
     * @param studyData
     * @return
     */
    public JSONArray generateStudiesJSON(List<Map<String, Object>> studyData) {
        JSONArray jsonObj = new JSONArray();
        // loop over the returned rows
        for (int i = 0; i < studyData.size(); i++) {
            Map<String, Object> study = studyData.get(i);
            jsonObj.add(generateStudyJSON(study));
        }
        return jsonObj;
    }

    /**
     * Generates a JSON object from a study
     * @param study
     * @return
     */
    public JSONObject generateStudyJSON(Map<String, Object> study) {
        JSONObject jsonObj = new JSONObject();
        jsonObj.put(SOURCE_TAG, SOURCE_VAL);
        String[] keySet = study.keySet().toArray(new String[]{});
        for (int j = 0; j < keySet.length; j++) {
            String key = keySet[j];
            if (j + 1 >= keySet.length) {
                jsonObj.put(key, StringEscapeUtils.escapeJson(String.valueOf(study.get(key))));
            } else {
                jsonObj.put(key, StringEscapeUtils.escapeJson(String.valueOf(study.get(key))));
            }
        }
        return jsonObj;
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }
}

