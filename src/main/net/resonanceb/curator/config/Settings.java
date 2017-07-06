package net.resonanceb.curator.config;

public class Settings {

    public static int closeCutoff = 2;

    public static int deleteCutoff = 3;

    public static int backupCutoff = 1;

    public static int forceMergeCutoff = 1;

    public static int maxSegments = 2;

    public static String[] prefixes = new String[]{"logstash-"};

    public static String cluster = "elasticsearch";

    public static String hostsLists = "localhost:9200,localhost:9300";

}
