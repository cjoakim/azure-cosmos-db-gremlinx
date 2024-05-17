package com.microsoft.cosmosdb.gremlinx;

import com.microsoft.cosmosdb.gremlinx.io.FileUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

/**
 * This class is the central point in the application for all configuration values,
 * such as environment variables, command-line arguments, and computed filesystem
 * locations.
 * Chris Joakim, Microsoft
 */

public class AppConfig implements AppConstants {

    // Class variables
    private static Logger logger = LogManager.getLogger(AppConfig.class);
    private static String[] commandLineArgs = new String[0];
    private static HashMap<String, String> dotEnv = null;


    public static void setCommandLineArgs(String[] args) {

        if (args != null) {
            commandLineArgs = args;
        }
    }

    public static boolean booleanArg(String flagArg) {

        for (int i = 0; i < commandLineArgs.length; i++) {
            if (commandLineArgs[i].equalsIgnoreCase(flagArg)) {
                return true;
            }
        }
        return false;
    }

    public static String flagArg(String flagArg) {

        for (int i = 0; i < commandLineArgs.length; i++) {
            if (commandLineArgs[i].equalsIgnoreCase(flagArg)) {
                return commandLineArgs[i + 1];
            }
        }
        return null;
    }

    public static long longFlagArg(String flagArg, long defaultValue) {

        try {
            return Long.parseLong(flagArg(flagArg));
        }
        catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public static boolean isVerbose() {

        return booleanArg("--verbose");
    }

    public static String getEnvVar(String name) {

        if (name != null) {
            // The git-ignored app/.env file takes precedence over system environment variables
            readDotEnv();
            if (dotEnv.containsKey(name)) {
                return (String) dotEnv.get(name);
            }
            return System.getenv(name);
        }
        return null;
    }

    public static int getIntEnvVar(String name, int defaultValue) {

        try {
            return Integer.parseInt(getEnvVar(name));
        }
        catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static void readDotEnv() {

        if (dotEnv != null) {
            return;
        }

        dotEnv = new HashMap<String, String>();
        try {
            FileUtil fu = new FileUtil();
            List<String> lines = fu.readLines(".env");
            System.out.println(".env line count: " + lines.size());
            if (lines != null) {
                for (int i = 0; i < lines.size(); i++) {
                    String line = lines.get(i);
                    int idx = line.indexOf('=');
                    if (idx > 0) {
                        try {
                            String name = line.substring(0, idx).trim();
                            String value = line.substring(idx + 1).trim();
                            dotEnv.put(name, value);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
