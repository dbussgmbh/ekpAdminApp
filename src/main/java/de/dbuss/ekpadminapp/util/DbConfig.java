package de.dbuss.ekpadminapp.util;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class DbConfig {
    private static final Properties properties = new Properties();

    static {
        try (InputStream input = new FileInputStream("config/db.properties");) {
            if (input == null) {
                throw new RuntimeException("db.properties nicht gefunden");
            }
            properties.load(input);
        } catch (Exception e) {
            throw new RuntimeException("Fehler beim Laden der Datenbankkonfiguration", e);
        }
    }

    public static String getUrl() {
        return properties.getProperty("db.url");
    }

    public static String getUser() {
        return properties.getProperty("db.user");
    }

    public static String getPassword() {
        return properties.getProperty("db.password");
    }
}
