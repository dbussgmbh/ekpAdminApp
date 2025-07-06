
Environment Variablen werden unter Java 8 nicht benötigt:
--module-path "C:\Java\javafx-sdk-24.0.1\lib" --add-modules javafx.controls,javafx.fxml

IDBC-Treiber raus, da mit Weblogic-Treiber nicht funktioniert
        <dependency>
            <groupId>com.oracle.database.jdbc</groupId>
            <artifactId>ojdbc8</artifactId>
            <version>19.3.0.0</version>
        </dependency>


Weblogic-Full-Client in repo ausfnehmen:
 mvn install:install-file  -Dfile=C:\Entwicklung\Weblogic\wlfullclient.jar -DgroupId=weblogic -DartifactId=wlclient -Dversion=12 -Dpackaging=jar