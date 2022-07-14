package com.database.copier.cli;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.database.copier.core.DataCopier;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DbCopierCli {

    public static void main(String[] args) {
        Options options = new Options();
        Option hOption = Option.builder("h").longOpt("help").hasArg(false).desc("print this message").build();
        Option dOption = Option.builder("d").longOpt("database").hasArg().desc("database url").required().build();
        Option uOption = Option.builder("u").longOpt("user").hasArg().desc("database user").required().build();
        Option pOption = Option.builder("p").longOpt("password").hasArg().desc("database password").required().build();
        Option sOption = Option.builder("s").longOpt("schema").hasArg().desc("database schema").required().build();
        Option tOption = Option.builder("t").longOpt("table").hasArg().desc("database table").required().build();
        Option cOption = Option.builder("c").longOpt("column").hasArg().desc("database column").required().build();
        Option vOption = Option.builder("v").longOpt("value").hasArg().desc("the column value").required().build();
        Option fkOption = Option.builder("fk").argName("foreign keys").hasArgs().valueSeparator(',')
                .desc("comma separated list of foreign keys from child tables").build();
        Option fOption = Option.builder("f").longOpt("file").hasArg().desc("file to save ddl commands").build();

        options.addOption(hOption);
        options.addOption(dOption);
        options.addOption(uOption);
        options.addOption(pOption);
        options.addOption(sOption);
        options.addOption(tOption);
        options.addOption(cOption);
        options.addOption(vOption);
        options.addOption(fkOption);
        options.addOption(fOption);

        DefaultParser parser = new DefaultParser();
        try {
            CommandLine line = parser.parse(options, args);
            if (line.hasOption(hOption)) {
                printUsage(options);
                return;
            }

            Connection connection = getConnection(line.getOptionValue(dOption), line.getOptionValue(uOption), line.getOptionValue(pOption));
            copyRecord(connection, line.getOptionValue(sOption), line.getOptionValue(tOption), line.getOptionValue(cOption),
                    line.getOptionValue(vOption), line.getOptionValues(fkOption), line.getOptionValue(fOption));

        } catch (ParseException e) {
            log.error(e.getMessage());
            printUsage(options);
        } catch (SQLException | FileNotFoundException e) {
            log.error("Copy data error: {}", e.getMessage(), e);
        }
    }

    private static void printUsage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("db-copier-cli", options);
    }

    private static Connection getConnection(String url, String user, String password) throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

    private static void copyRecord(Connection connection, String schema, String table, String column, String value, String[] fKeys, String fileName)
            throws FileNotFoundException, SQLException {
        Set<String> includeForeignKeys = fKeys == null ? Collections.emptySet() : Set.of(fKeys);

        StringBuilder output = new StringBuilder();

        DataCopier dataCopier = new DataCopier(connection);
        dataCopier.copyRow(schema, table, column, value, includeForeignKeys, output);

        String file = fileName == null ? "insert_" + table + "_" + value + ".sql" : fileName;
        try (PrintWriter printWriter = new PrintWriter(file)) {
            printWriter.print(output.toString());
            log.info("Saved in file: {}", file);
        }
    }
}
