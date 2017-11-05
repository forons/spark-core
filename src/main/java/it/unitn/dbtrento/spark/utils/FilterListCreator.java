package it.unitn.dbtrento.spark.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import scala.Tuple2;

public class FilterListCreator {

  public static List<Tuple2<String, String>> createFilterList(String filePath, FileSystemType fs)
      throws IOException {
    List<Tuple2<String, String>> filterList;
    switch (fs) {
      case FS:
        filterList = createFilterListFromFS(filePath);
        break;
      case HDFS:
        filterList = createFilterListFromHDFS(filePath);
        break;
      default:
        throw new IOException(fs.toString());
    }
    return filterList;
  }

  private static List<Tuple2<String, String>> createFilterListFromFS(String filePath) {
    String line;
    String cvsSplitBy = ",";
    List<Tuple2<String, String>> filterList = new ArrayList<>();
    try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
      while ((line = br.readLine()) != null) {
        String[] row = line.split(cvsSplitBy);
        if (row.length == 2) {
          filterList.add(new Tuple2<>(row[0], row[1]));
        }
      }
    } catch (IOException e) {
      return null;
    }
    return filterList;
  }

  private static List<Tuple2<String, String>> createFilterListFromHDFS(String hdfsPath) {
    List<Tuple2<String, String>> filterList;
    FileSystem fs;
    BufferedReader br;
    try {
      fs = FileSystem.get(new URI(hdfsPath), new Configuration());
      br = new BufferedReader(new InputStreamReader(fs.open(new Path(hdfsPath))));
      String line;
      String cvsSplitBy = ",";
      filterList = new ArrayList<>();
      while ((line = br.readLine()) != null) {
        String[] row = line.split(cvsSplitBy);
        if (row.length == 2) {
          filterList.add(new Tuple2<>(row[0], row[1]));
        }
      }
    } catch (IOException | URISyntaxException e) {
      return null;
    }
    return filterList;
  }

  public static List<String> createColumnList(String filePath, FileSystemType fs)
      throws IOException {
    List<String> colsToKeep;
    switch (fs) {
      case FS:
        colsToKeep = createColumnListFromFS(filePath);
        break;
      case HDFS:
        colsToKeep = createColumnListFromHDFS(filePath);
        break;
      default:
        throw new IOException(fs.toString());
    }
    return colsToKeep;
  }

  private static List<String> createColumnListFromFS(String filePath) {
    String line;
    List<String> colList = new ArrayList<>();
    try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
      while ((line = br.readLine()) != null) {
        if (!line.trim().startsWith("#") && !line.trim().isEmpty()) {
          colList.add(line.trim());
        }
      }
    } catch (IOException e) {
      return null;
    }
    return colList;
  }


  private static List<String> createColumnListFromHDFS(String hdfsPath) {
    FileSystem fs;
    BufferedReader br;
    List<String> colList;
    try {
      fs = FileSystem.get(new URI(hdfsPath), new Configuration());
      br = new BufferedReader(new InputStreamReader(fs.open(new Path(hdfsPath))));
      String line;
      colList = new ArrayList<>();
      while ((line = br.readLine()) != null) {
        if (!line.trim().startsWith("#") && !line.trim().isEmpty()) {
          colList.add(line.trim());
        }
      }
    } catch (IOException | URISyntaxException e) {
      return null;
    }
    return colList;
  }
}
