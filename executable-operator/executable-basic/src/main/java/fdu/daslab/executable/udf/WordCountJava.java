package fdu.daslab.executable.udf;

import scala.Int;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class WordCountJava {
    public static void main(String[] args) throws IOException {
        StringBuffer sb = readFile("/Users/zhuxingpo/Downloads/test.txt");
        Map<String,Integer> map = getWord(sb);
        List<Map.Entry<String,Integer>> list = sortValue(map);
        for(Map.Entry<String,Integer> entry : list) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }

    }

    private static StringBuffer readFile(String path) throws IOException {
        FileReader fis = new FileReader(path);
        BufferedReader br = new BufferedReader(fis);
        StringBuffer sb = new StringBuffer();
        String it = br.readLine();
        while (it != null) {
            sb.append(it);
            sb.append(" ");
            it = br.readLine();
        }
        return sb;
    }

    private static Map<String,Integer> getWord(StringBuffer sb) {
        Map<String, Integer> map = new TreeMap<>();
        StringBuffer word = new StringBuffer();
        for (int i = 0; i < sb.length(); i++) {
            char c = sb.charAt(i);
            if (c != ' ') {
                word.append(c);
            } else {
                String str = word.toString();
                if( map.containsKey(str)) {
                    Integer value = map.get(str);
                    map.put(str,++value);
                } else {
                    Integer value = 1;
                    map.put(str,value);
                }
                word = new StringBuffer();
            }
        }
        return map;
    }

    private static List<Map.Entry<String,Integer>> sortValue(Map<String,Integer> map) {
        List<Map.Entry<String,Integer>> list = new ArrayList<>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                return o1.getValue().compareTo(o2.getValue());
            }
        });
        return list;
    }
}

