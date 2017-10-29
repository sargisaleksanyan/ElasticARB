

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.RequestLine;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.json.JSONObject;

import java.io.*;
import java.util.*;

/**
 * Created by va on 21.10.17.
 */
public class Bulk {
    private final String couryType = "Arbitration Court";
    private final String couryTypeAbbr = "ac";
    int successCount = 0;
    int skipCount = 0;
    int failedCount = 0;
    private boolean isPrevious = false;
    private List<File> previousList =null;
    private RestClient restClient = getRestClient();
    private String country = "RU";
    private String countryLower = "ru";
    private String jurisdiction = "jurisdiction";

    private RestClient getRestClient() {
        RestClientBuilder restClientBuilder = RestClient.builder(
                new HttpHost("localhost", 9200, "http"),
                new HttpHost("localhost", 9201, "http"));
        restClientBuilder.setMaxRetryTimeoutMillis(20000);
        RestClient restClient_ = restClientBuilder.build();
        return restClient_;
    }

    public static void main(String[] args) {
        Bulk es = new Bulk();
      es.readSubDir(new File("/data/doc/решения_арбитражных_судов/arb_sud"));
      //   es.readSubDir(new File("/hdd/Russia/Filtered/решения_арбитражных_судов/arb_sud"));
        es.closeClient();
    }

    private String getContent(File file) {
        StringBuilder stringBuilder = new StringBuilder();
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line;
            while ((line = br.readLine()) != null) {
                stringBuilder.append(line);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return stringBuilder.toString();
    }


    private void closeClient() {
        try {
            restClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String getJsonIndex(File file) {
        //   String fileId = couryTypeAbbr + "_" + file.getName().substring(0, file.getName().length() - 4);
        String fileId = getFileId(file);
        JSONObject jsonObject = new JSONObject();
        JSONObject idObject = new JSONObject();
        idObject.put("_id", fileId);
        jsonObject.put("index", idObject);
        return jsonObject.toString();
    }

    private String getFileId(File file) {
        return couryTypeAbbr + "_" + file.getName().substring(0, file.getName().length() - 4);
    }

    private String getJsonContent(File file) {
        String content = getContent(file);
        String documentPath = file.getAbsolutePath();
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("documentContent", content);
        jsonObject.put("courtType", couryType);
        jsonObject.put("country", country);
        jsonObject.put("documentpath", documentPath);
        return jsonObject.toString();
    }

    //Written.txt
    private void readSubDir(File readerFile) {
        Queue<File> queue = new LinkedList<File>();
        List<File> fileList = new ArrayList<File>();
        queue.add(readerFile);
        while (!queue.isEmpty()) {
            File file = queue.remove();
            int fileSize = file.listFiles().length;
            for (int j = 0;j<fileSize; j++) {
                File subFile = file.listFiles()[j];
                if (subFile.isDirectory()) {
                    queue.add(subFile);
                } else {
                    if (subFile.getName().endsWith("txt")) {
                   //     if(count<432000){
                     //       count++;
                   //     }
                   //     else{
                            fileList.add(subFile);
                            if (fileList.size() == 1200) {
                                indexList(fileList);
                                fileList = null;
                                fileList = new ArrayList<File>();
                       //     }
                        }
                    }
                }
            }
        }
        if (fileList.size() > 0) {
            indexList(fileList);
        }
    }

    public void indexList(List<File> fileList) {
        StringBuilder bulkJsonBuilder = new StringBuilder();
        //String fileId=getFileId(fileList.get(0));
        if(!isPrevious) {
            boolean exist = isEixist(fileList.get(0));
            if (exist) {
              previousList=fileList;
              skipCount++;
              System.out.println("Skipping next 1200 from: " + fileList.get(0).getAbsolutePath());
              System.out.println("Skipping : " +skipCount);
              return;
            }

        }
        else{
            isPrevious=false;
        }
        // String fileId=fileList.get(1);
        for (int i = 0; i < fileList.size(); i++) {
            String idjson = getJsonIndex(fileList.get(i));
            String jsonContent = getJsonContent(fileList.get(i));
            if (idjson != null) {
                bulkJsonBuilder.append(idjson);
                bulkJsonBuilder.append("\n");
                bulkJsonBuilder.append(jsonContent);
                bulkJsonBuilder.append("\n");
            }
        }
        Map<String, String> params = Collections.emptyMap();
        HttpEntity entity = new NStringEntity(bulkJsonBuilder.toString(), ContentType.APPLICATION_JSON);
        restClient.performRequestAsync("PUT", countryLower + "/" + jurisdiction + "/" + "_bulk", params, entity, responseListener);
        if(previousList!=null){
            isPrevious=true;
            System.out.println(">>>>>>>>>> Returning to previous 1200 list from: " + previousList.get(0).getAbsolutePath());
            System.out.println(" Returning  " + successCount);
            previousList=null;
            List<File> newList=previousList;
            previousList=null;
            indexList(newList);
        }
    }

    private boolean isEixist(File file) {
        String fileId = getFileId(file);
        System.out.println("Id "+fileId);
        String query = queryBuilder(fileId);
        Map<String, String> params = Collections.emptyMap();

        HttpEntity entity = new NStringEntity(query, ContentType.APPLICATION_JSON);
        Response response = null;
        try {
            response = restClient.performRequest(
                    "GET",
                    "/ru/_search",
                    params,
                    entity);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == 200) {
            Header[] headers = response.getHeaders();
            try {
                String responseBody = EntityUtils.toString(response.getEntity());
                JSONObject responseJson = new JSONObject(responseBody);
                JSONObject hitsObject = responseJson.getJSONObject("hits");
                //String hitsObject=responseJson.getString("total");
                int total = hitsObject.getInt("total");
                if (total > 0) {
                    return true;
                }
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }
        return false;
    }

    private static String queryBuilder(String id) {
        JSONObject mainObject = new JSONObject();
        JSONObject queryObject = new JSONObject();
        JSONObject constantObject = new JSONObject();
        JSONObject filterObject = new JSONObject();
        JSONObject termObject = new JSONObject();
        termObject.put("_id", id);
        filterObject.put("term", termObject);
        constantObject.put("filter", filterObject);
        queryObject.put("constant_score", constantObject);
        mainObject.put("query", queryObject);
        // GET _search
        return mainObject.toString();
    }

    ResponseListener responseListener = new ResponseListener() {

        @Override
        public void onSuccess(Response response) {
            successCount++;

            System.out.println("_"+"Success " + successCount+"_");
        }

        @Override
        public void onFailure(Exception exception) {
            failedCount++;
            System.out.println("_"+"Failed " + failedCount + "-");
        }
    };


}
