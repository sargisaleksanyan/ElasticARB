

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
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
    private final String couryType="Arbitration Court";
    private final String couryTypeAbbr="ac";
    int successCount=0;
    int failedCount=0;
    private RestClient restClient = getRestClient();
    private String country = "RU";
    private String countryLower = "ru";
    private String jurisdiction = "jurisdiction";
    private RestClient getRestClient() {
        RestClientBuilder restClientBuilder = RestClient.builder(
                new HttpHost("localhost", 9200, "http"),
                new HttpHost("localhost", 9201, "http"));
        restClientBuilder.setMaxRetryTimeoutMillis(20000);

        RestClient restClient_=restClientBuilder.build();
        return restClient_;
    }

    public static void main(String[] args) {
        Bulk es=new Bulk();
       // es.readSubDir(new File("/hdd/Russia/Filtered/решения_cудов_общей_юрисдикции/sou"));
        es.readSubDir(new File("/data/doc/решения_арбитражных_судов/arb_sud"));
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


    private void closeClient(){
        try {
            restClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private String getJsonIndex(File file){

        String fileId=couryTypeAbbr+"_"+ file.getName().substring(0,file.getName().length()-4);
        JSONObject jsonObject=new JSONObject();
        JSONObject idObject=new JSONObject();
        idObject.put("_id",fileId);
        jsonObject.put("index",idObject);
        return jsonObject.toString();
    }
    private String getJsonContent(File file) {
        String content = getContent(file);
        String documentPath=file.getAbsolutePath();
        JSONObject jsonObject=new JSONObject();
        jsonObject.put("documentContent",content);
        jsonObject.put("courtType",couryType);
        jsonObject.put("country",country);
        jsonObject.put("documentpath",documentPath);
        return jsonObject.toString();
    }
    //Written.txt
    private void readSubDir(File readerFile) {
        Queue<File> queue = new LinkedList<File>();
        List<File> fileList=new ArrayList<File>();
        queue.add(readerFile);
        while (!queue.isEmpty()) {
            File file = queue.remove();
            for (int j =0;j< file.listFiles().length ; j++) {
                File subFile = file.listFiles()[j];
                if (subFile.isDirectory()) {
                    queue.add(subFile);
                } else {
                    if (subFile.getName().endsWith("txt")) {
                        fileList.add(subFile);
                        if(fileList.size()==1500){
                            List<File> files_=fileList;
                            indexList(files_);
                            fileList=null;
                            fileList=new ArrayList<File>();
                        }
                    }
                }
            }
        }
        if(fileList.size()>0){
            indexList(fileList);
        }
    }
    public void indexList(List<File> fileList){
        StringBuilder bulkJsonBuilder=new StringBuilder();
        for(int i=0;i<fileList.size();i++){
            String idjson=getJsonIndex(fileList.get(i));
            String jsonContent=getJsonContent(fileList.get(i));
            if(idjson!=null) {
                bulkJsonBuilder.append(idjson);
                bulkJsonBuilder.append("\n");
                bulkJsonBuilder.append(jsonContent);
                bulkJsonBuilder.append("\n");
            }
        }
        Map<String, String> params = Collections.emptyMap();
        HttpEntity entity = new NStringEntity(bulkJsonBuilder.toString(), ContentType.APPLICATION_JSON);
        Response response = null;
        restClient.performRequestAsync("PUT", countryLower+"/" + jurisdiction + "/" + "_bulk",params,entity,responseListener);
        fileList=null;
        bulkJsonBuilder=null;
    }
    ResponseListener responseListener = new ResponseListener() {

        @Override
        public void onSuccess(Response response) {
            successCount++;

            System.out.println("Success "+successCount);
        }

        @Override
        public void onFailure(Exception exception) {
            failedCount++;
            System.out.println("-----------Failed "+failedCount+"----------- ");
        }
    };


}
