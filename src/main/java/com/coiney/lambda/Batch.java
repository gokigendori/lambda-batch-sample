package com.coiney.lambda;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceStateName;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClient;
import com.amazonaws.services.simplesystemsmanagement.model.SendCommandRequest;
import com.amazonaws.services.simplesystemsmanagement.model.SendCommandResult;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Created by adomiki on 2017/03/08.
 */
public class Batch {
    public void handler(Context context) {

        LambdaLogger logger = context.getLogger();

        String webhookUrl = System.getenv("WEBHOOK_URL");

        // 環境変数にbatch名
        String batchName = System.getenv("BATCH_NAME");
        if (Objects.isNull(batchName) || batchName.isEmpty()) {
            logger.log("batch name is nothing\n");
            return;
        }

        Instance instance = getRandomInstance();
        if (Objects.isNull(instance)) {
            // 起動中のインスタンスがない
            logger.log("active instance is nothing\n");

            // Slackに通知
            if (Objects.nonNull(webhookUrl) && !webhookUrl.isEmpty()) {
                sendSlack(webhookUrl, logger, batchName + "の起動に失敗しました");
            }
            return;
        }

        SendCommandResult result = sendSsmCommand(instance.getInstanceId(), batchName);
        logger.log(result.toString());

    }

    /**
     * Slackに通知します
     * @param url post先
     * @param logger logger
     * @param message slackに投げるmessage
     */
    private void sendSlack(String url, LambdaLogger logger, String message) {
        ObjectMapper mapper = new ObjectMapper();

        RequestConfig config = RequestConfig.custom()
                .setConnectTimeout(1000)
                .setSocketTimeout(5000)
                .build();

        HttpClient httpclient = HttpClientBuilder.create().setDefaultRequestConfig(config).build();
        HttpPost post = new HttpPost(url);

        try {
            post.addHeader("Content-type", "application/json");
            post.setEntity(new StringEntity(
                    mapper.writeValueAsString(new HashMap<String, String>() {
                        {
                            put("text", message);
                        }
                    }), "UTF-8"));
            httpclient.execute(post);
        } catch (Exception e) {
            logger.log(e.toString() + "\n");
        }
    }

    /**
     * インスタンスを検索して返します
     * @return Reservationのリスト
     */
    private List<Reservation> describeInstances() {
        AmazonEC2Client ec2Client = Region.getRegion(Regions.AP_NORTHEAST_1)
                .createClient(AmazonEC2Client.class, null, null);

        // EC2検索条件 state:running && tag:spring-batchのもの
        DescribeInstancesRequest request = new DescribeInstancesRequest().withFilters(
                Arrays.asList(
                        new Filter().withName("instance-state-name").withValues(InstanceStateName.Running.toString()),
                        new Filter().withName("tag-value").withValues("spring-batch")
                )
        );
        return ec2Client.describeInstances(request).getReservations();
    }

    /**
     * Batch用のインスタンスをランダムに１つ返します
     * @return Instance
     */
    private Instance getRandomInstance() {
        return describeInstances().stream()
                .filter(reservation -> reservation.getInstances().size() > 0)
                .map(Reservation::getInstances)
                .map(instances -> instances.get(new Random().nextInt(instances.size())))
                .findFirst()
                .orElse(null);

    }

    /**
     * インスタンスにshellを実行させます
     * @param instanceId コマンド実行対象インスタンス
     * @param batchName 起動Batch名
     * @return SendCommandResult
     */
    private SendCommandResult sendSsmCommand(String instanceId, String batchName) {
        Map<String, List<String>> params = new HashMap<>();
        params.put("commands", Collections.singletonList(String.format("/tmp/%s.sh &", batchName)));

        SendCommandRequest command = new SendCommandRequest();
        command.setInstanceIds(Collections.singletonList(instanceId));
        command.setDocumentName("AWS-RunShellScript");
        command.setParameters(params);
        command.setComment(String.format("batch-name:%s", batchName));
        command.setTimeoutSeconds(30);

        AWSSimpleSystemsManagementClient ssm = new AWSSimpleSystemsManagementClient();
        ssm.setRegion(Region.getRegion(Regions.AP_NORTHEAST_1));
        return ssm.sendCommand(command);
    }
}
