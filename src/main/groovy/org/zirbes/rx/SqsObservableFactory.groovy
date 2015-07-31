package org.zirbes.rx

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.regions.Region
import com.amazonaws.regions.Regions
import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.services.sqs.AmazonSQSClient
import com.amazonaws.services.sqs.model.CreateQueueRequest
import com.amazonaws.services.sqs.model.DeleteMessageRequest
import com.amazonaws.services.sqs.model.DeleteQueueRequest
import com.amazonaws.services.sqs.model.Message
import com.amazonaws.services.sqs.model.ReceiveMessageRequest
import com.amazonaws.services.sqs.model.SendMessageRequest
import groovy.util.logging.Slf4j
import org.kohsuke.randname.RandomNameGenerator
import rx.Subscriber

import java.util.concurrent.atomic.AtomicInteger

import com.fasterxml.jackson.databind.ObjectMapper

import groovy.transform.CompileStatic

import rx.Observable

@CompileStatic
@Slf4j
class SqsObservableFactory {

    ObjectMapper objectMapper = new ObjectMapper()
    static final String SQS_QUEUE = "zorg-test-rx"
    AtomicInteger order = new AtomicInteger()
    RandomNameGenerator nameGiver = new RandomNameGenerator(0)

    AmazonSQS sqs
    String queueUrl

    SqsObservableFactory() {
        AWSCredentialsProvider credProvider = new ProfileCredentialsProvider()
        AWSCredentials creds = credProvider.credentials
        sqs = new AmazonSQSClient(creds)
        sqs.region = Region.getRegion(Regions.US_WEST_2)
        queueUrl = getQueueUrl(SQS_QUEUE)
    }

    void observe() {
        9.times{ sendMessage() }
        getMessages()
    }

    protected String getQueueUrl(String queueName) {
        CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName)
        String queueUrl = sqs.createQueue(createQueueRequest).queueUrl
        log.info "Discovered / Created SQS queue: ${queueUrl}"
        return queueUrl
    }

    void sendMessage() {
        // Send the message
        RoverCommand rove = new RoverCommand(order: order.getAndIncrement(), color: 'Red', who: nameGiver.next())
        String payload = objectMapper.writeValueAsString(rove)
        log.info "sending ${payload}"
        sqs.sendMessage(new SendMessageRequest(queueUrl, payload))
    }

    void getMessages() {
        sqsObservable.subscribe{ Message message ->
            RoverCommand command = objectMapper.readValue(message.body, RoverCommand)
            log.info "RoverCommand: ${command}"

            String messageRecieptHandle = message.receiptHandle
            sqs.deleteMessage(new DeleteMessageRequest(queueUrl, messageRecieptHandle))

        }
    }

    Observable<Message> getSqsObservable() {
        final ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl)

        return Observable.create({ Subscriber<Message> subscriber ->
                Thread.start {
                    while(!subscriber.unsubscribed) {
                        for (Message message : sqs.receiveMessage(receiveMessageRequest).messages) {
                            log.info "Received Message id=${message.messageId} md5=${message.getMD5OfBody()}"
                            if (subscriber.unsubscribed) { break }
                            subscriber.onNext(message)
                        }
                        // This stream never ends as long as the subscriber is subscribed
                    }
                }
            } as Observable.OnSubscribe<Message>
        )
    }

    void deleteQueue() {
        sqs.deleteQueue(new DeleteQueueRequest(queueUrl))
    }
}

// TODO:
// * filter
// * map / flatMap
// * groupBy
// * reduce
