using System;
using System.IO;
using System.Threading.Tasks;
using Amazon;
using Amazon.S3;
using Amazon.SQS;
using Amazon.SQS.Model;
using Amazon.S3.Model;
using Amazon.S3.Util;

class Program
{
    static async Task Main(string[] args)
    {
        string queueUrl = "https://sqs.us-east-1.amazonaws.com/563472300541/fldoh-as2test1";
        // The S3 bucket name will come from the S3 event, in our case here it will always be fldoh-as2test1

        var sqsClient = new AmazonSQSClient(RegionEndpoint.USEast1);
        var s3Client = new AmazonS3Client(RegionEndpoint.USEast1);

        Console.WriteLine($"Polling {queueUrl}");

        while (true)
        {
            var receiveMessageRequest = new ReceiveMessageRequest
            {
                QueueUrl = queueUrl,
                MaxNumberOfMessages = 1,
                WaitTimeSeconds = 20 // Long polling
            };

            var receiveMessageResponse = await sqsClient.ReceiveMessageAsync(receiveMessageRequest);

            if (receiveMessageResponse.Messages.Count > 0)
            {
                var message = receiveMessageResponse.Messages[0];

                Console.WriteLine(message.Body);
                // Deserialize S3 event record from the message body
                var record = S3EventNotification.ParseJson(message.Body);
                string BucketName = record.Records?[0].S3.Bucket.Name;
                string Key = record.Records?[0].S3.Object.Key;
                Console.WriteLine($"New object s3://{BucketName}/{Key}");

                string[] KeyParts = Key.Split('/');
                string FileName = KeyParts[KeyParts.Length - 1];

                // Download the object from S3
                var getObjectRequest = new GetObjectRequest
                {
                    BucketName = BucketName,
                    Key = Key
                };

                using (var response = await s3Client.GetObjectAsync(getObjectRequest))
                using (var responseStream = response.ResponseStream)
                using (var fileStream = File.Create($"downloaded_{FileName}"))
                {
                    responseStream.CopyTo(fileStream);
                }

                // Delete the message from SQS
                var deleteMessageRequest = new DeleteMessageRequest
                {
                    QueueUrl = queueUrl,
                    ReceiptHandle = message.ReceiptHandle
                };
                await sqsClient.DeleteMessageAsync(deleteMessageRequest);

                Console.WriteLine($"Message deleted for object: {Key}");
            }
            else
            {
                Console.WriteLine("No messages in the queue. Waiting...");
            }
        }
    }

    // Define a class to represent the S3 event record
    public class S3EventRecord
    {
        public S3Entity S3 { get; set; }
    }

    public class S3Entity
    {
        public S3ObjectEntity Object { get; set; }
    }

    public class S3ObjectEntity
    {
        public string Key { get; set; }
    }
}