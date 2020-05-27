using System;
using System.Globalization;
using System.IO;
using CsvHelper;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace TransactionFileProcessing
{
    public static class Function1
    {
 
        [FunctionName("Function1")]
        public async static void Run([BlobTrigger("input/{name}", Connection = "AzureWebJobsStorage")]Stream myBlob, string name, ILogger log)
        {
            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");

            //Provide connection details to establish connection with Storage Account
            var connectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
            var outputContainerName = "output";
            //var logContainerName = "logs";
            string CurrentDateTime = DateTime.Now.ToString("mm_dd_yyyy");
            var updateFileName = "transaction_" + CurrentDateTime + ".csv";
            //var fileName = "transaction_2020.csv";

            //Connecting with CloudStorage using a client
            CloudStorageAccount myStorageAccount = CloudStorageAccount.Parse(connectionString);
            CloudBlobClient blobClient = myStorageAccount.CreateCloudBlobClient();
            CloudBlobContainer cloudBlobContainer = blobClient.GetContainerReference(outputContainerName);
            log.LogInformation($"Connection Established Successfully");

            var myReader = new StreamReader(myBlob);
            var Content = myReader.ReadToEnd();
            log.LogInformation("file Content Read successfully");


            var csv = new CsvReader(myReader, CultureInfo.InvariantCulture);
            var records = csv.GetRecords<transactionModel>();
            
            foreach (var item in records)
            {
                log.LogInformation($"This is the data" +item.Trans_Product);
            }

            CloudBlockBlob cloudBlockBlob = cloudBlobContainer.GetBlockBlobReference(updateFileName);
            await cloudBlockBlob.UploadTextAsync(Content);

        }

        public class transactionModel
        {
            public int Trans_ID { get; set; }
            public int Trans_Account { get; set; }
            public string Trans_Product { get; set; }
            public string Trans_Date { get; set; }
            public int? Trans_Amount { get; set; }
        }


    }
}
