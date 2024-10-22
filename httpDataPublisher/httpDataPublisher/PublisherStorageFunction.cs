using System.IO;
using System.Threading.Tasks;
using Azure.Data.Tables;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using Newtonsoft.Json;

public class MetricsProcessor
{
    private readonly TableServiceClient _tableServiceClient;
    private readonly string _tableName = "MetricsTable"; // Your table name

    public MetricsProcessor(TableServiceClient tableServiceClient)
    {
        _tableServiceClient = tableServiceClient;
    }

    [Function("MetricsProcessor")]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequestData req,
        FunctionContext executionContext)
    {
        var logger = executionContext.GetLogger("MetricsProcessor");

        // Read the request body asynchronously
        string requestBody;
        using (var reader = new StreamReader(req.Body))
        {
            requestBody = await reader.ReadToEndAsync();
        }

        // Log the raw request body for debugging purposes
        logger.LogInformation($"Received request body: {requestBody}");

        // Initialize jsonObject to null
        JObject jsonObject = null;

        // Parse the JSON request body
        try
        {
            jsonObject = JObject.Parse(requestBody);
        }
        catch (JsonReaderException ex)
        {
            logger.LogError($"Error parsing JSON: {ex.Message}");
        }

        // Navigate through the JSON to extract metrics
        var resourceMetrics = jsonObject["resourceMetrics"];

        // Get reference to the table
        var tableClient = _tableServiceClient.GetTableClient(_tableName);
        await tableClient.CreateIfNotExistsAsync(); // Create table if it doesn't exist

        foreach (var resourceMetric in resourceMetrics)
        {
            // Initialize dictionary for dynamic attributes
            var dynamicAttributes = new Dictionary<string, string>();

            // Extract resource attributes
            var resourceAttributes = resourceMetric["resource"]["attributes"];
            foreach (var attribute in resourceAttributes)
            {
                string key = attribute["key"].ToString();
                string value = attribute["value"]["stringValue"].ToString();
                dynamicAttributes[key] = value; // Store each attribute dynamically
            }

            var scopeMetrics = resourceMetric["scopeMetrics"];
            foreach (var scopeMetric in scopeMetrics)
            {
                // Extract scope attributes
                var scopeAttributes = scopeMetric["scope"]["attributes"];
                foreach (var attribute in scopeAttributes)
                {
                    string key = attribute["key"].ToString();
                    string value = attribute["value"]["stringValue"].ToString();
                    dynamicAttributes[key] = value; // Store each attribute dynamically
                }

                var metrics = scopeMetric["metrics"];
                foreach (var metric in metrics)
                {
                    string metricName = metric["name"].ToString();

                    // Check for both sum and gauge metrics
                    var dataPoints = metric["sum"]?["dataPoints"] ?? metric["gauge"]?["dataPoints"];

                    if (dataPoints != null)
                    {
                        foreach (var dataPoint in dataPoints)
                        {
                            double metricValue = (double)dataPoint["asDouble"];
                            long timestamp = (long)dataPoint["timeUnixNano"];

                            // Create a new entity for Azure Table Storage
                            var entity = new TableEntity(dynamicAttributes.ContainsKey("hostId") ? dynamicAttributes["hostId"] : "Unknown", $"{metricName}_{timestamp}")
                            {
                                { "MetricName", metricName },
                                { "MetricValue", metricValue },
                                { "Timestamp", timestamp }
                            };

                            // Add all dynamic attributes to the entity
                            foreach (var kvp in dynamicAttributes)
                            {
                                entity[kvp.Key] = kvp.Value; // Add each dynamic attribute
                            }

                            // Insert or replace the entity in the table
                            await tableClient.UpsertEntityAsync(entity);
                        }
                    }
                }
            }
        }

        logger.LogInformation("Processed metrics successfully.");

        // Create and return the HTTP response asynchronously
        var response = req.CreateResponse(System.Net.HttpStatusCode.OK);
        await response.WriteStringAsync("Metrics processed successfully.");
        return response;
    }
}
