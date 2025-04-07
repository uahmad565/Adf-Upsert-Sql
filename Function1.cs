using Adf_Upsert_Sql.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Adf_Upsert_Sql
{
    public class Function1
    {
        private readonly ILogger<Function1> _logger;

        public Function1(ILogger<Function1> logger)
        {
            _logger = logger;
        }

        [Function("Function1")]
        public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequest req)
        {
            try
            {
                _logger.LogInformation("C# HTTP trigger function processed a request.");

                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                _logger.LogInformation($"request body: {requestBody}");
                FuncData funcData;
                try
                {
                    funcData = JsonConvert.DeserializeObject<FuncData>(requestBody);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, ex.Message);
                    // Create a JObject to represent a valid JSON object
                    throw;
                }
                var SQL_Conn_Str = Environment.GetEnvironmentVariable("DESTINATION_SQL_CONNECTION");
                CsvToSqlOperations obj = new CsvToSqlOperations(SQL_Conn_Str);
                TableMetadata destinationTableMeta = new TableMetadata();
                destinationTableMeta.primaryColumn = funcData.primaryColumn;
                destinationTableMeta.tableName = funcData.tableName;
                destinationTableMeta.schemaName = funcData.schema;
                var triggerStart = DateTime.Parse(funcData.triggerStart);
                string formattedDate = triggerStart.ToString("yyyy/MM/dd");

                string fileName = triggerStart.ToString("yyyyMMddHHmmssfff");

                // Concatenate the formatted date string with the '.csv' extension
                fileName += ".csv";

                // Concatenate 'incremental/' with the formatted date string
                string incrementalPath = $"incremental/{formattedDate}/{fileName}";
                _logger.LogInformation($"incrementalPath: {incrementalPath}");



                await obj.ProcessCsvFiles(Environment.GetEnvironmentVariable("STORAGE_ACCOUNT_CONNECTION"), "bhs-dwbi-storagecontainer-prod", incrementalPath, destinationTableMeta, _logger);

                // Create a JObject to represent a valid JSON object
                var responseObject = new JObject
                {
                    { "message", "success" }
                };
                return new OkObjectResult(responseObject);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
                throw;
                
            }



        }
    }
}
