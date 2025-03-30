using Microsoft.OpenApi.Models;
using Swashbuckle.AspNetCore.SwaggerGen;
using System.Linq;

namespace Client.Filters;
public class CustomDocumentFilter : IDocumentFilter
{
    public void Apply(OpenApiDocument swaggerDoc, DocumentFilterContext context)
    {
        // Identify paths to keep
        var pathsToKeep = swaggerDoc.Paths
            .Where(pathItem => pathItem.Key.StartsWith("/api/", System.StringComparison.OrdinalIgnoreCase))
            .ToDictionary(pathItem => pathItem.Key, pathItem => pathItem.Value);

        // Log the paths that are being included for debugging
        System.Console.WriteLine("Paths included in Swagger documentation: " + string.Join(", ", pathsToKeep.Keys));

        // Replace the Paths object with the filtered paths
        swaggerDoc.Paths = new OpenApiPaths();
        foreach (var path in pathsToKeep)
        {
            swaggerDoc.Paths.Add(path.Key, path.Value);
        }
    }
}
