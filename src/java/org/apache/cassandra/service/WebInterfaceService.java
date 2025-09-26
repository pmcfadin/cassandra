/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.service;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.JsonUtils;
import org.apache.cassandra.web.api.JmxMetricsReader;
import org.apache.cassandra.web.api.dto.CompactionMetricsDto;
import org.apache.cassandra.web.api.dto.OpsMetricsDto;
import org.apache.cassandra.web.api.dto.RingDto;
import org.apache.cassandra.web.api.dto.TablesDto;
import org.apache.cassandra.web.api.dto.SettingsDto;
import org.apache.cassandra.web.api.dto.CapabilitiesDto;

/**
 * Handles web interface server lifecycle and associated resources. Lazily initialized.
 */
public class WebInterfaceService
{
    private static final Logger logger = LoggerFactory.getLogger(WebInterfaceService.class);

    private HttpServer server = null;
    private boolean initialized = false;

    /**
     * Creates and configures the HTTP server using Java's built-in HTTP server.
     */
    @VisibleForTesting
    synchronized void initialize()
    {
        if (initialized)
            return;

        try
        {
            String bindAddress = DatabaseDescriptor.getWebInterfaceBindAddress();
            int port = DatabaseDescriptor.getWebInterfacePort();
            
            InetSocketAddress addr = new InetSocketAddress(bindAddress, port);
            server = HttpServer.create(addr, 0);

            // Add health endpoint
            server.createContext("/health", new HealthHandler());

            // Add status endpoint
            server.createContext("/api/status", new StatusHandler());

            // Add ops metrics endpoint
            server.createContext("/api/ops", new OpsHandler());

            // Add compaction metrics endpoint
            server.createContext("/api/compaction", new CompactionHandler());

            // Add ring/topology endpoint
            server.createContext("/api/ring", new RingHandler());

            // Add tables metrics endpoint
            server.createContext("/api/tables", new TablesHandler());

            // Add settings endpoint
            server.createContext("/api/settings", new SettingsHandler());

            // Add capabilities endpoint
            server.createContext("/api/capabilities", new CapabilitiesHandler());

            // Add static file handler for root path
            server.createContext("/", new StaticFileHandler());

            logger.info("Web interface initialized on {}:{}", bindAddress, port);
            initialized = true;
        }
        catch (Exception e)
        {
            logger.error("Failed to initialize web interface server", e);
            throw new RuntimeException("Failed to initialize web interface server", e);
        }
    }

    /**
     * Starts web interface server.
     */
    public void start()
    {
        try
        {
            initialize();
            server.start();
            logger.info("Web interface server started on {}:{}", 
                       DatabaseDescriptor.getWebInterfaceBindAddress(),
                       DatabaseDescriptor.getWebInterfacePort());
        }
        catch (Exception e)
        {
            logger.error("Failed to start web interface server", e);
            // Don't throw - we don't want web interface failures to prevent node startup
        }
    }

    /**
     * Stops currently running web interface server.
     */
    public void stop()
    {
        stop(false);
    }

    public void stop(boolean force)
    {
        if (server != null)
        {
            try
            {
                server.stop(force ? 0 : 1); // 0 = immediate, 1 = 1 second delay
                logger.info("Web interface server stopped");
            }
            catch (Exception e)
            {
                logger.error("Error stopping web interface server", e);
            }
        }
    }

    /**
     * Ultimately stops server and closes all resources.
     */
    public void destroy()
    {
        stop();
        server = null;
        initialized = false;
    }

    /**
     * @return true in case web interface server is running
     */
    public boolean isRunning()
    {
        return server != null && initialized;
    }

    @VisibleForTesting
    HttpServer getServer()
    {
        return server;
    }

    /**
     * Simple health endpoint handler
     */
    private static class HealthHandler implements HttpHandler
    {
        @Override
        public void handle(HttpExchange exchange) throws IOException
        {
            try
            {
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                String response = "{\"status\":\"UP\"}";
                exchange.sendResponseHeaders(200, response.length());
                OutputStream os = exchange.getResponseBody();
                os.write(response.getBytes());
                os.close();
            }
            catch (Exception e)
            {
                LoggerFactory.getLogger(HealthHandler.class).error("Error handling health request", e);
            }
        }
    }

    /**
     * Node status endpoint handler
     */
    private static class StatusHandler implements HttpHandler
    {
        @Override
        public void handle(HttpExchange exchange) throws IOException
        {
            try
            {
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                
                // Gather basic node information
                String clusterName = DatabaseDescriptor.getClusterName();
                String hostId = StorageService.instance.getLocalHostId();
                long uptimeMillis = ManagementFactory.getRuntimeMXBean().getUptime();
                String rpcAddress = DatabaseDescriptor.getRpcAddress().getHostAddress();
                
                // Build JSON response
                StringBuilder json = new StringBuilder();
                json.append("{");
                json.append("\"cluster\":\"").append(escapeJson(clusterName)).append("\",");
                json.append("\"hostId\":\"").append(hostId).append("\",");
                json.append("\"rpcAddress\":\"").append(rpcAddress).append("\",");
                json.append("\"uptimeMillis\":").append(uptimeMillis);
                json.append("}");
                
                String response = json.toString();
                exchange.sendResponseHeaders(200, response.length());
                OutputStream os = exchange.getResponseBody();
                os.write(response.getBytes());
                os.close();
            }
            catch (Exception e)
            {
                LoggerFactory.getLogger(StatusHandler.class).error("Error handling status request", e);
                // Send error response
                try
                {
                    String errorResponse = "{\"error\":\"Internal server error\"}";
                    exchange.sendResponseHeaders(500, errorResponse.length());
                    OutputStream os = exchange.getResponseBody();
                    os.write(errorResponse.getBytes());
                    os.close();
                }
                catch (Exception ex)
                {
                    LoggerFactory.getLogger(StatusHandler.class).error("Error sending error response", ex);
                }
            }
        }
        
        private String escapeJson(String str) 
        {
            if (str == null) return "";
            return str.replace("\\", "\\\\").replace("\"", "\\\"");
        }
    }

    /**
     * Static file handler for serving web assets
     */
    private static class StaticFileHandler implements HttpHandler
    {
        @Override
        public void handle(HttpExchange exchange) throws IOException
        {
            try
            {
                String path = exchange.getRequestURI().getPath();
                
                // Default to index.html for root path
                if ("/".equals(path))
                {
                    path = "/index.html";
                }
                
                // Security check - only allow files in web directory
                if (path.contains("..") || !path.startsWith("/"))
                {
                    send404(exchange);
                    return;
                }
                
                // Try to load the resource
                String resourcePath = "/web" + path;
                InputStream resourceStream = getClass().getResourceAsStream(resourcePath);
                
                if (resourceStream == null)
                {
                    send404(exchange);
                    return;
                }
                
                // Determine content type
                String contentType = getContentType(path);
                exchange.getResponseHeaders().set("Content-Type", contentType);
                
                // Read and send the file
                byte[] content = resourceStream.readAllBytes();
                exchange.sendResponseHeaders(200, content.length);
                
                OutputStream os = exchange.getResponseBody();
                os.write(content);
                os.close();
                
                resourceStream.close();
            }
            catch (Exception e)
            {
                LoggerFactory.getLogger(StaticFileHandler.class).error("Error serving static file", e);
                send500(exchange);
            }
        }
        
        private void send404(HttpExchange exchange) throws IOException
        {
            String response = "<!DOCTYPE html><html><head><title>404 Not Found</title></head>" +
                            "<body><h1>404 Not Found</h1><p>The requested resource was not found.</p></body></html>";
            exchange.getResponseHeaders().set("Content-Type", "text/html");
            exchange.sendResponseHeaders(404, response.length());
            OutputStream os = exchange.getResponseBody();
            os.write(response.getBytes(StandardCharsets.UTF_8));
            os.close();
        }
        
        private void send500(HttpExchange exchange) throws IOException
        {
            try
            {
                String response = "<!DOCTYPE html><html><head><title>500 Internal Server Error</title></head>" +
                                "<body><h1>500 Internal Server Error</h1></body></html>";
                exchange.getResponseHeaders().set("Content-Type", "text/html");
                exchange.sendResponseHeaders(500, response.length());
                OutputStream os = exchange.getResponseBody();
                os.write(response.getBytes(StandardCharsets.UTF_8));
                os.close();
            }
            catch (Exception ex)
            {
                // Best effort - if we can't send error response, at least log it
                LoggerFactory.getLogger(StaticFileHandler.class).error("Failed to send 500 response", ex);
            }
        }
        
        private String getContentType(String path) 
        {
            if (path.endsWith(".html") || path.endsWith(".htm"))
            {
                return "text/html; charset=utf-8";
            }
            else if (path.endsWith(".css"))
            {
                return "text/css; charset=utf-8";
            }
            else if (path.endsWith(".js"))
            {
                return "application/javascript; charset=utf-8";
            }
            else if (path.endsWith(".json"))
            {
                return "application/json; charset=utf-8";
            }
            else if (path.endsWith(".png"))
            {
                return "image/png";
            }
            else if (path.endsWith(".jpg") || path.endsWith(".jpeg"))
            {
                return "image/jpeg";
            }
            else if (path.endsWith(".gif"))
            {
                return "image/gif";
            }
            else if (path.endsWith(".svg"))
            {
                return "image/svg+xml";
            }
            else
            {
                return "text/plain; charset=utf-8";
            }
        }
    }

    /**
     * Operations metrics endpoint handler
     */
    private static class OpsHandler implements HttpHandler
    {
        @Override
        public void handle(HttpExchange exchange) throws IOException
        {
            try
            {
                if (!"GET".equals(exchange.getRequestMethod()))
                {
                    exchange.sendResponseHeaders(405, -1);
                    return;
                }

                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.getResponseHeaders().set("Cache-Control", "no-store");

                OpsMetricsDto metrics = JmxMetricsReader.readOpsMetrics();
                String response = JsonUtils.JSON_OBJECT_MAPPER.writeValueAsString(metrics);

                exchange.sendResponseHeaders(200, response.length());
                OutputStream os = exchange.getResponseBody();
                os.write(response.getBytes(StandardCharsets.UTF_8));
                os.close();
            }
            catch (Exception e)
            {
                LoggerFactory.getLogger(OpsHandler.class).error("Error handling ops request", e);
                sendErrorResponse(exchange, 503, "unavailable");
            }
        }
    }

    /**
     * Compaction metrics endpoint handler
     */
    private static class CompactionHandler implements HttpHandler
    {
        @Override
        public void handle(HttpExchange exchange) throws IOException
        {
            try
            {
                if (!"GET".equals(exchange.getRequestMethod()))
                {
                    exchange.sendResponseHeaders(405, -1);
                    return;
                }

                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.getResponseHeaders().set("Cache-Control", "no-store");

                CompactionMetricsDto metrics = JmxMetricsReader.readCompactionMetrics();
                String response = JsonUtils.JSON_OBJECT_MAPPER.writeValueAsString(metrics);

                exchange.sendResponseHeaders(200, response.length());
                OutputStream os = exchange.getResponseBody();
                os.write(response.getBytes(StandardCharsets.UTF_8));
                os.close();
            }
            catch (Exception e)
            {
                LoggerFactory.getLogger(CompactionHandler.class).error("Error handling compaction request", e);
                sendErrorResponse(exchange, 503, "unavailable");
            }
        }
    }

    /**
     * Ring/topology endpoint handler
     */
    private static class RingHandler implements HttpHandler
    {
        @Override
        public void handle(HttpExchange exchange) throws IOException
        {
            try
            {
                if (!"GET".equals(exchange.getRequestMethod()))
                {
                    exchange.sendResponseHeaders(405, -1);
                    return;
                }

                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.getResponseHeaders().set("Cache-Control", "no-store");

                RingDto ring = JmxMetricsReader.readRingInfo();
                String response = JsonUtils.JSON_OBJECT_MAPPER.writeValueAsString(ring);

                exchange.sendResponseHeaders(200, response.length());
                OutputStream os = exchange.getResponseBody();
                os.write(response.getBytes(StandardCharsets.UTF_8));
                os.close();
            }
            catch (Exception e)
            {
                LoggerFactory.getLogger(RingHandler.class).error("Error handling ring request", e);
                sendErrorResponse(exchange, 503, "unavailable");
            }
        }
    }

    /**
     * Tables metrics endpoint handler
     */
    private static class TablesHandler implements HttpHandler
    {
        @Override
        public void handle(HttpExchange exchange) throws IOException
        {
            try
            {
                if (!"GET".equals(exchange.getRequestMethod()))
                {
                    exchange.sendResponseHeaders(405, -1);
                    return;
                }

                // Parse keyspace filter from query parameters
                String query = exchange.getRequestURI().getQuery();
                String keyspaceFilter = null;
                if (query != null && query.contains("keyspace="))
                {
                    String[] params = query.split("&");
                    for (String param : params)
                    {
                        if (param.startsWith("keyspace="))
                        {
                            keyspaceFilter = param.substring("keyspace=".length());
                            break;
                        }
                    }
                }

                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.getResponseHeaders().set("Cache-Control", "no-store");

                TablesDto tables = JmxMetricsReader.readTablesInfo(keyspaceFilter);
                String response = JsonUtils.JSON_OBJECT_MAPPER.writeValueAsString(tables);

                exchange.sendResponseHeaders(200, response.length());
                OutputStream os = exchange.getResponseBody();
                os.write(response.getBytes(StandardCharsets.UTF_8));
                os.close();
            }
            catch (Exception e)
            {
                LoggerFactory.getLogger(TablesHandler.class).error("Error handling tables request", e);
                sendErrorResponse(exchange, 503, "unavailable");
            }
        }
    }

    /**
     * Settings endpoint handler
     */
    private static class SettingsHandler implements HttpHandler
    {
        @Override
        public void handle(HttpExchange exchange) throws IOException
        {
            try
            {
                if (!"GET".equals(exchange.getRequestMethod()))
                {
                    exchange.sendResponseHeaders(405, -1);
                    return;
                }

                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.getResponseHeaders().set("Cache-Control", "no-store");

                SettingsDto settings = JmxMetricsReader.readSettings();
                String response = JsonUtils.JSON_OBJECT_MAPPER.writeValueAsString(settings);

                exchange.sendResponseHeaders(200, response.length());
                OutputStream os = exchange.getResponseBody();
                os.write(response.getBytes(StandardCharsets.UTF_8));
                os.close();
            }
            catch (Exception e)
            {
                LoggerFactory.getLogger(SettingsHandler.class).error("Error handling settings request", e);
                sendErrorResponse(exchange, 503, "unavailable");
            }
        }
    }

    /**
     * Capabilities endpoint handler
     */
    private static class CapabilitiesHandler implements HttpHandler
    {
        @Override
        public void handle(HttpExchange exchange) throws IOException
        {
            try
            {
                if (!"GET".equals(exchange.getRequestMethod()))
                {
                    exchange.sendResponseHeaders(405, -1);
                    return;
                }

                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.getResponseHeaders().set("Cache-Control", "no-store");

                CapabilitiesDto capabilities = JmxMetricsReader.readCapabilities();
                String response = JsonUtils.JSON_OBJECT_MAPPER.writeValueAsString(capabilities);

                exchange.sendResponseHeaders(200, response.length());
                OutputStream os = exchange.getResponseBody();
                os.write(response.getBytes(StandardCharsets.UTF_8));
                os.close();
            }
            catch (Exception e)
            {
                LoggerFactory.getLogger(CapabilitiesHandler.class).error("Error handling capabilities request", e);
                sendErrorResponse(exchange, 503, "unavailable");
            }
        }
    }

    /**
     * Helper method to send error responses
     */
    private static void sendErrorResponse(HttpExchange exchange, int statusCode, String error) throws IOException
    {
        try
        {
            String errorResponse = String.format("{\"error\":\"%s\"}", error);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(statusCode, errorResponse.length());
            OutputStream os = exchange.getResponseBody();
            os.write(errorResponse.getBytes(StandardCharsets.UTF_8));
            os.close();
        }
        catch (Exception ex)
        {
            LoggerFactory.getLogger(WebInterfaceService.class).error("Error sending error response", ex);
        }
    }
}