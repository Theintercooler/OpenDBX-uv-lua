local odbx = require "odbxuv"
local bit = require "bit"

local connection = odbx.createConnection({
    type        = "mysql",
    host        = "localhost",
    port        = nil,
    database    = "test",
    username    = "test",
    password    = "test"
}, function(connection, ...)
    local function disconnect()
        connection:disconnect(function()
            p "Closing connection"
            connection:close()
        end)
    end

    connection:query("SHOW TABLES", function(query, ...)
        query:on("row", function(...)
            p("row", ...)
        end)
        query:on("fetched", function(...)
            query:close()
            p("finished fetching")
        end)
        query:fetch()
    end):on("error", function(...)
        p("Error during query", ...)
        query:close()
    end):on("close", function(...)
        p("Closed query", ...)
        disconnect()
    end)

end):on("close", function(...)
    p("Closed connection", ...)
end):on("error", function(...)
    p("Connection error", ...)
    connection:close()
end)