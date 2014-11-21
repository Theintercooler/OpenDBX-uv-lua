local odbx = require "odbxuv"
local bit = require "bit"
local time = require "os".clock

local createQueryBuilder = require "odbxuv.queryBuilder".createQueryBuilder

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

    local q = createQueryBuilder(connection)
    q   :select()
    q   :from("servers")
    q   :where ("id != :id")
    q   :bind("id", "world")
    q:finalize(function(err, data)
        p("created query", err, data, time())
        print(data)
        local query = connection:query(data, function(query, ...)
            p ("Query processed on db", ..., time())
            query:on("fetch", function(...)
                p("cols", query:getColumnCount(), query:getAffectedCount(), query:getAllColumnInfo())
            end)
            query:on("row", function(table, ...)
                p("row", {table = table, ...})
            end)
            query:on("fetched", function(...)
                query:close()
                p("finished fetching")
            end)
            query:fetch()
        end)
        query:on("error", function(...)
            p("Error during query", ...)
            query:close()
        end)
        query:on("close", function(...)
            p("Closed query", ...)
            disconnect()
        end)
    end)

end):on("close", function(...)
    p("Closed connection", ...)
end):on("error", function(...)
    p("Connection error", ...)
    connection:close()
end)