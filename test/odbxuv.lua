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

    local query = connection:query("SHOW FULL TABLES", function(query, ...)
        query:on("fetch", function(...)
            p("cols", query:getColumnCount(), query:getAffectedCount(), query:getAllColumnInfo())
        end)
        query:on("row", function(table, ...)
            p("row", {table = table, ...})
            local q2 = connection:query("SELECT * FROM "..table.."", function(q2, ...)
                q2:on("fetch", function(...)
                    p("cols", table, q2:getColumnCount(), q2:getAffectedCount(), q2:getAllColumnInfo())
                end)
                q2:on("row", function(...)
                    p("ROW", ...)
                end)
                q2:on("fetched", function()
                    p "FINISH"
                    q2:close()
                end)
                q2:fetch()
            end)
            q2:on("error", function()
                q2:close()
            end)
            q2:on("close", function()
            end)
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

end):on("close", function(...)
    p("Closed connection", ...)
end):on("error", function(...)
    p("Connection error", ...)
    connection:close()
end)