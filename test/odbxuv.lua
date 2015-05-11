local odbx = require "odbxuv"
local bit = require "bit"
local time = require "os".clock

local createQueryBuilder = require "odbxuv.queryBuilder".createQueryBuilder

local connection = odbx.createConnection({
    type        = "sqlite3",
    host        = "localhost",
    port        = nil,
    database    = "test",
    username    = "test",
    password    = "test"
}, function(err, connection, ...)
    if err then
        error(err)
    end

    local function disconnect()
        connection:disconnect(function()
            p "Closing connection"
            connection:close()
        end)
    end

    connection:query([[
        CREATE TABLE IF NOT EXISTS servers (id INT, world TEXT);
    ]], function(err, q)
        if err then
            error(err)
        end

        q:close(function() end)
    end)


    connection:query([[
        INSERT INTO servers VALUES (10, "TEST"), (11, "ELVES");
    ]], function(err, q)
        if err then
            error(err)
        end

        q:close(function() end)
    end)


    local q = createQueryBuilder(connection)
    q   :select()
    q   :from("servers")
    q   :where ("id != :id")
    q   :bind("id", "world")
    q:finalize(function(err, data)
        if err then
            error(err)
        end

        p("created query", err, data, time())
        print(data)
        local query = connection:query(data, function(err, query, ...)
            if err then
                error(err)
            end

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
